#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import argparse
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from yt_dlp import YoutubeDL


VIDEO_DIR = Path(r"C:\Users\whtit\OneDrive\Desktop\tvideo")
CORE_SUFFIX_RE = re.compile(r"^(?P<core>\d+_\d+)\.(?P<n>\d+)$")
CORE_ONLY_RE = re.compile(r"^(?P<tweet>\d+)_(?P<idx>\d+)$")

FFMPEG_CANDIDATES = [
    Path(
        r"C:\Users\whtit\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin\ffmpeg.exe"
    ),
    Path("ffmpeg"),
]
FFPROBE_CANDIDATES = [
    Path(
        r"C:\Users\whtit\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin\ffprobe.exe"
    ),
    Path("ffprobe"),
]


@dataclass
class VideoMeta:
    path: str
    name: str
    stem: str
    canonical_prefix: str
    suffix_num: int
    size: int
    sha256: str
    duration: float | None
    width: int | None
    height: int | None
    fps: float | None
    vcodec: str | None
    acodec: str | None
    vbitrate: int | None
    abitrate: int | None
    audio_channels: int | None
    audio_sample_rate: int | None
    visual_hashes: list[int | None]


def resolve_binary(candidates: list[Path]) -> str:
    for cand in candidates:
        if cand.is_absolute():
            if cand.exists():
                return str(cand)
        else:
            which = shutil.which(str(cand))
            if which:
                return which
    raise FileNotFoundError(f"Binary not found in candidates: {candidates}")


def run_json(cmd: list[str]) -> Any:
    completed = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(completed.stdout)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def parse_fraction(text: str | None) -> float | None:
    if not text or text in {"0/0", "N/A"}:
        return None
    if "/" in text:
        n, d = text.split("/", 1)
        try:
            n_f = float(n)
            d_f = float(d)
            if d_f == 0:
                return None
            return n_f / d_f
        except Exception:
            return None
    try:
        return float(text)
    except Exception:
        return None


def ffprobe_meta(path: Path, ffprobe_bin: str) -> dict[str, Any]:
    data = run_json(
        [
            ffprobe_bin,
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-of",
            "json",
            str(path),
        ]
    )
    streams = data.get("streams", [])
    fmt = data.get("format", {})
    v = next((s for s in streams if s.get("codec_type") == "video"), None)
    a = next((s for s in streams if s.get("codec_type") == "audio"), None)
    duration = None
    try:
        if fmt.get("duration") is not None:
            duration = float(fmt["duration"])
    except Exception:
        duration = None

    return {
        "duration": duration,
        "width": v.get("width") if v else None,
        "height": v.get("height") if v else None,
        "fps": parse_fraction(v.get("avg_frame_rate") if v else None),
        "vcodec": v.get("codec_name") if v else None,
        "acodec": a.get("codec_name") if a else None,
        "vbitrate": int(v["bit_rate"]) if v and v.get("bit_rate") else None,
        "abitrate": int(a["bit_rate"]) if a and a.get("bit_rate") else None,
        "audio_channels": int(a["channels"]) if a and a.get("channels") else None,
        "audio_sample_rate": int(a["sample_rate"])
        if a and a.get("sample_rate")
        else None,
    }


def ahash64_frame(video: Path, timestamp: float, ffmpeg_bin: str) -> int | None:
    cmd = [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{max(0.0, timestamp):.3f}",
        "-i",
        str(video),
        "-frames:v",
        "1",
        "-vf",
        "scale=8:8,format=gray",
        "-f",
        "rawvideo",
        "-",
    ]
    proc = subprocess.run(cmd, check=False, capture_output=True)
    if proc.returncode != 0:
        return None
    data = proc.stdout
    if len(data) < 64:
        return None
    pixels = data[:64]
    avg = sum(pixels) / 64.0
    bits = 0
    for px in pixels:
        bits = (bits << 1) | (1 if px >= avg else 0)
    return bits


def visual_hashes(path: Path, duration: float | None, ffmpeg_bin: str) -> list[int | None]:
    if not duration or duration <= 0.0:
        times = [0.0, 1.0, 2.0]
    else:
        times = [duration * 0.1, duration * 0.5, duration * 0.9]
    return [ahash64_frame(path, t, ffmpeg_bin) for t in times]


def hamming(a: int, b: int) -> int:
    return (a ^ b).bit_count()


def same_video_visual(a: VideoMeta, b: VideoMeta) -> bool:
    if a.duration is not None and b.duration is not None:
        if abs(a.duration - b.duration) > max(0.8, 0.03 * max(a.duration, b.duration)):
            return False

    pairs = 0
    close = 0
    for ha, hb in zip(a.visual_hashes, b.visual_hashes):
        if ha is None or hb is None:
            continue
        pairs += 1
        if hamming(ha, hb) <= 8:
            close += 1
    if pairs == 0:
        return False
    return close >= max(1, pairs - 1)


def better_keep(a: VideoMeta, b: VideoMeta) -> VideoMeta:
    def score(v: VideoMeta) -> tuple[int, int, int, int]:
        area = (v.width or 0) * (v.height or 0)
        bitrate = v.vbitrate or 0
        # prefer non-suffixed names, then lower suffix number
        suffix_pref = 9999 if v.suffix_num == 0 else (9999 - v.suffix_num)
        return (area, bitrate, v.size, suffix_pref)

    return a if score(a) >= score(b) else b


def canonical_prefix(stem: str) -> tuple[str, int]:
    m = CORE_SUFFIX_RE.match(stem)
    if not m:
        return stem, 0
    return m.group("core"), int(m.group("n"))


def build_metadata(videos: list[Path], ffprobe_bin: str, ffmpeg_bin: str) -> list[VideoMeta]:
    items: list[VideoMeta] = []
    total = len(videos)
    for idx, path in enumerate(videos, 1):
        print(f"[meta] {idx}/{total} {path.name}")
        prefix, suffix_num = canonical_prefix(path.stem)
        probe = ffprobe_meta(path, ffprobe_bin)
        sha = sha256_file(path)
        vhash = visual_hashes(path, probe["duration"], ffmpeg_bin)
        items.append(
            VideoMeta(
                path=str(path),
                name=path.name,
                stem=path.stem,
                canonical_prefix=prefix,
                suffix_num=suffix_num,
                size=path.stat().st_size,
                sha256=sha,
                duration=probe["duration"],
                width=probe["width"],
                height=probe["height"],
                fps=probe["fps"],
                vcodec=probe["vcodec"],
                acodec=probe["acodec"],
                vbitrate=probe["vbitrate"],
                abitrate=probe["abitrate"],
                audio_channels=probe["audio_channels"],
                audio_sample_rate=probe["audio_sample_rate"],
                visual_hashes=vhash,
            )
        )
    return items


def try_restore_deleted_like_files(video_dir: Path) -> dict[str, Any]:
    restored: list[str] = []
    failed: list[str] = []

    # Cores that currently only have ".1" are likely the ones affected by prior cleanup.
    core_to_suffixes: dict[str, set[int]] = {}
    for p in video_dir.glob("*.mp4"):
        m = CORE_SUFFIX_RE.match(p.stem)
        if not m:
            continue
        core = m.group("core")
        n = int(m.group("n"))
        core_to_suffixes.setdefault(core, set()).add(n)

    target_cores = sorted(
        core for core, nums in core_to_suffixes.items() if nums == {1} and CORE_ONLY_RE.match(core)
    )
    if not target_cores:
        return {"attempted_cores": 0, "restored_files": restored, "failed": failed}

    print(f"[restore] target cores: {len(target_cores)}")

    ydl_extract_opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "cookiesfrombrowser": ("firefox",),
    }
    ydl_download_base: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "cookiesfrombrowser": ("firefox",),
        "merge_output_format": "mp4",
        "noplaylist": False,
    }

    tweet_cache: dict[str, Any] = {}
    for core in target_cores:
        m = CORE_ONLY_RE.match(core)
        if not m:
            continue
        tweet_id = m.group("tweet")
        media_index = int(m.group("idx"))
        tweet_url = f"https://x.com/i/web/status/{tweet_id}"

        try:
            if tweet_id not in tweet_cache:
                with YoutubeDL(ydl_extract_opts) as ydl:
                    tweet_cache[tweet_id] = ydl.extract_info(tweet_url, download=False)
            info = tweet_cache[tweet_id]

            entries = info.get("entries") if isinstance(info, dict) else None
            if not entries or media_index < 1 or media_index > len(entries):
                failed.append(core)
                continue
            entry = entries[media_index - 1]
            formats = entry.get("formats") or []

            # Prefer progressive mp4 formats with both audio and video.
            candidates = [
                f
                for f in formats
                if f.get("ext") == "mp4"
                and f.get("vcodec") not in (None, "none")
                and f.get("acodec") not in (None, "none")
                and f.get("format_id")
            ]
            if not candidates:
                failed.append(core)
                continue

            # Highest quality first. We try up to 3 extra files (.2, .3, .4).
            candidates.sort(
                key=lambda f: (
                    int(f.get("height") or 0),
                    float(f.get("tbr") or 0),
                    int(f.get("filesize") or 0),
                    str(f.get("format_id")),
                ),
                reverse=True,
            )

            existing_sizes = {p.stat().st_size for p in video_dir.glob(f"{core}.*.mp4")}
            next_suffix = 2
            used_format_ids: set[str] = set()

            for fmt in candidates:
                if next_suffix > 4:
                    break
                fmt_id = str(fmt.get("format_id"))
                if fmt_id in used_format_ids:
                    continue
                used_format_ids.add(fmt_id)

                target = video_dir / f"{core}.{next_suffix}.mp4"
                if target.exists():
                    next_suffix += 1
                    continue

                outtmpl = str(video_dir / f"{core}.{next_suffix}.%(ext)s")
                opts = dict(ydl_download_base)
                opts["format"] = fmt_id
                opts["outtmpl"] = outtmpl

                with YoutubeDL(opts) as ydl:
                    ydl.download([tweet_url])

                if target.exists():
                    if target.stat().st_size in existing_sizes:
                        # Same-size immediate duplicate recovery; keep for now, later dedupe handles it.
                        pass
                    existing_sizes.add(target.stat().st_size)
                    restored.append(str(target))
                    next_suffix += 1

        except Exception:
            failed.append(core)

    return {"attempted_cores": len(target_cores), "restored_files": restored, "failed": failed}


def dedupe(videos: list[VideoMeta]) -> tuple[list[str], list[dict[str, Any]]]:
    by_hash: dict[str, list[VideoMeta]] = {}
    for v in videos:
        by_hash.setdefault(v.sha256, []).append(v)

    to_delete: set[str] = set()
    reasons: list[dict[str, Any]] = []

    # 1) Exact duplicates by SHA-256
    for sha, group in by_hash.items():
        if len(group) < 2:
            continue
        keep = group[0]
        for candidate in group[1:]:
            keep = better_keep(keep, candidate)
        for g in group:
            if g.path == keep.path:
                continue
            to_delete.add(g.path)
            reasons.append(
                {
                    "type": "exact_hash",
                    "sha256": sha,
                    "keep": keep.path,
                    "delete": g.path,
                }
            )

    # 2) Same prefix + visual match
    by_prefix: dict[str, list[VideoMeta]] = {}
    for v in videos:
        if v.path in to_delete:
            continue
        by_prefix.setdefault(v.canonical_prefix, []).append(v)

    for prefix, group in by_prefix.items():
        if len(group) < 2:
            continue

        # Union-find for visual equivalence inside prefix group
        parent = {i: i for i in range(len(group))}

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[rb] = ra

        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                if same_video_visual(group[i], group[j]):
                    union(i, j)

        clusters: dict[int, list[VideoMeta]] = {}
        for idx, item in enumerate(group):
            clusters.setdefault(find(idx), []).append(item)

        for _, cluster in clusters.items():
            if len(cluster) < 2:
                continue
            keep = cluster[0]
            for c in cluster[1:]:
                keep = better_keep(keep, c)
            for c in cluster:
                if c.path == keep.path:
                    continue
                if c.path in to_delete:
                    continue
                to_delete.add(c.path)
                reasons.append(
                    {
                        "type": "same_prefix_visual",
                        "prefix": prefix,
                        "keep": keep.path,
                        "delete": c.path,
                        "keep_size": keep.size,
                        "delete_size": c.size,
                        "keep_duration": keep.duration,
                        "delete_duration": c.duration,
                    }
                )

    return sorted(to_delete), reasons


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Restore missing suffix files when possible and dedupe videos by metadata/hash/visual similarity."
    )
    parser.add_argument(
        "--skip-restore",
        action="store_true",
        help="Skip restore attempts and run metadata-based dedupe only.",
    )
    args = parser.parse_args()

    if not VIDEO_DIR.exists():
        print(f"Video directory not found: {VIDEO_DIR}", file=sys.stderr)
        return 1

    ffmpeg_bin = resolve_binary(FFMPEG_CANDIDATES)
    ffprobe_bin = resolve_binary(FFPROBE_CANDIDATES)

    print(f"[env] ffmpeg={ffmpeg_bin}")
    print(f"[env] ffprobe={ffprobe_bin}")
    print(f"[env] video_dir={VIDEO_DIR}")

    if args.skip_restore:
        restore_result = {"attempted_cores": 0, "restored_files": [], "failed": []}
        print("[restore] skipped")
    else:
        restore_result = try_restore_deleted_like_files(VIDEO_DIR)
        print(
            f"[restore] attempted={restore_result['attempted_cores']} "
            f"restored={len(restore_result['restored_files'])} "
            f"failed={len(restore_result['failed'])}"
        )

    files = sorted(VIDEO_DIR.glob("*.mp4"))
    print(f"[scan] files={len(files)}")
    metas = build_metadata(files, ffprobe_bin, ffmpeg_bin)

    to_delete, reasons = dedupe(metas)
    print(f"[dedupe] delete_count={len(to_delete)}")

    for p in to_delete:
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    report = {
        "timestamp": datetime.now().isoformat(),
        "video_dir": str(VIDEO_DIR),
        "restore_result": restore_result,
        "before_count": len(metas),
        "delete_count": len(to_delete),
        "after_count": len(metas) - len(to_delete),
        "deletes": reasons,
        "metadata": [asdict(m) for m in metas],
    }

    report_path = VIDEO_DIR / f"dedupe_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[report] {report_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
