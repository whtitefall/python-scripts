#!/usr/bin/env python3
from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from yt_dlp import YoutubeDL


VIDEO_DIR = Path(r"C:\Users\whtit\OneDrive\Desktop\tvideo")
MISSING_FILE = Path(
    r"C:\Users\whtit\OneDrive\Desktop\Polar\backend\scripts\bookmarks_missing_cores.txt"
)
CORE_RE = re.compile(r"^(?P<tweet>\d+)_(?P<num>\d+)$")
FFMPEG_DIR = Path(
    r"C:\Users\whtit\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1-full_build\bin"
)


def local_cores(video_dir: Path) -> set[str]:
    cores: set[str] = set()
    stem_re = re.compile(r"^(?P<core>\d+_\d+)(?:\.\d+)?$")
    for p in video_dir.glob("*.mp4"):
        m = stem_re.match(p.stem)
        if m:
            cores.add(m.group("core"))
    return cores


def main() -> int:
    if not VIDEO_DIR.exists():
        print(f"Video dir not found: {VIDEO_DIR}", file=sys.stderr)
        return 1
    if not MISSING_FILE.exists():
        print(f"Missing list not found: {MISSING_FILE}", file=sys.stderr)
        return 1

    targets = [line.strip() for line in MISSING_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
    current = local_cores(VIDEO_DIR)
    to_fetch = [core for core in targets if core not in current]

    print(f"[plan] missing_list={len(targets)}")
    print(f"[plan] already_present={len(targets) - len(to_fetch)}")
    print(f"[plan] to_fetch={len(to_fetch)}")

    if not to_fetch:
        print("[done] nothing to download")
        return 0

    ffmpeg_location = str(FFMPEG_DIR) if FFMPEG_DIR.exists() else None
    downloaded = 0
    skipped = 0
    failed = 0

    with tempfile.TemporaryDirectory(prefix="missing_bookmark_dl_") as td:
        tmp = Path(td)
        for idx, core in enumerate(to_fetch, 1):
            m = CORE_RE.match(core)
            if not m:
                print(f"[skip] invalid core format: {core}")
                skipped += 1
                continue

            tweet_id = m.group("tweet")
            item_num = m.group("num")
            url = f"https://x.com/i/web/status/{tweet_id}"
            outtmpl = str(tmp / f"{core}.%(ext)s")

            print(f"[dl] {idx}/{len(to_fetch)} {core}")
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "cookiesfrombrowser": ("firefox",),
                "playlist_items": item_num,
                "format": "bestvideo*+bestaudio/best",
                "merge_output_format": "mp4",
                "outtmpl": outtmpl,
                "noplaylist": False,
                "retries": 2,
                "fragment_retries": 2,
                "ignoreerrors": True,
            }
            if ffmpeg_location:
                ydl_opts["ffmpeg_location"] = ffmpeg_location

            ok = False
            try:
                with YoutubeDL(ydl_opts) as ydl:
                    rc = ydl.download([url])
                ok = rc == 0
            except Exception:
                ok = False

            candidates = sorted(tmp.glob(f"{core}.*"), key=lambda p: p.stat().st_size, reverse=True)
            if not candidates:
                if ok:
                    # yt-dlp returned success but produced no local file.
                    failed += 1
                    continue
                failed += 1
                continue

            src = candidates[0]
            dst = VIDEO_DIR / f"{core}.mp4"
            if dst.exists():
                src.unlink(missing_ok=True)
                skipped += 1
                # cleanup leftovers
                for extra in candidates[1:]:
                    extra.unlink(missing_ok=True)
                continue

            shutil.move(str(src), str(dst))
            downloaded += 1

            for extra in candidates[1:]:
                extra.unlink(missing_ok=True)

    print(f"[result] downloaded={downloaded} skipped={skipped} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
