#!/usr/bin/env python3
"""
Download videos from saved X/Twitter tweets (bookmarks or likes).

Workflow:
1) Login once and save session:
   python download_x_saved_videos.py login
2) Sync videos:
   python download_x_saved_videos.py sync --source bookmarks
   python download_x_saved_videos.py sync --source likes --username your_handle

Dependencies:
- playwright
- yt-dlp
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - optional dependency at import time
    sync_playwright = None


STATUS_ID_RE = re.compile(r"/status/(\d+)")
DEFAULT_STATE_FILE = Path(__file__).resolve().parent / ".x_storage_state.json"
DEFAULT_OUTPUT_DIR = Path(r"C:\Users\whtit\OneDrive\Desktop\tvideo")
DEFAULT_ARCHIVE_FILE = Path(__file__).resolve().parent / ".x_download_archive.txt"
DEFAULT_HISTORY_FILE = Path(__file__).resolve().parent / ".x_scanned_tweet_ids.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto download videos from your X/Twitter bookmarks or likes."
    )
    parser.add_argument(
        "--state-file",
        default=str(DEFAULT_STATE_FILE),
        help="Playwright storage state JSON path.",
    )
    parser.add_argument(
        "--browser-channel",
        default=None,
        help="Optional browser channel, e.g. msedge, chrome.",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    login_parser = sub.add_parser("login", help="Login once and save account session.")
    login_parser.add_argument(
        "--timeout-ms",
        type=int,
        default=180000,
        help="Navigation timeout in milliseconds.",
    )

    sync_parser = sub.add_parser("sync", help="Scan and download videos.")
    sync_parser.add_argument(
        "--source",
        choices=("bookmarks", "likes"),
        default="bookmarks",
        help="Source feed to scan.",
    )
    sync_parser.add_argument(
        "--username",
        default=None,
        help="Your X handle. Required when source=likes.",
    )
    sync_parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode during sync.",
    )
    sync_parser.add_argument(
        "--max-scrolls",
        type=int,
        default=200,
        help="Maximum scroll rounds.",
    )
    sync_parser.add_argument(
        "--stop-after-idle-rounds",
        type=int,
        default=8,
        help="Stop after this many rounds without finding new tweet IDs.",
    )
    sync_parser.add_argument(
        "--scroll-wait-ms",
        type=int,
        default=1200,
        help="Wait between scroll rounds in milliseconds.",
    )
    sync_parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where videos will be saved.",
    )
    sync_parser.add_argument(
        "--archive-file",
        default=str(DEFAULT_ARCHIVE_FILE),
        help="yt-dlp archive file path (download de-duplication).",
    )
    sync_parser.add_argument(
        "--history-file",
        default=str(DEFAULT_HISTORY_FILE),
        help="Tweet ID history file (scan de-duplication).",
    )
    sync_parser.add_argument(
        "--rescan-all",
        action="store_true",
        help="Ignore history-file and scan/download all detected tweet IDs.",
    )
    return parser.parse_args()


def require_playwright() -> None:
    if sync_playwright is not None:
        return
    print(
        "playwright is not installed.\n"
        "Install with:\n"
        "  python -m pip install playwright\n"
        "  playwright install chromium",
        file=sys.stderr,
    )
    raise SystemExit(1)


def resolve_ytdlp_cmd() -> list[str]:
    binary = shutil.which("yt-dlp")
    if binary:
        return [binary]

    probe = subprocess.run(
        [sys.executable, "-m", "yt_dlp", "--version"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if probe.returncode == 0:
        return [sys.executable, "-m", "yt_dlp"]

    print(
        "yt-dlp is not installed.\n"
        "Install with:\n"
        "  python -m pip install -U yt-dlp",
        file=sys.stderr,
    )
    raise SystemExit(1)


def build_feed_url(source: str, username: str | None) -> str:
    if source == "bookmarks":
        return "https://x.com/i/bookmarks"
    if not username:
        raise ValueError("--username is required when --source likes")
    return f"https://x.com/{username.lstrip('@')}/likes"


def extract_status_ids(hrefs: Iterable[str]) -> set[str]:
    ids: set[str] = set()
    for href in hrefs:
        match = STATUS_ID_RE.search(href)
        if match:
            ids.add(match.group(1))
    return ids


def scan_tweet_ids(
    *,
    state_file: Path,
    source: str,
    username: str | None,
    channel: str | None,
    headless: bool,
    max_scrolls: int,
    stop_after_idle_rounds: int,
    scroll_wait_ms: int,
) -> tuple[list[str], list[dict]]:
    require_playwright()
    target_url = build_feed_url(source, username)
    if not state_file.exists():
        raise FileNotFoundError(
            f"State file not found: {state_file}\nRun `login` first."
        )

    launch_kwargs: dict = {"headless": headless}
    if channel:
        launch_kwargs["channel"] = channel

    with sync_playwright() as p:
        browser = p.chromium.launch(**launch_kwargs)
        context = browser.new_context(storage_state=str(state_file))
        page = context.new_page()

        print(f"[scan] Opening {target_url}")
        page.goto(target_url, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(2500)

        discovered_ids: set[str] = set()
        idle_rounds = 0

        for round_index in range(1, max_scrolls + 1):
            hrefs: list[str] = page.eval_on_selector_all(
                "a[href*='/status/']",
                "elements => elements.map(e => e.getAttribute('href') || '')",
            )
            ids = extract_status_ids(hrefs)
            before = len(discovered_ids)
            discovered_ids.update(ids)
            gained = len(discovered_ids) - before

            print(
                f"[scan] round {round_index}/{max_scrolls}: "
                f"+{gained}, total={len(discovered_ids)}"
            )

            if gained == 0:
                idle_rounds += 1
            else:
                idle_rounds = 0

            if idle_rounds >= stop_after_idle_rounds:
                print("[scan] stop: no new tweets found for multiple rounds")
                break

            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(scroll_wait_ms)

        cookies = context.cookies(["https://x.com", "https://twitter.com"])
        context.storage_state(path=str(state_file))
        browser.close()

    ids_sorted = sorted(discovered_ids, key=int, reverse=True)
    return ids_sorted, cookies


def write_netscape_cookies(cookies: Iterable[dict], path: Path) -> None:
    lines = [
        "# Netscape HTTP Cookie File",
        "# Generated by download_x_saved_videos.py",
    ]
    for cookie in cookies:
        domain = str(cookie.get("domain", "")).strip()
        name = str(cookie.get("name", "")).strip()
        value = str(cookie.get("value", ""))
        if not domain or not name:
            continue
        include_subdomains = "TRUE" if domain.startswith(".") else "FALSE"
        cookie_path = str(cookie.get("path", "/")) or "/"
        secure = "TRUE" if bool(cookie.get("secure", False)) else "FALSE"
        expires = int(cookie.get("expires", 0) or 0)
        lines.append(
            "\t".join(
                (
                    domain,
                    include_subdomains,
                    cookie_path,
                    secure,
                    str(expires),
                    name,
                    value,
                )
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def load_history(path: Path) -> set[str]:
    if not path.exists():
        return set()
    lines = path.read_text(encoding="utf-8").splitlines()
    return {line.strip() for line in lines if line.strip()}


def save_history(path: Path, ids: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_ids = sorted(ids, key=int, reverse=True)
    path.write_text("\n".join(sorted_ids) + ("\n" if sorted_ids else ""), encoding="utf-8")


def run_login(*, state_file: Path, channel: str | None, timeout_ms: int) -> int:
    require_playwright()
    state_file.parent.mkdir(parents=True, exist_ok=True)

    launch_kwargs: dict = {"headless": False}
    if channel:
        launch_kwargs["channel"] = channel

    with sync_playwright() as p:
        browser = p.chromium.launch(**launch_kwargs)
        context = browser.new_context()
        page = context.new_page()
        print("[login] Opening X login page...")
        page.goto("https://x.com/login", wait_until="domcontentloaded", timeout=timeout_ms)
        print("[login] Please complete login in the opened browser window.")
        print(f"[login] Waiting up to {timeout_ms // 1000} seconds for auth session...")

        start = time.monotonic()
        last_status_sec = -999
        auth_ok = False

        while (time.monotonic() - start) * 1000 < timeout_ms:
            cookies = context.cookies(["https://x.com", "https://twitter.com"])
            if any(c.get("name") == "auth_token" and c.get("value") for c in cookies):
                auth_ok = True
                break

            elapsed_sec = int(time.monotonic() - start)
            if elapsed_sec - last_status_sec >= 15:
                print(f"[login] Waiting... {elapsed_sec}s")
                last_status_sec = elapsed_sec
            try:
                page.wait_for_timeout(1000)
            except Exception:
                break

        if not auth_ok:
            browser.close()
            print(
                "[login] Login not detected before timeout. "
                "Please run login again and finish authentication in the browser.",
                file=sys.stderr,
            )
            return 1

        context.storage_state(path=str(state_file))
        browser.close()
    print(f"[login] Session saved: {state_file}")
    return 0


def run_sync(
    *,
    state_file: Path,
    channel: str | None,
    source: str,
    username: str | None,
    headless: bool,
    max_scrolls: int,
    stop_after_idle_rounds: int,
    scroll_wait_ms: int,
    output_dir: Path,
    archive_file: Path,
    history_file: Path,
    rescan_all: bool,
) -> int:
    ytdlp_cmd = resolve_ytdlp_cmd()

    ids, cookies = scan_tweet_ids(
        state_file=state_file,
        source=source,
        username=username,
        channel=channel,
        headless=headless,
        max_scrolls=max_scrolls,
        stop_after_idle_rounds=stop_after_idle_rounds,
        scroll_wait_ms=scroll_wait_ms,
    )
    if not ids:
        print("[sync] No tweet IDs found. Nothing to download.")
        return 0

    history_ids = load_history(history_file)
    if rescan_all:
        pending_ids = ids
    else:
        pending_ids = [tweet_id for tweet_id in ids if tweet_id not in history_ids]

    if not pending_ids:
        print("[sync] No new tweet IDs (history de-duplication).")
        return 0

    print(f"[sync] Candidate tweets to process: {len(pending_ids)}")

    output_dir.mkdir(parents=True, exist_ok=True)
    archive_file.parent.mkdir(parents=True, exist_ok=True)
    history_file.parent.mkdir(parents=True, exist_ok=True)

    urls = [f"https://x.com/i/web/status/{tweet_id}" for tweet_id in pending_ids]

    with tempfile.TemporaryDirectory(prefix="x_saved_videos_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)
        url_file = tmp_dir_path / "urls.txt"
        cookie_file = tmp_dir_path / "cookies.txt"

        url_file.write_text("\n".join(urls) + "\n", encoding="utf-8")
        write_netscape_cookies(cookies, cookie_file)

        output_template = str(output_dir / "%(uploader)s" / "%(upload_date)s_%(id)s_%(title).120B.%(ext)s")
        cmd = [
            *ytdlp_cmd,
            "--batch-file",
            str(url_file),
            "--cookies",
            str(cookie_file),
            "--download-archive",
            str(archive_file),
            "--ignore-errors",
            "--no-abort-on-error",
            "--restrict-filenames",
            "--concurrent-fragments",
            "4",
            "-f",
            "bestvideo*+bestaudio/best",
            "-o",
            output_template,
        ]

        print("[sync] Running yt-dlp...")
        completed = subprocess.run(cmd, check=False)

    if completed.returncode != 0:
        print(
            f"[sync] yt-dlp exited with code {completed.returncode}. "
            "History will not be updated so you can retry.",
            file=sys.stderr,
        )
        return completed.returncode

    history_ids.update(pending_ids)
    save_history(history_file, history_ids)
    print(f"[sync] Done. History updated: {history_file}")
    print(f"[sync] Output dir: {output_dir}")
    return 0


def main() -> int:
    args = parse_args()
    state_file = Path(args.state_file).expanduser().resolve()

    if args.command == "login":
        return run_login(
            state_file=state_file,
            channel=args.browser_channel,
            timeout_ms=args.timeout_ms,
        )

    if args.command == "sync":
        return run_sync(
            state_file=state_file,
            channel=args.browser_channel,
            source=args.source,
            username=args.username,
            headless=args.headless,
            max_scrolls=args.max_scrolls,
            stop_after_idle_rounds=args.stop_after_idle_rounds,
            scroll_wait_ms=args.scroll_wait_ms,
            output_dir=Path(args.output_dir).expanduser().resolve(),
            archive_file=Path(args.archive_file).expanduser().resolve(),
            history_file=Path(args.history_file).expanduser().resolve(),
            rescan_all=args.rescan_all,
        )

    print("Unknown command.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
