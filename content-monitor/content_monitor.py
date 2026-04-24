#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import smtplib
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote
import xml.etree.ElementTree as ET

import requests

YOUTUBE_FEED_REGEX = re.compile(
    r"https://www\.youtube\.com/feeds/videos\.xml\?channel_id=[A-Za-z0-9_-]+",
    re.IGNORECASE,
)
YOUTUBE_FEED_ESCAPED_REGEX = re.compile(
    r"https:\\/\\/www\.youtube\.com\\/feeds\\/videos\.xml\\?channel_id=[A-Za-z0-9_-]+",
    re.IGNORECASE,
)
YOUTUBE_CHANNEL_ID_REGEXES = [
    re.compile(r'"externalId"\s*:\s*"(UC[A-Za-z0-9_-]+)"'),
    re.compile(r'"browseId"\s*:\s*"(UC[A-Za-z0-9_-]+)"'),
    re.compile(r'"channelId"\s*:\s*"(UC[A-Za-z0-9_-]+)"'),
]


@dataclass(frozen=True)
class ContentUpdate:
    unique_id: str
    source: str
    publisher: str
    title: str
    url: str
    published_at: str
    summary: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def local_name(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_datetime_iso(value: str) -> datetime | None:
    text = normalize_text(value)
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_datetime_any(value: str) -> datetime | None:
    text = normalize_text(value)
    if not text:
        return None
    dt = parse_datetime_iso(text)
    if dt is not None:
        return dt
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def parse_datetime_to_utc_iso(value: str) -> str:
    dt = parse_datetime_any(value)
    if dt is None:
        return utc_now_iso()
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def format_published_time_with_age(published_at: str, reference_time: datetime | None = None) -> str:
    dt = parse_datetime_any(published_at)
    if dt is None:
        return f"未知 | {published_at}"
    now = reference_time or datetime.now(timezone.utc)
    delta_seconds = int((now - dt).total_seconds())
    if delta_seconds < 0:
        delta_seconds = 0
    delta_minutes = delta_seconds // 60
    days = delta_minutes // (60 * 24)
    hours = (delta_minutes % (60 * 24)) // 60
    minutes = delta_minutes % 60
    parts: list[str] = []
    if days > 0:
        parts.append(f"{days}天")
    if hours > 0:
        parts.append(f"{hours}小时")
    if minutes > 0:
        parts.append(f"{minutes}分")
    if not parts:
        parts.append("0分")
    exact = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"{''.join(parts)}前 | {exact}"


def split_updates_by_post_age(
    updates: list[ContentUpdate], max_age_days: int | None, reference_time: datetime | None = None
) -> tuple[list[ContentUpdate], list[ContentUpdate]]:
    if max_age_days is None or max_age_days <= 0:
        return updates, []
    now = reference_time or datetime.now(timezone.utc)
    max_age_seconds = max_age_days * 24 * 60 * 60
    kept: list[ContentUpdate] = []
    stale: list[ContentUpdate] = []
    for item in updates:
        dt = parse_datetime_any(item.published_at)
        if dt is None:
            stale.append(item)
            continue
        age_seconds = int((now - dt).total_seconds())
        if age_seconds < 0:
            age_seconds = 0
        if age_seconds <= max_age_seconds:
            kept.append(item)
        else:
            stale.append(item)
    return kept, stale


def sleep_with_jitter(base_delay: float, jitter: float) -> None:
    if base_delay <= 0 and jitter <= 0:
        return
    lower = max(0.0, base_delay - abs(jitter))
    upper = max(lower, base_delay + abs(jitter))
    time.sleep(random.uniform(lower, upper))


def find_first_child(element: ET.Element, names: set[str]) -> ET.Element | None:
    for child in element:
        if local_name(child.tag) in names:
            return child
    return None


def find_first_child_text(element: ET.Element, names: set[str]) -> str:
    child = find_first_child(element, names)
    if child is None:
        return ""
    return normalize_text(child.text)


def parse_feed_xml(source_name: str, source_kind: str, default_url: str, xml_text: str) -> list[ContentUpdate]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    root_name = local_name(root.tag).lower()
    updates: list[ContentUpdate] = []

    if root_name == "feed":
        feed_title = find_first_child_text(root, {"title"}) or source_name
        for entry in [child for child in root if local_name(child.tag).lower() == "entry"]:
            raw_id = find_first_child_text(entry, {"id"}) or find_first_child_text(entry, {"guid"})
            title = find_first_child_text(entry, {"title"}) or "Untitled"
            link_node = find_first_child(entry, {"link"})
            link = default_url
            if link_node is not None:
                href = normalize_text(link_node.attrib.get("href"))
                if href:
                    link = href
            published_raw = find_first_child_text(entry, {"published"}) or find_first_child_text(entry, {"updated"})
            summary = find_first_child_text(entry, {"summary"}) or find_first_child_text(entry, {"content"})

            unique_basis = raw_id or link or title
            unique_id = f"{source_kind}:{hashlib.sha1(unique_basis.encode('utf-8')).hexdigest()[:20]}"
            updates.append(
                ContentUpdate(
                    unique_id=unique_id,
                    source=source_kind,
                    publisher=feed_title,
                    title=title,
                    url=link,
                    published_at=parse_datetime_to_utc_iso(published_raw),
                    summary=summary,
                )
            )
        return updates

    channel = find_first_child(root, {"channel"})
    if channel is None:
        return updates

    publisher = find_first_child_text(channel, {"title"}) or source_name
    for item in [child for child in channel if local_name(child.tag).lower() == "item"]:
        raw_id = find_first_child_text(item, {"guid", "id"})
        title = find_first_child_text(item, {"title"}) or "Untitled"
        link = find_first_child_text(item, {"link"}) or default_url
        published_raw = (
            find_first_child_text(item, {"pubDate"}) or find_first_child_text(item, {"date"}) or find_first_child_text(item, {"updated"})
        )
        summary = find_first_child_text(item, {"description"}) or find_first_child_text(item, {"encoded", "content"})

        unique_basis = raw_id or link or title
        unique_id = f"{source_kind}:{hashlib.sha1(unique_basis.encode('utf-8')).hexdigest()[:20]}"
        updates.append(
            ContentUpdate(
                unique_id=unique_id,
                source=source_kind,
                publisher=publisher,
                title=title,
                url=link,
                published_at=parse_datetime_to_utc_iso(published_raw),
                summary=summary,
            )
        )
    return updates


def resolve_youtube_feed_url(source: dict[str, Any], session: requests.Session) -> str:
    feed_url = normalize_text(source.get("feed_url"))
    if feed_url:
        return feed_url

    channel_id = normalize_text(source.get("channel_id"))
    if channel_id.startswith("UC"):
        return f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

    channel_url = normalize_text(source.get("channel_url") or source.get("url"))
    if not channel_url:
        raise ValueError("YouTube source requires feed_url, channel_id, or channel_url.")

    resp = session.get(channel_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    html = resp.text

    match = YOUTUBE_FEED_REGEX.search(html)
    if match:
        return match.group(0)

    escaped_match = YOUTUBE_FEED_ESCAPED_REGEX.search(html)
    if escaped_match:
        return escaped_match.group(0).replace("\\/", "/")

    for pattern in YOUTUBE_CHANNEL_ID_REGEXES:
        id_match = pattern.search(html)
        if id_match:
            return f"https://www.youtube.com/feeds/videos.xml?channel_id={id_match.group(1)}"

    raise ValueError(f"Unable to resolve YouTube feed URL from: {channel_url}")


def fetch_rss_updates(source: dict[str, Any], session: requests.Session) -> list[ContentUpdate]:
    name = normalize_text(source.get("name")) or "RSS"
    url = normalize_text(source.get("url"))
    if not url:
        raise ValueError(f"RSS source '{name}' is missing url.")
    resp = session.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    return parse_feed_xml(name, "rss", url, resp.text)


def fetch_youtube_updates(source: dict[str, Any], session: requests.Session) -> list[ContentUpdate]:
    name = normalize_text(source.get("name")) or "YouTube"
    feed_url = resolve_youtube_feed_url(source, session)
    resp = session.get(feed_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    updates = parse_feed_xml(name, "youtube", feed_url, resp.text)
    if not updates:
        return []

    channel_name = updates[0].publisher or name
    normalized_updates: list[ContentUpdate] = []
    for item in updates:
        unique_id = item.unique_id
        if not unique_id.startswith("youtube:"):
            unique_id = f"youtube:{unique_id}"
        normalized_updates.append(
            ContentUpdate(
                unique_id=unique_id,
                source="youtube",
                publisher=channel_name,
                title=item.title,
                url=item.url,
                published_at=item.published_at,
                summary=item.summary,
            )
        )
    return normalized_updates


def read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "initialized": False,
            "seen_update_ids": [],
            "pending_notifications": [],
            "last_check_at": None,
        }
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        logging.warning("State file is invalid JSON. Rebuilding from scratch: %s", path)
        return {
            "initialized": False,
            "seen_update_ids": [],
            "pending_notifications": [],
            "last_check_at": None,
        }
    data.setdefault("initialized", False)
    data.setdefault("seen_update_ids", [])
    data.setdefault("pending_notifications", [])
    data.setdefault("last_check_at", None)
    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def deserialize_updates(items: list[dict[str, Any]]) -> list[ContentUpdate]:
    output: list[ContentUpdate] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            output.append(
                ContentUpdate(
                    unique_id=str(item.get("unique_id", "")).strip(),
                    source=str(item.get("source", "")).strip() or "unknown",
                    publisher=str(item.get("publisher", "")).strip() or "Unknown",
                    title=str(item.get("title", "")).strip() or "Untitled",
                    url=str(item.get("url", "")).strip(),
                    published_at=str(item.get("published_at", "")).strip() or utc_now_iso(),
                    summary=str(item.get("summary", "")).strip(),
                )
            )
        except Exception:
            continue
    return [item for item in output if item.unique_id]


def merge_unique_updates(items: list[ContentUpdate]) -> list[ContentUpdate]:
    merged: dict[str, ContentUpdate] = {}
    for item in items:
        merged[item.unique_id] = item
    return list(merged.values())


def collect_updates(config: dict[str, Any], session: requests.Session) -> list[ContentUpdate]:
    sources = config.get("sources") or {}
    request_delay = float(config.get("request_delay_seconds", 1.5))
    request_jitter = float(config.get("request_jitter_seconds", 0.5))
    all_updates: list[ContentUpdate] = []

    for source in sources.get("rss", []):
        sleep_with_jitter(request_delay, request_jitter)
        name = normalize_text(source.get("name")) or "RSS"
        try:
            updates = fetch_rss_updates(source, session)
            all_updates.extend(updates)
            logging.info("RSS        %-24s -> %d updates", name, len(updates))
        except requests.RequestException as exc:
            logging.warning("RSS fetch failed for %s: %s", name, exc)
        except ValueError as exc:
            logging.warning("RSS source config error for %s: %s", name, exc)

    for source in sources.get("youtube", []):
        sleep_with_jitter(request_delay, request_jitter)
        name = normalize_text(source.get("name")) or "YouTube"
        try:
            updates = fetch_youtube_updates(source, session)
            all_updates.extend(updates)
            logging.info("YouTube    %-24s -> %d updates", name, len(updates))
        except requests.RequestException as exc:
            logging.warning("YouTube fetch failed for %s: %s", name, exc)
        except ValueError as exc:
            logging.warning("YouTube source config error for %s: %s", name, exc)

    merged = merge_unique_updates(all_updates)
    return sorted(merged, key=lambda x: (x.publisher.lower(), x.title.lower(), x.unique_id))


def format_updates_for_email(updates: list[ContentUpdate]) -> str:
    now = datetime.now(timezone.utc)

    def sort_key(item: ContentUpdate) -> datetime:
        dt = parse_datetime_any(item.published_at)
        if dt is None:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        return dt.astimezone(timezone.utc)

    ordered = sorted(updates, key=sort_key, reverse=True)
    lines = ["发现新的内容更新：", ""]
    for idx, item in enumerate(ordered, 1):
        lines.append(f"{idx}. {item.publisher} | {item.title}")
        lines.append(f"   来源: {item.source}")
        lines.append(f"   发布时间: {format_published_time_with_age(item.published_at, reference_time=now)}")
        lines.append(f"   链接: {item.url}")
        if item.summary:
            summary = normalize_text(item.summary)
            if len(summary) > 220:
                summary = summary[:217] + "..."
            lines.append(f"   摘要: {summary}")
        lines.append("")
    lines.append(f"发送时间(UTC): {utc_now_iso()}")
    return "\n".join(lines)


def send_email(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    recipient_email: str,
    subject: str,
    body: str,
) -> None:
    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = recipient_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


def run_once(args: argparse.Namespace, config: dict[str, Any], state_path: Path, session: requests.Session) -> None:
    updates = collect_updates(config, session)
    state = load_state(state_path)

    seen_ids: set[str] = set(str(x) for x in state.get("seen_update_ids", []))
    pending_items = deserialize_updates(state.get("pending_notifications", []))
    pending_ids = {item.unique_id for item in pending_items}

    current_by_id = {item.unique_id: item for item in updates}
    current_ids = set(current_by_id.keys())

    to_notify: list[ContentUpdate] = []

    if not bool(state.get("initialized", False)):
        logging.info("First run detected. Building baseline with %d updates.", len(current_ids))
        state["initialized"] = True
        seen_ids.update(current_ids)
        if args.send_initial_snapshot:
            to_notify = updates
    else:
        fresh_updates = [item for item in updates if item.unique_id not in seen_ids and item.unique_id not in pending_ids]
        to_notify = merge_unique_updates([*pending_items, *fresh_updates])

    max_age_days = config.get("max_post_age_days_for_email")
    max_age_days_int = int(max_age_days) if isinstance(max_age_days, (int, float, str)) and str(max_age_days).strip() else None
    to_notify_recent, stale = split_updates_by_post_age(to_notify, max_age_days_int)
    if stale:
        logging.info("Skipped %d stale updates older than max_post_age_days_for_email.", len(stale))

    if to_notify_recent:
        subject = f"发现新的内容更新：{len(to_notify_recent)} 条"
        body = format_updates_for_email(to_notify_recent)
        smtp_host = str(config.get("smtp_host", "smtp.gmail.com")).strip()
        smtp_port = int(config.get("smtp_port", 465))
        recipient_email = str(config.get("recipient_email", "")).strip()
        smtp_user = os.getenv("SMTP_USER", "").strip()
        smtp_password = os.getenv("SMTP_PASSWORD", "").strip()

        if args.dry_run:
            logging.info("Dry run enabled. Would send %d updates.", len(to_notify_recent))
            for item in to_notify_recent[:5]:
                logging.info("DRY-RUN: %s | %s | %s", item.publisher, item.title, item.url)
            state["pending_notifications"] = [asdict(item) for item in to_notify_recent]
        elif not smtp_user or not smtp_password:
            logging.warning("SMTP_USER or SMTP_PASSWORD is not configured. Keeping %d updates pending.", len(to_notify_recent))
            state["pending_notifications"] = [asdict(item) for item in to_notify_recent]
        elif not recipient_email:
            logging.warning("recipient_email is missing in config. Keeping %d updates pending.", len(to_notify_recent))
            state["pending_notifications"] = [asdict(item) for item in to_notify_recent]
        else:
            try:
                send_email(
                    smtp_host=smtp_host,
                    smtp_port=smtp_port,
                    smtp_user=smtp_user,
                    smtp_password=smtp_password,
                    recipient_email=recipient_email,
                    subject=subject,
                    body=body,
                )
                logging.info("Email sent: %d new updates.", len(to_notify_recent))
                seen_ids.update(item.unique_id for item in to_notify_recent)
                state["pending_notifications"] = []
            except Exception as exc:
                logging.exception("Failed to send email: %s", exc)
                state["pending_notifications"] = [asdict(item) for item in to_notify_recent]
    else:
        logging.info("No new content updates.")
        state["pending_notifications"] = []

    state["seen_update_ids"] = sorted(seen_ids)
    state["last_check_at"] = utc_now_iso()
    save_state(state_path, state)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor RSS/YouTube updates and send email notifications.")
    parser.add_argument("--config", default="content-monitor/sources.json", help="Path to content monitor config JSON.")
    parser.add_argument("--state", default="content-monitor/.content_monitor_state.json", help="Path to state JSON.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument(
        "--send-initial-snapshot",
        action="store_true",
        help="On first run, send existing updates instead of only building baseline.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Collect and log updates without sending emails.")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    config_path = Path(args.config)
    state_path = Path(args.state)
    config = read_json_file(config_path)
    poll_interval_minutes = int(config.get("poll_interval_minutes", 60))
    if poll_interval_minutes < 1:
        poll_interval_minutes = 60

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (ContentMonitor/1.0)"})

    while True:
        try:
            run_once(args, config, state_path, session)
        except Exception as exc:
            logging.exception("Monitor cycle failed: %s", exc)

        if args.once:
            break

        time.sleep(poll_interval_minutes * 60)


if __name__ == "__main__":
    main()

