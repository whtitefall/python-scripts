#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
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
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

import requests

CANADA_REGION_WORDS = (
    "canada",
    "ontario",
    "quebec",
    "british columbia",
    "alberta",
    "manitoba",
    "saskatchewan",
    "nova scotia",
    "new brunswick",
    "newfoundland",
    "labrador",
    "prince edward island",
    "nunavut",
    "northwest territories",
    "yukon",
    "toronto",
    "ottawa",
    "montreal",
    "vancouver",
    "calgary",
    "edmonton",
    "waterloo",
    "victoria",
)

PROVINCE_CODE_PATTERN = re.compile(
    r"(?:,\s*|\(|\s)(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)(?:\)|\b)",
    re.IGNORECASE,
)

GOOGLE_CAREERS_RESULTS_URL = "https://www.google.com/about/careers/applications/jobs/results"
GOOGLE_CAREERS_APP_BASE_URL = "https://www.google.com/about/careers/applications/"
GOOGLE_CARD_SPLIT_PATTERN = re.compile(r'<li class="lLd3Je"(?=[\s>])', re.IGNORECASE)
GOOGLE_JOB_ID_PATTERN = re.compile(r"ssk='[^']*:(\d+)'")
GOOGLE_TITLE_PATTERN = re.compile(r'<h3 class="QJPWVe">\s*(.*?)\s*</h3>', re.IGNORECASE | re.DOTALL)
GOOGLE_LOCATION_PATTERN = re.compile(r'<span class="r0wTof[^"]*">\s*(.*?)\s*</span>', re.IGNORECASE | re.DOTALL)
GOOGLE_LEARN_MORE_LINK_PATTERN = re.compile(r'href="(jobs/results/[^"]+)"', re.IGNORECASE)
GOOGLE_LINK_ID_PATTERN = re.compile(r"jobs/results/(\d+)")
WORKDAY_CXS_PATTERN = re.compile(
    r"^https://[^/]+/wday/cxs/(?P<tenant>[^/]+)/(?P<site>[^/]+)/jobs/?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class JobPosting:
    unique_id: str
    source: str
    company: str
    title: str
    location: str
    url: str
    updated_at: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def is_canada_location(raw_location: str) -> bool:
    if not raw_location:
        return False
    text = raw_location.strip()
    lowered = text.lower()

    if any(word in lowered for word in CANADA_REGION_WORDS):
        return True

    return bool(PROVINCE_CODE_PATTERN.search(text))


def parse_datetime_from_epoch_ms(value: Any) -> str:
    if value is None:
        return utc_now_iso()
    try:
        epoch_ms = int(value)
    except (TypeError, ValueError):
        return utc_now_iso()
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).replace(microsecond=0).isoformat()


def parse_datetime_from_epoch_seconds(value: Any) -> str:
    if value is None:
        return utc_now_iso()
    try:
        epoch_sec = int(value)
    except (TypeError, ValueError):
        return utc_now_iso()
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).replace(microsecond=0).isoformat()


def sleep_with_jitter(base_delay: float, jitter: float) -> None:
    if base_delay <= 0 and jitter <= 0:
        return
    lower = max(0.0, base_delay - abs(jitter))
    upper = max(lower, base_delay + abs(jitter))
    time.sleep(random.uniform(lower, upper))


def title_matches_keywords(title: str, keywords: list[str]) -> bool:
    cleaned_keywords = [k.strip().lower() for k in keywords if k and k.strip()]
    if not cleaned_keywords:
        return True
    lowered_title = title.lower()
    return any(keyword in lowered_title for keyword in cleaned_keywords)


def normalize_html_text(fragment: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", fragment)
    unescaped = html.unescape(no_tags)
    return re.sub(r"\s+", " ", unescaped).strip()


def build_google_careers_search_url(source: dict[str, Any]) -> str:
    custom_url = str(source.get("url", "")).strip()
    if custom_url:
        return custom_url

    params: dict[str, str] = {}
    location = str(source.get("location", "Canada")).strip()
    if location:
        params["location"] = location

    keyword = str(source.get("q", "")).strip()
    if keyword:
        params["q"] = keyword

    if not params:
        return GOOGLE_CAREERS_RESULTS_URL
    return f"{GOOGLE_CAREERS_RESULTS_URL}?{urlencode(params)}"


def parse_google_careers_cards(html_text: str, search_url: str) -> list[tuple[str, str, str, str]]:
    cards = GOOGLE_CARD_SPLIT_PATTERN.split(html_text)
    parsed: list[tuple[str, str, str, str]] = []

    for card in cards[1:]:
        title_match = GOOGLE_TITLE_PATTERN.search(card)
        location_match = GOOGLE_LOCATION_PATTERN.search(card)
        link_match = GOOGLE_LEARN_MORE_LINK_PATTERN.search(card)
        if not title_match or not location_match or not link_match:
            continue

        title = normalize_html_text(title_match.group(1))
        location = normalize_html_text(location_match.group(1))
        relative_url = html.unescape(link_match.group(1)).strip()
        absolute_url = urljoin(GOOGLE_CAREERS_APP_BASE_URL, relative_url)

        job_id_match = GOOGLE_JOB_ID_PATTERN.search(card)
        if job_id_match:
            job_id = job_id_match.group(1)
        else:
            fallback_id_match = GOOGLE_LINK_ID_PATTERN.search(relative_url)
            if not fallback_id_match:
                continue
            job_id = fallback_id_match.group(1)

        if not title or not location:
            continue
        parsed.append((job_id, title, location, absolute_url))

    if parsed:
        return parsed

    # Fallback for future HTML changes: parse plain-text page dump lines.
    title_plain = re.compile(r"###\s+(.+)")
    location_plain = re.compile(r"Google\s+\|\s+(.+)")
    fallback: list[tuple[str, str, str, str]] = []
    lines = [line.strip() for line in html_text.splitlines() if line.strip()]
    for idx, line in enumerate(lines):
        title_match = title_plain.match(line)
        if not title_match:
            continue
        title = title_match.group(1).strip()
        location = "Unknown"
        for look_ahead in lines[idx : idx + 12]:
            location_match = location_plain.match(look_ahead)
            if location_match:
                location = location_match.group(1).strip()
                break
        if title:
            seed = f"{title}|{location}|{search_url}".encode("utf-8")
            fallback_id = f"fallback-{hashlib.sha1(seed).hexdigest()[:16]}"
            fallback.append((fallback_id, title, location, search_url))
    return fallback


def fetch_google_careers_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    search_url = build_google_careers_search_url(source)
    resp = session.get(search_url, timeout=30)
    resp.raise_for_status()

    parsed = parse_google_careers_cards(resp.text, search_url)
    jobs: list[JobPosting] = []
    timestamp = utc_now_iso()
    title_keywords = source.get("title_keywords") or []
    if not isinstance(title_keywords, list):
        title_keywords = [str(title_keywords)]
    for job_id, title, location, job_url in parsed:
        if not is_canada_location(location):
            continue
        if not title_matches_keywords(title, [str(x) for x in title_keywords]):
            continue
        jobs.append(
            JobPosting(
                unique_id=f"google:{job_id}",
                source="google_careers",
                company=company,
                title=title,
                location=location,
                url=job_url,
                updated_at=timestamp,
            )
        )
    return jobs


def fetch_microsoft_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    endpoint = str(source.get("endpoint", "https://apply.careers.microsoft.com/api/pcsx/search")).strip()
    domain = str(source.get("domain", "microsoft.com")).strip() or "microsoft.com"
    query = str(source.get("q", source.get("search_text", ""))).strip()
    location = str(source.get("location", "Canada")).strip() or "Canada"
    limit = int(source.get("limit", 20))
    max_pages = int(source.get("max_pages", 5))
    title_keywords = source.get("title_keywords") or []
    if not isinstance(title_keywords, list):
        title_keywords = [str(title_keywords)]
    keyword_list = [str(x) for x in title_keywords]

    jobs: list[JobPosting] = []
    offset = 0

    for _ in range(max_pages):
        params = {
            "domain": domain,
            "location": location,
            "limit": str(limit),
            "offset": str(offset),
        }
        if query:
            params["query"] = query

        resp = session.get(endpoint, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        positions = (payload.get("data") or {}).get("positions") or []
        if not positions:
            break

        for item in positions:
            title = str(item.get("name", "Untitled")).strip()
            location_text = "; ".join(str(x).strip() for x in (item.get("locations") or []) if str(x).strip())
            if not is_canada_location(location_text):
                continue
            if not title_matches_keywords(title, keyword_list):
                continue

            position_id = item.get("id")
            relative_url = str(item.get("positionUrl") or "").strip()
            if relative_url:
                job_url = urljoin("https://apply.careers.microsoft.com", relative_url)
                if "domain=" not in job_url:
                    joiner = "&" if "?" in job_url else "?"
                    job_url = f"{job_url}{joiner}domain={domain}"
            else:
                job_url = "https://apply.careers.microsoft.com/careers"

            jobs.append(
                JobPosting(
                    unique_id=f"microsoft:{position_id}",
                    source="microsoft_careers",
                    company=company,
                    title=title,
                    location=location_text or "Unknown",
                    url=job_url,
                    updated_at=parse_datetime_from_epoch_seconds(item.get("postedTs")),
                )
            )

        if len(positions) < limit:
            break
        offset += limit

    return jobs


def fetch_workday_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    endpoint = str(source.get("endpoint", "")).strip()
    if not endpoint:
        raise ValueError(f"Workday source for {company} is missing endpoint.")

    match = WORKDAY_CXS_PATTERN.match(endpoint)
    if not match:
        raise ValueError(f"Invalid Workday CXS endpoint format for {company}: {endpoint}")

    query = str(source.get("q", source.get("search_text", ""))).strip()
    limit = int(source.get("limit", 20))
    max_pages = int(source.get("max_pages", 5))
    title_keywords = source.get("title_keywords") or []
    if not isinstance(title_keywords, list):
        title_keywords = [str(title_keywords)]
    keyword_list = [str(x) for x in title_keywords]
    extra_payload = source.get("payload") if isinstance(source.get("payload"), dict) else {}

    base_for_links = endpoint.split("/wday/cxs/", 1)[0]
    jobs: list[JobPosting] = []
    offset = 0

    for _ in range(max_pages):
        payload: dict[str, Any] = {"limit": limit, "offset": offset}
        if query:
            payload["searchText"] = query
        payload.update(extra_payload)

        resp = session.post(endpoint, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        postings = data.get("jobPostings") or []
        if not postings:
            break

        for item in postings:
            title = str(item.get("title", "Untitled")).strip()
            location = str(item.get("locationsText", "")).strip()
            if not is_canada_location(location):
                continue
            if not title_matches_keywords(title, keyword_list):
                continue

            external_path = str(item.get("externalPath") or "").strip()
            if external_path:
                job_url = urljoin(base_for_links, external_path)
            else:
                job_url = endpoint.rsplit("/wday/cxs/", 1)[0]

            req_id = str(item.get("bulletFields", [])[-1] if item.get("bulletFields") else item.get("title", ""))
            unique_id_seed = f"{company}|{external_path}|{req_id}|{title}"
            unique_id = f"workday:{hashlib.sha1(unique_id_seed.encode('utf-8')).hexdigest()[:20]}"

            jobs.append(
                JobPosting(
                    unique_id=unique_id,
                    source="workday_cxs",
                    company=company,
                    title=title,
                    location=location or "Unknown",
                    url=job_url,
                    updated_at=utc_now_iso(),
                )
            )

        if len(postings) < limit:
            break
        offset += limit

    return jobs


def read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "initialized": False,
            "seen_job_ids": [],
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
            "seen_job_ids": [],
            "pending_notifications": [],
            "last_check_at": None,
        }

    data.setdefault("initialized", False)
    data.setdefault("seen_job_ids", [])
    data.setdefault("pending_notifications", [])
    data.setdefault("last_check_at", None)
    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def fetch_greenhouse_jobs(
    company: str,
    token: str,
    session: requests.Session,
    title_keywords: list[str] | None = None,
) -> list[JobPosting]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    jobs: list[JobPosting] = []
    keyword_list = title_keywords or []
    for item in payload.get("jobs", []):
        location = (item.get("location") or {}).get("name", "").strip()
        if not is_canada_location(location):
            continue
        title = str(item.get("title", "Untitled")).strip()
        if not title_matches_keywords(title, keyword_list):
            continue
        job_id = item.get("id")
        absolute_url = item.get("absolute_url", "")
        jobs.append(
            JobPosting(
                unique_id=f"greenhouse:{token}:{job_id}",
                source="greenhouse",
                company=company,
                title=title,
                location=location or "Unknown",
                url=absolute_url,
                updated_at=str(item.get("updated_at") or utc_now_iso()),
            )
        )
    return jobs


def fetch_lever_jobs(
    company: str,
    handle: str,
    session: requests.Session,
    title_keywords: list[str] | None = None,
) -> list[JobPosting]:
    url = f"https://api.lever.co/v0/postings/{handle}?mode=json"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    jobs: list[JobPosting] = []
    keyword_list = title_keywords or []
    for item in payload:
        categories = item.get("categories") or {}
        location = str(categories.get("location", "")).strip()
        if not is_canada_location(location):
            continue
        title = str(item.get("text", "Untitled")).strip()
        if not title_matches_keywords(title, keyword_list):
            continue
        jobs.append(
            JobPosting(
                unique_id=f"lever:{handle}:{item.get('id')}",
                source="lever",
                company=company,
                title=title,
                location=location or "Unknown",
                url=str(item.get("hostedUrl", "")).strip(),
                updated_at=parse_datetime_from_epoch_ms(item.get("createdAt")),
            )
        )
    return jobs


def collect_jobs(config: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    sources = config.get("sources") or {}
    all_jobs: list[JobPosting] = []
    processed_sources: set[tuple[str, str]] = set()
    request_delay = float(config.get("request_delay_seconds", 2.5))
    request_jitter = float(config.get("request_jitter_seconds", 1.0))

    def fetch_by_source(
        source_name: str,
        company: str,
        identifier: str,
        title_keywords: list[str] | None = None,
    ) -> None:
        normalized_key = (source_name, identifier.lower())
        if normalized_key in processed_sources:
            return
        processed_sources.add(normalized_key)
        sleep_with_jitter(request_delay, request_jitter)

        try:
            if source_name == "greenhouse":
                jobs = fetch_greenhouse_jobs(company, identifier, session, title_keywords=title_keywords)
                all_jobs.extend(jobs)
                logging.info("Greenhouse %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "lever":
                jobs = fetch_lever_jobs(company, identifier, session, title_keywords=title_keywords)
                all_jobs.extend(jobs)
                logging.info("Lever      %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "google_careers":
                jobs = fetch_google_careers_jobs(
                    company,
                    {"url": identifier, "title_keywords": title_keywords or []},
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("Google     %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "workday_cxs":
                jobs = fetch_workday_jobs(company, {"endpoint": identifier, "title_keywords": title_keywords or []}, session)
                all_jobs.extend(jobs)
                logging.info("Workday    %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "microsoft_careers":
                jobs = fetch_microsoft_jobs(
                    company,
                    {
                        "endpoint": identifier,
                        "title_keywords": title_keywords or [],
                    },
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("Microsoft  %-20s -> %d Canada jobs", company, len(jobs))
                return
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "unknown"
            logging.warning("%s fetch failed for %s (%s): HTTP %s", source_name.title(), company, identifier, code)
            return
        except requests.RequestException as exc:
            logging.warning("%s fetch failed for %s (%s): %s", source_name.title(), company, identifier, exc)
            return

    for source in sources.get("greenhouse", []):
        company = str(source.get("company", "")).strip()
        token = str(source.get("token", "")).strip()
        title_keywords = [str(x) for x in (source.get("title_keywords") or [])]
        if not company or not token:
            continue
        fetch_by_source("greenhouse", company, token, title_keywords=title_keywords)

    for source in sources.get("lever", []):
        company = str(source.get("company", "")).strip()
        handle = str(source.get("handle", "")).strip()
        title_keywords = [str(x) for x in (source.get("title_keywords") or [])]
        if not company or not handle:
            continue
        fetch_by_source("lever", company, handle, title_keywords=title_keywords)

    for source in sources.get("google_careers", []):
        company = str(source.get("company", "Google")).strip() or "Google"
        title_keywords = [str(x) for x in (source.get("title_keywords") or [])]
        sleep_with_jitter(request_delay, request_jitter)
        try:
            jobs = fetch_google_careers_jobs(company, source, session)
            all_jobs.extend(jobs)
            logging.info("Google     %-20s -> %d Canada jobs", company, len(jobs))
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "unknown"
            logging.warning("Google fetch failed for %s: HTTP %s", company, code)
        except requests.RequestException as exc:
            logging.warning("Google fetch failed for %s: %s", company, exc)
        if title_keywords:
            # source already handled title filter; this branch only keeps uniform metrics with explicit keywords.
            logging.debug("Google source %s title keywords: %s", company, ", ".join(title_keywords))

    for source in sources.get("microsoft_careers", []):
        company = str(source.get("company", "Microsoft")).strip() or "Microsoft"
        sleep_with_jitter(request_delay, request_jitter)
        try:
            jobs = fetch_microsoft_jobs(company, source, session)
            all_jobs.extend(jobs)
            logging.info("Microsoft  %-20s -> %d Canada jobs", company, len(jobs))
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "unknown"
            logging.warning("Microsoft fetch failed for %s: HTTP %s", company, code)
        except requests.RequestException as exc:
            logging.warning("Microsoft fetch failed for %s: %s", company, exc)

    for source in sources.get("workday_cxs", []):
        company = str(source.get("company", "Workday")).strip() or "Workday"
        sleep_with_jitter(request_delay, request_jitter)
        try:
            jobs = fetch_workday_jobs(company, source, session)
            all_jobs.extend(jobs)
            logging.info("Workday    %-20s -> %d Canada jobs", company, len(jobs))
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "unknown"
            logging.warning("Workday fetch failed for %s: HTTP %s", company, code)
        except requests.RequestException as exc:
            logging.warning("Workday fetch failed for %s: %s", company, exc)
        except ValueError as exc:
            logging.warning("Workday source config error for %s: %s", company, exc)

    for source in sources.get("career_pages", []):
        company = str(source.get("company", "")).strip()
        url = str(source.get("url", "")).strip()
        title_keywords = [str(x) for x in (source.get("title_keywords") or [])]
        if not url:
            continue
        parsed = parse_source_from_career_page(url)
        if parsed is None:
            logging.warning("Unsupported career page URL for %s: %s", company or "unknown", url)
            continue
        source_name, identifier = parsed
        effective_company = company or identifier
        fetch_by_source(source_name, effective_company, identifier, title_keywords=title_keywords)

    unsupported_companies = [str(x).strip() for x in (config.get("unsupported_companies") or []) if str(x).strip()]
    if unsupported_companies:
        logging.info("Configured but currently unavailable source(s): %s", ", ".join(unsupported_companies))

    unique_jobs: dict[str, JobPosting] = {}
    for job in all_jobs:
        unique_jobs[job.unique_id] = job
    return sorted(unique_jobs.values(), key=lambda j: (j.company.lower(), j.title.lower(), j.unique_id))


def parse_source_from_career_page(url: str) -> tuple[str, str] | None:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path_parts = [p for p in parsed.path.split("/") if p]
    if not path_parts:
        return None

    if "greenhouse.io" in host:
        token = path_parts[0].strip()
        if token == "embed" and len(path_parts) > 1:
            token = path_parts[1].strip()
        if token:
            return ("greenhouse", token)

    if "lever.co" in host and host.startswith("jobs."):
        handle = path_parts[0].strip()
        if handle:
            return ("lever", handle)

    if "google.com" in host and "/about/careers/applications/jobs/results" in parsed.path:
        return ("google_careers", url)

    return None


def render_email_body(jobs: list[JobPosting]) -> str:
    lines = [
        "发现新的加拿大科技岗位：",
        "",
    ]

    for idx, job in enumerate(jobs, start=1):
        lines.extend(
            [
                f"{idx}. {job.company} | {job.title}",
                f"   地点: {job.location}",
                f"   来源: {job.source}",
                f"   发布时间(UTC): {job.updated_at}",
                f"   链接: {job.url}",
                "",
            ]
        )

    lines.append(f"发送时间(UTC): {utc_now_iso()}")
    return "\n".join(lines)


def send_email(
    jobs: list[JobPosting],
    recipient: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
) -> bool:
    if not jobs:
        return True

    if not smtp_user or not smtp_password:
        logging.error("SMTP_USER / SMTP_PASSWORD 未配置，无法发送邮件。")
        return False

    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = recipient
    msg["Subject"] = f"[Canada Job Alert] {len(jobs)} new openings ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
    msg.set_content(render_email_body(jobs))

    try:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        logging.info("Email sent to %s with %d new jobs.", recipient, len(jobs))
        return True
    except Exception as exc:
        logging.error("Email send failed: %s", exc)
        return False


def index_pending_jobs(pending: list[dict[str, Any]]) -> dict[str, JobPosting]:
    indexed: dict[str, JobPosting] = {}
    for item in pending:
        try:
            job = JobPosting(**item)
        except TypeError:
            continue
        indexed[job.unique_id] = job
    return indexed


def run_check_cycle(
    config: dict[str, Any],
    state: dict[str, Any],
    state_path: Path,
    session: requests.Session,
    send_initial_snapshot: bool,
) -> None:
    recipient = str(config.get("recipient_email", "")).strip()
    if not recipient:
        raise ValueError("recipient_email is missing in config.")

    smtp_host = str(config.get("smtp_host", "smtp.gmail.com")).strip() or "smtp.gmail.com"
    smtp_port = int(config.get("smtp_port", 465))
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()

    jobs = collect_jobs(config, session)
    seen_ids = set(str(x) for x in state.get("seen_job_ids", []))
    pending_map = index_pending_jobs(state.get("pending_notifications", []))
    email_already_attempted = False

    if not state.get("initialized", False):
        if send_initial_snapshot and jobs:
            sent = send_email(jobs, recipient, smtp_host, smtp_port, smtp_user, smtp_password)
            email_already_attempted = True
            if sent:
                seen_ids.update(j.unique_id for j in jobs)
            else:
                pending_map = {job.unique_id: job for job in jobs}
        else:
            seen_ids.update(j.unique_id for j in jobs)
            logging.info("Initialized baseline with %d jobs. No initial email sent.", len(jobs))

        state["initialized"] = True
    else:
        for job in jobs:
            if job.unique_id in seen_ids or job.unique_id in pending_map:
                continue
            pending_map[job.unique_id] = job

    pending_jobs = sorted(pending_map.values(), key=lambda j: (j.company.lower(), j.title.lower(), j.unique_id))
    if pending_jobs and not email_already_attempted:
        sent = send_email(pending_jobs, recipient, smtp_host, smtp_port, smtp_user, smtp_password)
        if sent:
            seen_ids.update(job.unique_id for job in pending_jobs)
            pending_jobs = []
        else:
            logging.warning("Will retry %d pending jobs in next cycle.", len(pending_jobs))
    elif pending_jobs:
        logging.warning("Will retry %d pending jobs in next cycle.", len(pending_jobs))
    else:
        logging.info("No new jobs this cycle.")

    state["seen_job_ids"] = sorted(seen_ids)
    state["pending_notifications"] = [asdict(job) for job in pending_jobs]
    state["last_check_at"] = utc_now_iso()
    save_state(state_path, state)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Monitor software-developer jobs in Canada across multiple tech companies and send email alerts."
    )
    parser.add_argument(
        "--config",
        default="companies.json",
        help="Path to config JSON file (default: companies.json)",
    )
    parser.add_argument(
        "--state",
        default=".job_monitor_state.json",
        help="Path to state JSON file (default: .job_monitor_state.json)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one check cycle and exit.",
    )
    parser.add_argument(
        "--send-initial-snapshot",
        action="store_true",
        help="Send all currently found jobs on first run.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    args = parse_args()
    config_path = Path(args.config).resolve()
    state_path = Path(args.state).resolve()

    config = read_json_file(config_path)
    poll_interval = int(config.get("poll_interval_minutes", 60))
    if poll_interval < 1:
        raise ValueError("poll_interval_minutes must be >= 1.")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
            "DNT": "1",
        }
    )

    state = load_state(state_path)

    if args.once:
        run_check_cycle(config, state, state_path, session, args.send_initial_snapshot)
        return

    logging.info("Job monitor started. Interval=%d minutes, config=%s", poll_interval, config_path)
    while True:
        try:
            run_check_cycle(config, state, state_path, session, args.send_initial_snapshot)
            state = load_state(state_path)
        except Exception:
            logging.exception("Unexpected error in check cycle.")
        time.sleep(poll_interval * 60)


if __name__ == "__main__":
    main()
