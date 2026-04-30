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
IBM_JOB_ID_PATTERN = re.compile(r"[?&]jobId=(\d+)", re.IGNORECASE)
YELP_CAREERS_JOB_PATTERN = re.compile(
    r'"reqId":"(?P<req_id>\d+)".+?"title":"(?P<title>[^"]+)".+?"postedDate":"(?P<posted_date>[^"]+)".+?'
    r'"dateCreated":"(?P<created_date>[^"]+)".+?"applyUrl":"(?P<apply_url>https:(?:\/\/|\\\/\\\/)cancareers-yelp\.icims\.com[^"]+\/job)".+?'
    r'"location":"(?P<location>[^"]+)"',
    re.DOTALL,
)
INTUIT_SEARCH_DEFAULT_URL = "https://jobs.intuit.com/search-jobs?acm=68357&l=Canada&orgIds=27595"
INTUIT_JOB_CARD_PATTERN = re.compile(
    r'<a href="(?P<href>/job/[^"]+/\d+/\d+)"[^>]*data-title="(?P<title>[^"]+)"[^>]*>.*?'
    r'<span class="job-location">(?P<location>.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
INTUIT_TOTAL_PAGES_PATTERN = re.compile(r'data-total-pages="(?P<total_pages>\d+)"', re.IGNORECASE)
INTUIT_JOBPOSTING_JSONLD_PATTERN = re.compile(
    r"<script[^>]*type=[\"']application/ld\\+json[\"'][^>]*>(?P<payload>.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
INTUIT_DATE_ONLY_PATTERN = re.compile(r"^(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})$")
GITHUB_MODELS_INFERENCE_URL = "https://models.github.ai/inference/chat/completions"
EXPERIENCE_WORD_TO_NUM = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
}
EXPERIENCE_NUMBER_PATTERN = r"(?:\d{1,2}|zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen)"
EXPERIENCE_REQUIRED_PATTERN = re.compile(
    rf"""
    (?:
        (?P<min_a>{EXPERIENCE_NUMBER_PATTERN})
        \s*(?:\+|plus)?\s*
        (?:(?:-|to|–|—)\s*(?P<max_a>{EXPERIENCE_NUMBER_PATTERN})\s*)?
        (?:years?|yrs?)
        (?:\s+of)?
        (?:\s+\w+){{0,5}}
        \s+experience
    )
    |
    (?:
        (?:experience|exp\.?)
        (?:\s+of)?
        \s*(?P<min_b>{EXPERIENCE_NUMBER_PATTERN})
        \s*(?:\+|plus)?\s*
        (?:(?:-|to|–|—)\s*(?P<max_b>{EXPERIENCE_NUMBER_PATTERN})\s*)?
        (?:years?|yrs?)
    )
    |
    (?:
        (?:at\s+least|minimum(?:\s+of)?|min\.?)
        \s*(?P<min_c>{EXPERIENCE_NUMBER_PATTERN})
        \s*(?:years?|yrs?)
        (?:\s+of)?
        (?:\s+\w+){{0,5}}
        \s+experience
    )
    """,
    re.IGNORECASE | re.VERBOSE,
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


def parse_datetime_iso(value: str) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_datetime_to_utc_iso(value: str) -> str:
    dt = parse_datetime_iso(value)
    if dt is None:
        return utc_now_iso()
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def parse_amazon_posted_date_to_utc_iso(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return utc_now_iso()
    parsed_iso = parse_datetime_iso(text)
    if parsed_iso is not None:
        return parsed_iso.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return dt.replace(microsecond=0).isoformat()
        except ValueError:
            continue
    return utc_now_iso()


def parse_intuit_date_to_utc_iso(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return utc_now_iso()

    parsed_iso = parse_datetime_iso(text)
    if parsed_iso is not None:
        return parsed_iso.astimezone(timezone.utc).replace(microsecond=0).isoformat()

    match = INTUIT_DATE_ONLY_PATTERN.match(text)
    if match:
        try:
            dt = datetime(
                int(match.group("year")),
                int(match.group("month")),
                int(match.group("day")),
                tzinfo=timezone.utc,
            )
            return dt.replace(microsecond=0).isoformat()
        except ValueError:
            pass

    return utc_now_iso()


def decode_json_escaped_text(value: str) -> str:
    try:
        return json.loads(f'"{value}"')
    except json.JSONDecodeError:
        return value.replace("\\/", "/").replace('\\"', '"')


def format_uber_location_item(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if not isinstance(value, dict):
        return ""
    city = str(value.get("city", "")).strip()
    region = str(value.get("region", "")).strip()
    country = str(value.get("countryName") or value.get("country") or "").strip()
    parts = [part for part in [city, region, country] if part]
    return ", ".join(parts)


def docattributes_to_dict(values: Any) -> dict[str, str]:
    output: dict[str, str] = {}
    if not isinstance(values, list):
        return output
    for item in values:
        if not isinstance(item, dict):
            continue
        for key, value in item.items():
            if key not in output:
                output[key] = str(value)
    return output


def extract_json_array_from_html(text: str, key: str) -> list[dict[str, Any]]:
    marker = f'"{key}":['
    start = text.find(marker)
    if start < 0:
        return []
    array_start = start + len(marker) - 1
    depth = 0
    array_end = -1
    for idx in range(array_start, len(text)):
        ch = text[idx]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                array_end = idx
                break
    if array_end < 0:
        return []
    array_text = text[array_start : array_end + 1]
    try:
        parsed = json.loads(array_text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def sleep_with_jitter(base_delay: float, jitter: float) -> None:
    if base_delay <= 0 and jitter <= 0:
        return
    lower = max(0.0, base_delay - abs(jitter))
    upper = max(lower, base_delay + abs(jitter))
    time.sleep(random.uniform(lower, upper))


def to_keyword_list(raw_keywords: Any) -> list[str]:
    if raw_keywords is None:
        return []
    if isinstance(raw_keywords, list):
        candidates = raw_keywords
    else:
        candidates = [raw_keywords]
    return [str(k).strip() for k in candidates if str(k).strip()]


def merge_keywords(base_keywords: list[str], extra_keywords: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for keyword in [*base_keywords, *extra_keywords]:
        normalized = keyword.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        merged.append(keyword.strip())
    return merged


def title_matches_keywords(title: str, keywords: list[str]) -> bool:
    cleaned_keywords = [k.lower() for k in to_keyword_list(keywords)]
    if not cleaned_keywords:
        return True
    lowered_title = title.lower()
    return any(keyword in lowered_title for keyword in cleaned_keywords)


def title_has_excluded_keywords(title: str, keywords: list[str]) -> bool:
    cleaned_keywords = [k.lower() for k in to_keyword_list(keywords)]
    if not cleaned_keywords:
        return False
    lowered_title = title.lower()
    return any(keyword in lowered_title for keyword in cleaned_keywords)


def parse_year_token(token: str | None) -> int | None:
    if token is None:
        return None
    normalized = token.strip().lower()
    if not normalized:
        return None
    if normalized.isdigit():
        return int(normalized)
    return EXPERIENCE_WORD_TO_NUM.get(normalized)


def to_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def extract_min_experience_years(text: str) -> list[int]:
    if not text:
        return []

    candidates: list[int] = []
    for match in EXPERIENCE_REQUIRED_PATTERN.finditer(text):
        for group_name in ("min_a", "min_b", "min_c"):
            value = parse_year_token(match.group(group_name))
            if value is not None:
                candidates.append(value)
                break
    return candidates


def requires_experience_at_or_above(text: str, threshold: int | None) -> bool:
    if threshold is None:
        return False
    if threshold <= 0:
        return False
    min_years = extract_min_experience_years(text)
    return any(years >= threshold for years in min_years)


def requires_experience_min_at_or_above(text: str, threshold: int | None) -> bool:
    if threshold is None:
        return False
    if threshold <= 0:
        return False
    min_years = extract_min_experience_years(text)
    if not min_years:
        return False
    # Workday pages can include multiple tracks (e.g. Software Engineer + Senior Software Engineer).
    # In those cases, use the smallest explicitly stated requirement.
    return min(min_years) >= threshold


def flatten_text(value: Any) -> str:
    parts: list[str] = []

    def walk(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, str):
            parts.append(node)
            return
        if isinstance(node, dict):
            for child in node.values():
                walk(child)
            return
        if isinstance(node, list):
            for child in node:
                walk(child)

    walk(value)
    return normalize_html_text(" ".join(parts))


def normalize_html_text(fragment: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", fragment)
    unescaped = html.unescape(no_tags)
    return re.sub(r"\s+", " ", unescaped).strip()


def parse_json_object_from_text(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    candidate = text.strip()
    if not candidate:
        return None

    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\s*", "", candidate, flags=re.IGNORECASE)
        candidate = re.sub(r"\s*```$", "", candidate, flags=re.IGNORECASE)
        candidate = candidate.strip()

    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    start = candidate.find("{")
    end = candidate.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        parsed = json.loads(candidate[start : end + 1])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def apply_ai_filter(
    jobs: list[JobPosting],
    config: dict[str, Any],
    session: requests.Session,
) -> tuple[list[JobPosting], set[str]]:
    raw_cfg = config.get("ai_filter")
    if not isinstance(raw_cfg, dict):
        return jobs, set()
    if not to_bool(raw_cfg.get("enabled"), False):
        return jobs, set()
    if not jobs:
        return jobs, set()

    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token:
        logging.warning("AI filter enabled but GITHUB_TOKEN is missing. Falling back to hard rules only.")
        return jobs, set()

    endpoint = str(raw_cfg.get("endpoint", GITHUB_MODELS_INFERENCE_URL)).strip() or GITHUB_MODELS_INFERENCE_URL
    model = str(raw_cfg.get("model", "openai/gpt-4o")).strip() or "openai/gpt-4o"
    timeout_seconds = to_optional_int(raw_cfg.get("timeout_seconds"))
    timeout_seconds = timeout_seconds if timeout_seconds and timeout_seconds > 0 else 35
    priority_mode = to_bool(raw_cfg.get("priority_mode"), False)
    max_jobs = to_optional_int(raw_cfg.get("max_jobs_per_cycle"))
    if priority_mode:
        max_jobs = len(jobs)
    else:
        max_jobs = max_jobs if max_jobs and max_jobs > 0 else 20
    max_detail_chars = to_optional_int(raw_cfg.get("max_detail_chars"))
    max_detail_chars = max_detail_chars if max_detail_chars and max_detail_chars > 0 else 1400
    fallback_allow = to_bool(raw_cfg.get("fallback_allow_on_error"), True)
    preferred_min_years = to_optional_int(raw_cfg.get("preferred_min_experience_years"))
    preferred_domains = to_keyword_list(raw_cfg.get("preferred_role_domains"))
    ignore_domains = to_keyword_list(raw_cfg.get("ignore_role_domains"))
    custom_instruction = str(raw_cfg.get("custom_instruction", "")).strip()

    review_jobs = jobs[:max_jobs]
    passthrough_jobs = jobs[max_jobs:]
    if len(jobs) > max_jobs:
        logging.info("AI filter evaluates first %d jobs in this cycle; remaining %d pass through.", max_jobs, len(passthrough_jobs))

    detail_cache: dict[str, str] = {}
    payload_jobs: list[dict[str, str]] = []
    for job in review_jobs:
        details_text = detail_cache.get(job.url)
        if details_text is None:
            try:
                details_resp = session.get(job.url, timeout=30)
                details_resp.raise_for_status()
                details_text = normalize_html_text(details_resp.text)
            except requests.RequestException:
                details_text = ""
            detail_cache[job.url] = details_text

        payload_jobs.append(
            {
                "unique_id": job.unique_id,
                "company": job.company,
                "source": job.source,
                "title": job.title,
                "location": job.location,
                "url": job.url,
                "updated_at": job.updated_at,
                "detail_excerpt": details_text[:max_detail_chars],
            }
        )

    system_prompt = (
        "You are a strict but practical recruiter assistant for Canada software-engineering alerts. "
        "Return ONLY valid JSON using schema: "
        '{"decisions":[{"unique_id":"<id>","allow":true,"reason":"<short reason>"}]}. '
        "Rules: prefer individual contributor software roles in Canada. "
        "Reject clearly non-target roles (manager/director/head, QA/test/SDET-only, principal/staff-level). "
        "Reject hardware-centric roles (ASIC/chip/design verification/firmware/board/electrical/hardware validation). "
        "If years-of-experience requirements appear multiple times, use the smallest minimum year value to judge. "
        "Treat 3+ years and above as acceptable and do NOT reject solely for being senior by years. "
        "If uncertain, set allow=true."
    )
    if preferred_domains:
        system_prompt += " Prefer these role families: " + ", ".join(preferred_domains) + "."
    if ignore_domains:
        system_prompt += " De-prioritize/reject these domains: " + ", ".join(ignore_domains) + "."
    if preferred_min_years is not None and preferred_min_years > 0:
        system_prompt += f" Roles requiring around {preferred_min_years}+ years are explicitly acceptable."
    if custom_instruction:
        system_prompt += " Additional user preference: " + custom_instruction
    user_prompt = (
        "Decide whether each job should be emailed to the user. "
        "Input JSON:\n"
        + json.dumps({"jobs": payload_jobs}, ensure_ascii=False)
    )

    request_payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0,
    }

    try:
        resp = session.post(
            endpoint,
            json=request_payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
            },
            timeout=timeout_seconds,
        )
        resp.raise_for_status()
        body = resp.json()
    except requests.RequestException as exc:
        logging.warning("AI filter request failed: %s", exc)
        if fallback_allow:
            return jobs, set()
        return passthrough_jobs, {j.unique_id for j in review_jobs}

    content = (
        ((body.get("choices") or [{}])[0].get("message") or {}).get("content")
        if isinstance(body, dict)
        else None
    )
    parsed = parse_json_object_from_text(str(content or ""))
    if not parsed:
        logging.warning("AI filter returned non-JSON content. Falling back to hard rules only.")
        if fallback_allow:
            return jobs, set()
        return passthrough_jobs, {j.unique_id for j in review_jobs}

    decisions_raw = parsed.get("decisions")
    if not isinstance(decisions_raw, list):
        logging.warning("AI filter JSON missing decisions array. Falling back to hard rules only.")
        if fallback_allow:
            return jobs, set()
        return passthrough_jobs, {j.unique_id for j in review_jobs}

    decision_map: dict[str, bool] = {}
    for item in decisions_raw:
        if not isinstance(item, dict):
            continue
        unique_id = str(item.get("unique_id", "")).strip()
        if not unique_id:
            continue
        decision_map[unique_id] = to_bool(item.get("allow"), True)

    kept: list[JobPosting] = []
    rejected_ids: set[str] = set()
    for job in review_jobs:
        allow = decision_map.get(job.unique_id, True)
        if allow:
            kept.append(job)
        else:
            rejected_ids.add(job.unique_id)

    logging.info("AI filter kept %d/%d jobs this cycle.", len(kept), len(review_jobs))
    kept.extend(passthrough_jobs)
    return kept, rejected_ids


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


def parse_google_careers_cards(html_text: str, search_url: str) -> list[tuple[str, str, str, str, str]]:
    cards = GOOGLE_CARD_SPLIT_PATTERN.split(html_text)
    parsed: list[tuple[str, str, str, str, str]] = []

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
        parsed.append((job_id, title, location, absolute_url, normalize_html_text(card)))

    if parsed:
        return parsed

    # Fallback for future HTML changes: parse plain-text page dump lines.
    title_plain = re.compile(r"###\s+(.+)")
    location_plain = re.compile(r"Google\s+\|\s+(.+)")
    fallback: list[tuple[str, str, str, str, str]] = []
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
            fallback.append((fallback_id, title, location, search_url, ""))
    return fallback


def fetch_google_careers_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    search_url = build_google_careers_search_url(source)
    resp = session.get(search_url, timeout=30)
    resp.raise_for_status()

    parsed = parse_google_careers_cards(resp.text, search_url)
    jobs: list[JobPosting] = []
    timestamp = utc_now_iso()
    title_keywords = to_keyword_list(source.get("title_keywords"))
    exclude_title_keywords = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))
    for job_id, title, location, job_url, qualifications_text in parsed:
        if not is_canada_location(location):
            continue
        if not title_matches_keywords(title, title_keywords):
            continue
        if title_has_excluded_keywords(title, exclude_title_keywords):
            continue
        if requires_experience_at_or_above(qualifications_text, experience_threshold):
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


def fetch_microsoft_jobs(
    company: str,
    source: dict[str, Any],
    session: requests.Session,
    detail_text_cache: dict[str, str] | None = None,
) -> list[JobPosting]:
    endpoint = str(source.get("endpoint", "https://apply.careers.microsoft.com/api/pcsx/search")).strip()
    domain = str(source.get("domain", "microsoft.com")).strip() or "microsoft.com"
    query = str(source.get("q", source.get("search_text", ""))).strip()
    location = str(source.get("location", "Canada")).strip() or "Canada"
    limit = int(source.get("limit", 20))
    max_pages = int(source.get("max_pages", 5))
    keyword_list = to_keyword_list(source.get("title_keywords"))
    exclude_keyword_list = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))

    jobs: list[JobPosting] = []
    offset = 0
    details_cache = detail_text_cache if detail_text_cache is not None else {}

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
            if title_has_excluded_keywords(title, exclude_keyword_list):
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

            details_text = details_cache.get(job_url)
            if details_text is None:
                try:
                    details_resp = session.get(job_url, timeout=30)
                    details_resp.raise_for_status()
                    details_text = normalize_html_text(details_resp.text)
                except requests.RequestException:
                    details_text = ""
                details_cache[job_url] = details_text

            if requires_experience_at_or_above(details_text, experience_threshold):
                continue

            jobs.append(
                JobPosting(
                    unique_id=(
                        f"microsoft:{position_id}"
                        if domain.lower() == "microsoft.com"
                        else f"pcsx:{domain.lower()}:{position_id}"
                    ),
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


def fetch_workday_jobs(
    company: str,
    source: dict[str, Any],
    session: requests.Session,
    detail_text_cache: dict[str, str] | None = None,
) -> list[JobPosting]:
    endpoint = str(source.get("endpoint", "")).strip()
    if not endpoint:
        raise ValueError(f"Workday source for {company} is missing endpoint.")

    match = WORKDAY_CXS_PATTERN.match(endpoint)
    if not match:
        raise ValueError(f"Invalid Workday CXS endpoint format for {company}: {endpoint}")

    query = str(source.get("q", source.get("search_text", ""))).strip()
    limit = int(source.get("limit", 20))
    max_pages = int(source.get("max_pages", 5))
    keyword_list = to_keyword_list(source.get("title_keywords"))
    exclude_keyword_list = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))
    extra_payload = source.get("payload") if isinstance(source.get("payload"), dict) else {}

    base_for_links = endpoint.split("/wday/cxs/", 1)[0]
    site_name = (match.group("site") or "").strip("/")
    jobs: list[JobPosting] = []
    offset = 0
    details_cache = detail_text_cache if detail_text_cache is not None else {}

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
            external_path = str(item.get("externalPath") or "").strip()
            location = str(item.get("locationsText", "")).strip()
            # Some Workday listings use "2 Locations" and only expose geography in externalPath.
            location_hint = f"{location} {external_path.replace('/', ' ')}"
            if not is_canada_location(location_hint):
                continue
            if not title_matches_keywords(title, keyword_list):
                continue
            if title_has_excluded_keywords(title, exclude_keyword_list):
                continue

            if external_path:
                normalized_path = external_path
                if normalized_path.startswith("/job/") and site_name:
                    normalized_path = f"/{site_name}{normalized_path}"
                job_url = urljoin(base_for_links, normalized_path)
            else:
                job_url = endpoint.rsplit("/wday/cxs/", 1)[0]

            details_text = details_cache.get(job_url)
            if details_text is None:
                try:
                    details_resp = session.get(job_url, timeout=30)
                    details_resp.raise_for_status()
                    details_text = normalize_html_text(details_resp.text)
                except requests.RequestException:
                    details_text = ""
                details_cache[job_url] = details_text

            if requires_experience_min_at_or_above(details_text, experience_threshold):
                continue

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


def fetch_jibe_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    endpoint = str(source.get("endpoint", "")).strip()
    if not endpoint:
        raise ValueError(f"Jibe source for {company} is missing endpoint.")

    query = str(source.get("q", source.get("search_text", ""))).strip()
    country = str(source.get("country", source.get("location", "Canada"))).strip() or "Canada"
    limit = int(source.get("limit", 20))
    max_pages = int(source.get("max_pages", 5))
    keyword_list = to_keyword_list(source.get("title_keywords"))
    exclude_keyword_list = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))
    extra_params = source.get("params") if isinstance(source.get("params"), dict) else {}

    parsed_endpoint = urlparse(endpoint)
    endpoint_host = parsed_endpoint.netloc or "jibe"
    jobs: list[JobPosting] = []

    for page in range(1, max_pages + 1):
        params: dict[str, Any] = {"page": page, "limit": limit, "country": country}
        if query:
            params["keywords"] = query
        params.update(extra_params)

        resp = session.get(endpoint, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        postings = payload.get("jobs") or []
        if not postings:
            break

        for item in postings:
            data = item.get("data") if isinstance(item, dict) else {}
            if not isinstance(data, dict):
                continue

            title = str(data.get("title", "Untitled")).strip()
            location = str(data.get("full_location") or data.get("location_name") or data.get("short_location") or "").strip()
            country_text = str(data.get("country", "")).strip()
            if not is_canada_location(location) and country_text.lower() != "canada":
                continue
            if not title_matches_keywords(title, keyword_list):
                continue
            if title_has_excluded_keywords(title, exclude_keyword_list):
                continue

            details_text = flatten_text(
                [
                    data.get("description"),
                    data.get("responsibilities"),
                    data.get("qualifications"),
                    data.get("summary"),
                    data.get("meta_data"),
                ]
            )
            if requires_experience_at_or_above(details_text, experience_threshold):
                continue

            slug = str(data.get("slug") or data.get("req_id") or "").strip()
            if not slug:
                unique_seed = f"{company}|{title}|{location}|{data.get('apply_url', '')}"
                slug = hashlib.sha1(unique_seed.encode("utf-8")).hexdigest()[:16]

            meta_data = data.get("meta_data") if isinstance(data.get("meta_data"), dict) else {}
            canonical_url = str(meta_data.get("canonical_url", "")).strip()
            apply_url = str(data.get("apply_url", "")).strip()
            job_url = canonical_url or apply_url or endpoint

            posted_raw = str(data.get("posted_date") or data.get("update_date") or data.get("create_date") or "").strip()
            posted_dt = parse_datetime_iso(posted_raw)
            updated_at = (
                posted_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat() if posted_dt else utc_now_iso()
            )

            jobs.append(
                JobPosting(
                    unique_id=f"jibe:{endpoint_host}:{slug}",
                    source="jibe",
                    company=company,
                    title=title,
                    location=location or country_text or "Unknown",
                    url=job_url,
                    updated_at=updated_at,
                )
            )

        if len(postings) < limit:
            break

    return jobs


def fetch_uber_careers_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    careers_url = str(source.get("careers_url", "https://www.uber.com/us/en/careers/list/")).strip()
    filter_endpoint = str(source.get("filter_endpoint", "https://www.uber.com/api/loadFilterOptions")).strip()
    search_endpoint = str(source.get("endpoint", "https://www.uber.com/api/loadSearchJobsResults")).strip()
    locale_code = str(source.get("locale_code", "en")).strip() or "en"
    query = str(source.get("q", source.get("search_text", ""))).strip()
    limit = int(source.get("limit", 20))
    max_pages = int(source.get("max_pages", 5))
    keyword_list = to_keyword_list(source.get("title_keywords"))
    exclude_keyword_list = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))
    location_cities = {str(x).strip().lower() for x in (source.get("location_cities") or []) if str(x).strip()}
    if not careers_url:
        raise ValueError(f"Uber source for {company} is missing careers_url.")
    if not filter_endpoint or not search_endpoint:
        raise ValueError(f"Uber source for {company} is missing endpoint settings.")

    session.get(careers_url, timeout=30)
    base_url = urlparse(careers_url)
    origin = f"{base_url.scheme}://{base_url.netloc}" if base_url.scheme and base_url.netloc else "https://www.uber.com"
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "Content-Type": "application/json",
        "x-csrf-token": "x",
        "Origin": origin,
        "Referer": careers_url,
    }
    filter_url = f"{filter_endpoint}?localeCode={locale_code}"
    search_url = f"{search_endpoint}?localeCode={locale_code}"
    filter_resp = session.post(filter_url, json={}, headers=headers, timeout=30)
    filter_resp.raise_for_status()
    filter_payload = filter_resp.json()
    if str(filter_payload.get("status", "")).lower() != "success":
        raise ValueError(f"Uber filter endpoint returned failure status for {company}.")

    all_locations = ((filter_payload.get("data") or {}).get("location")) or []
    canada_locations: list[dict[str, Any]] = []
    for item in all_locations:
        if not isinstance(item, dict):
            continue
        country_code = str(item.get("country", "")).strip().upper()
        country_name = str(item.get("countryName", "")).strip().lower()
        city = str(item.get("city", "")).strip().lower()
        if country_code != "CAN" and "canada" not in country_name:
            continue
        if location_cities and city not in location_cities:
            continue
        canada_locations.append(item)
    if not canada_locations:
        raise ValueError(f"Uber source for {company} has no Canada locations in filter options.")

    jobs: list[JobPosting] = []
    for page in range(max_pages):
        params_payload: dict[str, Any] = {"location": canada_locations}
        if query:
            params_payload["query"] = query

        body = {"limit": limit, "page": page, "params": params_payload}
        resp = session.post(search_url, json=body, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        if str(payload.get("status", "")).lower() != "success":
            data = payload.get("data") or {}
            message = str(data.get("message", "")).strip()
            logging.warning("Uber search returned failure status for %s (page=%d): %s", company, page, message or "unknown")
            break

        data = payload.get("data") or {}
        results = data.get("results") or []
        if not results:
            break

        total_raw = data.get("totalResults")
        total_low = None
        if isinstance(total_raw, dict):
            total_low = to_optional_int(total_raw.get("low"))
        elif isinstance(total_raw, (int, float, str)):
            total_low = to_optional_int(total_raw)

        for item in results:
            title = str(item.get("title", "Untitled")).strip()
            if not title_matches_keywords(title, keyword_list):
                continue
            if title_has_excluded_keywords(title, exclude_keyword_list):
                continue

            all_locs = item.get("allLocations")
            location_parts: list[str] = []
            if isinstance(all_locs, list) and all_locs:
                for loc_item in all_locs:
                    formatted = format_uber_location_item(loc_item)
                    if formatted:
                        location_parts.append(formatted)
            else:
                formatted = format_uber_location_item(item.get("location"))
                if formatted:
                    location_parts.append(formatted)
            location_text = "; ".join(location_parts).strip() or "Unknown"
            if not is_canada_location(location_text):
                continue

            details_text = normalize_html_text(str(item.get("description", "")))
            if requires_experience_at_or_above(details_text, experience_threshold):
                continue

            job_id_raw = item.get("id")
            job_id = str(job_id_raw).strip() if job_id_raw is not None else ""
            if not job_id:
                unique_seed = f"{company}|{title}|{location_text}|{details_text[:120]}"
                job_id = hashlib.sha1(unique_seed.encode("utf-8")).hexdigest()[:16]
            job_url = f"{origin}/us/en/careers/list/{job_id}"
            updated_raw = str(item.get("creationDate") or item.get("updatedDate") or "").strip()

            jobs.append(
                JobPosting(
                    unique_id=f"uber:{job_id}",
                    source="uber_careers",
                    company=company,
                    title=title,
                    location=location_text,
                    url=job_url,
                    updated_at=parse_datetime_to_utc_iso(updated_raw),
                )
            )

        if len(results) < limit:
            break
        if total_low is not None and (page + 1) * limit >= total_low:
            break

    return jobs


def extract_intuit_jobposting_data(page_text: str) -> tuple[str, str]:
    for match in INTUIT_JOBPOSTING_JSONLD_PATTERN.finditer(page_text):
        payload_text = html.unescape(match.group("payload") or "").strip()
        if not payload_text:
            continue

        parsed_payload: Any
        try:
            parsed_payload = json.loads(payload_text)
        except json.JSONDecodeError:
            parsed_payload = parse_json_object_from_text(payload_text)

        candidates: list[dict[str, Any]] = []
        if isinstance(parsed_payload, dict):
            candidates = [parsed_payload]
        elif isinstance(parsed_payload, list):
            candidates = [item for item in parsed_payload if isinstance(item, dict)]

        for item in candidates:
            if str(item.get("@type", "")).strip().lower() != "jobposting":
                continue
            date_posted = str(item.get("datePosted", "")).strip()
            description = flatten_text(item.get("description"))
            if date_posted or description:
                return date_posted, description

    return "", ""


def fetch_intuit_careers_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    endpoint = str(source.get("endpoint", INTUIT_SEARCH_DEFAULT_URL)).strip() or INTUIT_SEARCH_DEFAULT_URL
    query = str(source.get("q", source.get("search_text", ""))).strip()
    max_pages = int(source.get("max_pages", 6))
    title_keywords = to_keyword_list(source.get("title_keywords"))
    exclude_title_keywords = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))
    request_headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"}
    if max_pages < 1:
        max_pages = 1

    jobs: list[JobPosting] = []
    seen_ids: set[str] = set()
    total_pages = max_pages
    parsed_endpoint = urlparse(endpoint)

    for page in range(1, max_pages + 1):
        if parsed_endpoint.query:
            page_url = f"{endpoint}&p={page}"
        else:
            page_url = f"{endpoint}?p={page}"
        if query:
            page_url += "&k=" + requests.utils.quote(query)

        resp = session.get(page_url, headers=request_headers, timeout=30)
        resp.raise_for_status()
        page_text = resp.text

        if page == 1:
            total_match = INTUIT_TOTAL_PAGES_PATTERN.search(page_text)
            if total_match:
                parsed_total = to_optional_int(total_match.group("total_pages"))
                if parsed_total and parsed_total > 0:
                    total_pages = min(max_pages, parsed_total)

        matches = list(INTUIT_JOB_CARD_PATTERN.finditer(page_text))
        if not matches:
            break

        new_on_page = 0
        for match in matches:
            href = html.unescape((match.group("href") or "").strip())
            title = normalize_html_text(html.unescape(match.group("title") or "Untitled"))
            location = normalize_html_text(html.unescape(match.group("location") or "Unknown"))
            if not href or not title:
                continue
            if not is_canada_location(location):
                continue
            if not title_matches_keywords(title, title_keywords):
                continue
            if title_has_excluded_keywords(title, exclude_title_keywords):
                continue

            absolute_url = urljoin("https://jobs.intuit.com", href)
            job_id_match = re.search(r"/(\d+)$", absolute_url)
            if job_id_match:
                job_id = job_id_match.group(1)
            else:
                unique_seed = f"{company}|{title}|{location}|{absolute_url}"
                job_id = hashlib.sha1(unique_seed.encode("utf-8")).hexdigest()[:16]

            if job_id in seen_ids:
                continue

            details_text = ""
            date_posted_raw = ""
            try:
                details_resp = session.get(absolute_url, headers=request_headers, timeout=30)
                details_resp.raise_for_status()
                details_page = details_resp.text
                date_posted_raw, details_text = extract_intuit_jobposting_data(details_page)
                if not details_text:
                    details_text = normalize_html_text(details_page)
            except requests.RequestException:
                details_text = ""

            if requires_experience_at_or_above(details_text, experience_threshold):
                continue

            seen_ids.add(job_id)
            new_on_page += 1
            jobs.append(
                JobPosting(
                    unique_id=f"intuit:{job_id}",
                    source="intuit_careers",
                    company=company,
                    title=title,
                    location=location,
                    url=absolute_url,
                    updated_at=parse_intuit_date_to_utc_iso(date_posted_raw),
                )
            )

        if page >= total_pages:
            break
        if new_on_page == 0:
            break

    return jobs


def fetch_yelp_careers_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    endpoint = str(source.get("endpoint", "https://www.yelp.careers/us/en/search-results")).strip()
    start_step = int(source.get("start_step", 10))
    max_pages = int(source.get("max_pages", 8))
    title_keywords = to_keyword_list(source.get("title_keywords"))
    exclude_title_keywords = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))
    if not endpoint:
        raise ValueError(f"Yelp source for {company} is missing endpoint.")
    if start_step < 1:
        start_step = 10
    if max_pages < 1:
        max_pages = 1

    seen_ids: set[str] = set()
    jobs: list[JobPosting] = []
    for page_index in range(max_pages):
        offset = page_index * start_step
        if offset == 0:
            page_url = endpoint
        else:
            separator = "&" if "?" in endpoint else "?"
            page_url = f"{endpoint}{separator}from={offset}&s=1"

        resp = session.get(page_url, timeout=30)
        resp.raise_for_status()
        page_text = resp.text
        matches = list(YELP_CAREERS_JOB_PATTERN.finditer(page_text))
        if not matches:
            if page_index > 0:
                break
            continue

        new_on_page = 0
        for match in matches:
            req_id = decode_json_escaped_text(match.group("req_id")).strip()
            if not req_id or req_id in seen_ids:
                continue
            seen_ids.add(req_id)
            new_on_page += 1

            title = decode_json_escaped_text(match.group("title")).strip()
            if not title_matches_keywords(title, title_keywords):
                continue
            if title_has_excluded_keywords(title, exclude_title_keywords):
                continue

            location = decode_json_escaped_text(match.group("location")).strip()
            if not is_canada_location(location):
                continue
            if requires_experience_at_or_above(title, experience_threshold):
                continue

            posted_date = decode_json_escaped_text(match.group("posted_date")).strip()
            created_date = decode_json_escaped_text(match.group("created_date")).strip()
            apply_url = decode_json_escaped_text(match.group("apply_url")).strip()
            updated_raw = posted_date or created_date

            jobs.append(
                JobPosting(
                    unique_id=f"yelp:{req_id}",
                    source="yelp_careers",
                    company=company,
                    title=title,
                    location=location or "Unknown",
                    url=apply_url or endpoint,
                    updated_at=parse_datetime_to_utc_iso(updated_raw),
                )
            )

        if new_on_page == 0:
            break
        if len(matches) < start_step:
            break

    return jobs


def fetch_amazon_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    endpoint = str(source.get("endpoint", "https://www.amazon.jobs/en/search.json")).strip()
    query = str(source.get("q", source.get("search_text", ""))).strip()
    loc_query = str(source.get("loc_query", source.get("location", "Canada"))).strip() or "Canada"
    country = str(source.get("country", "CAN")).strip() or "CAN"
    limit = int(source.get("limit", 20))
    max_pages = int(source.get("max_pages", 5))
    keyword_list = to_keyword_list(source.get("title_keywords"))
    exclude_keyword_list = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))
    extra_params = source.get("params") if isinstance(source.get("params"), dict) else {}
    if not endpoint:
        raise ValueError(f"Amazon source for {company} is missing endpoint.")
    if limit < 1:
        limit = 20
    if max_pages < 1:
        max_pages = 1

    headers = {"Accept-Encoding": "identity", "User-Agent": "Mozilla/5.0"}
    jobs: list[JobPosting] = []
    for page in range(max_pages):
        offset = page * limit
        params: dict[str, Any] = {"offset": offset, "result_limit": limit, "loc_query": loc_query, "country": country}
        if query:
            params["base_query"] = query
        params.update(extra_params)

        resp = session.get(endpoint, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("jobs") or []
        if not items:
            break

        total_hits = to_optional_int(payload.get("hits"))
        for item in items:
            title = str(item.get("title", "Untitled")).strip()
            if not title_matches_keywords(title, keyword_list):
                continue
            if title_has_excluded_keywords(title, exclude_keyword_list):
                continue

            location_parts = [
                str(item.get("location", "")).strip(),
                str(item.get("city", "")).strip(),
                str(item.get("state", "")).strip(),
                str(item.get("country_code", "")).strip(),
            ]
            location_text = ", ".join(part for part in location_parts if part)
            country_code = str(item.get("country_code", "")).strip().upper()
            if not is_canada_location(location_text) and country_code != "CAN":
                continue

            details_text = flatten_text(
                [
                    item.get("description"),
                    item.get("description_short"),
                    item.get("basic_qualifications"),
                    item.get("preferred_qualifications"),
                ]
            )
            if requires_experience_at_or_above(details_text, experience_threshold):
                continue

            job_id = str(item.get("id_icims") or item.get("id") or "").strip()
            if not job_id:
                unique_seed = f"{company}|{title}|{location_text}|{item.get('job_path')}"
                job_id = hashlib.sha1(unique_seed.encode("utf-8")).hexdigest()[:16]

            job_path = str(item.get("job_path", "")).strip()
            fallback_url = str(item.get("url_next_step", "")).strip()
            job_url = urljoin("https://www.amazon.jobs", job_path) if job_path else fallback_url or endpoint

            updated_at = parse_amazon_posted_date_to_utc_iso(str(item.get("posted_date", "")).strip())
            jobs.append(
                JobPosting(
                    unique_id=f"amazon:{job_id}",
                    source="amazon_jobs",
                    company=company,
                    title=title,
                    location=location_text or "Unknown",
                    url=job_url,
                    updated_at=updated_at,
                )
            )

        if len(items) < limit:
            break
        if total_hits is not None and (offset + len(items)) >= total_hits:
            break

    return jobs


def fetch_ashby_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    board = str(source.get("board", "")).strip()
    endpoint = str(source.get("endpoint", f"https://jobs.ashbyhq.com/{board}" if board else "")).strip()
    if not endpoint:
        raise ValueError(f"Ashby source for {company} is missing endpoint or board.")
    board_name = board or endpoint.rstrip("/").split("/")[-1]
    title_keywords = to_keyword_list(source.get("title_keywords"))
    exclude_title_keywords = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))

    resp = session.get(endpoint, timeout=30)
    resp.raise_for_status()
    postings = extract_json_array_from_html(resp.text, "jobPostings")
    if not postings:
        return []

    jobs: list[JobPosting] = []
    for item in postings:
        if not bool(item.get("isListed", True)):
            continue
        title = str(item.get("title", "Untitled")).strip()
        if not title_matches_keywords(title, title_keywords):
            continue
        if title_has_excluded_keywords(title, exclude_title_keywords):
            continue

        locations: list[str] = []
        primary_location = str(item.get("locationName", "")).strip()
        if primary_location:
            locations.append(primary_location)
        secondary_locations = item.get("secondaryLocations")
        if isinstance(secondary_locations, list):
            for secondary in secondary_locations:
                if not isinstance(secondary, dict):
                    continue
                secondary_name = str(
                    secondary.get("locationExternalName") or secondary.get("locationName") or secondary.get("name") or ""
                ).strip()
                if secondary_name:
                    locations.append(secondary_name)
        location_text = "; ".join(dict.fromkeys(locations))
        if not is_canada_location(location_text):
            continue

        details_text = flatten_text(
            [
                title,
                item.get("teamName"),
                item.get("departmentName"),
                item.get("compensationTierSummary"),
            ]
        )
        if requires_experience_at_or_above(details_text, experience_threshold):
            continue

        posting_id = str(item.get("id") or item.get("jobId") or "").strip()
        if not posting_id:
            unique_seed = f"{company}|{title}|{location_text}"
            posting_id = hashlib.sha1(unique_seed.encode("utf-8")).hexdigest()[:16]
        job_url = f"https://jobs.ashbyhq.com/{board_name}/{posting_id}"
        published_raw = str(item.get("publishedDate") or item.get("updatedAt") or "").strip()
        updated_at = parse_datetime_to_utc_iso(published_raw)
        jobs.append(
            JobPosting(
                unique_id=f"ashby:{board_name}:{posting_id}",
                source="ashby",
                company=company,
                title=title,
                location=location_text or "Unknown",
                url=job_url,
                updated_at=updated_at,
            )
        )
    return jobs


def fetch_ibm_careers_jobs(company: str, source: dict[str, Any], session: requests.Session) -> list[JobPosting]:
    endpoint = str(
        source.get(
            "endpoint",
            "https://www-api.ibm.com/search/api/v1-1/ibmcom/appid/careers/responseFormat/json",
        )
    ).strip()
    if not endpoint:
        raise ValueError(f"IBM source for {company} is missing endpoint.")
    scope = str(source.get("scope", "careers2")).strip() or "careers2"
    app_id = str(source.get("app_id", "careers")).strip() or "careers"
    query = str(source.get("q", source.get("search_text", ""))).strip()
    limit = int(source.get("limit", 20))
    max_pages = int(source.get("max_pages", 5))
    sort_by = str(source.get("sort_by", "dcdate")).strip() or "dcdate"
    title_keywords = to_keyword_list(source.get("title_keywords"))
    exclude_title_keywords = to_keyword_list(source.get("exclude_title_keywords"))
    experience_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))
    if limit < 1:
        limit = 20
    if max_pages < 1:
        max_pages = 1

    jobs: list[JobPosting] = []
    for page_index in range(max_pages):
        offset = page_index * limit
        params = {
            "scope": scope,
            "appid": app_id,
            "rmdt": "ALL",
            "sortby": sort_by,
            "query": query,
            "fr": str(offset),
            "nr": str(limit),
            "page": str(page_index + 1),
        }
        resp = session.get(
            endpoint,
            params=params,
            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        search_results = ((payload.get("resultset") or {}).get("searchresults") or {})
        items = search_results.get("searchresultlist") or []
        if not isinstance(items, list) or not items:
            break

        total_results = to_optional_int(search_results.get("totalresults"))
        for item in items:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "Untitled")).strip()
            if not title_matches_keywords(title, title_keywords):
                continue
            if title_has_excluded_keywords(title, exclude_title_keywords):
                continue

            attrs = docattributes_to_dict(item.get("docattributes"))
            location = (
                str(attrs.get("field_keyword_19", "")).strip()
                or str(attrs.get("city", "")).strip()
                or str(attrs.get("location", "")).strip()
            )
            country_code = str(attrs.get("country", "")).strip().upper()
            location_hint = f"{location} {country_code}".strip()
            if not is_canada_location(location_hint) and country_code not in {"CA", "CAN"}:
                continue

            details_text = flatten_text([item.get("description"), item.get("summary"), attrs])
            if requires_experience_at_or_above(details_text, experience_threshold):
                continue

            job_url = str(item.get("url", "")).strip() or "https://www.ibm.com/careers/search"
            match = IBM_JOB_ID_PATTERN.search(job_url)
            job_id = match.group(1) if match else str(item.get("id", "")).strip()
            if not job_id:
                unique_seed = f"{company}|{title}|{location}|{job_url}"
                job_id = hashlib.sha1(unique_seed.encode("utf-8")).hexdigest()[:16]
            updated_raw = str(attrs.get("dcdate") or attrs.get("effectivedate") or "").strip()
            updated_at = parse_datetime_to_utc_iso(updated_raw) if updated_raw else utc_now_iso()
            jobs.append(
                JobPosting(
                    unique_id=f"ibm:{job_id}",
                    source="ibm_careers_api",
                    company=company,
                    title=title,
                    location=location or "Canada",
                    url=job_url,
                    updated_at=updated_at,
                )
            )

        if len(items) < limit:
            break
        if total_results is not None and offset + len(items) >= total_results:
            break
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
            "ai_rejected_job_ids": [],
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
            "ai_rejected_job_ids": [],
            "last_check_at": None,
        }

    data.setdefault("initialized", False)
    data.setdefault("seen_job_ids", [])
    data.setdefault("pending_notifications", [])
    data.setdefault("ai_rejected_job_ids", [])
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
    exclude_title_keywords: list[str] | None = None,
    experience_threshold: int | None = None,
) -> list[JobPosting]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    jobs: list[JobPosting] = []
    keyword_list = to_keyword_list(title_keywords)
    excluded_keyword_list = to_keyword_list(exclude_title_keywords)
    for item in payload.get("jobs", []):
        location = (item.get("location") or {}).get("name", "").strip()
        if not is_canada_location(location):
            continue
        title = str(item.get("title", "Untitled")).strip()
        if not title_matches_keywords(title, keyword_list):
            continue
        if title_has_excluded_keywords(title, excluded_keyword_list):
            continue
        if requires_experience_at_or_above(normalize_html_text(str(item.get("content") or "")), experience_threshold):
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
    exclude_title_keywords: list[str] | None = None,
    experience_threshold: int | None = None,
) -> list[JobPosting]:
    url = f"https://api.lever.co/v0/postings/{handle}?mode=json"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    jobs: list[JobPosting] = []
    keyword_list = to_keyword_list(title_keywords)
    excluded_keyword_list = to_keyword_list(exclude_title_keywords)
    for item in payload:
        categories = item.get("categories") or {}
        location = str(categories.get("location", "")).strip()
        if not is_canada_location(location):
            continue
        title = str(item.get("text", "Untitled")).strip()
        if not title_matches_keywords(title, keyword_list):
            continue
        if title_has_excluded_keywords(title, excluded_keyword_list):
            continue
        lever_details_text = flatten_text(
            [
                item.get("description"),
                item.get("descriptionPlain"),
                item.get("descriptionBody"),
                item.get("descriptionBodyPlain"),
                item.get("additional"),
                item.get("additionalPlain"),
                item.get("opening"),
                item.get("openingPlain"),
                item.get("lists"),
            ]
        )
        if requires_experience_at_or_above(lever_details_text, experience_threshold):
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
    global_excluded_title_keywords = to_keyword_list(config.get("exclude_title_keywords"))
    global_experience_threshold = to_optional_int(config.get("exclude_required_experience_years_at_or_above"))
    detail_text_cache: dict[str, str] = {}

    def effective_excluded_keywords(source: dict[str, Any]) -> list[str]:
        source_excluded_keywords = to_keyword_list(source.get("exclude_title_keywords"))
        return merge_keywords(global_excluded_title_keywords, source_excluded_keywords)

    def effective_experience_threshold(source: dict[str, Any]) -> int | None:
        source_threshold = to_optional_int(source.get("exclude_required_experience_years_at_or_above"))
        return global_experience_threshold if source_threshold is None else source_threshold

    def fetch_by_source(
        source_name: str,
        company: str,
        identifier: str,
        title_keywords: list[str] | None = None,
        exclude_title_keywords: list[str] | None = None,
        experience_threshold: int | None = None,
        source_config: dict[str, Any] | None = None,
    ) -> None:
        normalized_key = (source_name, identifier.lower())
        if normalized_key in processed_sources:
            return
        processed_sources.add(normalized_key)
        sleep_with_jitter(request_delay, request_jitter)
        source = source_config if isinstance(source_config, dict) else {}

        try:
            if source_name == "greenhouse":
                jobs = fetch_greenhouse_jobs(
                    company,
                    identifier,
                    session,
                    title_keywords=title_keywords,
                    exclude_title_keywords=exclude_title_keywords,
                    experience_threshold=experience_threshold,
                )
                all_jobs.extend(jobs)
                logging.info("Greenhouse %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "lever":
                jobs = fetch_lever_jobs(
                    company,
                    identifier,
                    session,
                    title_keywords=title_keywords,
                    exclude_title_keywords=exclude_title_keywords,
                    experience_threshold=experience_threshold,
                )
                all_jobs.extend(jobs)
                logging.info("Lever      %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "google_careers":
                jobs = fetch_google_careers_jobs(
                    company,
                    {
                        "url": identifier,
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("Google     %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "workday_cxs":
                jobs = fetch_workday_jobs(
                    company,
                    {
                        "endpoint": identifier,
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                    detail_text_cache=detail_text_cache,
                )
                all_jobs.extend(jobs)
                logging.info("Workday    %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "microsoft_careers":
                jobs = fetch_microsoft_jobs(
                    company,
                    {
                        "endpoint": identifier,
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                    detail_text_cache=detail_text_cache,
                )
                all_jobs.extend(jobs)
                logging.info("Microsoft  %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "uber_careers":
                jobs = fetch_uber_careers_jobs(
                    company,
                    {
                        "careers_url": identifier,
                        "filter_endpoint": source.get("filter_endpoint", "https://www.uber.com/api/loadFilterOptions"),
                        "endpoint": source.get("endpoint", "https://www.uber.com/api/loadSearchJobsResults"),
                        "locale_code": source.get("locale_code", "en"),
                        "q": source.get("q", source.get("search_text", "")),
                        "limit": source.get("limit", 20),
                        "max_pages": source.get("max_pages", 5),
                        "location_cities": source.get("location_cities", []),
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("Uber       %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "jibe":
                jobs = fetch_jibe_jobs(
                    company,
                    {
                        "endpoint": identifier,
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("Jibe       %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "yelp_careers":
                jobs = fetch_yelp_careers_jobs(
                    company,
                    {
                        "endpoint": identifier,
                        "start_step": source.get("start_step", 10),
                        "max_pages": source.get("max_pages", 8),
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("Yelp       %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "amazon_jobs":
                jobs = fetch_amazon_jobs(
                    company,
                    {
                        "endpoint": identifier,
                        "q": source.get("q", source.get("search_text", "")),
                        "loc_query": source.get("loc_query", source.get("location", "Canada")),
                        "country": source.get("country", "CAN"),
                        "limit": source.get("limit", 20),
                        "max_pages": source.get("max_pages", 5),
                        "params": source.get("params", {}),
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("Amazon     %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "ashby":
                jobs = fetch_ashby_jobs(
                    company,
                    {
                        "board": source.get("board", ""),
                        "endpoint": identifier,
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("Ashby      %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "ibm_careers_api":
                jobs = fetch_ibm_careers_jobs(
                    company,
                    {
                        "endpoint": identifier,
                        "scope": source.get("scope", "careers2"),
                        "app_id": source.get("app_id", "careers"),
                        "q": source.get("q", source.get("search_text", "")),
                        "limit": source.get("limit", 20),
                        "max_pages": source.get("max_pages", 5),
                        "sort_by": source.get("sort_by", "dcdate"),
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("IBM API    %-20s -> %d Canada jobs", company, len(jobs))
                return
            if source_name == "intuit_careers":
                jobs = fetch_intuit_careers_jobs(
                    company,
                    {
                        "endpoint": identifier,
                        "q": source.get("q", source.get("search_text", "")),
                        "max_pages": source.get("max_pages", 6),
                        "title_keywords": title_keywords or [],
                        "exclude_title_keywords": exclude_title_keywords or [],
                        "exclude_required_experience_years_at_or_above": experience_threshold,
                    },
                    session,
                )
                all_jobs.extend(jobs)
                logging.info("Intuit     %-20s -> %d Canada jobs", company, len(jobs))
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
        title_keywords = to_keyword_list(source.get("title_keywords"))
        exclude_title_keywords = effective_excluded_keywords(source)
        experience_threshold = effective_experience_threshold(source)
        if not company or not token:
            continue
        fetch_by_source(
            "greenhouse",
            company,
            token,
            title_keywords=title_keywords,
            exclude_title_keywords=exclude_title_keywords,
            experience_threshold=experience_threshold,
        )

    for source in sources.get("lever", []):
        company = str(source.get("company", "")).strip()
        handle = str(source.get("handle", "")).strip()
        title_keywords = to_keyword_list(source.get("title_keywords"))
        exclude_title_keywords = effective_excluded_keywords(source)
        experience_threshold = effective_experience_threshold(source)
        if not company or not handle:
            continue
        fetch_by_source(
            "lever",
            company,
            handle,
            title_keywords=title_keywords,
            exclude_title_keywords=exclude_title_keywords,
            experience_threshold=experience_threshold,
        )

    for source in sources.get("google_careers", []):
        company = str(source.get("company", "Google")).strip() or "Google"
        title_keywords = to_keyword_list(source.get("title_keywords"))
        source_payload = dict(source)
        source_payload["exclude_title_keywords"] = effective_excluded_keywords(source)
        source_payload["exclude_required_experience_years_at_or_above"] = effective_experience_threshold(source)
        sleep_with_jitter(request_delay, request_jitter)
        try:
            jobs = fetch_google_careers_jobs(company, source_payload, session)
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
        source_payload = dict(source)
        source_payload["exclude_title_keywords"] = effective_excluded_keywords(source)
        source_payload["exclude_required_experience_years_at_or_above"] = effective_experience_threshold(source)
        sleep_with_jitter(request_delay, request_jitter)
        try:
            jobs = fetch_microsoft_jobs(company, source_payload, session, detail_text_cache=detail_text_cache)
            all_jobs.extend(jobs)
            logging.info("Microsoft  %-20s -> %d Canada jobs", company, len(jobs))
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "unknown"
            logging.warning("Microsoft fetch failed for %s: HTTP %s", company, code)
        except requests.RequestException as exc:
            logging.warning("Microsoft fetch failed for %s: %s", company, exc)

    for source in sources.get("uber_careers", []):
        company = str(source.get("company", "Uber")).strip() or "Uber"
        careers_url = str(source.get("careers_url", "https://www.uber.com/us/en/careers/list/")).strip()
        title_keywords = to_keyword_list(source.get("title_keywords"))
        exclude_title_keywords = effective_excluded_keywords(source)
        experience_threshold = effective_experience_threshold(source)
        if not careers_url:
            continue
        fetch_by_source(
            "uber_careers",
            company,
            careers_url,
            title_keywords=title_keywords,
            exclude_title_keywords=exclude_title_keywords,
            experience_threshold=experience_threshold,
            source_config=source,
        )

    for source in sources.get("workday_cxs", []):
        company = str(source.get("company", "Workday")).strip() or "Workday"
        source_payload = dict(source)
        source_payload["exclude_title_keywords"] = effective_excluded_keywords(source)
        source_payload["exclude_required_experience_years_at_or_above"] = effective_experience_threshold(source)
        sleep_with_jitter(request_delay, request_jitter)
        try:
            jobs = fetch_workday_jobs(company, source_payload, session, detail_text_cache=detail_text_cache)
            all_jobs.extend(jobs)
            logging.info("Workday    %-20s -> %d Canada jobs", company, len(jobs))
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "unknown"
            logging.warning("Workday fetch failed for %s: HTTP %s", company, code)
        except requests.RequestException as exc:
            logging.warning("Workday fetch failed for %s: %s", company, exc)
        except ValueError as exc:
            logging.warning("Workday source config error for %s: %s", company, exc)

    for source in sources.get("jibe", []):
        company = str(source.get("company", "Jibe")).strip() or "Jibe"
        source_payload = dict(source)
        source_payload["exclude_title_keywords"] = effective_excluded_keywords(source)
        source_payload["exclude_required_experience_years_at_or_above"] = effective_experience_threshold(source)
        sleep_with_jitter(request_delay, request_jitter)
        try:
            jobs = fetch_jibe_jobs(company, source_payload, session)
            all_jobs.extend(jobs)
            logging.info("Jibe       %-20s -> %d Canada jobs", company, len(jobs))
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "unknown"
            logging.warning("Jibe fetch failed for %s: HTTP %s", company, code)
        except requests.RequestException as exc:
            logging.warning("Jibe fetch failed for %s: %s", company, exc)
        except ValueError as exc:
            logging.warning("Jibe source config error for %s: %s", company, exc)

    for source in sources.get("yelp_careers", []):
        company = str(source.get("company", "Yelp")).strip() or "Yelp"
        endpoint = str(source.get("endpoint", "https://www.yelp.careers/us/en/search-results")).strip()
        title_keywords = to_keyword_list(source.get("title_keywords"))
        exclude_title_keywords = effective_excluded_keywords(source)
        experience_threshold = effective_experience_threshold(source)
        if not endpoint:
            continue
        fetch_by_source(
            "yelp_careers",
            company,
            endpoint,
            title_keywords=title_keywords,
            exclude_title_keywords=exclude_title_keywords,
            experience_threshold=experience_threshold,
            source_config=source,
        )

    for source in sources.get("amazon_jobs", []):
        company = str(source.get("company", "Amazon")).strip() or "Amazon"
        endpoint = str(source.get("endpoint", "https://www.amazon.jobs/en/search.json")).strip()
        title_keywords = to_keyword_list(source.get("title_keywords"))
        exclude_title_keywords = effective_excluded_keywords(source)
        experience_threshold = effective_experience_threshold(source)
        if not endpoint:
            continue
        fetch_by_source(
            "amazon_jobs",
            company,
            endpoint,
            title_keywords=title_keywords,
            exclude_title_keywords=exclude_title_keywords,
            experience_threshold=experience_threshold,
            source_config=source,
        )

    for source in sources.get("ashby", []):
        company = str(source.get("company", "Ashby")).strip() or "Ashby"
        endpoint = str(source.get("endpoint", "")).strip()
        board = str(source.get("board", "")).strip()
        if not endpoint and board:
            endpoint = f"https://jobs.ashbyhq.com/{board}"
        title_keywords = to_keyword_list(source.get("title_keywords"))
        exclude_title_keywords = effective_excluded_keywords(source)
        experience_threshold = effective_experience_threshold(source)
        if not endpoint:
            continue
        fetch_by_source(
            "ashby",
            company,
            endpoint,
            title_keywords=title_keywords,
            exclude_title_keywords=exclude_title_keywords,
            experience_threshold=experience_threshold,
            source_config=source,
        )

    for source in sources.get("ibm_careers_api", []):
        company = str(source.get("company", "IBM")).strip() or "IBM"
        endpoint = str(
            source.get(
                "endpoint",
                "https://www-api.ibm.com/search/api/v1-1/ibmcom/appid/careers/responseFormat/json",
            )
        ).strip()
        title_keywords = to_keyword_list(source.get("title_keywords"))
        exclude_title_keywords = effective_excluded_keywords(source)
        experience_threshold = effective_experience_threshold(source)
        if not endpoint:
            continue
        fetch_by_source(
            "ibm_careers_api",
            company,
            endpoint,
            title_keywords=title_keywords,
            exclude_title_keywords=exclude_title_keywords,
            experience_threshold=experience_threshold,
            source_config=source,
        )

    for source in sources.get("intuit_careers", []):
        company = str(source.get("company", "Intuit")).strip() or "Intuit"
        endpoint = str(source.get("endpoint", INTUIT_SEARCH_DEFAULT_URL)).strip() or INTUIT_SEARCH_DEFAULT_URL
        title_keywords = to_keyword_list(source.get("title_keywords"))
        exclude_title_keywords = effective_excluded_keywords(source)
        experience_threshold = effective_experience_threshold(source)
        fetch_by_source(
            "intuit_careers",
            company,
            endpoint,
            title_keywords=title_keywords,
            exclude_title_keywords=exclude_title_keywords,
            experience_threshold=experience_threshold,
            source_config=source,
        )

    for source in sources.get("career_pages", []):
        company = str(source.get("company", "")).strip()
        url = str(source.get("url", "")).strip()
        title_keywords = to_keyword_list(source.get("title_keywords"))
        exclude_title_keywords = effective_excluded_keywords(source)
        experience_threshold = effective_experience_threshold(source)
        if not url:
            continue
        parsed = parse_source_from_career_page(url)
        if parsed is None:
            logging.warning("Unsupported career page URL for %s: %s", company or "unknown", url)
            continue
        source_name, identifier = parsed
        effective_company = company or identifier
        fetch_by_source(
            source_name,
            effective_company,
            identifier,
            title_keywords=title_keywords,
            exclude_title_keywords=exclude_title_keywords,
            experience_threshold=experience_threshold,
        )

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

    if "yelp.careers" in host and "/search-results" in parsed.path:
        return ("yelp_careers", url)

    if "uber.com" in host and "/careers/list" in parsed.path:
        return ("uber_careers", url)

    if "amazon.jobs" in host and "/search" in parsed.path:
        return ("amazon_jobs", "https://www.amazon.jobs/en/search.json")

    if "ashbyhq.com" in host and path_parts:
        return ("ashby", f"https://jobs.ashbyhq.com/{path_parts[0]}")

    if "ibm.com" in host and "/careers/search" in parsed.path:
        return ("ibm_careers_api", "https://www-api.ibm.com/search/api/v1-1/ibmcom/appid/careers/responseFormat/json")

    if "jobs.intuit.com" in host and "/search-jobs" in parsed.path:
        return ("intuit_careers", "https://jobs.intuit.com/search-jobs?acm=68357&l=Canada&orgIds=27595")

    return None


def format_posted_time_with_age(updated_at: str, reference_time: datetime | None = None) -> str:
    posted_at = parse_datetime_iso(updated_at)
    if posted_at is None:
        return f"未知 | {updated_at}"

    now = reference_time or datetime.now(timezone.utc)
    delta_seconds = int((now - posted_at).total_seconds())
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
    relative = "".join(parts) + "前"

    exact_time = posted_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"{relative} | {exact_time}"


def split_jobs_by_post_age(
    jobs: list[JobPosting],
    max_age_days: int | None,
    reference_time: datetime | None = None,
    treat_unknown_as_stale: bool = True,
) -> tuple[list[JobPosting], list[JobPosting]]:
    if max_age_days is None or max_age_days <= 0:
        return jobs, []

    now = reference_time or datetime.now(timezone.utc)
    max_age_seconds = max_age_days * 24 * 60 * 60
    kept: list[JobPosting] = []
    stale: list[JobPosting] = []

    for job in jobs:
        posted_at = parse_datetime_iso(job.updated_at)
        if posted_at is None:
            if treat_unknown_as_stale:
                stale.append(job)
            else:
                kept.append(job)
            continue
        age_seconds = int((now - posted_at).total_seconds())
        if age_seconds < 0:
            age_seconds = 0
        if age_seconds <= max_age_seconds:
            kept.append(job)
        else:
            stale.append(job)
    return kept, stale


def sort_jobs_by_updated_desc(jobs: list[JobPosting]) -> list[JobPosting]:
    def sort_key(job: JobPosting) -> tuple[int, float, str, str, str]:
        posted_at = parse_datetime_iso(job.updated_at)
        if posted_at is None:
            return (1, 0.0, job.company.lower(), job.title.lower(), job.unique_id)
        return (0, -posted_at.timestamp(), job.company.lower(), job.title.lower(), job.unique_id)

    return sorted(jobs, key=sort_key)


def render_email_body(jobs: list[JobPosting]) -> str:
    lines = [
        "发现新的加拿大科技岗位：",
        "",
    ]
    now = datetime.now(timezone.utc)
    sorted_jobs = sort_jobs_by_updated_desc(jobs)

    for idx, job in enumerate(sorted_jobs, start=1):
        lines.extend(
            [
                f"{idx}. {job.company} | {job.title}",
                f"   地点: {job.location}",
                f"   来源: {job.source}",
                f"   发布时间: {format_posted_time_with_age(job.updated_at, now)}",
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
    ai_cfg = config.get("ai_filter") if isinstance(config.get("ai_filter"), dict) else {}
    ai_filter_enabled = to_bool(ai_cfg.get("enabled"), False)
    max_post_age_days = to_optional_int(config.get("max_post_age_days_for_email"))
    if max_post_age_days is None:
        max_post_age_days = 2
    seen_ids = set(str(x) for x in state.get("seen_job_ids", []))
    ai_rejected_ids = set(str(x) for x in state.get("ai_rejected_job_ids", []))
    pending_map = index_pending_jobs(state.get("pending_notifications", []))
    email_already_attempted = False

    if max_post_age_days > 0 and pending_map:
        kept_pending, stale_pending = split_jobs_by_post_age(
            list(pending_map.values()),
            max_post_age_days,
            reference_time=datetime.now(timezone.utc),
            treat_unknown_as_stale=True,
        )
        if stale_pending:
            pending_map = {job.unique_id: job for job in kept_pending}
            seen_ids.update(job.unique_id for job in stale_pending)
            logging.info(
                "Age filter removed %d pending jobs older than %d days.",
                len(stale_pending),
                max_post_age_days,
            )

    if ai_filter_enabled and jobs:
        jobs, newly_rejected = apply_ai_filter(jobs, config, session)
        ai_rejected_ids.difference_update(job.unique_id for job in jobs)
        ai_rejected_ids.update(newly_rejected)
        for rejected_id in newly_rejected:
            pending_map.pop(rejected_id, None)
        for rejected_id in ai_rejected_ids:
            pending_map.pop(rejected_id, None)

    if max_post_age_days > 0 and jobs:
        recent_jobs, stale_jobs = split_jobs_by_post_age(
            jobs,
            max_post_age_days,
            reference_time=datetime.now(timezone.utc),
            treat_unknown_as_stale=True,
        )
        if stale_jobs:
            seen_ids.update(job.unique_id for job in stale_jobs)
            for stale_job in stale_jobs:
                pending_map.pop(stale_job.unique_id, None)
            logging.info(
                "Age filter kept %d/%d jobs within %d days.",
                len(recent_jobs),
                len(jobs),
                max_post_age_days,
            )
        jobs = recent_jobs

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
            if job.unique_id in seen_ids or job.unique_id in pending_map or job.unique_id in ai_rejected_ids:
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
    state["ai_rejected_job_ids"] = sorted(ai_rejected_ids)
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
