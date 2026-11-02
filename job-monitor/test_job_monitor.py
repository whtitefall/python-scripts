import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import job_monitor


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.payload)


class MonitorTimingTests(unittest.TestCase):
    def test_continuous_mode_counts_fetch_time_toward_poll_interval(self):
        args = SimpleNamespace(
            config="companies.json", state="state.json", once=False, send_initial_snapshot=False
        )
        with (
            patch.object(job_monitor, "parse_args", return_value=args),
            patch.object(job_monitor, "read_json_file", return_value={"poll_interval_minutes": 5}),
            patch.object(job_monitor, "load_state", return_value={}),
            patch.object(job_monitor.requests, "Session"),
            patch.object(job_monitor, "run_check_cycle"),
            patch.object(job_monitor.time, "monotonic", side_effect=[100.0, 280.0]),
            patch.object(job_monitor.time, "sleep", side_effect=KeyboardInterrupt) as sleep,
        ):
            with self.assertRaises(KeyboardInterrupt):
                job_monitor.main()
        sleep.assert_called_once_with(120.0)

    def test_seven_hour_gap_is_reported_and_persisted(self):
        state = {
            "initialized": True,
            "last_check_started_at": (datetime.now(timezone.utc) - timedelta(hours=7)).isoformat(),
        }
        config = {"recipient_email": "alerts@example.com", "poll_interval_minutes": 5}
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            with (
                patch.object(job_monitor, "collect_jobs", return_value=[]),
                self.assertLogs(level="WARNING") as logs,
            ):
                job_monitor.run_check_cycle(config, state, state_path, FakeSession({}), False)
            saved = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertTrue(any("Monitoring delayed" in line for line in logs.output))
        self.assertAlmostEqual(7 * 60 * 60, saved["last_check_gap_seconds"], delta=2)
        self.assertIn("last_check_started_at", saved)
        self.assertGreaterEqual(saved["last_check_duration_seconds"], 0)


class UberMonitorTests(unittest.TestCase):
    def test_intern_titles_are_excluded_without_matching_internationalization(self):
        keywords = ["intern", "internship", "co-op", "co op"]

        self.assertTrue(job_monitor.title_has_excluded_keywords("Software Engineer Intern", keywords))
        self.assertTrue(job_monitor.title_has_excluded_keywords("Software Engineering Internship", keywords))
        self.assertTrue(job_monitor.title_has_excluded_keywords("Software Developer Co-op", keywords))
        self.assertFalse(
            job_monitor.title_has_excluded_keywords("Software Engineer, Internationalization", keywords)
        )

    def test_existing_pending_intern_job_is_removed_before_email(self):
        intern_job = job_monitor.JobPosting(
            unique_id="test:intern-1",
            source="test",
            company="Example",
            title="Software Developer Intern",
            location="Toronto, Canada",
            url="https://example.com/jobs/intern-1",
            updated_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        )
        state = {
            "initialized": True,
            "seen_job_ids": [],
            "pending_notifications": [intern_job.__dict__],
            "ai_rejected_job_ids": [],
        }
        config = {
            "recipient_email": "alerts@example.com",
            "max_post_age_days_for_email": 2,
            "exclude_title_keywords": ["intern", "internship", "co-op", "co op"],
            "sources": {},
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            with (
                patch.object(job_monitor, "collect_jobs", return_value=[]),
                patch.object(job_monitor, "send_email") as send_email,
            ):
                job_monitor.run_check_cycle(config, state, state_path, FakeSession({}), False)

            saved = json.loads(state_path.read_text(encoding="utf-8"))
            send_email.assert_not_called()
            self.assertEqual([], saved["pending_notifications"])
            self.assertIn("test:intern-1", saved["seen_job_ids"])

    def test_new_uber_api_and_minimum_experience_are_supported(self):
        payload = {
            "jobs": [
                {
                    "Id": "12345",
                    "Reference": "12345",
                    "Title": "Software Engineer II",
                    "DisplayDate": "2026-08-27T02:00:00Z",
                    "Description": (
                        "Basic Qualifications: 3+ years of software engineering experience. "
                        "Preferred Qualifications: 5+ years of experience."
                    ),
                    "Locations": [
                        {"City": "Toronto", "Region": "Ontario", "Country": "Canada"}
                    ],
                    "Urls": [{"Url": "/en/jobs/12345/", "IsDefault": True}],
                },
                {
                    "Id": "99999",
                    "Title": "Staff Software Engineer",
                    "DisplayDate": "2026-08-27T01:00:00Z",
                    "Description": "3+ years of experience.",
                    "Locations": [
                        {"City": "Toronto", "Region": "Ontario", "Country": "Canada"}
                    ],
                    "Urls": [{"Url": "/en/jobs/99999/", "IsDefault": True}],
                },
            ],
            "totalPages": 1,
            "totalJobs": 2,
            "page": 1,
            "pageSize": 10,
        }
        session = FakeSession(payload)
        source = {
            "endpoint": job_monitor.UBER_JOBS_SEARCH_DEFAULT_URL,
            "base_url": job_monitor.UBER_JOBS_BASE_URL,
            "country": "Canada",
            "limit": 10,
            "max_pages": 5,
            "location_cities": ["Toronto", "Montreal"],
            "title_keywords": ["software engineer"],
            "exclude_title_keywords": ["staff"],
            "exclude_required_experience_years_at_or_above": 5,
        }

        jobs = job_monitor.fetch_uber_careers_jobs("Uber", source, session)

        self.assertEqual(1, len(jobs))
        self.assertEqual("uber:12345", jobs[0].unique_id)
        self.assertEqual("https://jobs.uber.com/en/jobs/12345/", jobs[0].url)
        self.assertEqual("Toronto, Ontario, Canada", jobs[0].location)
        self.assertEqual("Canada", session.calls[0][1]["params"]["countries"])

    def test_email_failure_preserves_pending_jobs_and_fails_cycle(self):
        job = job_monitor.JobPosting(
            unique_id="test:1",
            source="test",
            company="Example",
            title="Software Engineer",
            location="Toronto, Canada",
            url="https://example.com/jobs/1",
            updated_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        )
        state = {
            "initialized": True,
            "seen_job_ids": [],
            "pending_notifications": [],
            "ai_rejected_job_ids": [],
        }
        config = {
            "recipient_email": "alerts@example.com",
            "max_post_age_days_for_email": 2,
            "sources": {},
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            with (
                patch.object(job_monitor, "collect_jobs", return_value=[job]),
                patch.object(job_monitor, "send_email", return_value=False),
            ):
                with self.assertRaisesRegex(RuntimeError, "pending job notifications were preserved"):
                    job_monitor.run_check_cycle(config, state, state_path, FakeSession({}), False)

            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual("test:1", saved["pending_notifications"][0]["unique_id"])


if __name__ == "__main__":
    unittest.main()
