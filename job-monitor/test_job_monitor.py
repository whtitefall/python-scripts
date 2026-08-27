import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
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


class UberMonitorTests(unittest.TestCase):
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
