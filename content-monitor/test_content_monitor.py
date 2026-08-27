import argparse
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import content_monitor


class ContentMonitorTests(unittest.TestCase):
    def test_email_failure_preserves_pending_updates_and_fails_cycle(self):
        update = content_monitor.ContentUpdate(
            unique_id="rss:test:1",
            source="rss",
            publisher="Example",
            title="New post",
            url="https://example.com/post",
            published_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            summary="Summary",
        )
        args = argparse.Namespace(send_initial_snapshot=False, dry_run=False)
        config = {
            "recipient_email": "alerts@example.com",
            "max_post_age_days_for_email": 2,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "initialized": True,
                        "seen_update_ids": [],
                        "pending_notifications": [],
                    }
                ),
                encoding="utf-8",
            )
            with (
                patch.object(content_monitor, "collect_updates", return_value=[update]),
                patch.dict("os.environ", {"SMTP_USER": "sender@example.com", "SMTP_PASSWORD": "bad"}),
                patch.object(content_monitor, "send_email", side_effect=RuntimeError("SMTP rejected")),
            ):
                with self.assertRaisesRegex(RuntimeError, "pending content notifications were preserved"):
                    content_monitor.run_once(args, config, state_path, object())

            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual("rss:test:1", saved["pending_notifications"][0]["unique_id"])

    def test_once_mode_propagates_cycle_failure(self):
        args = argparse.Namespace(
            config="config.json",
            state="state.json",
            once=True,
            send_initial_snapshot=False,
            dry_run=False,
            log_level="INFO",
        )
        with (
            patch.object(content_monitor, "parse_args", return_value=args),
            patch.object(content_monitor, "read_json_file", return_value={"poll_interval_minutes": 60}),
            patch.object(content_monitor, "run_once", side_effect=RuntimeError("delivery failed")),
        ):
            with self.assertRaisesRegex(RuntimeError, "delivery failed"):
                content_monitor.main()


if __name__ == "__main__":
    unittest.main()
