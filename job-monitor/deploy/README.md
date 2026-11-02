# Continuous cloud monitoring

Run the monitor as a systemd service on an existing Ubuntu server. This avoids
depending on GitHub's delayed schedule events. The target is one check starting
every five minutes, including fetch time; slow source responses can extend a cycle.

This file is a deployment recipe. Committing the service does not install it.

## Files and identity

- Create a dedicated system user and group named `job-monitor` without a login shell.
- Install `job_monitor.py`, `companies.json` and `requirements.txt` from the same
  commit in `/opt/canada-job-monitor`, owned by root.
- Create `/opt/canada-job-monitor/.venv` and install `requirements.txt` there.
- Configure `SMTP_USER` and `SMTP_PASSWORD` in `/etc/canada-job-monitor.env`,
  owned by root with mode `0600`. Never commit this file or print its contents.
- Copy `canada-job-monitor.service` to `/etc/systemd/system/`.

## Preserve existing notification state

1. Finish or cancel any currently running job alert Action before migrating.
2. Transfer the latest cached `.job_monitor_state.json` to
   `/var/lib/canada-job-monitor/state.json`, owned by `job-monitor:job-monitor`
   with mode `0600`. Do not start with an empty state: it resets deduplication.
3. Set repository variable `JOB_MONITOR_BACKEND=ec2` to prevent the GitHub job
   from sending alerts while the service runs. The content monitor is separate.
4. Run `sudo systemctl daemon-reload` and
   `sudo systemctl enable --now canada-job-monitor.service`.
5. Check at least two consecutive cycles with
   `sudo journalctl -u canada-job-monitor.service --since '20 minutes ago'`.
   Confirm both the actual interval and SMTP delivery before completing migration.

The service starts on reboot and restarts after process exits. systemd creates
the state directory; create it before step 2 when importing existing state.

## Rollback

Stop the service before re-enabling GitHub. Transfer the current server state back
to the Actions cache so already sent jobs are not sent again, then unset
`JOB_MONITOR_BACKEND`. Do not run independent copies with separate state files.
