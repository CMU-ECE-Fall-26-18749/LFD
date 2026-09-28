# 18-749 local fault detector

Run one LFD on each replica's physical machine. It heartbeats the local replica,
registers with the GFD, answers the GFD's heartbeats, and reports membership changes.

Keep `LFD`, `server`, and `client` as sibling folders. From their parent:

```text
python server/demo.py setup
python server/demo.py lfd1 --gfd-host COORDINATOR_IP --replica-host THIS_LAPTOP_IP
```

Use `lfd2` or `lfd3` for the other replicas. The local server connection uses
localhost, but the advertised URL must use an IP reachable from the client laptop.
`--heartbeat 1` means a one-second interval; `--timeout 3` means a three-second timeout.

The original `lfd.py` flags remain available. Omit `--gfd_url` for Milestone 1 local
monitoring. See the sibling server README for startup order, full commands, tests,
design, and rollback. Do not restart a crashed replica during Milestone 2 traffic:
its new boot ID is excluded until a full clean demo restart.
