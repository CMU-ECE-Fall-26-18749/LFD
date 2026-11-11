# Milestone 4 Local Fault Detector

Each replica laptop runs one LFD beside its replica:

- LFD1 monitors S1.
- LFD2 monitors S2.
- LFD3 monitors S3.

Leave the LFD running when you manually stop and relaunch its replica.
It reports the new boot ID to the GFD and logs the restart. This reports
that the process is alive; checkpoint recovery determines when it is
ready to serve requests.

Use the launcher in the sibling server folder. The complete machine assignments,
startup order, and terminal commands are in [`../server/README.md`](../server/README.md).
