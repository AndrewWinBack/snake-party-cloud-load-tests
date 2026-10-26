# Cloud-only WebSocket load validation

Four independent standard GitHub-hosted Linux runners simulate equal parts of one 800-player game. Each player has a separate identity, WebSocket, game state and score verification. All players join before a common 60-second or 180-second round starts. One in five players deliberately disconnects for five seconds and recovers authoritative state.

This repository contains only the test harness and workflow. Test-room data and temporary credentials are stored only in private repository Actions secrets. They are compressed and encrypted before upload; the cloud job reconstructs them into a temporary runtime file. No fixture bundle is committed. No application source, production password, or player data is published.

Only manual dispatch is enabled. No self-hosted runners, local load, Grafana Cloud, paid runners, or automatic reruns. Reports are retained for one day. Tests target an isolated staging deployment, not active event rooms. Independent source addresses are verified by the application server.

Snapshot cadence checks cover live host/display updates; protocol 3 intentionally omits unchanged player snapshots. Original ACK, readiness, scoring, recovery and error SLAs are retained. Each node reports its actual metrics; percentile values are not averaged.
