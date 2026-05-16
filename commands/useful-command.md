# Useful Commands

Quick command list for local development.

## Saved commands

- `lsof -nP -iTCP:5173 -sTCP:LISTEN` — Shows which process is listening on frontend port `5173` on macOS.
- `kill -9 $(lsof -tiTCP:5173 -sTCP:LISTEN)` — Hard-kills the process currently listening on frontend port `5173`.
- `chmod +x orders.sh` — Adds execute permission to `orders.sh` (fixes "permission denied" when trying to run it).