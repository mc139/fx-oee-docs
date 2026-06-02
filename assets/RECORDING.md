# Recording the README hero GIF

The Phase 17 todo calls for a GIF of the live order book updating in the browser.

## Steps

1. Start the stack: `docker compose up --build`
2. Open http://localhost:8080
3. Use a screen recorder (macOS: Cmd+Shift+5 → Record Selected Portion, or [Peek](https://github.com/phw/peek) on Linux)
4. Place a few limit orders on EUR/USD so bids/asks update; wait for a cross if possible
5. Export as GIF (Peek) or convert: `ffmpeg -i demo.mov -vf "fps=10,scale=800:-1" -loop 0 orderbook-demo.gif`
6. Save as `docs/assets/orderbook-demo.gif`
7. Uncomment the hero image line in root `README.md`

Target: ~800px wide, 10 fps, 5–10 seconds, under 5 MB.
