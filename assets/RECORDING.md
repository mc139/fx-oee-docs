# README hero GIF

`orderbook-demo.gif` in this directory is a **placeholder** (1×1 px) until a real capture is recorded.

## Record a replacement

1. Start the stack: `docker-compose up` (or `mvn spring-boot:run` + `cd frontend && npm run dev`).
2. Open the trading UI at `http://localhost` (or `http://localhost:5173` in dev).
3. Log in, select **EUR/USD**, and place a few limit orders on both sides so the book updates visibly.
4. Use macOS **Screenshot → Record Selected Portion** (or OBS / ScreenToGif) on the order-book panel for ~5–10 s.
5. Export as GIF (≤ ~2 MB) and overwrite `docs/assets/orderbook-demo.gif`.
6. Confirm `README.md` hero image renders: `![Live order book](docs/assets/orderbook-demo.gif)`.

## Tips

- Hide browser chrome; use a fixed viewport width (~1200 px) for consistent README layout.
- Dark theme matches the default terminal UI.
- Trim the clip so the first frame already shows a populated book.
