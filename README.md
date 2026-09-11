# Option Money Leaders AVWAP — Railway + Neon

Separate project for all NSE stock-futures underlyings. It examines nearest-expiry
ATM ±3 CE/PE contracts, takes quote baselines at 09:15 IST, and freezes two
leaders per stock at 09:20: maximum traded-money and maximum positive fresh-OI
money. A contract winning both is tagged `BOTH`.

Money values:

```text
traded_money_cr = max(volume_0920 - volume_0915, 0) × premium_0920 × lot_size / 1 crore
fresh_oi_money_cr = max(oi_0920 - oi_0915, 0) × premium_0920 × lot_size / 1 crore
```

The collector downloads completed 3-minute candles for frozen contracts,
calculates continuing AVWAP High/Low from 09:15, freezes the separate completed
09:15–10:15 hourly high/low, and records subsequent AVWAP crossings. All display
times use `Asia/Kolkata`; Neon timestamps remain timezone-aware.

## Railway services

Collector start command:

```text
python option_avwap_collector.py
```

Dashboard start command:

```text
streamlit run streamlit_app.py --server.port $PORT --server.address 0.0.0.0
```

Collector variables: `NEON_DATABASE_URL`, `UPSTOX_ACCESS_TOKEN`, optional
`OPTION_STRIKES_EACH_SIDE=3`, and `LOG_LEVEL=INFO`. Dashboard needs only
`NEON_DATABASE_URL`.

No cron is required. Keep the collector deployed continuously; it wakes at
09:10 IST and manages the session itself. Update the Upstox token when it expires.

Tables: `option_avwap_universe`, `option_avwap_3m`, and
`option_avwap_heartbeat`.

Important: the collector must be running at 09:15 because historical candles do
not reconstruct the exact 09:15 quote-level OI and cumulative-volume baseline.
