# Energy Trading — Workshop Pack

A self-contained, offline reference pack for exploring the `energy_trading`
dataset. Open **`index.html`** in any browser to start — everything links from there.

> All figures in the interactive pages are **sample values shaped from the source
> notebook's scenarios**. Nothing here connects to Databricks. This is a learning
> and reference aid, not a live report.

## What's inside

**Interactive (open in a browser)**
- `pages/model.html` — Data model explorer: search all 10 tables, their columns, grain and the 13 relationships.
- `pages/dashboard.html` — Activity dashboard: filter by commodity / season / direction and watch KPIs and charts update.
- `pages/genie.html` — Genie question bank: 12 ready-to-paste questions in a demo arc (warm-up → surveillance finish).

**Static reference (print / slides)** — in `diagrams/`, each as PNG and SVG
1. ER diagram — the full star schema
2. Common join paths
3. KPI map — grouped by business question
4. Whose data is this? (market-wide, not one company)

**Printable** — `EnergyTrading_Workshop_Pack.pdf` (title page + all five diagrams).

## Key framing for participants
- This is a **market-wide trade tape** — every participant, every deal — the view an exchange or regulator has, **not one company's books**.
- **Money totals mix EUR/GBP/USD.** State a single currency before trusting any notional/revenue figure.
- **Cancelled trades** inflate volumes unless excluded.
- `fact_market_price` has **no instrument key** — one price per hub, not per product.

## Folder layout
```
index.html                      landing page (start here)
README.md
EnergyTrading_Workshop_Pack.pdf printable pack
pages/   model.html  dashboard.html  genie.html  shared.css
diagrams/  01..04  (.png and .svg)
```

No build step, no server, no internet needed . Chart.js is bundled locally in `pages/vendor/`, so no internet is required at all.
copy of Chart.js next to `dashboard.html`.
