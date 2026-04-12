#!/usr/bin/env python3
"""
Padel Occupancy Dashboard
=========================
Reads occupancy_summary.csv and writes occupancy_dashboard.html.
Run this script any time to regenerate the dashboard with the latest data.

    python occupancy_dashboard.py
"""

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

CSV_PATH = Path(__file__).parent.parent / "occupancy_summary.csv"
HTML_PATH = Path(__file__).parent.parent / "occupancy_dashboard.html"

VENUES = ["Terwegen", "Vinkenveld"]

PERIODS = {
    "Morning":   ( 8, 12),   # 08:00 – 12:00
    "Afternoon": (12, 17),   # 12:00 – 17:00
    "Evening":   (17, 23),   # 17:00 – 23:00
}

VENUE_COLORS = {
    "Terwegen":   {"bg": "rgba(59,130,246,0.75)",  "border": "rgba(59,130,246,1)"},
    "Vinkenveld": {"bg": "rgba(16,185,129,0.75)",  "border": "rgba(16,185,129,1)"},
}


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def slot_hour(slot: str) -> float:
    h, m = map(int, slot.split(":"))
    return h + m / 60


def load_csv() -> list[dict]:
    with CSV_PATH.open(newline="") as f:
        return list(csv.DictReader(f))


def period_averages(rows: list[dict]) -> dict:
    """
    Returns {venue: {period: avg_pct}} where avg_pct is the mean occupancy
    percentage across all observations that fall within that period's hours.
    """
    buckets: dict[str, dict[str, list[float]]] = {
        v: {p: [] for p in PERIODS} for v in VENUES
    }
    for row in rows:
        venue = row["venue"]
        if venue not in VENUES:
            continue
        hour = slot_hour(row["time_slot"])
        pct  = float(row["occupancy_pct"])
        for period, (start, end) in PERIODS.items():
            if start <= hour < end:
                buckets[venue][period].append(pct)
                break

    return {
        venue: {
            period: (round(sum(vals) / len(vals), 1) if vals else None)
            for period, vals in periods.items()
        }
        for venue, periods in buckets.items()
    }


def slot_averages(rows: list[dict]) -> dict:
    """
    Returns {venue: {slot_label: avg_pct}} for the per-slot line chart,
    restricted to slots within operating hours (08:00 onwards; upper bound varies
    per venue and day — Terwegen closes at 20:00 on Saturday and 19:00 on Sunday).
    """
    buckets: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        venue = row["venue"]
        if venue not in VENUES:
            continue
        slot = row["time_slot"]
        if not (8 <= slot_hour(slot) < 23):
            continue
        buckets[venue][slot].append(float(row["occupancy_pct"]))

    result = {}
    for venue in VENUES:
        slots = sorted(buckets[venue], key=slot_hour)
        result[venue] = {s: round(sum(v) / len(v), 1) for s, v in buckets[venue].items()}
    return result


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def render(averages: dict, slot_avgs: dict, n_rows: int) -> str:
    period_labels = list(PERIODS.keys())
    generated_at  = datetime.now().strftime("%d %b %Y, %H:%M")

    # --- Bar chart datasets ---
    bar_datasets = []
    for venue in VENUES:
        c = VENUE_COLORS[venue]
        data = [averages[venue][p] for p in period_labels]
        bar_datasets.append({
            "label":           venue,
            "data":            data,
            "backgroundColor": c["bg"],
            "borderColor":     c["border"],
            "borderWidth":     2,
            "borderRadius":    6,
        })

    # --- Line chart: all slots in order ---
    all_slots = sorted(
        set(s for v in slot_avgs.values() for s in v),
        key=slot_hour,
    )
    line_datasets = []
    for venue in VENUES:
        c = VENUE_COLORS[venue]
        line_datasets.append({
            "label":       venue,
            "data":        [slot_avgs[venue].get(s) for s in all_slots],
            "borderColor": c["border"],
            "backgroundColor": c["bg"],
            "tension":     0.35,
            "pointRadius": 4,
            "fill":        False,
            "spanGaps":    True,
        })

    # --- Summary cards ---
    def overall(venue: str) -> str:
        vals = [v for v in averages[venue].values() if v is not None]
        return f"{round(sum(vals)/len(vals), 1)}%" if vals else "—"

    cards_html = ""
    for venue in VENUES:
        c = VENUE_COLORS[venue]
        border_color = c["border"]
        cards_html += f"""
        <div class="card" style="border-top: 4px solid {border_color};">
          <div class="card-venue">{venue}</div>
          <div class="card-avg">{overall(venue)}</div>
          <div class="card-label">overall avg occupancy</div>
          <div class="card-periods">
            {"".join(
                f'<div class="period-row"><span class="period-name">{p}</span>'
                f'<span class="period-val">'
                f'{f"{averages[venue][p]}%" if averages[venue][p] is not None else "—"}'
                f'</span></div>'
                for p in period_labels
            )}
          </div>
        </div>
        """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Padel Occupancy Dashboard</title>
  <meta http-equiv="refresh" content="1800">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f0f4f8;
      color: #1e293b;
      padding: 2rem 1.5rem;
    }}
    header {{
      display: flex;
      align-items: baseline;
      gap: 1rem;
      margin-bottom: 2rem;
    }}
    header h1 {{ font-size: 1.6rem; font-weight: 700; }}
    header .meta {{ font-size: 0.8rem; color: #64748b; }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1.25rem;
      margin-bottom: 2.5rem;
    }}
    .card {{
      background: #fff;
      border-radius: 12px;
      padding: 1.25rem 1.5rem;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
    }}
    .card-venue {{ font-size: 0.75rem; font-weight: 600; text-transform: uppercase;
                   letter-spacing: .06em; color: #64748b; margin-bottom: .25rem; }}
    .card-avg   {{ font-size: 2.4rem; font-weight: 700; line-height: 1; margin-bottom: .2rem; }}
    .card-label {{ font-size: 0.72rem; color: #94a3b8; margin-bottom: 1rem; }}
    .period-row {{ display: flex; justify-content: space-between;
                   padding: .3rem 0; border-top: 1px solid #f1f5f9;
                   font-size: .85rem; }}
    .period-name {{ color: #475569; }}
    .period-val  {{ font-weight: 600; }}
    .charts {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
      gap: 1.5rem;
    }}
    .chart-box {{
      background: #fff;
      border-radius: 12px;
      padding: 1.5rem;
      box-shadow: 0 1px 4px rgba(0,0,0,.08);
    }}
    .chart-box h2 {{ font-size: .95rem; font-weight: 600; margin-bottom: 1.25rem;
                     color: #334155; }}
    .footer {{
      text-align: center;
      margin-top: 2.5rem;
      font-size: .75rem;
      color: #94a3b8;
    }}
  </style>
</head>
<body>

<header>
  <h1>Padel Occupancy Dashboard</h1>
  <span class="meta">Generated {generated_at} &nbsp;·&nbsp; {n_rows} observations</span>
</header>

<div class="cards">
  {cards_html}
</div>

<div class="charts">
  <div class="chart-box">
    <h2>Average occupancy by time of day</h2>
    <canvas id="barChart"></canvas>
  </div>
  <div class="chart-box">
    <h2>Occupancy by 30-minute slot</h2>
    <canvas id="lineChart"></canvas>
  </div>
</div>

<div class="footer">
  Occupancy = courts booked ÷ total courts &nbsp;·&nbsp;
  Morning 08–12 &nbsp;·&nbsp; Afternoon 12–17 &nbsp;·&nbsp; Evening 17–23
</div>

<script>
const barData = {{
  labels: {json.dumps(period_labels)},
  datasets: {json.dumps(bar_datasets)},
}};

const lineData = {{
  labels: {json.dumps(all_slots)},
  datasets: {json.dumps(line_datasets)},
}};

const commonScales = {{
  y: {{
    min: 0, max: 100,
    ticks: {{ callback: v => v + "%" }},
    grid: {{ color: "#f1f5f9" }},
  }},
  x: {{ grid: {{ display: false }} }},
}};

new Chart(document.getElementById("barChart"), {{
  type: "bar",
  data: barData,
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ position: "top" }},
      tooltip: {{ callbacks: {{ label: ctx => ` ${{ctx.dataset.label}}: ${{ctx.parsed.y ?? "—"}}%` }} }},
    }},
    scales: commonScales,
  }},
}});

new Chart(document.getElementById("lineChart"), {{
  type: "line",
  data: lineData,
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ position: "top" }},
      tooltip: {{ callbacks: {{ label: ctx => ` ${{ctx.dataset.label}}: ${{ctx.parsed.y ?? "—"}}%` }} }},
    }},
    scales: commonScales,
  }},
}});
</script>

</body>
</html>
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    rows      = load_csv()
    averages  = period_averages(rows)
    slot_avgs = slot_averages(rows)
    html      = render(averages, slot_avgs, len(rows))
    HTML_PATH.write_text(html, encoding="utf-8")
    print(f"Dashboard written to {HTML_PATH.resolve()}")
    print()
    for venue in VENUES:
        print(f"  {venue}")
        for period, pct in averages[venue].items():
            val = f"{pct}%" if pct is not None else "no data"
            print(f"    {period:<12} {val}")
