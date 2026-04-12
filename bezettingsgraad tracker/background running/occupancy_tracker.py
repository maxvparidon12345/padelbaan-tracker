#!/usr/bin/env python3
"""
Padel Court Occupancy Tracker
==============================
Estimates padel court occupancy at Terwegen and Vinkenveld by checking
booking availability every 15 minutes and storing observations in SQLite.

Terwegen   — RacketIQ platform: direct API call, no browser needed.
Vinkenveld — MeetAndPlay Livewire: Playwright DOM scraping.

Usage:
    python occupancy_tracker.py            # Run continuously every 15 minutes
    python occupancy_tracker.py --once     # Single scrape then exit
    python occupancy_tracker.py --debug    # Verbose logging + Vinkenveld screenshot
    python occupancy_tracker.py --once --debug
"""

import asyncio
import csv
import json
import logging
import sqlite3
import sys
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
from pathlib import Path

from playwright.async_api import async_playwright, Page

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TERWEGEN_API = (
    "https://racketiq.meetandplay.nl/web/api/group/2013/v2/bookings/checkcart"
    "?from={from_time}&to={to_time}&camera=false&favourite=false&availability=1&date={date}"
)
VINKENVELD_URL = "https://meetandplay.nl/club/88165?sport=padel"

VENUES = {
    "Terwegen": {
        "total_courts": 8,
        "type": "racketiq",
        "slot_step_minutes": 60,
        # close_hours: index 0=Mon … 4=Fri, 5=Sat, 6=Sun
        "close_hours": [23, 23, 23, 23, 23, 20, 19],
        "known_courts": [
            "Padelbaan 1 (Panorama court)", "Padelbaan 2", "Padelbaan 3",
            "Padelbaan 4", "Padelbaan 5", "Padelbaan 6", "Padelbaan 7", "Padelbaan 8",
        ],
    },
    "Vinkenveld": {
        "total_courts": 4,
        "type": "livewire",
        "slot_step_minutes": 30,
        "close_hours": [23, 23, 23, 23, 23, 23, 23],
        "known_courts": [
            "Padelbaan 1 / PURE Tennis & Padel", "Padelbaan 2", "Padelbaan 3", "Padelbaan 4",
        ],
    },
}

def venue_close_hour(venue: str, dt: Optional[datetime] = None) -> int:
    """Return the closing hour for *venue* on the given day (default: today)."""
    if dt is None:
        dt = _now()
    return VENUES[venue]["close_hours"][dt.weekday()]

DB_PATH = Path("occupancy.db")
CSV_PATH = Path(__file__).parent.parent / "occupancy_summary.csv"
OPEN_HOUR   = 7   # first snapshot at 07:45 — checks the 08:00 opening slot
OPEN_MINUTE = 45
CLOSE_HOUR  = 23  # latest possible close across all venues; drives the main-loop guard
SLOTS_TO_CHECK = 7  # T, T+30, T+60, T+90, T+120, T+150, T+180 (3 hours)
SLOT_STEP_MINUTES = 30

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

_AMS = ZoneInfo("Europe/Amsterdam")

def _now() -> datetime:
    """Naive datetime in Amsterdam local time (correct on UTC GitHub Actions runners)."""
    return datetime.now(_AMS).replace(tzinfo=None)


DEBUG = "--debug" in sys.argv
RUN_ONCE = "--once" in sys.argv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at  DATETIME NOT NULL,
            venue       TEXT NOT NULL,
            court_name  TEXT NOT NULL,
            slot_time   DATETIME NOT NULL,
            available   BOOLEAN NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_obs_lookup
        ON observations(venue, court_name, slot_time, scraped_at)
    """)
    conn.commit()
    conn.close()
    log.debug("Database ready: %s", DB_PATH)


def save_observations(rows: list[dict]) -> None:
    if not rows:
        return
    conn = sqlite3.connect(DB_PATH)
    conn.executemany(
        """INSERT INTO observations (scraped_at, venue, court_name, slot_time, available)
           VALUES (:scraped_at, :venue, :court_name, :slot_time, :available)""",
        rows,
    )
    conn.commit()
    conn.close()


def get_known_courts(venue: str) -> list[str]:
    """Return known courts: config baseline merged with any extra courts seen in the DB."""
    configured = list(VENUES[venue].get("known_courts", []))
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            "SELECT DISTINCT court_name FROM observations WHERE venue = ? ORDER BY court_name",
            (venue,),
        ).fetchall()
        conn.close()
        db_courts = [r[0] for r in rows]
    except Exception:
        db_courts = []
    # Merge: configured courts first, then any extras discovered via scraping
    merged = list(configured)
    for c in db_courts:
        if c not in merged:
            merged.append(c)
    return merged



# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

_CSV_FIELDS = ["timestamp", "venue", "time_slot", "total_courts",
               "true_occupied", "occupancy_pct", "notes"]


def append_csv(row: dict) -> None:
    exists = CSV_PATH.exists()
    with CSV_PATH.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
        if not exists:
            w.writeheader()
        w.writerow(row)


# ---------------------------------------------------------------------------
# Slot-time helpers
# ---------------------------------------------------------------------------

def next_snapshot_time() -> datetime:
    """Return the next :15 or :45 wall-clock time."""
    now = _now()
    if now.minute < 15:
        return now.replace(minute=15, second=0, microsecond=0)
    if now.minute < 45:
        return now.replace(minute=45, second=0, microsecond=0)
    return (now + timedelta(hours=1)).replace(minute=15, second=0, microsecond=0)


def current_slot() -> datetime:
    """Return the next upcoming 30-minute slot (always strictly in the future)."""
    now = _now()
    remainder = now.minute % SLOT_STEP_MINUTES
    delta = (SLOT_STEP_MINUTES - remainder) if remainder != 0 else SLOT_STEP_MINUTES
    return (now + timedelta(minutes=delta)).replace(second=0, microsecond=0)


def slots_to_check(close_hour: int = CLOSE_HOUR) -> list[datetime]:
    base = current_slot()
    close = base.replace(hour=close_hour, minute=0, second=0, microsecond=0)
    return [
        base + timedelta(minutes=i * SLOT_STEP_MINUTES)
        for i in range(SLOTS_TO_CHECK)
        if base + timedelta(minutes=i * SLOT_STEP_MINUTES) + timedelta(minutes=60) <= close
    ]


def fmt(dt: datetime) -> str:
    return dt.strftime("%H:%M")


def today_str() -> str:
    return _now().strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Terwegen — RacketIQ checkcart API
# ---------------------------------------------------------------------------

def fetch_terwegen_slot(date: str, slot_dt: datetime) -> tuple[set[str], set[str]]:
    """
    Query the API for a specific 60-min slot.
    Returns (available_courts, all_courts_seen) — the API returns all courts including
    booked ones, so we can discover court names even when they have no availability.
    """
    slot_hhmm = slot_dt.strftime("%H:%M")
    to_hhmm = (slot_dt + timedelta(minutes=60)).strftime("%H:%M")
    from_enc = slot_hhmm.replace(":", "%3A")
    to_enc = to_hhmm.replace(":", "%3A")
    url = TERWEGEN_API.format(date=date, from_time=from_enc, to_time=to_enc)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        court_availability = json.loads(resp.read())["court_availability"]

    available: set[str] = set()
    all_courts: set[str] = set()
    for entry in court_availability:
        if entry["court"].get("sport", "").lower() != "padel":
            continue
        court_name = entry["court"]["name"]
        all_courts.add(court_name)
        for dur in entry.get("durations", []):
            if str(dur.get("duration")) == "60":
                for s in dur.get("availability", []):
                    if s.get("start_date_time", "")[11:16] == slot_hhmm:
                        available.add(court_name)
    return available, all_courts


def scrape_terwegen(target_slots: list[datetime]) -> list[dict]:
    """
    Scrape Terwegen: one API call per target slot to get exact availability.
    Returns list of {court_name, slot_time, available}.
    """
    date = today_str()
    known_courts: set[str] = set(get_known_courts("Terwegen"))
    results = []

    for slot_dt in target_slots:
        try:
            available_courts, seen_courts = fetch_terwegen_slot(date, slot_dt)
            known_courts |= seen_courts  # discover courts even when they're booked
            for court in available_courts:
                results.append({"court_name": court, "slot_time": slot_dt, "available": True})
            for court in known_courts - available_courts:
                results.append({"court_name": court, "slot_time": slot_dt, "available": False})
        except Exception as exc:
            log.error("[Terwegen] API call failed for slot %s: %s", fmt(slot_dt), exc, exc_info=DEBUG)

    log.debug("[Terwegen] %d observations across %d slots, %d courts known",
              len(results), len(target_slots), len(known_courts))
    return results


# ---------------------------------------------------------------------------
# Vinkenveld — MeetAndPlay Livewire DOM scraping
# ---------------------------------------------------------------------------

async def dismiss_cookie_banners(page: Page) -> None:
    candidates = [
        "button:has-text('Alles toestaan')",
        "button:has-text('Accepteer')",
        "button:has-text('Accepteren')",
        "button:has-text('Akkoord')",
        "button:has-text('Alles accepteren')",
        "button:has-text('Accept all')",
        "#cookie-consent-accept",
        ".cookie-accept",
        "[class*='cookie'] button",
        "[class*='consent'] button",
    ]
    for sel in candidates:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible(timeout=700):
                await btn.click()
                await page.wait_for_timeout(500)
                log.debug("Cookie banner dismissed: %s", sel)
                return
        except Exception:
            continue


async def scrape_vinkenveld(page: Page, target_slots: list[datetime]) -> list[dict]:
    """
    Scrape Vinkenveld via Playwright.

    The Livewire page renders available slots as <a class="timeslot v2 ..."> elements.
    Each contains:
      - .timeslot-time  → "12:00 - 13:00\n60 minuten"
      - .timeslot-name  → "Padelbaan 2"
    All rendered timeslot elements are AVAILABLE. Courts not shown are booked.
    """
    target_map = {fmt(s): s for s in target_slots}

    try:
        await page.goto(VINKENVELD_URL, wait_until="networkidle", timeout=45_000)
        await dismiss_cookie_banners(page)
        await page.wait_for_timeout(2_000)

        if DEBUG:
            shot = f"debug_vinkenveld_{_now().strftime('%H%M')}.png"
            await page.screenshot(path=shot, full_page=True)
            log.debug("Screenshot: %s", shot)

        slots = await page.evaluate(r"""
            () => {
                const results = [];
                // Each available slot is an <a class="timeslot v2 ...">
                const slotEls = document.querySelectorAll('a.timeslot, a[class*="timeslot"]');
                slotEls.forEach(el => {
                    // Court name
                    const nameEl = el.querySelector('.timeslot-name');
                    if (!nameEl) return;
                    const courtName = nameEl.childNodes[0]
                        ? nameEl.childNodes[0].textContent.trim()
                        : nameEl.textContent.trim();
                    if (!courtName) return;

                    // Time: "12:00 - 13:00"
                    const timeEl = el.querySelector('.timeslot-time');
                    if (!timeEl) return;
                    const timeText = timeEl.textContent.trim();
                    const m = timeText.match(/(\d{1,2}:\d{2})\s*[-–]\s*\d{1,2}:\d{2}/);
                    if (!m) return;

                    // Duration: look for "60 minuten" in the text
                    const durText = timeEl.querySelector('small') || timeEl;
                    const durMatch = (durText.textContent || '').match(/(\d+)\s*min/i);
                    const duration = durMatch ? parseInt(durMatch[1]) : 60;
                    if (duration !== 60) return;

                    results.push({ court: courtName, time: m[1] });
                });
                return results;
            }
        """)

        log.debug("[Vinkenveld] Found %d available timeslot elements", len(slots))

        # Build available set from the page
        available: dict[tuple, bool] = {}
        for s in slots:
            hhmm = s["time"]
            if len(hhmm) == 4:  # "9:00" → "09:00"
                hhmm = hhmm.zfill(5)
            if hhmm in target_map:
                available[(s["court"], hhmm)] = True

        # Gather all courts seen on this page (even those with no target slots)
        courts_this_page = {s["court"] for s in slots}
        known_courts = set(get_known_courts("Vinkenveld")) | courts_this_page

        results = []
        for court in known_courts:
            for hhmm, slot_dt in target_map.items():
                # A court is available only if it appeared as a timeslot element on the page
                # with the matching start time. Otherwise it's booked.
                avail = (court, hhmm) in available
                results.append({"court_name": court, "slot_time": slot_dt, "available": avail})

        log.debug("[Vinkenveld] %d observations from %d known courts", len(results), len(known_courts))
        return results

    except Exception as exc:
        log.error("[Vinkenveld] Scrape failed: %s", exc, exc_info=DEBUG)
        return []


# ---------------------------------------------------------------------------
# Main scrape cycle
# ---------------------------------------------------------------------------

async def run_scrape_cycle() -> None:
    scraped_at = _now()
    time_label = scraped_at.strftime("%H:%M")
    date_str = today_str()

    terwegen_slots   = slots_to_check(venue_close_hour("Terwegen"))
    vinkenveld_slots = slots_to_check(venue_close_hour("Vinkenveld"))

    # --- Terwegen: no browser needed ---
    log.debug("[Terwegen] Fetching API for %s", date_str)
    terwegen_obs = scrape_terwegen(terwegen_slots)

    # --- Vinkenveld: browser ---
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            locale="nl-NL",
            timezone_id="Europe/Amsterdam",
            viewport={"width": 1280, "height": 900},
        )
        page = await ctx.new_page()
        vinkenveld_obs = await scrape_vinkenveld(page, vinkenveld_slots)
        await browser.close()

    # --- Persist and report ---
    venue_slots_map = {"Terwegen": terwegen_slots, "Vinkenveld": vinkenveld_slots}

    for venue_name, obs_list in [("Terwegen", terwegen_obs), ("Vinkenveld", vinkenveld_obs)]:
        total_courts = VENUES[venue_name]["total_courts"]

        db_rows = [
            {
                "scraped_at": scraped_at.isoformat(),
                "venue": venue_name,
                "court_name": obs["court_name"],
                "slot_time": obs["slot_time"].isoformat(),
                "available": obs["available"],
            }
            for obs in obs_list
        ]
        save_observations(db_rows)

        venue_slots = venue_slots_map[venue_name]
        if not venue_slots:
            log.info("[%s] Outside operating hours — no slots to check.", venue_name)
            continue
        if not obs_list:
            print(f"[{time_label}] {venue_name} — scrape returned no data")
            continue

        # Occupancy for the primary (next upcoming) slot only
        primary_slot = venue_slots[0]
        minutes_until = int((primary_slot - scraped_at).total_seconds() / 60)
        slot_obs = [o for o in obs_list if o["slot_time"] == primary_slot]
        available_courts = sorted(o["court_name"] for o in slot_obs if o["available"])
        booked_courts = sorted(o["court_name"] for o in slot_obs if not o["available"])
        unknown_count = max(0, total_courts - len(slot_obs))
        n_occupied = len(booked_courts) + unknown_count
        pct = n_occupied / total_courts * 100 if total_courts else 0.0

        print(
            f"[{time_label}] {venue_name} — "
            f"slot {fmt(primary_slot)} (T-{minutes_until}min): "
            f"{n_occupied}/{total_courts} booked ({pct:.0f}%)"
        )
        if booked_courts:
            print(f"           Booked:    {', '.join(booked_courts)}")
        if available_courts:
            print(f"           Available: {', '.join(available_courts)}")
        if unknown_count > 0:
            print(f"           Unknown:   {unknown_count} court(s) not yet seen, assumed booked")

        notes = "; ".join(filter(None, [
            f"booked: {','.join(booked_courts)}" if booked_courts else "",
            f"available: {','.join(available_courts)}" if available_courts else "",
            f"{unknown_count} unknown" if unknown_count else "",
        ]))
        append_csv({
            "timestamp": scraped_at.isoformat(),
            "venue": venue_name,
            "time_slot": fmt(primary_slot),
            "total_courts": total_courts,
            "true_occupied": n_occupied,
            "occupancy_pct": f"{pct:.1f}",
            "notes": notes,
        })

    # Regenerate the dashboard after every scrape cycle.
    try:
        import occupancy_dashboard as _dash
        _dash.HTML_PATH.write_text(
            _dash.render(
                _dash.period_averages(_dash.load_csv()),
                _dash.slot_averages(_dash.load_csv()),
                sum(1 for _ in _dash.CSV_PATH.open()) - 1,  # subtract header
            ),
            encoding="utf-8",
        )
        log.debug("Dashboard updated: %s", _dash.HTML_PATH)
    except Exception as exc:
        log.warning("Dashboard update failed: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def wall_clock_sleep(target: datetime) -> None:
    """Sleep until *target* wall-clock time, checking every 60 s.

    Using short chunks instead of one long asyncio.sleep means macOS system
    sleep (which pauses the monotonic clock) does not prevent the tracker from
    waking up on time.
    """
    while True:
        remaining = (target - _now()).total_seconds()
        if remaining <= 0:
            break
        try:
            await asyncio.sleep(min(60, remaining))
        except asyncio.CancelledError:
            raise


async def main() -> None:
    init_db()
    log.info("Padel Occupancy Tracker")
    log.info("  DB:  %s", DB_PATH.resolve())
    log.info("  CSV: %s", CSV_PATH.resolve())
    if DEBUG:
        log.info("  Mode: DEBUG")

    if RUN_ONCE:
        await run_scrape_cycle()
        return

    log.info("Scraping at :15 and :45 past each hour, starting %02d:%02d until %02d:00 — Ctrl+C to stop.",
             OPEN_HOUR, OPEN_MINUTE, CLOSE_HOUR)
    while True:
        now = _now()
        before_open = (now.hour, now.minute) < (OPEN_HOUR, OPEN_MINUTE)
        after_close = now.hour >= CLOSE_HOUR
        if before_open or after_close:
            # Outside business hours — sleep until 07:45.
            if after_close:
                next_open = (now + timedelta(days=1)).replace(
                    hour=OPEN_HOUR, minute=OPEN_MINUTE, second=0, microsecond=0
                )
            else:
                next_open = now.replace(
                    hour=OPEN_HOUR, minute=OPEN_MINUTE, second=0, microsecond=0
                )
            log.info("Outside business hours (%02d:%02d). Sleeping until %s.",
                     now.hour, now.minute, next_open.strftime("%H:%M"))
            try:
                await wall_clock_sleep(next_open)
            except asyncio.CancelledError:
                break
            continue

        try:
            await run_scrape_cycle()
        except KeyboardInterrupt:
            log.info("Stopped.")
            return
        except Exception as exc:
            log.error("Scrape cycle error: %s", exc, exc_info=True)
        try:
            nxt = next_snapshot_time()
            await wall_clock_sleep(nxt)
        except asyncio.CancelledError:
            break


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped.")
