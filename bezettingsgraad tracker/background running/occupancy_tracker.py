#!/usr/bin/env python3
"""
Padel Court Occupancy Tracker
==============================
Estimates padel court occupancy by checking booking availability every 15
minutes and storing observations in SQLite.

Which clubs are tracked is configured in venues.json — not in this file. Each
club names a *platform*, and every platform has one scraper here:

    racketiq   — RacketIQ checkcart API            (HTTP, geen browser)
    playtomic  — Playtomic availability API        (HTTP, geen browser)
    foys       — FOYS court-booking API (Peakz)    (HTTP, geen browser)
    padelos    — PadelOS searchByDate API          (HTTP, geen browser)
    livewire   — KNLTB Meet & Play clubpagina      (Playwright DOM scraping)

Een nieuwe club op een bestaand platform toevoegen = een blok in venues.json;
deze module hoeft dan niet gewijzigd te worden.

Usage:
    python occupancy_tracker.py                      # Run continuously every 15 minutes
    python occupancy_tracker.py --once               # Single scrape then exit
    python occupancy_tracker.py --debug              # Verbose logging + screenshots
    python occupancy_tracker.py --venue Terwegen     # Alleen deze club (herhaalbaar)
"""

import asyncio
import csv
import json
import logging
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
from pathlib import Path

from playwright.async_api import async_playwright, Page

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VENUES_PATH = Path(__file__).parent / "venues.json"

# Hoeveel clubs tegelijk worden opgehaald. Alles tegelijk laten lopen verzadigt
# de verbinding en levert DNS- en timeoutfouten op.
BROWSER_CONCURRENCY = 3
HTTP_CONCURRENCY = 4


def load_venues() -> dict:
    """Read venues.json and return {naam: config} for the enabled clubs only."""
    with VENUES_PATH.open(encoding="utf-8") as f:
        raw = json.load(f)
    return {v["naam"]: v for v in raw["venues"] if v.get("enabled", True)}


VENUES = load_venues()


def venue_close_hour(venue: str, dt: Optional[datetime] = None) -> int:
    """Return the closing hour for *venue* on the given day (default: today)."""
    if dt is None:
        dt = _now()
    return VENUES[venue]["close_hours"][dt.weekday()]

DB_PATH = Path(__file__).parent / "occupancy.db"
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


def _venue_filter() -> Optional[set]:
    """Parse --venue <naam> flags; return None when no filter was given."""
    names = set()
    for i, arg in enumerate(sys.argv):
        if arg == "--venue" and i + 1 < len(sys.argv):
            names.add(sys.argv[i + 1])
        elif arg.startswith("--venue="):
            names.add(arg.split("=", 1)[1])
    return names or None


VENUE_FILTER = _venue_filter()

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
# HTTP helper
# ---------------------------------------------------------------------------

def http_json(url: str, headers: Optional[dict] = None, payload: Optional[dict] = None,
              timeout: int = 30, attempts: int = 3):
    """
    GET (or POST when *payload* is given) a JSON endpoint and return the parsed body.

    Retries on transient network errors: with a dozen clubs fetching at once, DNS
    and connection timeouts are common and almost always succeed on a second try.
    """
    hdrs = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    hdrs.update(headers or {})
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        hdrs["Content-Type"] = "application/json"

    last: Exception = RuntimeError("no attempt made")
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers=hdrs, data=body)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except Exception as exc:
            last = exc
            if attempt < attempts:
                time.sleep(2 * attempt)
                log.debug("retry %d/%d for %s: %s", attempt, attempts, url[:80], exc)
    raise last


def build_observations(venue: str, target_slots: list[datetime],
                       available: dict[str, set[str]],
                       seen_courts: set[str]) -> list[dict]:
    """
    Turn a per-slot set of available court names into observation rows.

    *available* maps "HH:MM" -> set of court names free at that slot. Any court that
    is known but not in that set counts as booked — the same assumption the tracker
    has always made for platforms that only publish free slots.
    """
    known = set(get_known_courts(venue)) | seen_courts
    rows = []
    for slot_dt in target_slots:
        free = available.get(fmt(slot_dt), set())
        for court in known | free:
            rows.append({
                "court_name": court,
                "slot_time": slot_dt,
                "available": court in free,
            })
    return rows


# ---------------------------------------------------------------------------
# RacketIQ checkcart API  (Terwegen)
# ---------------------------------------------------------------------------

def fetch_racketiq_slot(api_url: str, date: str, slot_dt: datetime) -> tuple[set[str], set[str]]:
    """
    Query the API for a specific 60-min slot.
    Returns (available_courts, all_courts_seen) — the API returns all courts including
    booked ones, so we can discover court names even when they have no availability.
    """
    slot_hhmm = slot_dt.strftime("%H:%M")
    to_hhmm = (slot_dt + timedelta(minutes=60)).strftime("%H:%M")
    from_enc = slot_hhmm.replace(":", "%3A")
    to_enc = to_hhmm.replace(":", "%3A")
    url = api_url.format(date=date, from_time=from_enc, to_time=to_enc)
    court_availability = http_json(url)["court_availability"]

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


def scrape_racketiq(venue: str, cfg: dict, target_slots: list[datetime]) -> list[dict]:
    """One API call per target slot; the API reports booked courts too."""
    api_url = cfg["platform_config"]["api_url"]
    date = today_str()
    known_courts: set[str] = set(get_known_courts(venue))
    results = []

    for slot_dt in target_slots:
        try:
            available_courts, seen_courts = fetch_racketiq_slot(api_url, date, slot_dt)
            known_courts |= seen_courts  # discover courts even when they're booked
            for court in available_courts:
                results.append({"court_name": court, "slot_time": slot_dt, "available": True})
            for court in known_courts - available_courts:
                results.append({"court_name": court, "slot_time": slot_dt, "available": False})
        except Exception as exc:
            log.error("[%s] API call failed for slot %s: %s", venue, fmt(slot_dt), exc, exc_info=DEBUG)

    log.debug("[%s] %d observations across %d slots, %d courts known",
              venue, len(results), len(target_slots), len(known_courts))
    return results


# ---------------------------------------------------------------------------
# Playtomic  (Padelife Rottemeren, Plaza Padel Laren)
# ---------------------------------------------------------------------------

_PLAYTOMIC_COURTS: dict[str, dict[str, str]] = {}
PLAYTOMIC_CACHE_PATH = Path(__file__).parent / "playtomic_courts.json"
PLAYTOMIC_CACHE_MAX_AGE_DAYS = 7


def _fetch_playtomic_court_names(slug: str) -> dict[str, str]:
    """Read the resource list out of the club page's server-rendered payload."""
    import re
    # The club page is ~350 kB and playtomic.com is often slow, so allow a long
    # timeout and retry twice before giving up.
    for attempt in (1, 2, 3):
        try:
            req = urllib.request.Request(f"https://playtomic.com/clubs/{slug}",
                                         headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=90) as resp:
                html = resp.read().decode("utf-8", errors="replace")
            break
        except Exception:
            if attempt == 3:
                raise
            log.debug("[playtomic] retrying court-name fetch for %s", slug)
            time.sleep(3 * attempt)

    pairs = re.findall(
        r'resourceId\\":\\"([0-9a-f-]{36})\\",\\"name\\":\\"(.*?)\\",\\"sport\\":\\"(\w+)\\"',
        html)
    return {rid: name.strip() for rid, name, sport in pairs if sport == "PADEL"}


def playtomic_court_names(slug: str) -> dict[str, str]:
    """
    Map resource_id -> court name; the availability API returns UUIDs only.

    Court names change rarely, so the map is cached on disk and only refreshed
    weekly. That keeps the slow, failure-prone page fetch out of most cycles —
    and a stale cache still beats losing a whole scrape.
    """
    if slug in _PLAYTOMIC_COURTS:
        return _PLAYTOMIC_COURTS[slug]

    cache = {}
    if PLAYTOMIC_CACHE_PATH.exists():
        try:
            cache = json.loads(PLAYTOMIC_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("[playtomic] unreadable court cache, rebuilding: %s", exc)

    entry = cache.get(slug)
    fresh = False
    if entry:
        age = (_now() - datetime.fromisoformat(entry["fetched_at"])).days
        fresh = age < PLAYTOMIC_CACHE_MAX_AGE_DAYS
    if entry and fresh:
        _PLAYTOMIC_COURTS[slug] = entry["courts"]
        return entry["courts"]

    try:
        mapping = _fetch_playtomic_court_names(slug)
    except Exception as exc:
        if entry:
            log.warning("[playtomic] refresh failed for %s, using stale cache: %s", slug, exc)
            _PLAYTOMIC_COURTS[slug] = entry["courts"]
            return entry["courts"]
        raise

    cache[slug] = {"fetched_at": _now().isoformat(), "courts": mapping}
    PLAYTOMIC_CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False),
                                    encoding="utf-8")
    _PLAYTOMIC_COURTS[slug] = mapping
    return mapping


def scrape_playtomic(venue: str, cfg: dict, target_slots: list[datetime]) -> list[dict]:
    """One call returns the whole day's free slots per court resource."""
    pc = cfg["platform_config"]
    # Bail out rather than fall back to raw UUIDs: those would be stored as court
    # names and permanently inflate the club's known-court list.
    names = playtomic_court_names(pc["slug"])

    data = http_json(
        "https://playtomic.com/api/clubs/availability"
        f"?tenant_id={pc['tenant_id']}&date={today_str()}&sport_id=PADEL"
    )

    available: dict[str, set[str]] = {}
    for resource in data:
        court = names.get(resource["resource_id"], resource["resource_id"])
        for slot in resource.get("slots", []):
            if int(slot.get("duration", 0)) != 60:
                continue
            available.setdefault(slot["start_time"][:5], set()).add(court)

    return build_observations(venue, target_slots, available, set(names.values()))


# ---------------------------------------------------------------------------
# FOYS court-booking API  (Peakz Padel)
# ---------------------------------------------------------------------------

def scrape_foys(venue: str, cfg: dict, target_slots: list[datetime]) -> list[dict]:
    """
    One call returns every court with its full day of slots, each carrying an
    explicit isAvailable flag — so booked courts are reported directly.
    """
    pc = cfg["platform_config"]
    org = pc["organisation_id"]
    data = http_json(
        "https://api.foys.io/court-booking/public/api/v1/locations/search"
        f"?reservationTypeId=6&locationId={pc['location_id']}"
        f"&playingTimes%5B%5D=60&date={today_str()}T00:00",
        headers={"x-organisationid": org, "x-federationid": org},
    )
    if not data:
        log.warning("[%s] FOYS returned no location", venue)
        return []

    target_hhmm = {fmt(s) for s in target_slots}
    slot_map = {fmt(s): s for s in target_slots}
    rows, courts = [], set()
    for item in data[0].get("inventoryItemsTimeSlots", []):
        court = item["name"].strip()
        courts.add(court)
        for ts in item.get("timeSlots", []):
            if int(ts.get("duration", 0)) != 60:
                continue
            hhmm = ts["startTime"][11:16]
            if hhmm in target_hhmm:
                rows.append({"court_name": court, "slot_time": slot_map[hhmm],
                             "available": bool(ts.get("isAvailable"))})

    log.debug("[%s] %d observations from %d courts", venue, len(rows), len(courts))
    return rows


# ---------------------------------------------------------------------------
# PadelOS searchByDate  (Padelclub Rotterdam)
# ---------------------------------------------------------------------------

_PADELOS_CACHE: dict[tuple, dict] = {}


def padelos_company_data(company_id, date: str) -> dict:
    """
    Fetch a whole PadelOS company once per cycle: one POST covers all its clubs.
    Returns {club_id: club payload}.
    """
    key = (str(company_id), date)
    if key in _PADELOS_CACHE:
        return _PADELOS_CACHE[key]

    clubs = http_json(f"https://api.padelos.co/customers/fetch-company-clubs/{company_id}",
                      headers={"x-clubos-channel": "CLUBOS-WEB"})
    club_ids = ",".join(str(r["id"]) for r in clubs["data"]["rows"])

    body = http_json(
        "https://api.padelos.co/customers/searchByDate",
        headers={
            "x-clubos-channel": "CLUBOS-WEB",
            "x-clubos-company": str(company_id),
            "x-clubos-domain": "PADELOSCO",
            "x-clubos-club-info": club_ids,
            "x-client-route": f"/company/{company_id}",
            "version": "2.4",
        },
        payload={"date": date, "sport": "padel", "courtType": "", "courtSize": "",
                 "courtTurf": "", "courtFeature": "", "searchTerm": "",
                 "limit": "", "offset": "", "type": ""},
    )
    if not body.get("success"):
        raise RuntimeError(f"PadelOS searchByDate failed: {body.get('data')}")

    result = {str(c["id"]): c for c in body["data"]}
    _PADELOS_CACHE[key] = result
    return result


def scrape_padelos(venue: str, cfg: dict, target_slots: list[datetime]) -> list[dict]:
    """PadelOS lists only free courts per slot, so unseen courts count as booked."""
    pc = cfg["platform_config"]
    clubs = padelos_company_data(pc["company_id"], today_str())
    club = clubs.get(str(pc["club_id"]))
    if club is None:
        log.warning("[%s] club %s not present in PadelOS response", venue, pc["club_id"])
        return []

    available: dict[str, set[str]] = {}
    seen: set[str] = set()
    for duration in club.get("availability", []):
        if str(duration.get("duration")) != "60":
            continue
        for slot in duration.get("slots", []):
            names = {c["name"].strip() for c in slot.get("courts", [])}
            seen |= names
            available.setdefault(slot["startTime"][:5], set()).update(names)

    return build_observations(venue, target_slots, available, seen)


# ---------------------------------------------------------------------------
# KNLTB Meet & Play — Livewire DOM scraping
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


async def scrape_livewire(page: Page, venue: str, cfg: dict,
                          target_slots: list[datetime]) -> list[dict]:
    """
    Scrape a KNLTB Meet & Play club page via Playwright.

    The Livewire page renders available slots as <a class="timeslot v2 ..."> elements.
    Each contains:
      - .timeslot-time  → "12:00 - 13:00\n60 minuten"
      - .timeslot-name  → "Padelbaan 2"
    All rendered timeslot elements are AVAILABLE. Courts not shown are booked.
    The DOM is identical across clubs; only the club id in the URL differs.
    """
    target_map = {fmt(s): s for s in target_slots}
    url = f"https://meetandplay.nl/club/{cfg['platform_config']['club_id']}?sport=padel"

    try:
        # networkidle is unreachable here: the page keeps loading Google Maps tiles
        # and analytics beacons. Wait for the DOM, then for the slot list itself.
        await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
        await dismiss_cookie_banners(page)
        try:
            await page.wait_for_selector("a[class*='timeslot'], .no-timeslots, text=geen tijdslots",
                                         timeout=20_000)
        except Exception:
            log.debug("[%s] no timeslot markers appeared; reading page as-is", venue)
        await page.wait_for_timeout(2_000)

        if DEBUG:
            safe = venue.replace(" ", "_").replace("/", "-")
            shot = f"debug_{safe}_{_now().strftime('%H%M')}.png"
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

        log.debug("[%s] Found %d available timeslot elements", venue, len(slots))

        # Build the per-slot availability set from the page
        available: dict[str, set[str]] = {}
        for s in slots:
            hhmm = s["time"]
            if len(hhmm) == 4:  # "9:00" → "09:00"
                hhmm = hhmm.zfill(5)
            if hhmm in target_map:
                available.setdefault(hhmm, set()).add(s["court"])

        # Courts seen on this page count as known, even outside the target slots
        courts_this_page = {s["court"] for s in slots}
        return build_observations(venue, target_slots, available, courts_this_page)

    except Exception as exc:
        log.error("[%s] Scrape failed: %s", venue, exc, exc_info=DEBUG)
        return None


# ---------------------------------------------------------------------------
# Scraper registry
# ---------------------------------------------------------------------------

# Platforms served over plain HTTP — no browser, safe to run in a thread pool.
SYNC_SCRAPERS = {
    "racketiq": scrape_racketiq,
    "playtomic": scrape_playtomic,
    "foys": scrape_foys,
    "padelos": scrape_padelos,
}

# Platforms that need a rendered page.
ASYNC_SCRAPERS = {
    "livewire": scrape_livewire,
}


# ---------------------------------------------------------------------------
# Main scrape cycle
# ---------------------------------------------------------------------------

def active_venues() -> dict:
    """The enabled venues, narrowed to --venue when that flag was given."""
    if VENUE_FILTER is None:
        return VENUES
    unknown = VENUE_FILTER - set(VENUES)
    if unknown:
        log.warning("Unknown or disabled venue(s) in --venue: %s", ", ".join(sorted(unknown)))
    return {n: c for n, c in VENUES.items() if n in VENUE_FILTER}


async def _run_sync_scraper(sem, venue: str, cfg: dict, slots: list[datetime]) -> list[dict]:
    """Run an HTTP scraper off the event loop so venues fetch in parallel."""
    fn = SYNC_SCRAPERS[cfg["platform"]]
    async with sem:
        try:
            return await asyncio.to_thread(fn, venue, cfg, slots)
        except Exception as exc:
            log.error("[%s] scraper failed: %s", venue, exc, exc_info=DEBUG)
            return None


async def _run_browser_scraper(ctx, sem, venue: str, cfg: dict,
                               slots: list[datetime]) -> list[dict]:
    """Run a Playwright scraper in its own page, capped by *sem*."""
    fn = ASYNC_SCRAPERS[cfg["platform"]]
    async with sem:
        page = await ctx.new_page()
        try:
            return await fn(page, venue, cfg, slots)
        except Exception as exc:
            log.error("[%s] scraper failed: %s", venue, exc, exc_info=DEBUG)
            return None
        finally:
            await page.close()


async def run_scrape_cycle() -> None:
    scraped_at = _now()
    time_label = scraped_at.strftime("%H:%M")

    venues = active_venues()
    venue_slots_map = {n: slots_to_check(venue_close_hour(n)) for n in venues}

    sync_venues = {n: c for n, c in venues.items() if c["platform"] in SYNC_SCRAPERS}
    browser_venues = {n: c for n, c in venues.items() if c["platform"] in ASYNC_SCRAPERS}
    unsupported = set(venues) - set(sync_venues) - set(browser_venues)
    for n in sorted(unsupported):
        log.warning("[%s] no scraper for platform '%s' — skipped", n, venues[n]["platform"])

    # None = the scrape failed; [] = the scrape ran but every court was booked.
    observations: dict[str, Optional[list[dict]]] = {n: None for n in venues}

    # --- HTTP platforms: all in parallel, no browser ---
    if sync_venues:
        names = list(sync_venues)
        sem = asyncio.Semaphore(HTTP_CONCURRENCY)
        results = await asyncio.gather(*(
            _run_sync_scraper(sem, n, sync_venues[n], venue_slots_map[n]) for n in names
        ))
        observations.update(dict(zip(names, results)))

    # --- Browser platforms: shared browser, one page each ---
    if browser_venues:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx = await browser.new_context(
                user_agent=USER_AGENT,
                locale="nl-NL",
                timezone_id="Europe/Amsterdam",
                viewport={"width": 1280, "height": 900},
            )
            sem = asyncio.Semaphore(BROWSER_CONCURRENCY)
            names = list(browser_venues)
            results = await asyncio.gather(*(
                _run_browser_scraper(ctx, sem, n, browser_venues[n], venue_slots_map[n])
                for n in names
            ))
            observations.update(dict(zip(names, results)))
            await browser.close()

    # --- Persist and report ---
    for venue_name in venues:
        obs_list = observations[venue_name]
        total_courts = VENUES[venue_name]["total_courts"]

        db_rows = [
            {
                "scraped_at": scraped_at.isoformat(),
                "venue": venue_name,
                "court_name": obs["court_name"],
                "slot_time": obs["slot_time"].isoformat(),
                "available": obs["available"],
            }
            for obs in (obs_list or [])
        ]
        save_observations(db_rows)

        venue_slots = venue_slots_map[venue_name]
        if not venue_slots:
            log.info("[%s] Outside operating hours — no slots to check.", venue_name)
            continue
        if obs_list is None:
            print(f"[{time_label}] {venue_name} — scrape failed, no data recorded")
            continue
        # An empty list means the scrape succeeded and found nothing free: fully booked.

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
    venues = active_venues()
    log.info("Padel Occupancy Tracker")
    log.info("  DB:  %s", DB_PATH.resolve())
    log.info("  CSV: %s", CSV_PATH.resolve())
    log.info("  Clubs: %d actief — %s", len(venues), ", ".join(sorted(venues)))
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
