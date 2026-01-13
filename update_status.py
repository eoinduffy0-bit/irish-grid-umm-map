import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import parser as dtparser

UMM_API = "https://ummapi.nordpoolgroup.com/messages"
IRELAND_AREA = "10Y1001A1001A59C"
API_LIMIT = 800

GENERATORS_CSV = os.path.join("data", "generators.csv")
OUT_GEOJSON = os.path.join("docs", "status.geojson")

GU_RE = re.compile(r"GU_\d+")


@dataclass
class Generator:
    infrastructure: str
    gu_code: str
    lat: float
    lon: float
    fuel: str


def extract_gu(text: str) -> Optional[str]:
    if not isinstance(text, str):
        return None
    m = GU_RE.search(text.upper())
    return m.group(0) if m else None


def iso_to_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    dt = dtparser.isoparse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_generators(path: str) -> Dict[str, Generator]:
    """
    data/generators.csv:
      infrastructure,lat,lon,fuel

    We key strictly by GU code extracted from infrastructure.
    Rows without GU_####### are skipped to keep matching deterministic.
    """
    gens: Dict[str, Generator] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            infra = (row.get("infrastructure") or "").strip()
            if not infra:
                continue

            gu = extract_gu(infra)
            if not gu:
                # Skip non-GU rows in GU-only mode
                continue

            gens[gu] = Generator(
                infrastructure=infra,
                gu_code=gu,
                lat=float(row["lat"]),
                lon=float(row["lon"]),
                fuel=(row.get("fuel") or "").strip(),
            )
    return gens


def fetch_messages() -> Dict[str, Any]:
    params = {
        "areas": IRELAND_AREA,
        "limit": str(API_LIMIT),
        "IncludeOutdated": "true",
    }
    r = requests.get(UMM_API, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def parse_time_period_for_now(gu_obj: Dict[str, Any], now: datetime) -> Optional[Tuple[float, float]]:
    """
    generationUnits[].timePeriods[] items contain:
      availableCapacity, unavailableCapacity, eventStart, eventStop
    Return (available_mw, unavailable_mw) for the timePeriod covering 'now'.
    """
    tps = gu_obj.get("timePeriods")
    if not isinstance(tps, list):
        return None

    def num(x: Any) -> Optional[float]:
        return float(x) if isinstance(x, (int, float)) else None

    for tp in tps:
        if not isinstance(tp, dict):
            continue

        start = iso_to_dt(tp.get("eventStart"))
        stop = iso_to_dt(tp.get("eventStop"))
        if not start or not stop:
            continue

        if not (start <= now <= stop):
            continue

        available = num(tp.get("availableCapacity"))
        unavailable = num(tp.get("unavailableCapacity"))

        if available is None or unavailable is None:
            continue

        return (available, unavailable)

    return None


def status_from_availability(avail: float, unavail: float) -> str:
    if unavail == 0:
        return "online"
    if avail == 0:
        return "offline"
    return "partial"


def main() -> None:
    os.makedirs("docs", exist_ok=True)

    generators = load_generators(GENERATORS_CSV)

    payload = fetch_messages()
    items = payload.get("items", [])
    if not isinstance(items, list):
        items = []

    now = datetime.now(timezone.utc)

    # State by GU code
    state: Dict[str, Tuple[float, float]] = {}

    for msg in items:
        if not isinstance(msg, dict):
            continue

        gus = msg.get("generationUnits")
        if not isinstance(gus, list):
            continue

        for gu_obj in gus:
            if not isinstance(gu_obj, dict):
                continue

            name = gu_obj.get("name")
            gu = extract_gu(name) if isinstance(name, str) else None
            if not gu:
                continue

            if gu not in generators:
                continue

            period = parse_time_period_for_now(gu_obj, now)
            if not period:
                continue

            avail, unavail = period

            # If multiple messages match same unit, take max unavailable (conservative)
            prev = state.get(gu)
            if prev is None or unavail > prev[1]:
                state[gu] = (avail, unavail)

    features: List[Dict[str, Any]] = []
    for gu, gen in generators.items():
        if gu in state:
            avail, unavail = state[gu]
            status = status_from_availability(avail, unavail)
        else:
            # No active timePeriod => assume online but MW unknown -> show 0/0
            avail, unavail = (0.0, 0.0)
            status = "online"

        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [gen.lon, gen.lat]},
                "properties": {
                    "infrastructure": gen.infrastructure,
                    "fuel": gen.fuel,
                    "available_mw": round(avail, 1),
                    "unavailable_mw": round(unavail, 1),
                    "status": status,
                },
            }
        )

    geojson = {
        "type": "FeatureCollection",
        "generated_at_utc": now.isoformat(),
        "features": features,
    }

    with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2)

    print(f"Wrote {OUT_GEOJSON} ({len(features)} features)")


if __name__ == "__main__":
    main()
