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

GU_RE = re.compile(r"GU_\\d+")

@dataclass
class Generator:
    infrastructure: str
    gu_code: Optional[str]
    lat: float
    lon: float
    fuel: str

def extract_gu(text: str) -> Optional[str]:
    if not isinstance(text, str):
        return None
    m = GU_RE.search(text.upper())
    return m.group(0) if m else None

def load_generators(path: str) -> Dict[str, Generator]:
    gens: Dict[str, Generator] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            infra = row["infrastructure"].strip()
            gu = extract_gu(infra)
            key = gu if gu else infra
            gens[key] = Generator(
                infrastructure=infra,
                gu_code=gu,
                lat=float(row["lat"]),
                lon=float(row["lon"]),
                fuel=row.get("fuel", "").strip(),
            )
    return gens

def iso_to_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    dt = dtparser.isoparse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def fetch_messages() -> Dict[str, Any]:
    params = {
        "areas": IRELAND_AREA,
        "limit": str(API_LIMIT),
        "IncludeOutdated": "true",
    }
    r = requests.get(UMM_API, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def is_active_now(msg: Dict[str, Any], now: datetime) -> bool:
    start = iso_to_dt(msg.get("eventStart"))
    stop = iso_to_dt(msg.get("eventStop"))
    if not start or not stop:
        return False
    return start <= now <= stop

def get_available_unavailable(msg: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    def num(x):
        return float(x) if isinstance(x, (int, float)) else None

    available = num(msg.get("available")) or num(msg.get("availableMw"))
    unavailable = (
        num(msg.get("unavailable")) or
        num(msg.get("unavailableMw")) or
        num(msg.get("capacityUnavailable"))
    )

    cap = msg.get("capacity")
    if isinstance(cap, dict):
        if available is None:
            available = num(cap.get("available"))
        if unavailable is None:
            unavailable = num(cap.get("unavailable"))

    return available, unavailable

def extract_message_key(msg: Dict[str, Any]) -> Optional[str]:
    for field in ("infrastructure", "stationName", "unitName", "name"):
        text = msg.get(field)
        gu = extract_gu(text) if isinstance(text, str) else None
        if gu:
            return gu
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

    now = datetime.now(timezone.utc)
    state: Dict[str, Tuple[float, float]] = {}

    for msg in items:
        if not is_active_now(msg, now):
            continue

        avail, unavail = get_available_unavailable(msg)
        if avail is None or unavail is None:
            continue

        key = extract_message_key(msg)
        if key and key in generators:
            state[key] = (avail, unavail)

    features = []
    for key, gen in generators.items():
        avail, unavail = state.get(key, (0.0, 0.0))
        status = status_from_availability(avail, unavail)

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [gen.lon, gen.lat]
            },
            "properties": {
                "infrastructure": gen.infrastructure,
                "fuel": gen.fuel,
                "available_mw": round(avail, 1),
                "unavailable_mw": round(unavail, 1),
                "status": status
            }
        })

    geojson = {
        "type": "FeatureCollection",
        "generated_at_utc": now.isoformat(),
        "features": features
    }

    with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(geojson, f, indent=2)

    print(f"Wrote {OUT_GEOJSON}")

if __name__ == "__main__":
    main()
