import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dateutil import parser as dtparser

UMM_API = "https://ummapi.nordpoolgroup.com/messages"
IRELAND_AREA = "10Y1001A1001A59C"
API_LIMIT = 500

GENERATORS_CSV = os.path.join("data", "generators.csv")
OUT_GEOJSON = os.path.join("public", "status.geojson")

@dataclass
class Generator:
    station_name: str
    lat: float
    lon: float
    installed_mw: float
    fuel: str = ""

def load_generators(path: str) -> Dict[str, Generator]:
    gens: Dict[str, Generator] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["station_name"].strip()
            gens[name.lower()] = Generator(
                station_name=name,
                lat=float(row["lat"]),
                lon=float(row["lon"]),
                installed_mw=float(row["installed_mw"]),
                fuel=(row.get("fuel") or "").strip(),
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
        "IncludeOutdated": "true",
        "limit": str(API_LIMIT),
        "eventDate": "today",
    }
    r = requests.get(UMM_API, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def extract_station_names(msg: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    stations = msg.get("stations")
    if isinstance(stations, list):
        for s in stations:
            if isinstance(s, dict) and s.get("name"):
                names.append(str(s["name"]).strip())
            elif isinstance(s, str):
                names.append(s.strip())

    if not names:
        sname = msg.get("stationName") or (msg.get("station") or {}).get("name")
        if isinstance(sname, str) and sname.strip():
            names.append(sname.strip())

    return names

def message_time_window(msg: Dict[str, Any]) -> tuple[Optional[datetime], Optional[datetime]]:
    start = iso_to_dt(msg.get("eventStart") or msg.get("eventStartTime") or msg.get("eventStartDate"))
    stop = iso_to_dt(msg.get("eventStop") or msg.get("eventStopTime") or msg.get("eventStopDate"))
    return start, stop

def is_active_now(msg: Dict[str, Any], now: datetime) -> bool:
    status = (msg.get("status") or "").lower()
    if status == "cancelled":
        return False
    start, stop = message_time_window(msg)
    if not start or not stop:
        return False
    return start <= now <= stop

def get_unavailable_mw(msg: Dict[str, Any]) -> Optional[float]:
    for k in ("unavailableCapacity", "unavailableCapacityMw", "capacityUnavailable", "unavailability"):
        v = msg.get(k)
        if isinstance(v, (int, float)):
            return float(v)
    cap = msg.get("capacity")
    if isinstance(cap, dict):
        v = cap.get("unavailable")
        if isinstance(v, (int, float)):
            return float(v)
    return None

def build_geojson(generators: Dict[str, Generator], messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)

    # max unavailable MW per station among active messages
    unavailable_by_station: Dict[str, float] = {}

    for msg in messages:
        if not isinstance(msg, dict):
            continue

        category = (msg.get("messageCategory") or msg.get("category") or "").lower()
        if category and "unavailability" not in category:
            continue

        if not is_active_now(msg, now):
            continue

        unavailable = get_unavailable_mw(msg)
        if unavailable is None:
            continue

        for station in extract_station_names(msg):
            key = station.lower()
            prev = unavailable_by_station.get(key, 0.0)
            unavailable_by_station[key] = max(prev, unavailable)

    features: List[Dict[str, Any]] = []
    for key, gen in generators.items():
        unavail = unavailable_by_station.get(key, 0.0)
        available = max(0.0, gen.installed_mw - unavail)

        status = "online"
        if gen.installed_mw > 0 and unavail >= gen.installed_mw * 0.95:
            status = "offline"
        elif unavail > 0:
            status = "partial"

        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [gen.lon, gen.lat]},
                "properties": {
                    "station": gen.station_name,
                    "fuel": gen.fuel,
                    "installed_mw": gen.installed_mw,
                    "unavailable_mw": round(unavail, 1),
                    "available_mw": round(available, 1),
                    "status": status,
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "generated_at_utc": now.isoformat(),
        "features": features,
    }

def main() -> None:
    os.makedirs("public", exist_ok=True)

    generators = load_generators(GENERATORS_CSV)
    payload = fetch_messages()
    items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        items = []

    geojson = build_geojson(generators, items)

    with open(OUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUT_GEOJSON} with {len(geojson['features'])} generators.")

if __name__ == "__main__":
    main()
