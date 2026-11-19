import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


DATA_DIR = Path(__file__).resolve().parent / "data"

CITIES = [
    ("Casablanca", "CAS", "GHT-Atlantique", 33.5731, -7.5898),
    ("Rabat", "RAB", "GHT-Atlantique", 34.0209, -6.8416),
    ("Marrakech", "MAR", "GHT-Sud", 31.6295, -7.9811),
    ("Tanger", "TAN", "GHT-Nord", 35.7595, -5.8340),
]

FACILITY_TYPES = (
    ["CHU"] + ["HOPITAL_REGIONAL"] * 3
    + ["CLINIQUE"] * 3 + ["CENTRE_SANTE"] * 3
)

DEPARTMENTS = ["Traumatologie", "Cardiologie", "Pédiatrie", "Médecine générale"]


def make_facilities(rng):
    facilities = []

    for city, city_code, group, latitude, longitude in CITIES:
        for number, facility_type in enumerate(FACILITY_TYPES, start=1):
            facilities.append({
                "facility_id": f"F-{len(facilities) + 1:03d}",
                "name": f"{city} - {facility_type} {number:02d}",
                "city": city,
                "hospital_group": group,
                "type": facility_type,
                "beds": rng.randint(20, 450),
                "latitude": round(latitude + rng.uniform(-0.05, 0.05), 6),
                "longitude": round(longitude + rng.uniform(-0.05, 0.05), 6),
                "city_code": city_code,
            })

    return facilities


def make_visit(visit_number, facilities, rng):
    facility = rng.choice(facilities)
    priority = rng.randint(1, 5)
    wait_minutes = rng.randint(5, 90)
    arrival = datetime(2026, 11, 15, tzinfo=timezone.utc) + timedelta(
        seconds=rng.randrange(7 * 24 * 3600)
    )
    transfer_to = None

    if rng.random() < 0.12:
        destinations = [
            other for other in facilities
            if other["facility_id"] != facility["facility_id"]
            and (
                other["hospital_group"] == facility["hospital_group"]
                or other["type"] == "CHU"
            )
        ]
        transfer_to = rng.choice(destinations)["facility_id"]

    common = {
        "visit_id": f"V-{visit_number:07d}",
        "patient_id": f"P-{rng.randint(1, 8_000):05d}",
        "facility_id": facility["facility_id"],
        "city": facility["city"],
        "department": rng.choice(DEPARTMENTS),
    }

    timeline = [
        ("ARRIVED", 0),
        ("TRIAGED", rng.randint(1, min(5, wait_minutes))),
        ("CARE_STARTED", wait_minutes),
        ("TRANSFERRED" if transfer_to else "DISCHARGED",
         wait_minutes + rng.randint(15, 180)),
    ]

    return [
        {
            **common,
            "status": status,
            "priority": None if status == "ARRIVED" else priority,
            "wait_minutes": wait_minutes if status == "CARE_STARTED" else None,
            "desk_id": (
                f"T-{facility['city_code']}-{rng.randint(1, 5):02d}"
                if status == "TRIAGED" else None
            ),
            "transfer_to": transfer_to if status == "TRANSFERRED" else None,
            "event_time": (
                (arrival + timedelta(minutes=minutes))
                .strftime("%Y-%m-%dT%H:%M:%SZ")
            ),
        }
        for status, minutes in timeline
    ]


def write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["file"], required=True)
    parser.add_argument("--n", type=int, default=50_000)
    args = parser.parse_args()

    if args.n < 4:
        parser.error("--n doit être au moins égal à 4")

    rng = random.Random(42)
    facilities = make_facilities(rng)
    visit_count = (args.n - round(args.n * 0.02)) // 4
    events = [
        event
        for visit_number in range(1, visit_count + 1)
        for event in make_visit(visit_number, facilities, rng)
    ]
    duplicate_count = args.n - len(events)
    events.extend(rng.choices(events, k=duplicate_count))
    events.sort(key=lambda event: event["event_time"])

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    write_jsonl(
        DATA_DIR / "facilities.json",
        (
            {key: value for key, value in facility.items() if key != "city_code"}
            for facility in facilities
        ),
    )
    write_jsonl(DATA_DIR / "urgences_history.jsonl", events)

    print(f"Établissements : {len(facilities)}")
    print(f"Passages uniques : {visit_count}")
    print(f"Événements : {len(events)}")
    print(f"Doublons injectés : {duplicate_count}")
    print(f"Fichiers enregistrés dans : {DATA_DIR}")


if __name__ == "__main__":
    main()