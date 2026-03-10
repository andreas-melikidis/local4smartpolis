"""
SmartPolis — Data Loader
Φορτώνει όλους τους δήμους από τα smartpolis.json + smartpolis_tourism.json
στον πίνακα municipalities.

Χρήση:
    pip install mysql-connector-python
    python load_data.py
"""

import json
import os
import logging
import mysql.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("smartpolis.loader")

# ─────────────────────────────────────────
#  CONFIG — βάλε τα δικά σου credentials
# ─────────────────────────────────────────

DB_CONFIG = {
    "host":     "127.0.0.1",
    "user":     "root",
    "password": "aNDREASMELIKIDIS2007!",
    "database": "smartpolis",
    "charset":  "utf8mb4",
}

SMARTPOLIS_JSON  = "smartpolis.json"
TOURISM_JSON     = "smartpolis_tourism.json"


# ─────────────────────────────────────────
#  HELPERS — εξαγωγή δεδομένων από sections
# ─────────────────────────────────────────

def extract_population(mun: dict) -> dict:
    """Παίρνει τα totals από την πρώτη γραμμή (geo_code row)."""
    rows = mun.get("population_by_education_employment", [])
    # Η πρώτη γραμμή έχει education_level == name_el (το summary row)
    total_row = next(
        (r for r in rows if r.get("education_level") == mun["name_el"]),
        rows[0] if rows else {}
    )
    return {
        "population_total":     total_row.get("population_total"),
        "employed":             total_row.get("employed"),
        "unemployed":           total_row.get("unemployed"),
        "economically_active":  total_row.get("economically_active_total"),
        "economically_inactive":total_row.get("economically_inactive_total"),
        "students":             total_row.get("students"),
        "retired":              total_row.get("retired"),
        "other_inactive":       total_row.get("other_inactive"),
    }


def extract_unemployment_rate(pop: dict) -> float | None:
    """unemployment_rate = unemployed / economically_active * 100"""
    try:
        return round(pop["unemployed"] / pop["economically_active"] * 100, 2)
    except (TypeError, ZeroDivisionError):
        return None


def extract_employed_education(mun: dict) -> dict:
    rows = mun.get("employed_by_education", [])
    if not rows:
        return {}
    r = rows[0]
    return {
        "employed_university":    r.get("employed_phd_masters_university"),
        "employed_post_secondary":r.get("employed_post_secondary_iek"),
        "employed_lyceum":        r.get("employed_lyceum"),
        "employed_below_lyceum":  (
            (r.get("employed_vocational_gymnasium") or 0)
            + (r.get("employed_primary") or 0)
            + (r.get("employed_below_primary") or 0)
        ) or None,
    }


def extract_unemployed_education(mun: dict) -> dict:
    rows = mun.get("unemployed_by_education", [])
    if not rows:
        return {}
    r = rows[0]
    return {
        "unemployed_university":   r.get("unemployed_phd_masters_university"),
        "unemployed_lyceum":       r.get("unemployed_lyceum"),
        "unemployed_below_lyceum": (
            (r.get("unemployed_vocational_gymnasium") or 0)
            + (r.get("unemployed_primary") or 0)
            + (r.get("unemployed_below_primary") or 0)
        ) or None,
    }


def extract_dwellings(mun: dict) -> dict:
    # Totals από dwellings_by_occupancy_density (summary row)
    occ_rows = mun.get("dwellings_by_occupancy_density", [])
    total_row = next(
        (r for r in occ_rows if r.get("occupancy_density") == mun["name_el"]),
        occ_rows[0] if occ_rows else {}
    )

    # Ανά τύπο κτιρίου
    bld_rows = mun.get("dwellings_by_building_type", [])
    def get_building(btype):
        row = next((r for r in bld_rows if r.get("building_type") == btype), {})
        return row.get("dwellings_total")

    return {
        "dwellings_total":          total_row.get("dwellings_total"),
        "dwellings_owner_occupied": total_row.get("dwellings_owner_occupied"),
        "dwellings_rented_total":   total_row.get("dwellings_rented_total"),
        "dwellings_detached":       get_building("Μονοκατοικία"),
        "dwellings_semi_detached":  get_building("Διπλοκατοικία"),
        "dwellings_apartment":      get_building("Πολυκατοικία"),
    }


def extract_households(mun: dict) -> dict:
    rows = mun.get("households_by_size", [])
    total_row = next(
        (r for r in rows if r.get("household_size") == mun["name_el"]),
        rows[0] if rows else {}
    )

    def get_size(label):
        row = next((r for r in rows if r.get("household_size") == label), {})
        return row.get("num_households")

    return {
        "households_total":        total_row.get("num_households"),
        "household_members_total": total_row.get("num_members"),
        "households_1person":      get_size("1 άτομο"),
        "households_2person":      get_size("2 άτομα"),
        "households_3person":      get_size("3 άτομα"),
        "households_4person":      get_size("4 άτομα"),
        "households_5plus":        get_size("5+ άτομα"),
    }


def extract_tourism(tourism_json: dict, year: int = 2024) -> dict:
    """
    Τα tourism data είναι εθνικού επιπέδου (1 αρχείο),
    οπότε τα ίδια νούμερα μπαίνουν σε όλους τους δήμους
    ως reference τιμές της χώρας.
    """
    ts = tourism_json["data"]["τουρισμος"]["files"][0]["timeseries"]
    def get_val(metric):
        row = next((t for t in ts if t["metric"] == metric and t["year"] == year), None)
        return row["value"] if row else None

    return {
        "tourism_total_declarations": get_val("Συνολικός αριθμός δηλώσεων"),
        "tourism_total_rental_days":  get_val("Συνολικός αριθμός ημερών μίσθωσης"),
        "tourism_domestic_clients":   get_val("Mε ημεδαπούς πελάτες"),
        "tourism_foreign_clients":    get_val("Mε αλλοδαπούς πελάτες"),
        "tourism_data_year":          year,
    }


# ─────────────────────────────────────────
#  INSERT QUERY
# ─────────────────────────────────────────

INSERT_SQL = """
INSERT INTO municipalities (
    geo_code, name_el, region,
    population_total, employed, unemployed,
    economically_active, economically_inactive,
    students, retired, other_inactive, unemployment_rate,
    employed_university, employed_post_secondary, employed_lyceum, employed_below_lyceum,
    unemployed_university, unemployed_lyceum, unemployed_below_lyceum,
    dwellings_total, dwellings_owner_occupied, dwellings_rented_total,
    dwellings_detached, dwellings_semi_detached, dwellings_apartment,
    households_total, household_members_total,
    households_1person, households_2person, households_3person,
    households_4person, households_5plus,
    tourism_total_declarations, tourism_total_rental_days,
    tourism_domestic_clients, tourism_foreign_clients, tourism_data_year
)
VALUES (
    %(geo_code)s, %(name_el)s, %(region)s,
    %(population_total)s, %(employed)s, %(unemployed)s,
    %(economically_active)s, %(economically_inactive)s,
    %(students)s, %(retired)s, %(other_inactive)s, %(unemployment_rate)s,
    %(employed_university)s, %(employed_post_secondary)s, %(employed_lyceum)s, %(employed_below_lyceum)s,
    %(unemployed_university)s, %(unemployed_lyceum)s, %(unemployed_below_lyceum)s,
    %(dwellings_total)s, %(dwellings_owner_occupied)s, %(dwellings_rented_total)s,
    %(dwellings_detached)s, %(dwellings_semi_detached)s, %(dwellings_apartment)s,
    %(households_total)s, %(household_members_total)s,
    %(households_1person)s, %(households_2person)s, %(households_3person)s,
    %(households_4person)s, %(households_5plus)s,
    %(tourism_total_declarations)s, %(tourism_total_rental_days)s,
    %(tourism_domestic_clients)s, %(tourism_foreign_clients)s, %(tourism_data_year)s
)
ON DUPLICATE KEY UPDATE
    name_el                    = VALUES(name_el),
    population_total           = VALUES(population_total),
    employed                   = VALUES(employed),
    unemployed                 = VALUES(unemployed),
    unemployment_rate          = VALUES(unemployment_rate),
    updated_at                 = CURRENT_TIMESTAMP
"""


# ─────────────────────────────────────────
#  MAIN LOADER
# ─────────────────────────────────────────

def load():
    # Φόρτωσε JSONs
    log.info("Loading JSONs...")
    with open(SMARTPOLIS_JSON, encoding="utf-8") as f:
        sp_data = json.load(f)
    with open(TOURISM_JSON, encoding="utf-8") as f:
        tourism_data = json.load(f)

    municipalities = sp_data["data"]["municipalities"]
    tourism        = extract_tourism(tourism_data)

    # Σύνδεση στη βάση
    conn = mysql.connector.connect(**DB_CONFIG)
    cur  = conn.cursor()
    log.info("Connected to DB. Starting insert for %d municipalities...", len(municipalities))

    ok = 0
    errors = 0

    for geo_code, mun in municipalities.items():
        try:
            pop    = extract_population(mun)
            record = {
                "geo_code": geo_code,
                "name_el":  mun["name_el"],
                "region":   mun.get("region", ""),
                **pop,
                "unemployment_rate": extract_unemployment_rate(pop),
                **extract_employed_education(mun),
                **extract_unemployed_education(mun),
                **extract_dwellings(mun),
                **extract_households(mun),
                **tourism,
            }
            cur.execute(INSERT_SQL, record)
            ok += 1

        except Exception as e:
            log.warning("❌ Skipped %s (%s): %s", geo_code, mun.get("name_el"), e)
            errors += 1

    conn.commit()
    cur.close()
    conn.close()

    log.info("✅ Done! Inserted/updated: %d | Errors: %d", ok, errors)


if __name__ == "__main__":
    load()