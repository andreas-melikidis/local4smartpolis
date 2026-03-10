"""
pdf_to_json.py  v3 (final)
==========================
Μετατρέπει τα ΕΛΣΤΑΤ PDF (ΤΟΥΡΙΣΜΟΣ) σε JSON για το SMARTPOLIS.

Χρήση:
    python pdf_to_json.py --root "C:\\Users\\andre\\OneDrive\\Desktop\\ΕΛΣΤΑΤ_Excels" --output smartpolis_pdf.json
    python pdf_to_json.py --folder "C:\\...\\ΤΟΥΡΙΣΜΟΣ" --output smartpolis_pdf.json
"""

import os, re, json, argparse
import pdfplumber


# ─────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────

def clean(val):
    if val is None: return None
    s = str(val).strip().replace("\n", " ")
    while "  " in s: s = s.replace("  ", " ")
    return s if s else None


def to_number(s):
    if s is None: return None
    s = str(s).strip().replace(" ", "").replace("%", "")
    if re.match(r"^-?\d{1,3}(\.\d{3})*(,\d+)?$", s):
        s = s.replace(".", "").replace(",", ".")
    elif re.match(r"^-?\d+(,\d+)$", s):
        s = s.replace(",", ".")
    try: return float(s)
    except: return None


def is_year(s):
    return bool(re.match(r"^20\d{2}$", str(s).strip()))


def load_fields(folder_path):
    p = os.path.join(folder_path, "Fields.txt")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return [l.strip() for l in f if l.strip()]
    return []


# ─────────────────────────────────────────────────────────
# PARSER A — Timeseries (Πίνακας 1)
# Δηλώσεις & ημέρες μίσθωσης 2019-2024
# ─────────────────────────────────────────────────────────

def parse_timeseries(table):
    if not table or len(table) < 2: return []
    header = table[0]
    year_cols = {i: int(h.strip()) for i, h in enumerate(header) if h and is_year(h)}
    if not year_cols: return []
    records = []
    for row in table[1:]:
        if not row or not row[0]: continue
        metric = clean(row[0])
        if not metric: continue
        for col_idx, year in year_cols.items():
            if col_idx >= len(row): continue
            val = to_number(row[col_idx])
            if val is None: continue
            records.append({"metric": metric, "year": year, "value": val})
    return records


# ─────────────────────────────────────────────────────────
# PARSER B — Accommodations by region (Πίνακας 2)
# Αριθμός καταλυμάτων ανά περιφέρεια 2024
# ─────────────────────────────────────────────────────────

REGION_KEYWORDS = ["ΑΤΤΙΚ","ΜΑΚΕΔΟΝ","ΚΡΗΤ","ΑΙΓΑΙ","ΘΡΑΚ","ΘΕΣΣΑΛ",
                   "ΗΠΕΙΡ","ΙΟΝΙ","ΠΕΛΟΠΟΝ","ΣΤΕΡΕ","ΔΥΤΙΚ","ΒΟΡΕΙ",
                   "ΝΟΤΙ","ΑΝΑΤΟΛ","ΚΕΝΤΡ","ΠΕΡΙΦΕΡ","ΣΥΝΟΛΟ"]


def _is_region(s):
    if not s: return False
    return any(kw in s.upper() for kw in REGION_KEYWORDS)


def parse_regional_simple(table):
    if not table or len(table) < 2: return []
    header = [clean(h) for h in table[0]]
    if sum(1 for row in table[1:8] if row and _is_region(clean(row[0]))) < 2:
        return []
    records = []
    for row in table[1:]:
        if not row or not row[0]: continue
        region = clean(row[0])
        if not region: continue
        rec = {"region": region}
        for i, h in enumerate(header[1:], 1):
            if i < len(row):
                key = h or f"col_{i}"
                num = to_number(row[i])
                rec[key] = num if num is not None else clean(row[i])
        records.append(rec)
    return records


# ─────────────────────────────────────────────────────────
# PARSER C — Rental days by region (Πίνακας 3)
# Word-coordinate based για σωστό handling split ονομάτων
# ─────────────────────────────────────────────────────────

KNOWN_REGIONS = [
    "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ & ΘΡΑΚΗΣ", "ΑΤΤΙΚΗΣ", "ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ",
    "ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ", "ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ", "ΗΠΕΙΡΟΥ", "ΘΕΣΣΑΛΙΑΣ",
    "ΙΟΝΙΩΝ ΝΗΣΩΝ", "ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ", "ΚΡΗΤΗΣ",
    "ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ", "ΠΕΛΟΠΟΝΝΗΣΟΥ", "ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ",
]

# x-ranges ανά στήλη (βαθμονομημένα από word analysis)
COL_YEAR_X = {2019:(85,130), 2020:(130,175), 2021:(215,265),
               2022:(295,345), 2023:(380,430), 2024:(465,520)}
COL_PCT_X  = {"pct_2020_2019":(175,215), "pct_2021_2020":(260,300),
               "pct_2022_2021":(345,385), "pct_2023_2022":(430,470),
               "pct_2024_2023":(515,555)}
COL_CONTRIB_X = (555, 600)
REGION_X_MAX  = 85


def _assign_col(x0):
    for yr, (xmin, xmax) in COL_YEAR_X.items():
        if xmin <= x0 < xmax: return str(yr)
    for name, (xmin, xmax) in COL_PCT_X.items():
        if xmin <= x0 < xmax: return name
    if COL_CONTRIB_X[0] <= x0 < COL_CONTRIB_X[1]: return "pct_contribution_2024"
    return None


def _reconstruct_region(parts):
    candidate = " ".join(p.strip() for p in parts if p.strip()).upper()
    for known in KNOWN_REGIONS:
        if candidate == known: return known
    for known in KNOWN_REGIONS:
        if candidate in known and len(candidate) >= 5: return known
    return candidate if candidate else None


def parse_regional_timeseries_words(page):
    words = page.extract_words(x_tolerance=3, y_tolerance=3)
    if not words: return []

    full_text = " ".join(w["text"] for w in words)
    # Ελέγχουμε ότι η σελίδα έχει τον Πίνακα 3 ΚΑΙ πραγματικά δεδομένα περιφερειών
    if "Πίνακας 3" not in full_text or "ΑΝΑΤΟΛΙΚΗΣ" not in full_text:
        return []

    # Ομαδοποίηση σε γραμμές (4pt snap)
    lines = {}
    for w in words:
        top = round(w["top"] / 4) * 4
        lines.setdefault(top, []).append(w)
    sorted_tops = sorted(lines.keys())

    # Header: γραμμή με "2019" ΚΑΙ "2024"
    header_top = next(
        (t for t in sorted_tops
         if "2019" in [w["text"] for w in lines[t]]
         and "2024" in [w["text"] for w in lines[t]]),
        None
    )
    if header_top is None: return []

    # Data start: πρώτη γραμμή > header+15 με αριθμό στη data zone
    data_start_top = None
    for t in sorted_tops:
        if t <= header_top + 30: continue
        for w in lines[t]:
            if w["x0"] > REGION_X_MAX and to_number(w["text"]) is not None:
                data_start_top = t
                break
        if data_start_top: break
    if data_start_top is None: return []

    # Footer rows
    footer_tops = {t for t in sorted_tops
                   if any(kw in " ".join(w["text"] for w in lines[t])
                          for kw in ["Γενικό", "Ημεδαποί", "Αλλοδαποί"])}

    data_tops = [t for t in sorted_tops
                 if t >= data_start_top and t not in footer_tops]

    all_data = [{"top": t, "x0": w["x0"], "text": w["text"]}
                for t in data_tops for w in lines[t]]

    # Γραμμές με αριθμητικά values στη data zone
    numeric_tops = sorted({
        item["top"] for item in all_data
        if item["x0"] > REGION_X_MAX and to_number(item["text"]) is not None
    })

    records = []
    used_name_tops = set()

    for num_top in numeric_tops:
        # Data values αυτής της γραμμής
        row_data = {}
        for w in all_data:
            if w["top"] == num_top and w["x0"] > REGION_X_MAX:
                col = _assign_col(w["x0"])
                if col:
                    val = to_number(w["text"])
                    if val is not None: row_data[col] = val

        # Κρατάμε μόνο γραμμές με τουλάχιστον ένα rental_days value
        if not any(row_data.get(str(yr)) for yr in [2019,2020,2021,2022,2023,2024]):
            continue

        # Όνομα περιφέρειας: words με x0 < REGION_X_MAX σε ±20pt
        name_candidates = sorted(
            [(item["top"], item["text"]) for item in all_data
             if item["x0"] < REGION_X_MAX
             and abs(item["top"] - num_top) <= 20
             and item["top"] not in used_name_tops],
            key=lambda x: x[0]
        )
        region_name = _reconstruct_region([p[1] for p in name_candidates])
        used_name_tops.update(p[0] for p in name_candidates)

        # pct_contribution: ψάχνουμε και σε ±16pt (offset rendering)
        contrib = row_data.get("pct_contribution_2024")
        if contrib is None:
            for item in all_data:
                if item["x0"] >= COL_CONTRIB_X[0] and abs(item["top"] - num_top) <= 16:
                    val = to_number(item["text"])
                    if val is not None:
                        contrib = val
                        break

        rec = {"region": region_name or "UNKNOWN"}
        for yr in [2019, 2020, 2021, 2022, 2023, 2024]:
            rec[f"rental_days_{yr}"] = row_data.get(str(yr))
        rec["pct_change_2020_2019"] = row_data.get("pct_2020_2019")
        rec["pct_change_2021_2020"] = row_data.get("pct_2021_2020")
        rec["pct_change_2022_2021"] = row_data.get("pct_2022_2021")
        rec["pct_change_2023_2022"] = row_data.get("pct_2023_2022")
        rec["pct_change_2024_2023"] = row_data.get("pct_2024_2023")
        rec["pct_contribution_2024"] = contrib
        records.append(rec)

    # Footer rows (Γενικό Άθροισμα, Ημεδαποί, Αλλοδαποί)
    for ft in sorted(footer_tops):
        fw = [w for w in all_data if w["top"] == ft] + \
             [{"top": ft, "x0": w["x0"], "text": w["text"]}
              for w in lines.get(ft, [])]
        label = " ".join(w["text"] for w in fw if w["x0"] < REGION_X_MAX).strip()
        row_data = {}
        for w in fw:
            if w["x0"] > REGION_X_MAX:
                col = _assign_col(w["x0"])
                if col:
                    val = to_number(w["text"])
                    if val is not None: row_data[col] = val
        if not any(row_data.get(str(yr)) for yr in [2019,2020,2021,2022,2023,2024]):
            continue
        rec = {"region": label or "TOTAL"}
        for yr in [2019, 2020, 2021, 2022, 2023, 2024]:
            rec[f"rental_days_{yr}"] = row_data.get(str(yr))
        rec["pct_change_2020_2019"] = row_data.get("pct_2020_2019")
        rec["pct_change_2021_2020"] = row_data.get("pct_2021_2020")
        rec["pct_change_2022_2021"] = row_data.get("pct_2022_2021")
        rec["pct_change_2023_2022"] = row_data.get("pct_2023_2022")
        rec["pct_change_2024_2023"] = row_data.get("pct_2024_2023")
        rec["pct_contribution_2024"] = row_data.get("pct_contribution_2024")
        records.append(rec)

    return records


# ─────────────────────────────────────────────────────────
# PARSER D — Monthly seasonality (Γράφημα 3)
# x-coordinate matching ανά μήνα
# ─────────────────────────────────────────────────────────

MONTHS_EL   = ["ΙΑΝ","ΦΕΒ","ΜΑΡ","ΑΠΡ","ΜΑΙ","ΙΟΥΝ","ΙΟΥΛ","ΑΥΓ","ΣΕΠ","ΟΚΤ","ΝΟΕ","ΔΕΚ"]
MONTHS_VAR  = ["ΙΑΝ","ΦΕΒ","ΜΑΡ","ΑΠΡ","ΜΑΙ","ΜAI","ΙΟΥΝ","ΙΟΥΛ","ΑΥΓ","ΣΕΠ","ΟΚΤ","ΝΟΕ","ΔΕΚ"]
MONTH_NAMES = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]


def parse_seasonality_words(page):
    words = page.extract_words()
    full_text = " ".join(w["text"] for w in words)
    if "Εποχικότητα" not in full_text: return []

    # Month labels sorted by x0
    month_words = sorted(
        [(w["x0"], w["text"]) for w in words if w["text"] in MONTHS_VAR],
        key=lambda x: x[0]
    )
    if len(month_words) < 12: return []

    month_label_top = next((w["top"] for w in words if w["text"] in MONTHS_VAR), None)
    if month_label_top is None: return []

    # % values above month labels
    pct_data = [
        (round(w["x0"]), to_number(w["text"]))
        for w in words
        if "%" in w["text"] and w["top"] < month_label_top
        and to_number(w["text"]) is not None and 0 < to_number(w["text"]) < 25
    ]
    if len(pct_data) < 12: return []

    # x-matching (tolerance 25pt)
    records = []
    for i, (mx, _) in enumerate(month_words[:12]):
        best_val, best_dist = None, 999
        for px, val in pct_data:
            d = abs(px - mx)
            if d < best_dist and d < 25:
                best_dist, best_val = d, val
        records.append({
            "month_index":          i + 1,
            "month_el":             MONTHS_EL[i],
            "month_en":             MONTH_NAMES[i],
            "rental_days_pct_2024": best_val
        })
    return records if any(r["rental_days_pct_2024"] is not None for r in records) else []


# ─────────────────────────────────────────────────────────
# Main PDF processor
# ─────────────────────────────────────────────────────────

def process_pdf(pdf_path, fields_hint=None):
    print(f"  📄 {os.path.basename(pdf_path)}")
    result = {
        "source_file":              os.path.basename(pdf_path),
        "fields_of_interest":       fields_hint or [],
        "timeseries":               [],
        "accommodations_by_region": [],
        "rental_days_by_region":    [],
        "monthly_seasonality":      [],
    }
    try:
        with pdfplumber.open(pdf_path) as pdf:
            print(f"     Σελίδες: {len(pdf.pages)}")
            for page_num, page in enumerate(pdf.pages, 1):
                # Word-based parsers
                if not result["rental_days_by_region"]:
                    rdt = parse_regional_timeseries_words(page)
                    if rdt:
                        result["rental_days_by_region"] = rdt
                        print(f"     ✅ Σελ.{page_num}: rental_days_by_region ({len(rdt)} records)")

                if not result["monthly_seasonality"]:
                    sea = parse_seasonality_words(page)
                    if sea:
                        result["monthly_seasonality"] = sea
                        print(f"     ✅ Σελ.{page_num}: monthly_seasonality (12 μήνες)")

                # Table-based parsers
                for t_idx, raw in enumerate(page.extract_tables() or []):
                    table = [[clean(c) for c in row] for row in raw]
                    table = [r for r in table if any(c for c in r)]

                    if not result["timeseries"]:
                        ts = parse_timeseries(table)
                        if ts:
                            result["timeseries"] = ts
                            print(f"     ✅ Σελ.{page_num} T{t_idx+1}: timeseries ({len(ts)} records)")
                            continue

                    if not result["accommodations_by_region"]:
                        reg = parse_regional_simple(table)
                        if reg:
                            result["accommodations_by_region"] = reg
                            print(f"     ✅ Σελ.{page_num} T{t_idx+1}: accommodations_by_region ({len(reg)} records)")
    except Exception as e:
        import traceback; traceback.print_exc()
        result["error"] = str(e)
    return result


def process_folder(folder_path):
    fields = load_fields(folder_path)
    pdfs = sorted(f for f in os.listdir(folder_path) if f.lower().endswith(".pdf"))
    if not pdfs: return None
    return {
        "source_folder":      os.path.basename(folder_path),
        "fields_of_interest": fields,
        "files": [process_pdf(os.path.join(folder_path, f), fields) for f in pdfs]
    }


# ─────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="ΕΛΣΤΑΤ PDF → JSON (SMARTPOLIS)")
    ap.add_argument("--root",   default=None)
    ap.add_argument("--folder", default=None)
    ap.add_argument("--output", default="smartpolis_pdf.json")
    ap.add_argument("--indent", type=int, default=2)
    args = ap.parse_args()
    if not args.root and not args.folder:
        print("❌ Δώσε --root ή --folder"); return

    output = {
        "metadata": {
            "project": "SMARTPOLIS", "source": "ΕΛΣΤΑΤ",
            "description": "Tourism & short-term rental data",
            "generated_by": "pdf_to_json.py v3"
        },
        "data": {}
    }

    if args.folder:
        folders = [args.folder]
    else:
        folders = [
            os.path.join(args.root, d) for d in os.listdir(args.root)
            if os.path.isdir(os.path.join(args.root, d))
            and any(f.lower().endswith(".pdf") for f in os.listdir(os.path.join(args.root, d)))
        ]

    print(f"\n🔍 {len(folders)} φάκελοι με PDF\n")
    for fp in sorted(folders):
        fn = os.path.basename(fp)
        print(f"📁 {fn}")
        res = process_folder(fp)
        if res: output["data"][fn.lower()] = res
        print()

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=args.indent, default=str)
    print(f"✅ {args.output}")


if __name__ == "__main__":
    main()