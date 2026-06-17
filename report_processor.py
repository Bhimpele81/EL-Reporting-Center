"""
EL Reporting Center — Report Processor
----------------------------------------
Transforms raw camp management CSV exports into formatted Excel workbooks.
"""

import csv
import datetime
import io
import json
import os
import re
from datetime import date

from openpyxl import Workbook
from openpyxl.styles import (
    Alignment, Border, Font, PatternFill, Side
)
from openpyxl.utils import get_column_letter


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_bunk_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return json.load(f)


def save_bunk_config(config_path: str, config: dict) -> None:
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)


def _norm(s: str) -> str:
    """Normalize a bunk name for matching: strip, collapse spaces, normalize dashes."""
    return re.sub(r"\s+", " ", str(s).strip()).replace("–", "-").replace("—", "-")


def get_bunk_lookup(config: dict) -> dict:
    """Return {normalized_bunk_name: {number, camp, original}} from config."""
    lookup = {}
    for camp in config.get("camps", []):
        for bunk in camp.get("bunks", []):
            lookup[_norm(bunk["name"])] = {
                "number": bunk["number"],
                "camp": camp["name"],
                "original": bunk["name"],
            }
    return lookup


def get_ordered_bunks(config: dict) -> list:
    """Return list of bunk names in display order (camp order, then bunk number order)."""
    bunks = []
    for camp in config.get("camps", []):
        for bunk in sorted(camp.get("bunks", []), key=lambda b: int(b.get("number") or 999)):
            bunks.append(_norm(bunk["name"]))
    return bunks


# ---------------------------------------------------------------------------
# Grade normalizer
# ---------------------------------------------------------------------------

def normalize_grade(raw: str) -> str:
    g = str(raw).strip()
    if not g or g.lower() == "nan":
        return ""
    if g.lower().startswith("pre"):
        return "P"
    if g.lower() == "k":
        return "K"
    m = re.match(r"^(\d+)", g)
    if m:
        return m.group(1)
    return g


# ---------------------------------------------------------------------------
# Raw CSV parser
# ---------------------------------------------------------------------------

WEEK_RE = re.compile(r"Week\s+(\d+)", re.IGNORECASE)


def _detect_col(header: list, keywords: list, fallback: int) -> int:
    """Return the first column index whose header contains ALL keywords (case-insensitive)."""
    for i, h in enumerate(header):
        h_lower = str(h).lower()
        if all(kw in h_lower for kw in keywords):
            return i
    return fallback


def _rows_to_campers(rows: list) -> list:
    """
    Convert a list of rows (list-of-strings) into camper dicts.

    Expected columns (0-indexed):
      0  row#
      1  Last name
      2  First name
      3  Bunk name   (e.g. "01 Munchkins")
      4  Session name (e.g. "Week 1, Week 3 (Camp Photos), Week 4")
      5  Age + months
      6  Current grade
      7  Monday?     (Yes / No / blank)
      8  Tuesday?
      9  Wednesday?
      10 Thursday?
      11 Friday?
      ?  Driver      (detected by header name — column position varies)
    """
    # Detect the driver column by scanning header for any cell containing "driver"
    header = rows[0] if rows else []
    driver_col = None
    for i, h in enumerate(header):
        if "driver" in str(h).strip().lower():
            driver_col = i
            break

    campers = []
    for row in rows[1:]:          # skip header
        if len(row) < 4 or not str(row[0]).strip().isdigit():
            continue

        last     = str(row[1]).strip()
        first    = str(row[2]).strip()
        bunk     = _norm(row[3])
        sessions = str(row[4]).strip() if len(row) > 4 else ""
        age      = str(row[5]).strip() if len(row) > 5 else ""
        grade    = normalize_grade(row[6]) if len(row) > 6 else ""
        mon      = str(row[7]).strip()  if len(row) > 7  else ""
        tue      = str(row[8]).strip()  if len(row) > 8  else ""
        wed      = str(row[9]).strip()  if len(row) > 9  else ""
        thu      = str(row[10]).strip() if len(row) > 10 else ""
        fri      = str(row[11]).strip() if len(row) > 11 else ""

        # Driver: read from detected column; skip None/nan/empty values
        raw_driver = str(row[driver_col]).strip() if (driver_col is not None and driver_col < len(row)) else ""
        driver = "" if raw_driver.lower() in ("", "none", "nan", "n/a", "#n/a") else raw_driver

        weeks = [0] * 8
        for part in sessions.split(","):
            m = WEEK_RE.search(part)
            if m:
                wk = int(m.group(1))
                if 1 <= wk <= 8:
                    weeks[wk - 1] = 1

        any_day_specified = any(
            d.lower() in ("yes", "no") for d in [mon, tue, wed, thu, fri]
        )
        if any_day_specified:
            day_m = "M" if mon.lower() == "yes" else None
            day_t = "T" if tue.lower() == "yes" else None
            day_w = "W" if wed.lower() == "yes" else None
            day_r = "R" if thu.lower() == "yes" else None
            day_f = "F" if fri.lower() == "yes" else None
        else:
            day_m, day_t, day_w, day_r, day_f = "M", "T", "W", "R", "F"

        campers.append({
            "name":   f"{last}, {first}",
            "bunk":   bunk,
            "weeks":  weeks,
            "days":   [day_m, day_t, day_w, day_r, day_f],
            "age":    age,
            "grade":  grade,
            "driver": driver,
        })

    return campers


def parse_raw_csv(file_bytes: bytes) -> list:
    """Parse a raw bunk-snapshot export — accepts CSV or XLSX."""
    # XLSX files start with the ZIP magic bytes PK\x03\x04
    if file_bytes[:4] == b'PK\x03\x04':
        from openpyxl import load_workbook
        wb = load_workbook(filename=io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb.active
        rows = [[str(cell.value) if cell.value is not None else "" for cell in row]
                for row in ws.iter_rows()]
        wb.close()
        return _rows_to_campers(rows)

    content = file_bytes.decode("utf-8-sig", errors="replace")
    reader  = csv.reader(io.StringIO(content))
    rows    = list(reader)
    return _rows_to_campers(rows)


def parse_driver_csv(file_bytes: bytes) -> list:
    """
    Parse a raw Driver Totals export (CSV or XLSX).

    Expected columns (0-indexed, detected by header name with fallbacks):
      0  row#
      1  Last name
      2  First name
      3  Driver        (header contains "driver")
      4  Bunk name     (header contains "bunk")
      5  Session name  (header contains "session")
      6  Age           (header contains "age")
      7  Grade         (header contains "grade")
      8  Monday?       (header contains "monday" or "mon")
      9  Tuesday?
      10 Wednesday?
      11 Thursday?
      12 Friday?
    """
    if file_bytes[:4] == b'PK\x03\x04':
        from openpyxl import load_workbook as _lw
        _wb = _lw(filename=io.BytesIO(file_bytes), read_only=True, data_only=True)
        _ws = _wb.active
        rows = [[str(c.value) if c.value is not None else "" for c in r]
                for r in _ws.iter_rows()]
        _wb.close()
    else:
        content = file_bytes.decode("utf-8-sig", errors="replace")
        rows = list(csv.reader(io.StringIO(content)))

    if not rows:
        return []

    header = rows[0]
    driver_col  = _detect_col(header, ["driver"],    3)
    stop_col    = _detect_col(header, ["stop"],      None)   # optional column
    bunk_col    = _detect_col(header, ["bunk"],      5)
    session_col = _detect_col(header, ["session"],   6)
    age_col     = _detect_col(header, ["age"],       7)
    grade_col   = _detect_col(header, ["grade"],     8)
    mon_col     = _detect_col(header, ["monday"],    9)
    tue_col     = _detect_col(header, ["tuesday"],   10)
    wed_col     = _detect_col(header, ["wednesday"], 11)
    thu_col     = _detect_col(header, ["thursday"],  12)
    fri_col     = _detect_col(header, ["friday"],    13)

    def _val(row, col):
        return str(row[col]).strip() if col < len(row) else ""

    campers = []
    for row in rows[1:]:
        if len(row) < 2 or not str(row[0]).strip().isdigit():
            continue

        last     = _val(row, 1)
        first    = _val(row, 2)
        raw_drv  = _val(row, driver_col)
        driver   = "" if raw_drv.lower() in ("", "none", "nan", "n/a", "#n/a") else raw_drv

        # Stop #: optional — only present in newer exports
        if stop_col is not None and stop_col < len(row):
            try:
                stop_val = int(float(_val(row, stop_col)))
            except (ValueError, TypeError):
                stop_val = None
        else:
            stop_val = None

        bunk     = _val(row, bunk_col)
        sessions = _val(row, session_col)
        raw_age  = _val(row, age_col)
        raw_grade = _val(row, grade_col)

        # Age: keep as float if possible
        try:
            age_val = float(raw_age)
        except (ValueError, TypeError):
            age_val = raw_age if raw_age else None

        grade_val = normalize_grade(raw_grade)

        mon = _val(row, mon_col)
        tue = _val(row, tue_col)
        wed = _val(row, wed_col)
        thu = _val(row, thu_col)
        fri = _val(row, fri_col)

        weeks = [0] * 8
        for part in sessions.split(","):
            m = WEEK_RE.search(part)
            if m:
                wk = int(m.group(1))
                if 1 <= wk <= 8:
                    weeks[wk - 1] = 1

        any_day_specified = any(d.lower() in ("yes", "no") for d in [mon, tue, wed, thu, fri])
        if any_day_specified:
            day_m = "M" if mon.lower() == "yes" else None
            day_t = "T" if tue.lower() == "yes" else None
            day_w = "W" if wed.lower() == "yes" else None
            day_r = "R" if thu.lower() == "yes" else None
            day_f = "F" if fri.lower() == "yes" else None
        else:
            day_m, day_t, day_w, day_r, day_f = "M", "T", "W", "R", "F"

        campers.append({
            "name":   f"{last}, {first}",
            "bunk":   bunk,
            "driver": driver,
            "stop":   stop_val,
            "weeks":  weeks,
            "days":   [day_m, day_t, day_w, day_r, day_f],
            "age":    age_val,
            "grade":  grade_val,
        })

    return campers


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

# ---- Styles ----------------------------------------------------------------

BRAND     = "6D1F2F"
BRAND_ALT = "F5E6E9"
WHITE     = "FFFFFF"
LIGHT_GREY = "F2F2F2"
DARK_GREY  = "1A1018"

_thin = Side(style="thin", color="CCCCCC")
_med  = Side(style="medium", color="AAAAAA")
THIN_BORDER = Border(left=_thin, right=_thin, top=_thin, bottom=_thin)
MED_BORDER  = Border(left=_med,  right=_med,  top=_med,  bottom=_med)

HEADER_FONT   = Font(name="Calibri", bold=True, color=WHITE, size=10)
SUBHDR_FONT   = Font(name="Calibri", bold=True, size=10)
BODY_FONT     = Font(name="Calibri", size=10)
TOTAL_FONT    = Font(name="Calibri", bold=True, size=10)
DATE_FONT     = Font(name="Calibri", bold=True, size=11)

BRAND_FILL    = PatternFill("solid", fgColor=BRAND)
ALT_FILL      = PatternFill("solid", fgColor=BRAND_ALT)
LGREY_FILL    = PatternFill("solid", fgColor="EEEEEE")
TOTAL_FILL    = PatternFill("solid", fgColor="D9D9D9")

CENTER = Alignment(horizontal="center", vertical="center")
LEFT   = Alignment(horizontal="left",   vertical="center")
RIGHT  = Alignment(horizontal="right",  vertical="center")


def _cell(ws, row, col, value, font=None, fill=None, align=None, border=None):
    c = ws.cell(row=row, column=col, value=value)
    if font:   c.font   = font
    if fill:   c.fill   = fill
    if align:  c.alignment = align
    if border: c.border = border
    return c


# ---------------------------------------------------------------------------
# Build the "Report" sheet
# ---------------------------------------------------------------------------

def build_report_sheet(ws, campers: list, bunk_lookup: dict,
                        ordered_bunks: list, report_date: date):

    # ----- Row 1: date header -----------------------------------------------
    ws.row_dimensions[1].height = 18
    _cell(ws, 1, 1, "Report Date:", font=DATE_FONT, align=RIGHT)
    _cell(ws, 1, 2, report_date.strftime("%-m/%-d/%Y") if os.name != "nt"
          else report_date.strftime("%#m/%#d/%Y"),
          font=DATE_FONT, align=Alignment(horizontal="center", vertical="center"))

    # ----- Row 2: column headers --------------------------------------------
    headers = [
        "Child", "Bunk",
        "#1", "#2", "#3", "#4", "#5", "#6", "#7", "#8",
        "Days",    # merged across cols 11-15
        None, None, None, None,   # M T W R F placeholders
        "Age", "Grade", None,
    ]
    day_labels = ["M", "T", "W", "R", "F"]

    ws.row_dimensions[2].height = 15
    for ci, h in enumerate(headers, start=1):
        if h is None:
            continue
        c = ws.cell(row=2, column=ci, value=h)
        c.font   = HEADER_FONT
        c.fill   = BRAND_FILL
        c.alignment = CENTER
        c.border = THIN_BORDER

    # Merge "Days" across cols 11-15
    ws.merge_cells(start_row=2, start_column=11,
                   end_row=2,   end_column=15)
    ws.cell(row=2, column=11).font      = HEADER_FONT
    ws.cell(row=2, column=11).fill      = BRAND_FILL
    ws.cell(row=2, column=11).alignment = CENTER
    ws.cell(row=2, column=11).border    = THIN_BORDER

    # ----- Group campers by bunk -------------------------------------------
    # First: collect all bunk names that appear in the data
    bunk_groups = {}
    for c in campers:
        bunk_groups.setdefault(c["bunk"], []).append(c)

    # Sort campers alphabetically within each bunk
    for bk in bunk_groups:
        bunk_groups[bk].sort(key=lambda x: x["name"])

    # Build the ordered list: bunks in config order, then unknown bunks
    display_order = []
    for bk in ordered_bunks:
        if bk in bunk_groups:
            display_order.append(bk)
    for bk in sorted(bunk_groups.keys()):
        if bk not in display_order:
            display_order.append(bk)   # unknowns at bottom as "unassigned"

    # Separate "unassigned" (not in config) from known bunks
    unknown_bunks = [b for b in display_order if b not in bunk_lookup]
    known_bunks   = [b for b in display_order if b in bunk_lookup]
    display_order = unknown_bunks + known_bunks

    # ----- Write rows -------------------------------------------------------
    row = 3
    total_col = 18   # Column R
    max_col_a = len("Child")   # track max width for column A autofit
    max_col_r = len("Total:   00")  # track max width for column R autofit

    for bk_idx, bunk_name in enumerate(display_order):
        group = bunk_groups[bunk_name]
        week_sums = [0] * 8

        for ci, camper in enumerate(group):
            alt = (ci % 2 == 1)
            fill = ALT_FILL if alt else None

            _cell(ws, row, 1,  camper["name"],  font=BODY_FONT, fill=fill, align=LEFT,   border=THIN_BORDER)
            _cell(ws, row, 2,  bunk_name,        font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
            max_col_a = max(max_col_a, len(str(camper["name"] or "")))

            for wi, wv in enumerate(camper["weeks"]):
                _cell(ws, row, 3 + wi, wv,
                      font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
                week_sums[wi] += wv

            for di, dv in enumerate(camper["days"]):
                _cell(ws, row, 11 + di, dv,
                      font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)

            # Store age/grade as numbers — try float first to handle decimals
            age_val = camper["age"]
            try: age_val = float(str(age_val).strip())
            except (ValueError, TypeError): age_val = None
            grade_val = camper["grade"]
            try: grade_val = int(float(str(grade_val).strip()))
            except (ValueError, TypeError): grade_val = None

            _cell(ws, row, 16, age_val,   font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
            _cell(ws, row, 17, grade_val, font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
            row += 1

        # --- Subtotal row ---
        total_text = f"Total:   {len(group)}"
        max_col_r = max(max_col_r, len(total_text))
        _cell(ws, row, 1,  None,  font=TOTAL_FONT, fill=TOTAL_FILL, border=THIN_BORDER)
        _cell(ws, row, 2,  None,  font=TOTAL_FONT, fill=TOTAL_FILL, align=CENTER, border=THIN_BORDER)
        for wi, ws_val in enumerate(week_sums):
            _cell(ws, row, 3 + wi, ws_val,
                  font=TOTAL_FONT, fill=TOTAL_FILL, align=CENTER, border=THIN_BORDER)
        for di in range(5):
            _cell(ws, row, 11 + di, None, fill=TOTAL_FILL, border=THIN_BORDER)
        _cell(ws, row, 16, None, fill=TOTAL_FILL, border=THIN_BORDER)
        _cell(ws, row, 17, None, fill=TOTAL_FILL, border=THIN_BORDER)
        _cell(ws, row, total_col,
              total_text,
              font=TOTAL_FONT, fill=TOTAL_FILL, align=LEFT, border=THIN_BORDER)
        row += 1

        # Page break after each bunk (except the last)
        if bk_idx < len(display_order) - 1:
            from openpyxl.worksheet.pagebreak import Break
            ws.row_breaks.append(Break(id=row - 1))

    # ----- Column widths ----------------------------------------------------
    # Autofit A and R by scanning actual written cell values
    last_row = row - 1
    max_a = max((len(str(ws.cell(row=r, column=1).value or ""))
                 for r in range(1, last_row + 1)), default=5)
    max_r = max((len(str(ws.cell(row=r, column=18).value or ""))
                 for r in range(1, last_row + 1)), default=5)

    ws.column_dimensions["A"].width = int(max_a * 0.9)
    ws.column_dimensions["B"].width = 16
    for col_letter in [get_column_letter(c) for c in range(3, 11)]:
        ws.column_dimensions[col_letter].width = 5   # #1-#8
    for col_letter in [get_column_letter(c) for c in range(11, 16)]:
        ws.column_dimensions[col_letter].width = 4   # M T W R F
    ws.column_dimensions["P"].width = 6
    ws.column_dimensions["Q"].width = 6
    ws.column_dimensions["R"].width = int(max_r * 0.9)

    # ----- Suppress green error indicators in P and Q ----------------------
    # openpyxl 3.1.x has no IgnoredErrors helper — inject XML directly
    if last_row >= 3:
        try:
            from lxml import etree
            ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
            ie_elem = etree.SubElement(ws._element,
                                       f"{{{ns}}}ignoredErrors")
            err = etree.SubElement(ie_elem, f"{{{ns}}}ignoredError")
            err.set("sqref", f"P3:Q{last_row}")
            err.set("numberStoredAsText", "1")
            err.set("formula", "1")
            err.set("formulaRange", "1")
        except Exception:
            pass

    # Freeze panes below header
    ws.freeze_panes = "A3"

    # ----- Print settings: landscape, fit to 1 page wide, header rows repeat -
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToPage   = True
    ws.page_setup.fitToWidth  = 1
    ws.page_setup.fitToHeight = 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    ws.print_title_rows = "1:2"


# ---------------------------------------------------------------------------
# Build the "Totals" sheet
# ---------------------------------------------------------------------------

def build_totals_sheet(ws, campers: list, config: dict,
                        bunk_lookup: dict, report_date: date):

    # Pre-compute per-bunk counts and week totals
    bunk_count = {}   # bunk_name -> total campers
    bunk_weeks = {}   # bunk_name -> [w1..w8]

    for c in campers:
        bk = c["bunk"]
        bunk_count[bk] = bunk_count.get(bk, 0) + 1
        if bk not in bunk_weeks:
            bunk_weeks[bk] = [0] * 8
        for wi, wv in enumerate(c["weeks"]):
            bunk_weeks[bk][wi] += wv

    # Per-camp totals
    camp_count = {}   # camp -> total
    camp_weeks = {}   # camp -> [w1..w8]
    for camp in config["camps"]:
        cn = camp["name"]
        camp_count[cn] = 0
        camp_weeks[cn] = [0] * 8
        for bunk in camp["bunks"]:
            bk = bunk["name"]
            camp_count[cn] += bunk_count.get(bk, 0)
            for wi in range(8):
                camp_weeks[cn][wi] += bunk_weeks.get(bk, [0]*8)[wi]

    grand_total = sum(camp_count.values())
    grand_weeks = [sum(camp_weeks[c][wi] for c in camp_weeks) for wi in range(8)]

    # ---- Layout constants --------------------------------------------------
    #  LEFT block   : cols A-C  (Camp | Bunk | Count)
    #  GAP          : col D
    #  MIDDLE block : cols E-G  (Camp | Total | gap)
    #  GAP          : col H
    #  RIGHT block  : cols I-Q  (Group totals by week, #1-#8)
    # Then a gap row, then Bunk Totals by Week block below

    LEFT_C   = 1   # Camp col
    LEFT_B   = 2   # Bunk col
    LEFT_N   = 3   # Count col
    MID_C    = 5   # Camp col
    MID_T    = 6   # Group total
    RT_LABEL = 9   # Right-section label col
    RT_W1    = 10  # Right #1 .. #8

    # ----- Row 1: date ------------------------------------------------------
    _cell(ws, 1, 1, "Report Date", font=SUBHDR_FONT)
    _cell(ws, 1, 2, report_date.strftime("%-m/%-d/%Y") if os.name != "nt"
          else report_date.strftime("%#m/%#d/%Y"),
          font=BODY_FONT)

    # ----- Row 2: section headers -------------------------------------------
    _cell(ws, 2, LEFT_C, "Bunk Totals",          font=HEADER_FONT, fill=BRAND_FILL, align=CENTER, border=THIN_BORDER)
    _cell(ws, 2, MID_C,  "Group Totals",          font=HEADER_FONT, fill=BRAND_FILL, align=CENTER, border=THIN_BORDER)
    _cell(ws, 2, RT_LABEL, "Group Totals by Week",font=HEADER_FONT, fill=BRAND_FILL, align=CENTER, border=THIN_BORDER)

    # Merge Bunk Totals header across 3 cols
    ws.merge_cells(start_row=2, start_column=LEFT_C, end_row=2, end_column=LEFT_N)
    ws.merge_cells(start_row=2, start_column=MID_C,  end_row=2, end_column=MID_T)
    ws.merge_cells(start_row=2, start_column=RT_LABEL, end_row=2, end_column=RT_W1+7)

    # ----- Row 3: sub-headers -----------------------------------------------
    for ci, h in enumerate(["Camp", "Bunk", "Total"], start=LEFT_C):
        _cell(ws, 3, ci, h, font=SUBHDR_FONT, fill=LGREY_FILL, align=CENTER, border=THIN_BORDER)
    _cell(ws, 3, MID_C, "Camp",  font=SUBHDR_FONT, fill=LGREY_FILL, align=CENTER, border=THIN_BORDER)
    _cell(ws, 3, MID_T, "Total", font=SUBHDR_FONT, fill=LGREY_FILL, align=CENTER, border=THIN_BORDER)

    # Right section week headers in row 3
    _cell(ws, 3, RT_LABEL, None, fill=LGREY_FILL, border=THIN_BORDER)
    for wi in range(8):
        _cell(ws, 3, RT_W1 + wi, f"#{wi+1}",
              font=SUBHDR_FONT, fill=LGREY_FILL, align=CENTER, border=THIN_BORDER)

    # ----- Data rows --------------------------------------------------------
    data_row  = 4
    mid_row   = 3    # separate pointer for middle section (starts at row 3 + 1 offset)
    right_row = 4    # separate pointer for right section

    # Right section: group totals by week
    camp_names = [c["name"] for c in config["camps"]]
    for ri, cn in enumerate(camp_names):
        r = right_row + ri
        _cell(ws, r, RT_LABEL, cn, font=BODY_FONT, fill=ALT_FILL if ri%2 else None, align=LEFT, border=THIN_BORDER)
        for wi in range(8):
            _cell(ws, r, RT_W1 + wi, camp_weeks[cn][wi],
                  font=BODY_FONT, fill=ALT_FILL if ri%2 else None, align=CENTER, border=THIN_BORDER)

    # Grand total row for right section
    r_total = right_row + len(camp_names)
    _cell(ws, r_total, RT_LABEL, "Total", font=TOTAL_FONT, fill=TOTAL_FILL, align=LEFT, border=THIN_BORDER)
    for wi in range(8):
        _cell(ws, r_total, RT_W1 + wi, grand_weeks[wi],
              font=TOTAL_FONT, fill=TOTAL_FILL, align=CENTER, border=THIN_BORDER)

    # Left section: per-bunk rows
    for ci, camp in enumerate(config["camps"]):
        cn = camp["name"]
        for bi, bunk in enumerate(sorted(camp["bunks"], key=lambda b: int(b.get("number") or 999))):
            bk = bunk["name"]
            alt = (data_row % 2 == 0)
            fill = ALT_FILL if alt else None
            _cell(ws, data_row, LEFT_C, cn,  font=BODY_FONT, fill=fill, align=LEFT,   border=THIN_BORDER)
            _cell(ws, data_row, LEFT_B, bk,  font=BODY_FONT, fill=fill, align=LEFT,   border=THIN_BORDER)
            _cell(ws, data_row, LEFT_N, bunk_count.get(bk, 0),
                  font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
            data_row += 1

    # Grand total row (left section)
    _cell(ws, data_row, LEFT_C, "TOTAL", font=TOTAL_FONT, fill=TOTAL_FILL, align=LEFT, border=THIN_BORDER)
    _cell(ws, data_row, LEFT_B, None,    font=TOTAL_FONT, fill=TOTAL_FILL, border=THIN_BORDER)
    _cell(ws, data_row, LEFT_N, grand_total, font=TOTAL_FONT, fill=TOTAL_FILL, align=CENTER, border=THIN_BORDER)

    # Middle section: consecutive rows (one per camp), independent of bunk rows
    mid_row = 4
    for ci, camp in enumerate(config["camps"]):
        cn = camp["name"]
        alt = (ci % 2 == 1)
        fill = ALT_FILL if alt else None
        _cell(ws, mid_row, MID_C, cn,            font=BODY_FONT,  fill=fill,       align=LEFT,   border=THIN_BORDER)
        _cell(ws, mid_row, MID_T, camp_count[cn], font=BODY_FONT, fill=fill,       align=CENTER, border=THIN_BORDER)
        mid_row += 1

    # Grand total row (middle section)
    _cell(ws, mid_row, MID_C, "Total",     font=TOTAL_FONT, fill=TOTAL_FILL, align=LEFT,   border=THIN_BORDER)
    _cell(ws, mid_row, MID_T, grand_total, font=TOTAL_FONT, fill=TOTAL_FILL, align=CENTER, border=THIN_BORDER)

    # ----- Bunk Totals by Week section (below right section gap) -----------
    bunk_wk_start = r_total + 2
    _cell(ws, bunk_wk_start, RT_LABEL, "Bunk Totals by Week",
          font=HEADER_FONT, fill=BRAND_FILL, align=CENTER, border=THIN_BORDER)
    ws.merge_cells(start_row=bunk_wk_start, start_column=RT_LABEL,
                   end_row=bunk_wk_start, end_column=RT_W1+7)

    # Sub-header
    bwh = bunk_wk_start + 1
    _cell(ws, bwh, RT_LABEL, None, fill=LGREY_FILL, border=THIN_BORDER)
    for wi in range(8):
        _cell(ws, bwh, RT_W1 + wi, f"#{wi+1}",
              font=SUBHDR_FONT, fill=LGREY_FILL, align=CENTER, border=THIN_BORDER)

    bwr = bwh + 1
    all_bunks_ordered = []
    for camp in config["camps"]:
        all_bunks_ordered.extend([b["name"] for b in sorted(camp["bunks"], key=lambda b: int(b.get("number") or 999))])

    for bi, bk in enumerate(all_bunks_ordered):
        if bk not in bunk_weeks:
            continue
        alt = (bi % 2 == 1)
        fill = ALT_FILL if alt else None
        _cell(ws, bwr, RT_LABEL, bk, font=BODY_FONT, fill=fill, align=LEFT, border=THIN_BORDER)
        for wi in range(8):
            _cell(ws, bwr, RT_W1 + wi, bunk_weeks[bk][wi],
                  font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
        bwr += 1

    # Grand total for bunk-by-week section
    _cell(ws, bwr, RT_LABEL, "Total", font=TOTAL_FONT, fill=TOTAL_FILL, align=LEFT, border=THIN_BORDER)
    for wi in range(8):
        _cell(ws, bwr, RT_W1 + wi, grand_weeks[wi],
              font=TOTAL_FONT, fill=TOTAL_FILL, align=CENTER, border=THIN_BORDER)

    # ----- Column widths ----------------------------------------------------
    ws.column_dimensions["A"].width = 10   # Camp
    ws.column_dimensions["B"].width = 16   # Bunk
    ws.column_dimensions["C"].width = 7    # Count
    ws.column_dimensions["D"].width = 3    # gap
    ws.column_dimensions["E"].width = 10   # Camp
    ws.column_dimensions["F"].width = 7    # Total
    ws.column_dimensions["G"].width = 3    # gap
    ws.column_dimensions["H"].width = 3    # gap
    ws.column_dimensions["I"].width = 18   # Label
    for wi in range(8):
        ws.column_dimensions[get_column_letter(RT_W1 + wi)].width = 6

    # ---- Print settings: landscape, single page ----------------------------
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToPage   = True
    ws.page_setup.fitToWidth  = 1
    ws.page_setup.fitToHeight = 1
    ws.sheet_properties.pageSetUpPr.fitToPage = True


# ---------------------------------------------------------------------------
# Group Attendance parser + builder
# ---------------------------------------------------------------------------

def parse_group_attendance(file_bytes: bytes) -> list:
    """
    Parse raw group attendance CSV/XLSX export.

    Expected columns (0-indexed):
      0  row#
      1  Bunk name
      2  Last name
      3  First name
      4  Monday?   (Yes / No / blank)
      5  Tuesday?
      6  Wednesday?
      7  Thursday?
      8  Friday?
    """
    if file_bytes[:4] == b'PK\x03\x04':
        from openpyxl import load_workbook as _lw
        _wb = _lw(filename=io.BytesIO(file_bytes), read_only=True, data_only=True)
        _ws = _wb.active
        rows = [[str(c.value) if c.value is not None else "" for c in r]
                for r in _ws.iter_rows()]
        _wb.close()
    else:
        content = file_bytes.decode("utf-8-sig", errors="replace")
        rows = list(csv.reader(io.StringIO(content)))

    campers = []
    for row in rows[1:]:
        if len(row) < 4 or not str(row[0]).strip().isdigit():
            continue
        bunk  = str(row[1]).strip()
        last  = str(row[2]).strip()
        first = str(row[3]).strip()
        mon   = str(row[4]).strip() if len(row) > 4 else ""
        tue   = str(row[5]).strip() if len(row) > 5 else ""
        wed   = str(row[6]).strip() if len(row) > 6 else ""
        thu   = str(row[7]).strip() if len(row) > 7 else ""
        fri   = str(row[8]).strip() if len(row) > 8 else ""

        any_specified = any(d.lower() in ("yes", "no") for d in [mon, tue, wed, thu, fri])
        if any_specified:
            enrolled = (
                ("M" if mon.lower() == "yes" else "") +
                ("T" if tue.lower() == "yes" else "") +
                ("W" if wed.lower() == "yes" else "") +
                ("R" if thu.lower() == "yes" else "") +
                ("F" if fri.lower() == "yes" else "")
            )
            if enrolled == "MTWRF":
                enrolled = ""   # full week — treat same as blank
        else:
            enrolled = ""

        campers.append({"name": f"{last}, {first}", "bunk": bunk, "enrolled": enrolled})

    return campers


def build_group_attendance_sheet(ws, campers: list, config: dict,
                                  report_date=None) -> None:
    """
    Build the Data1 sheet for Group Attendance.

    Column layout:
      A  – Bunk name  (merged + rotated 90° for entire bunk group)
      B  – Camper     (bold 16pt)
      C  – MON        (blank signing cell; pre-filled 'C' if camper absent that day)
      D  – TUES
      E  – WED
      F  – THURS
      G  – FRI
      H  – Enrolled
    """
    from openpyxl.worksheet.pagebreak import Break

    if report_date is None:
        report_date = date.today()

    # Bunk sort order from config — use normalized names to avoid exact-match failures
    bunk_order = {}
    for idx, bunk in enumerate(
        b for camp in config.get("camps", []) for b in camp.get("bunks", [])
    ):
        bunk_order[_norm(bunk["name"])] = idx

    def _bunk_sort_key(c):
        # Primary: config order (normalized match); secondary: leading bunk number;
        # tertiary: full bunk name; quaternary: camper name
        config_pos = bunk_order.get(_norm(c["bunk"]), None)
        m = re.match(r'^(\d+)', str(c["bunk"]).strip())
        num = int(m.group(1)) if m else 9999
        if config_pos is not None:
            return (0, config_pos, num, c["name"])
        return (1, 9999, num, c["name"])

    campers_sorted = sorted(campers, key=_bunk_sort_key)

    seen, groups = [], {}
    for c in campers_sorted:
        bk = c["bunk"]
        if bk not in groups:
            groups[bk] = []
            seen.append(bk)
        groups[bk].append(c)

    # ---- Styles ----
    _thin = Side(style="thin")
    _med  = Side(style="medium")
    T_ALL = Border(left=_thin, right=_thin, top=_thin, bottom=_thin)

    F_WH_LG   = Font(name="Calibri", bold=True,  size=16, color=WHITE)
    F_WH_SM   = Font(name="Calibri", bold=True,  size=11, color=WHITE)
    F_LABEL   = Font(name="Calibri", bold=True,  size=22)
    F_NAME    = Font(name="Calibri", bold=True,  size=16)
    F_ENROLL  = Font(name="Calibri", bold=False, size=16)
    F_COUNT   = Font(name="Calibri", bold=True,  size=16)
    F_WEEK_HDR = Font(name="Calibri", bold=True,  size=24)
    F_DATE_HDR = Font(name="Calibri", bold=True,  size=12)
    F_ABSENT   = Font(name="Calibri", bold=False, size=20, color="999999")

    BRAND_FILL = PatternFill("solid", fgColor=BRAND)
    ALT_FILL   = PatternFill("solid", fgColor="D9D9D9")
    CTR        = Alignment(horizontal="center", vertical="center")
    RIGHT_AL   = Alignment(horizontal="right",  vertical="center")
    VERT_CTR   = Alignment(horizontal="center", vertical="center", text_rotation=90)

    # ---- Row 1: Week/date header (pre-filled; user completes week # and dates in Excel) ----
    ws.row_dimensions[1].height = 36
    ws.merge_cells(start_row=1, start_column=2, end_row=1, end_column=7)
    c = ws.cell(row=1, column=2, value="WEEK # :")
    c.font = F_WEEK_HDR; c.alignment = CTR

    date_str = (report_date.strftime("%-m/%-d/%Y") if os.name != "nt"
                else report_date.strftime("%#m/%#d/%Y"))
    c = ws.cell(row=1, column=8, value=f"Report Date: {date_str}")
    c.font = F_DATE_HDR; c.alignment = RIGHT_AL

    # ---- Row 2: column headers ----
    ws.row_dimensions[2].height = 20
    hdr = [("A", None, ""),
           ("B", F_WH_LG, "Camper"),
           ("C", F_WH_SM, "MON"),
           ("D", F_WH_SM, "TUES"),
           ("E", F_WH_SM, "WED"),
           ("F", F_WH_SM, "THURS"),
           ("G", F_WH_SM, "FRI"),
           ("H", F_WH_SM, "Enrolled")]
    for col_letter, font, label in hdr:
        col_idx = ord(col_letter) - ord("A") + 1
        c = ws.cell(row=2, column=col_idx, value=label or None)
        if font:
            c.font = font; c.fill = BRAND_FILL; c.alignment = CTR; c.border = T_ALL

    _DAY_LETTERS = "MTWRF"  # maps day index 0-4 to letters used in enrolled string

    # ---- Data rows: one bunk per page ----
    row = 3
    total_count = 0

    for bk_idx, bk in enumerate(seen):
        group    = groups[bk]
        count    = len(group)
        total_count += count
        bk_start = row

        for camper in group:
            ws.row_dimensions[row].height = 31.5
            use_alt = (row % 2 == 0)

            # Col B: camper name
            c = ws.cell(row=row, column=2, value=camper["name"])
            c.font = F_NAME; c.alignment = CTR; c.border = T_ALL
            if use_alt: c.fill = ALT_FILL

            # Cols C–G: blank signing cells; pre-fill 'C' for days camper won't attend
            enrolled = camper["enrolled"]   # e.g. "TWRF" or "" (all days)
            for di, letter in enumerate(_DAY_LETTERS):
                col = 3 + di   # C=3, D=4, E=5, F=6, G=7
                if enrolled and letter not in enrolled:
                    c = ws.cell(row=row, column=col, value="C")
                    c.font = F_ABSENT; c.alignment = CTR; c.border = T_ALL
                    if use_alt: c.fill = ALT_FILL
                else:
                    c = ws.cell(row=row, column=col)
                    c.border = T_ALL
                    if use_alt: c.fill = ALT_FILL

            # Col H: enrolled
            c = ws.cell(row=row, column=8, value=camper["enrolled"] or None)
            c.font = F_ENROLL; c.alignment = CTR; c.border = T_ALL
            if use_alt: c.fill = ALT_FILL

            row += 1

        # Subtotal row
        ws.row_dimensions[row].height = 31.5
        use_alt = (row % 2 == 0)
        c = ws.cell(row=row, column=2, value=count)
        c.font = F_COUNT; c.alignment = CTR; c.border = T_ALL
        if use_alt: c.fill = ALT_FILL
        for ci in range(3, 9):
            c = ws.cell(row=row, column=ci)
            c.border = T_ALL
            if use_alt: c.fill = ALT_FILL
        bk_end = row
        row += 1

        # Merge col A for entire bunk group, rotate text 90°
        ws.merge_cells(start_row=bk_start, start_column=1,
                       end_row=bk_end,     end_column=1)
        c = ws.cell(row=bk_start, column=1, value=bk)
        c.font = F_LABEL; c.alignment = VERT_CTR

        # Page break after each bunk (except the last)
        if bk_idx < len(seen) - 1:
            ws.row_breaks.append(Break(id=bk_end))

    # Grand total row
    ws.row_dimensions[row].height = 31.5
    use_alt = (row % 2 == 0)
    c = ws.cell(row=row, column=2, value=total_count)
    c.font = F_COUNT; c.alignment = CTR; c.border = T_ALL
    if use_alt: c.fill = ALT_FILL
    for ci in range(3, 9):
        ws.cell(row=row, column=ci).border = T_ALL

    # ---- Column widths ----
    ws.column_dimensions["A"].width = 4
    ws.column_dimensions["B"].width = 32
    for col in ["C", "D", "E", "F", "G"]:
        ws.column_dimensions[col].width = 12
    ws.column_dimensions["H"].width = 10

    # ---- Footer: legend printed on every page (spread across L/C/R) ----
    ws.oddFooter.left.text   = "&32✓&16 = Camper in Attendance"
    ws.oddFooter.center.text = "&32C&16 = Camper Confirmed Absent"
    ws.oddFooter.right.text  = "&32O&16 = Camper Not Present"

    # ---- Print settings ----
    ws.print_title_rows = "1:2"          # repeat week header + column headers on every page
    ws.page_setup.orientation = "portrait"
    ws.page_setup.fitToPage   = True
    ws.page_setup.fitToWidth  = 1
    ws.page_setup.fitToHeight = 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True


# ---------------------------------------------------------------------------
# AM / PM Extend parser + builder
# ---------------------------------------------------------------------------

_EXT_TIME_RE    = re.compile(r"(?:Hours|Drop-off)\s+(\d+(?::\d+)?)\s*[-–]", re.IGNORECASE)
_PM_EXT_TIME_RE = re.compile(r"Pick-up\s+\d+(?::\d+)?\s*[^\d\s]\s*(\d+(?::\d+)?)", re.IGNORECASE)

# Week date ranges for 2026 camp season (used by Group Attendance header)
_WEEK_DATES = [
    "June 23 - 27, 2026",
    "June 30 - July 4, 2026",
    "July 7 - 11, 2026",
    "July 14 - 18, 2026",
    "July 21 - 25, 2026",
    "July 28 - Aug 1, 2026",
    "Aug 4 - 8, 2026",
    "Aug 11 - 15, 2026",
]


def _parse_ext_time(token: str) -> datetime.time:
    """Convert '7', '7:30', '8', '8:30' to datetime.time."""
    if ":" in token:
        h, m = token.split(":")
        return datetime.time(int(h), int(m))
    return datetime.time(int(token), 0)


def parse_extend(file_bytes: bytes, period: str = "am") -> list:
    """
    Parse raw AM/PM Extended Hours export (XLSX or CSV).

    Expected columns (0-indexed):
      0  row#
      1  Last name
      2  First name
      3  Bunk name
      4  Enrollment string  (e.g. "AM Extended Hours 8-8:30 drop-off: 5 Days 6 Wks")
      5  Monday?   (Yes / No / blank)
      6  Tuesday?
      7  Wednesday?
      8  Thursday?
      9  Friday?
    """
    if file_bytes[:4] == b'PK\x03\x04':
        from openpyxl import load_workbook as _lw
        _wb = _lw(filename=io.BytesIO(file_bytes), read_only=True, data_only=True)
        _ws = _wb.active
        rows = [[str(c.value) if c.value is not None else "" for c in r]
                for r in _ws.iter_rows()]
        _wb.close()
    else:
        content = file_bytes.decode("utf-8-sig", errors="replace")
        rows = list(csv.reader(io.StringIO(content)))

    if period == "am":
        # "AM Extended Hours drop-off" (legacy) or bare "Drop-off" (current)
        keywords = ["am extended", "drop-off"]
    else:
        # "PM Extended Hours Pick-up" (legacy) or bare "Pick-up" (current)
        keywords = ["pm extended", "pick-up"]
    campers = []
    for row in rows[1:]:
        if len(row) < 4 or not str(row[0]).strip().isdigit():
            continue
        enrollment = str(row[4]).strip() if len(row) > 4 else ""
        enroll_lower = enrollment.lower()
        if not any(kw in enroll_lower for kw in keywords):
            continue

        last  = str(row[1]).strip()
        first = str(row[2]).strip()
        bunk  = str(row[3]).strip()
        mon    = str(row[5]).strip() if len(row) > 5 else ""
        tue    = str(row[6]).strip() if len(row) > 6 else ""
        wed    = str(row[7]).strip() if len(row) > 7 else ""
        thu    = str(row[8]).strip() if len(row) > 8 else ""
        fri    = str(row[9]).strip() if len(row) > 9 else ""
        gender = str(row[10]).strip() if len(row) > 10 else ""

        # Extract time from enrollment string
        # AM: use start time (before dash); PM: use end/pickup time (after dash)
        time_re = _PM_EXT_TIME_RE if period == "pm" else _EXT_TIME_RE
        m = time_re.search(enrollment)
        start_time = _parse_ext_time(m.group(1)) if m else None

        # Days/Wk
        any_specified = any(d.lower() in ("yes", "no") for d in [mon, tue, wed, thu, fri])
        if any_specified:
            days_wk = (
                ("M" if mon.lower() == "yes" else "") +
                ("T" if tue.lower() == "yes" else "") +
                ("W" if wed.lower() == "yes" else "") +
                ("R" if thu.lower() == "yes" else "") +
                ("F" if fri.lower() == "yes" else "")
            )
            if days_wk == "MTWRF":
                days_wk = ""
        else:
            days_wk = ""

        campers.append({
            "name":      f"{last}, {first}",
            "bunk":      bunk,
            "time":      start_time,
            "days_wk":   days_wk,
            "gender":    gender,
        })

    # Sort alphabetically by name
    campers.sort(key=lambda c: c["name"].lower())
    return campers


def build_extend_sheet(ws, campers: list, period: str) -> None:
    """
    Build the single sheet for AM/PM Extend report.

    AM layout (9 cols A-I):  CAMPER, BUNK, TIME, MON-FRI (1 col each), Days/Wk
    PM layout (14 cols A-N): CAMPER, BUNK, TIME, MON-FRI (2 merged cols each), Days/Wk
    """
    _thin = Side(style="thin")
    _med  = Side(style="medium")
    T_BOT_THIN = Border(bottom=_thin)
    T_BOT_MED  = Border(bottom=_med)
    T_ALL_THIN = Border(left=_thin, right=_thin, top=_thin, bottom=_thin)

    if period == "pm":
        HDR_COLOR = "6A1330"
        ALT_COLOR = "DCDCDC"
        FONT_NAME = "Aptos Narrow"
        DAYS_COL  = 14
        SIGN_RANGE = range(4, 14)   # D–M (10 signing cols, 2 per day)
    else:
        HDR_COLOR = BRAND
        ALT_COLOR = "D9D9D9"
        FONT_NAME = "Calibri"
        DAYS_COL  = 9
        SIGN_RANGE = range(4, 9)    # D–H (5 signing cols)

    HDR_FILL = PatternFill("solid", fgColor=HDR_COLOR)
    ALT_FILL = PatternFill("solid", fgColor=ALT_COLOR)

    F_HDR  = Font(name=FONT_NAME, bold=True,  size=11, color=WHITE)
    F_NAME = Font(name=FONT_NAME, bold=False, size=11)
    F_BUNK = Font(name=FONT_NAME, bold=False, size=9)
    F_TIME = Font(name=FONT_NAME, bold=True,  size=11)
    F_DAYS = Font(name=FONT_NAME, bold=False, size=11)
    F_WEEK = Font(name=FONT_NAME, bold=True,  size=11)

    CTR  = Alignment(horizontal="center", vertical="center")
    WRAP = Alignment(horizontal="center", vertical="center", wrap_text=True)
    LEFT = Alignment(horizontal="left",   vertical="center")

    # ---- Row 1: WEEK label ----
    ws.row_dimensions[1].height = 14.65
    c = ws.cell(row=1, column=1, value="WEEK:")
    c.font = F_WEEK

    # ---- Row 2: header ----
    ws.row_dimensions[2].height = 44.35

    def _hdr(col, val, align=CTR):
        c = ws.cell(row=2, column=col, value=val)
        c.font = F_HDR; c.fill = HDR_FILL
        c.alignment = align; c.border = T_BOT_MED

    _hdr(1, "CAMPER"); _hdr(2, "BUNK"); _hdr(3, "TIME")
    _hdr(DAYS_COL, "Days/Wk")

    if period == "pm":
        day_pairs = [
            (4, 5,  "MON\nDate\nTime     Initial"),
            (6, 7,  "TUES\nDate\nTime     Initial"),
            (8, 9,  "WED\nDate\nTime     Initial"),
            (10, 11, "THURS\nDate\nTime     Initial"),
            (12, 13, "FRI\nDate\nTime     Initial"),
        ]
        for c1, c2, lbl in day_pairs:
            ws.merge_cells(start_row=2, start_column=c1, end_row=2, end_column=c2)
            _hdr(c1, lbl, WRAP)
            ws.cell(row=2, column=c2).border = T_BOT_MED
    else:
        for ci, lbl in [(4, "MON\nDate\nTime"), (5, "TUES\nDate\nTime"),
                        (6, "WED\nDate\nTime"), (7, "THURS\nDate\nTime"),
                        (8, "FRI\nDate\nTime")]:
            _hdr(ci, lbl, WRAP)

    # ---- Data rows ----
    for i, camper in enumerate(campers):
        r = i + 3
        ws.row_dimensions[r].height = 23.75
        af = ALT_FILL if (i % 2 == 1) else None

        def _set(col, val, font, align=CTR):
            cell = ws.cell(row=r, column=col, value=val)
            cell.font = font; cell.alignment = align; cell.border = T_BOT_THIN
            if af: cell.fill = af

        _set(1, camper["name"], F_NAME, LEFT)
        _set(2, camper["bunk"], F_BUNK)

        t = camper["time"]
        time_str = (f"{t.hour}:{t.minute:02d}" if t.minute else str(t.hour)) if t else None
        _set(3, time_str, F_TIME)

        for ci in SIGN_RANGE:
            cell = ws.cell(row=r, column=ci)
            cell.border = T_ALL_THIN
            if af: cell.fill = af

        _set(DAYS_COL, camper["days_wk"] or None, F_DAYS)

    # ---- Column widths ----
    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 9
    ws.column_dimensions["C"].width = 9
    if period == "pm":
        ws.column_dimensions["D"].width = 6.27
        ws.column_dimensions[get_column_letter(DAYS_COL)].width = 11.6
    else:
        for col in ["D", "E", "F", "G", "H", "I"]:
            ws.column_dimensions[col].width = 11.6

    # ---- Print settings ----
    ws.page_setup.orientation = "portrait"
    ws.page_setup.fitToPage   = True
    ws.page_setup.fitToWidth  = 1
    ws.page_setup.fitToHeight = 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    ws.print_title_rows = "1:2"


# ---------------------------------------------------------------------------
# PM GRP Extend helpers
# ---------------------------------------------------------------------------

def parse_pm_grp_extend(file_bytes: bytes, config: dict) -> list:
    """
    Parse PM Extended data and annotate each camper with their group code.
    Grp is resolved entirely from bunk_config.json (by bunk number).
    Group order is derived dynamically from the config — sorted by the
    lowest bunk number in each group — so no hardcoded mappings are needed.
    Returns campers sorted by group order, bunk number, then name.
    """
    # Build number → grp, name → grp, AND track the minimum bunk number per group
    num_to_grp:  dict[int, str] = {}
    name_to_grp: dict[str, str] = {}   # fallback for bunks with no leading number
    grp_min_bunk: dict[str, int] = {}

    for camp in config.get("camps", []):
        for bunk in camp.get("bunks", []):
            grp  = bunk.get("grp", "").strip()
            num  = bunk.get("number")
            name = bunk.get("name", "").strip().lower()
            if grp and num is not None:
                num_to_grp[num] = grp
                if grp not in grp_min_bunk or num < grp_min_bunk[grp]:
                    grp_min_bunk[grp] = num
            if grp and name:
                name_to_grp[name] = grp

    # Dynamic group order — sorted by the first bunk number in each group
    grp_idx = {g: i for i, g in enumerate(
        sorted(grp_min_bunk.keys(), key=lambda g: grp_min_bunk[g])
    )}

    # Bunks whose group depends on gender rather than bunk_config
    # Girls → Up1, Boys → Up2
    _GENDER_SPLIT_BUNKS = {30, 31}

    campers = parse_extend(file_bytes, period="pm")
    for c in campers:
        m = re.match(r'^(\d+)', c["bunk"].strip())
        bunk_num = int(m.group(1)) if m else None
        c["bunk_num"] = bunk_num if bunk_num is not None else 999
        if bunk_num in _GENDER_SPLIT_BUNKS:
            g = c.get("gender", "").lower()
            c["grp"] = "Up1" if ("girl" in g or "female" in g or g == "f") else "Up2"
        elif bunk_num is not None:
            # Numeric bunk — look up by number
            c["grp"] = num_to_grp.get(bunk_num, "Unknown")
        else:
            # No leading number (e.g. "FT CITs") — look up by name
            c["grp"] = name_to_grp.get(c["bunk"].strip().lower(), "Unknown")

    campers.sort(key=lambda c: (grp_idx.get(c["grp"], 99), c["bunk_num"], c["name"].lower()))
    return campers


def build_pm_grp_extend_sheet(ws, campers: list) -> None:
    """
    PM GRP EXTEND: landscape, 10 cols (Grp|BUNK|CAMPER|Pick Up|Mon–Fri|Days),
    grouped by Grp with a subtotal count row after each group.
    """
    _thin = Side(style="thin")
    T_ALL = Border(left=_thin, right=_thin, top=_thin, bottom=_thin)

    HDR_FILL = PatternFill("solid", fgColor="6A1330")
    FONT_NAME = "Aptos Narrow"

    F_HDR  = Font(name=FONT_NAME, bold=True,  size=16, color=WHITE)
    F_DATA = Font(name=FONT_NAME, bold=False, size=16)
    F_WK   = Font(name=FONT_NAME, bold=False, size=16)

    CTR  = Alignment(horizontal="center", vertical="center")
    LEFT = Alignment(horizontal="left",   vertical="center")

    # ---- Row 1: Week label ----
    ws.row_dimensions[1].height = 23.5
    c = ws.cell(row=1, column=2, value="Week:")
    c.font = F_WK; c.alignment = CTR

    # ---- Row 2: Header ----
    ws.row_dimensions[2].height = 23.5
    for ci, lbl in enumerate(
        ["Grp", "BUNK", "CAMPER", "Pick Up", "Mon", "Tue", "Wed", "Thu", "Fri", "Days"], 1
    ):
        c = ws.cell(row=2, column=ci, value=lbl)
        c.font = F_HDR; c.fill = HDR_FILL; c.alignment = CTR; c.border = T_ALL

    from openpyxl.worksheet.pagebreak import Break

    # ---- Data rows (grouped) ----
    r = 3
    current_grp = None
    group_start = r
    group_count = 0

    def _flush_subtotal():
        nonlocal group_start, group_count
        if group_count:
            ws.row_dimensions[r].height = 23.5
            ws.cell(row=r, column=1, value=group_count)

    for camper in campers:
        if camper["grp"] != current_grp:
            if current_grp is not None:
                _flush_subtotal()
                # Insert page break after the subtotal row so each group prints on its own page
                ws.row_breaks.append(Break(id=r))
                r += 1
            current_grp = camper["grp"]
            group_count = 0

        ws.row_dimensions[r].height = 23.5
        t = camper["time"]
        time_str = (f"{t.hour}:{t.minute:02d}" if t.minute else str(t.hour)) if t else None

        for col, val, align in [
            (1,  camper["grp"],             CTR),
            (2,  camper["bunk"],            CTR),
            (3,  camper["name"],            LEFT),
            (4,  time_str,                  CTR),
            (10, camper["days_wk"] or None, CTR),
        ]:
            cell = ws.cell(row=r, column=col, value=val)
            cell.font = F_DATA; cell.alignment = align; cell.border = T_ALL

        for ci in range(5, 10):
            ws.cell(row=r, column=ci).border = T_ALL

        r += 1
        group_count += 1

    # flush last group
    if current_grp is not None:
        _flush_subtotal()

    # ---- Column widths ----
    # Scaled to fill landscape Letter page (10.5" content width)
    ws.column_dimensions["A"].width = 6.0    # Grp
    ws.column_dimensions["B"].width = 18.5   # BUNK
    ws.column_dimensions["C"].width = 27.0   # CAMPER
    ws.column_dimensions["D"].width = 10.5   # Pick Up
    ws.column_dimensions["E"].width = 15.0   # Mon
    ws.column_dimensions["F"].width = 13.5   # Tue
    ws.column_dimensions["G"].width = 13.5   # Wed
    ws.column_dimensions["H"].width = 13.5   # Thu
    ws.column_dimensions["I"].width = 13.5   # Fri
    ws.column_dimensions["J"].width = 14.5   # Days

    # ---- Print settings ----
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToPage   = True
    ws.page_setup.fitToWidth  = 1
    ws.page_setup.fitToHeight = 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    ws.print_title_rows = "1:2"

    # ---- Margins (inches) ----
    ws.page_margins.top    = 0.5
    ws.page_margins.bottom = 0.5
    ws.page_margins.left   = 0.25
    ws.page_margins.right  = 0.25
    ws.page_margins.header = 0.3
    ws.page_margins.footer = 0.3
    ws.print_options.horizontalCentered = True
    ws.print_options.verticalCentered   = False


# ---------------------------------------------------------------------------
# Driver Totals builder
# ---------------------------------------------------------------------------

def build_driver_totals_sheet(ws, campers: list, report_date: date, week_num: int = None) -> None:
    """
    Build the Driver Totals sheet.

    Column layout (18 cols A-R):
      A  Child       B  Bunk
      C-J  #1-#8 (week indicators)
      K-O  M T W R F (day letters)
      P  Age   Q  Grade   R  Driver

    Per driver group: data rows (alternating gray/white) → SUM row → COUNT row
    Grand totals: GRAND COUNT row → GRAND SUM row
    Each driver group prints on its own page.
    """
    from openpyxl.worksheet.pagebreak import Break

    PLAIN_FONT    = Font(name="Calibri", bold=False, size=12)
    BOLD_FONT     = Font(name="Calibri", bold=True,  size=12)
    CENTER_AL     = Alignment(horizontal="center", vertical="center")
    LEFT_AL       = Alignment(horizontal="left",   vertical="center")
    ROW_ALT_FILL   = PatternFill("solid", fgColor="EEEEEE")  # light gray alternating rows
    AGE_WARN_FILL  = PatternFill("solid", fgColor="92D050")  # green for age < 8
    BUNK_WARN_FILL = PatternFill("solid", fgColor="FFC000")  # orange for bunks 1-7
    NAME_WEEK_FILL = PatternFill("solid", fgColor="FFFF00")  # yellow: attending this week

    def _set(r, c, val=None, font=None, align=None, fill=None):
        cell = ws.cell(row=r, column=c, value=val)
        if font:  cell.font      = font
        if align: cell.alignment = align
        if fill:  cell.fill      = fill
        return cell

    # ----- Row 1: date header -----------------------------------------------
    # "Report Date:" sits in col A; the date value goes in col C (Bunk column)
    # because col B (Stp#) is intentionally narrow and would clip the date.
    date_lbl = ws.cell(row=1, column=1, value="Report Date:")
    date_lbl.font = BOLD_FONT
    date_lbl.alignment = Alignment(horizontal="right", vertical="center")
    date_str = (report_date.strftime("%-m/%-d/%Y") if os.name != "nt"
                else report_date.strftime("%#m/%#d/%Y"))
    date_val = ws.cell(row=1, column=3, value=date_str)
    date_val.font = BOLD_FONT
    date_val.alignment = CENTER_AL

    # Column layout (19 cols A-S):
    #   A(1)=Child  B(2)=Stp#  C(3)=Bunk
    #   D-K(4-11)=#1-#8   L-P(12-16)=M/T/W/R/F
    #   Q(17)=Age  R(18)=Grade  S(19)=Driver
    COL_CHILD  = 1
    COL_STOP   = 2
    COL_BUNK   = 3
    COL_WK1    = 4    # weeks occupy cols 4-11
    COL_DAY1   = 12   # days  occupy cols 12-16
    COL_AGE    = 17
    COL_GRADE  = 18
    COL_DRIVER = 19
    TOTAL_COLS = 19

    # ----- Row 2: column headers --------------------------------------------
    col_headers = [
        "Child", "Stp#", "Bunk",
        "#1", "#2", "#3", "#4", "#5", "#6", "#7", "#8",
        "M", "T", "W", "R", "F",
        "Age", "Grade", "Driver",
    ]
    for ci, h in enumerate(col_headers, start=1):
        hdr_fill = NAME_WEEK_FILL if (week_num is not None and ci == COL_WK1 + week_num - 1) else None
        _set(2, ci, h, font=BOLD_FONT, align=LEFT_AL if ci == 1 else CENTER_AL, fill=hdr_fill)

    # ----- Group and sort campers -------------------------------------------
    driver_groups: dict[str, list] = {}
    for camper in campers:
        drv = camper["driver"] or "(No Driver)"
        driver_groups.setdefault(drv, []).append(camper)

    # Sort by stop # when available, fall back to alphabetical name
    has_stops = any(c.get("stop") is not None for c in campers)
    for drv in driver_groups:
        if has_stops:
            driver_groups[drv].sort(
                key=lambda x: (x["stop"] is None, x["stop"] or 0, x["name"].lower())
            )
        else:
            driver_groups[drv].sort(key=lambda x: x["name"].lower())

    sorted_drivers = sorted(driver_groups.keys())

    # ----- Write rows -------------------------------------------------------
    row = 3
    grand_week_sums = [0] * 8
    grand_count = 0

    for drv_idx, drv in enumerate(sorted_drivers):
        group      = driver_groups[drv]
        week_sums  = [0] * 8
        count      = len(group)
        grand_count += count

        for ci, camper in enumerate(group):
            fill = ROW_ALT_FILL if (ci % 2 == 1) else None

            if fill:
                for col in range(1, TOTAL_COLS + 1):
                    ws.cell(row=row, column=col).fill = fill

            bunk_val  = camper["bunk"]
            bunk_num  = int(m.group()) if (m := __import__('re').match(r'\d+', str(bunk_val or ""))) else None
            bunk_fill = BUNK_WARN_FILL if (bunk_num is not None and 1 <= bunk_num <= 7) else fill

            # Highlight name yellow if this camper has a 1 in the selected week column
            attending_this_week = (
                week_num is not None
                and 1 <= week_num <= len(camper["weeks"])
                and camper["weeks"][week_num - 1] == 1
            )
            name_fill = NAME_WEEK_FILL if attending_this_week else fill

            _set(row, COL_CHILD, camper["name"],     font=PLAIN_FONT, align=LEFT_AL,   fill=name_fill)
            _set(row, COL_STOP,  camper.get("stop"), font=PLAIN_FONT, align=CENTER_AL, fill=fill)
            _set(row, COL_BUNK,  bunk_val,           font=PLAIN_FONT, align=CENTER_AL, fill=bunk_fill)

            for wi, wv in enumerate(camper["weeks"]):
                is_selected_week = (week_num is not None and wi == week_num - 1)
                wk_fill = NAME_WEEK_FILL if (is_selected_week and wv == 1) else fill
                _set(row, COL_WK1 + wi, wv, font=PLAIN_FONT, align=CENTER_AL, fill=wk_fill)
                week_sums[wi]       += wv
                grand_week_sums[wi] += wv

            for di, dv in enumerate(camper["days"]):
                _set(row, COL_DAY1 + di, dv, font=PLAIN_FONT, align=CENTER_AL, fill=fill)

            age_val  = camper["age"]
            age_fill = AGE_WARN_FILL if (age_val is not None and age_val < 8) else fill
            _set(row, COL_AGE, age_val, font=PLAIN_FONT, align=CENTER_AL, fill=age_fill)
            _set(row, COL_GRADE,  camper["grade"] or None,font=PLAIN_FONT, align=CENTER_AL, fill=fill)
            _set(row, COL_DRIVER, drv,                    font=PLAIN_FONT, align=LEFT_AL,   fill=fill)

            row += 1

        # --- SUM row: week totals + "[Driver] Total" label ------------------
        for wi, wsum in enumerate(week_sums):
            _set(row, COL_WK1 + wi, wsum, font=PLAIN_FONT, align=CENTER_AL)
        _set(row, COL_DRIVER, "Total", font=BOLD_FONT, align=LEFT_AL)
        row += 1

        # --- COUNT row: driver label + count --------------------------------
        _set(row, COL_GRADE,  drv,   font=BOLD_FONT,  align=LEFT_AL)
        _set(row, COL_DRIVER, count, font=PLAIN_FONT, align=CENTER_AL)
        row += 1

        # Page break after every driver group (grand totals print on their own page)
        ws.row_breaks.append(Break(id=row - 1))

    # ----- Grand totals -----------------------------------------------------
    # GRAND COUNT row
    _set(row, COL_GRADE,  "Grand",       font=BOLD_FONT,  align=LEFT_AL)
    _set(row, COL_DRIVER, grand_count,   font=PLAIN_FONT, align=CENTER_AL)
    row += 1

    # GRAND SUM row
    for wi, gs in enumerate(grand_week_sums):
        _set(row, COL_WK1 + wi, gs, font=PLAIN_FONT, align=CENTER_AL)
    _set(row, COL_DRIVER, "Grand Total", font=BOLD_FONT, align=LEFT_AL)

    last_row = row   # used for border pass below

    # ----- Vertical separator borders ---------------------------------------
    # Thin right border on: col B (Stp# | Bunk), col K (#8 | M),
    # and col P (F | Age) — runs from the header row to the last data row.
    _vert = Side(style="thin", color="000000")
    for r in range(2, last_row + 1):
        for col in (COL_STOP,           # right edge of Stp# col
                    COL_WK1 + 7,        # right edge of #8 col  (col K)
                    COL_DAY1 + 4):      # right edge of F col   (col P)
            cell = ws.cell(row=r, column=col)
            eb = cell.border
            cell.border = Border(
                left=eb.left, right=_vert, top=eb.top, bottom=eb.bottom
            )

    # ----- Column widths ----------------------------------------------------
    ws.column_dimensions["A"].width = 20    # Child
    ws.column_dimensions["B"].width = 4.5   # Stp#
    ws.column_dimensions["C"].width = 18    # Bunk
    for wi in range(8):                      # #1-#8  (cols D-K)
        ws.column_dimensions[get_column_letter(COL_WK1 + wi)].width = 4.5
    for di in range(5):                      # M T W R F  (cols L-P)
        ws.column_dimensions[get_column_letter(COL_DAY1 + di)].width = 3
    ws.column_dimensions["Q"].width = 5     # Age
    ws.column_dimensions["R"].width = 10    # Grade (also holds driver count labels)
    ws.column_dimensions["S"].width = 16    # Driver

    # ----- Print settings ---------------------------------------------------
    ws.page_setup.orientation = "landscape"
    ws.page_setup.scale       = 95
    ws.print_title_rows       = "1:2"
    ws.page_margins.top    = 0.25
    ws.page_margins.bottom = 0.25
    ws.page_margins.left   = 0.25
    ws.page_margins.right  = 0.25
    ws.page_margins.header = 0.15
    ws.page_margins.footer = 0.15


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_report(file_bytes: bytes, report_type: str,
                   config: dict, job_id: str, output_dir: str,
                   week_num: int = None) -> dict:

    supported = ("bunk_snapshot", "group_attendance", "am_extend", "pm_extend", "pm_grp_extend", "driver_totals")
    if report_type not in supported:
        return {
            "success": False,
            "message": (
                f"Report type '{report_type}' is not configured. "
                f"Currently supported: {', '.join(repr(s) for s in supported)}."
            ),
        }

    report_date = date.today()
    os.makedirs(output_dir, exist_ok=True)

    # ---- Bunk Snapshot ----
    if report_type == "bunk_snapshot":
        try:
            campers = parse_raw_csv(file_bytes)
        except Exception as e:
            return {"success": False, "message": f"Could not parse file: {e}"}
        if not campers:
            return {"success": False, "message": "No camper data found in file. Check the file format."}

        bunk_lookup   = get_bunk_lookup(config)
        ordered_bunks = get_ordered_bunks(config)

        wb = Workbook()
        ws_report = wb.active
        ws_report.title = "Report"
        ws_totals = wb.create_sheet("Totals")
        build_report_sheet(ws_report, campers, bunk_lookup, ordered_bunks, report_date)
        build_totals_sheet(ws_totals, campers, config, bunk_lookup, report_date)

        out_filename = f"Bunk Snapshot {report_date.strftime('%m%d%Y')}.xlsx"
        out_path = os.path.join(output_dir, out_filename)
        wb.save(out_path)

        return {
            "success":  True,
            "message":  f"Processed {len(campers)} campers successfully.",
            "filename": out_filename,
            "rows":     len(campers),
        }

    # ---- Group Attendance ----
    if report_type == "group_attendance":
        try:
            campers = parse_group_attendance(file_bytes)
        except Exception as e:
            return {"success": False, "message": f"Could not parse file: {e}"}
        if not campers:
            return {"success": False, "message": "No camper data found in file. Check the file format."}

        wb = Workbook()
        ws = wb.active
        ws.title = "Data1"
        build_group_attendance_sheet(ws, campers, config, report_date)

        out_filename = f"Group Attendance {report_date.strftime('%m%d%Y')}.xlsx"
        out_path = os.path.join(output_dir, out_filename)
        wb.save(out_path)

        return {
            "success":  True,
            "message":  f"Processed {len(campers)} campers successfully.",
            "filename": out_filename,
            "rows":     len(campers),
        }

    # ---- AM Extend ----
    if report_type == "am_extend":
        try:
            campers = parse_extend(file_bytes, period="am")
        except Exception as e:
            return {"success": False, "message": f"Could not parse file: {e}"}
        if not campers:
            return {"success": False, "message": "No AM Extended campers found in file."}

        wb = Workbook()
        ws = wb.active
        ws.title = "AM Extend"
        build_extend_sheet(ws, campers, period="am")

        out_filename = f"AM Extend {report_date.strftime('%m%d%Y')}.xlsx"
        out_path = os.path.join(output_dir, out_filename)
        wb.save(out_path)

        return {
            "success":  True,
            "message":  f"Processed {len(campers)} campers successfully.",
            "filename": out_filename,
            "rows":     len(campers),
        }

    # ---- PM Extend ----
    if report_type == "pm_extend":
        try:
            campers = parse_extend(file_bytes, period="pm")
        except Exception as e:
            return {"success": False, "message": f"Could not parse file: {e}"}
        if not campers:
            return {"success": False, "message": "No PM Extended campers found in file."}

        wb = Workbook()
        ws = wb.active
        ws.title = "PM Extend"
        build_extend_sheet(ws, campers, period="pm")

        out_filename = f"PM Extend {report_date.strftime('%m%d%Y')}.xlsx"
        out_path = os.path.join(output_dir, out_filename)
        wb.save(out_path)

        return {
            "success":  True,
            "message":  f"Processed {len(campers)} campers successfully.",
            "filename": out_filename,
            "rows":     len(campers),
        }

    # ---- PM GRP Extend ----
    if report_type == "pm_grp_extend":
        try:
            campers = parse_pm_grp_extend(file_bytes, config)
        except Exception as e:
            return {"success": False, "message": f"Could not parse file: {e}"}
        if not campers:
            return {"success": False, "message": "No PM Extended campers found in file."}

        wb = Workbook()
        ws = wb.active
        ws.title = "PM GRP Extend"
        build_pm_grp_extend_sheet(ws, campers)

        out_filename = f"PM GRP Extend {report_date.strftime('%m%d%Y')}.xlsx"
        out_path = os.path.join(output_dir, out_filename)
        wb.save(out_path)

        return {
            "success":  True,
            "message":  f"Processed {len(campers)} campers successfully.",
            "filename": out_filename,
            "rows":     len(campers),
        }

    # ---- Driver Totals ----
    if report_type == "driver_totals":
        try:
            campers = parse_driver_csv(file_bytes)
        except Exception as e:
            return {"success": False, "message": f"Could not parse file: {e}"}
        # Filter to campers who have a driver assigned
        campers = [c for c in campers if c.get("driver")]
        if not campers:
            return {"success": False, "message": "No campers with a driver assignment found in file."}

        wb = Workbook()
        ws = wb.active
        ws.title = "Sheet1"
        build_driver_totals_sheet(ws, campers, report_date, week_num=week_num)

        out_filename = f"Driver Totals {report_date.strftime('%m%d%Y')}.xlsx"
        out_path = os.path.join(output_dir, out_filename)
        wb.save(out_path)

        return {
            "success":  True,
            "message":  f"Processed {len(campers)} campers across drivers successfully.",
            "filename": out_filename,
            "rows":     len(campers),
        }
