"""
EL Reporting Center — Report Processor
----------------------------------------
Transforms raw camp management CSV exports into formatted Excel workbooks.
"""

import csv
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


def get_bunk_lookup(config: dict) -> dict:
    """Return {bunk_name: {number, camp}} from config."""
    lookup = {}
    for camp in config.get("camps", []):
        for bunk in camp.get("bunks", []):
            lookup[bunk["name"]] = {
                "number": bunk["number"],
                "camp": camp["name"],
            }
    return lookup


def get_ordered_bunks(config: dict) -> list:
    """Return list of bunk names in display order (camp order, then bunk order)."""
    bunks = []
    for camp in config.get("camps", []):
        for bunk in camp.get("bunks", []):
            bunks.append(bunk["name"])
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
    """
    campers = []
    for row in rows[1:]:          # skip header
        if len(row) < 4 or not str(row[0]).strip().isdigit():
            continue

        last     = str(row[1]).strip()
        first    = str(row[2]).strip()
        bunk     = str(row[3]).strip()
        sessions = str(row[4]).strip() if len(row) > 4 else ""
        age      = str(row[5]).strip() if len(row) > 5 else ""
        grade    = normalize_grade(row[6]) if len(row) > 6 else ""
        mon      = str(row[7]).strip()  if len(row) > 7  else ""
        tue      = str(row[8]).strip()  if len(row) > 8  else ""
        wed      = str(row[9]).strip()  if len(row) > 9  else ""
        thu      = str(row[10]).strip() if len(row) > 10 else ""
        fri      = str(row[11]).strip() if len(row) > 11 else ""

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
            "name":  f"{last}, {first}",
            "bunk":  bunk,
            "weeks": weeks,
            "days":  [day_m, day_t, day_w, day_r, day_f],
            "age":   age,
            "grade": grade,
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
    _cell(ws, 1, 1, "Report Date:", font=DATE_FONT)
    _cell(ws, 1, 2, report_date.strftime("%-m/%-d/%Y") if os.name != "nt"
          else report_date.strftime("%#m/%#d/%Y"),
          font=DATE_FONT)

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
            display_order.insert(0, bk)   # unknowns at top as "unassigned"

    # Separate "unassigned" (not in config) from known bunks
    unknown_bunks = [b for b in display_order if b not in bunk_lookup]
    known_bunks   = [b for b in display_order if b in bunk_lookup]
    display_order = unknown_bunks + known_bunks

    # ----- Write rows -------------------------------------------------------
    row = 3
    total_col = 18   # Column R

    for bk_idx, bunk_name in enumerate(display_order):
        group = bunk_groups[bunk_name]
        week_sums = [0] * 8

        for ci, camper in enumerate(group):
            alt = (ci % 2 == 1)
            fill = ALT_FILL if alt else None

            _cell(ws, row, 1,  camper["name"],  font=BODY_FONT, fill=fill, align=LEFT,   border=THIN_BORDER)
            _cell(ws, row, 2,  bunk_name,        font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)

            for wi, wv in enumerate(camper["weeks"]):
                _cell(ws, row, 3 + wi, wv if wv else None,
                      font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
                week_sums[wi] += wv

            for di, dv in enumerate(camper["days"]):
                _cell(ws, row, 11 + di, dv,
                      font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)

            _cell(ws, row, 16, camper["age"],   font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
            _cell(ws, row, 17, camper["grade"], font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
            row += 1

        # --- Subtotal row ---
        _cell(ws, row, 1,  None,         font=TOTAL_FONT, fill=TOTAL_FILL, border=THIN_BORDER)
        _cell(ws, row, 2,  "-",           font=TOTAL_FONT, fill=TOTAL_FILL, align=CENTER, border=THIN_BORDER)
        for wi, ws_val in enumerate(week_sums):
            _cell(ws, row, 3 + wi, ws_val,
                  font=TOTAL_FONT, fill=TOTAL_FILL, align=CENTER, border=THIN_BORDER)
        for di in range(5):
            _cell(ws, row, 11 + di, None, fill=TOTAL_FILL, border=THIN_BORDER)
        _cell(ws, row, 16, None, fill=TOTAL_FILL, border=THIN_BORDER)
        _cell(ws, row, 17, None, fill=TOTAL_FILL, border=THIN_BORDER)
        _cell(ws, row, total_col,
              f"Total:   {len(group)}",
              font=TOTAL_FONT, fill=TOTAL_FILL, align=LEFT, border=THIN_BORDER)
        row += 1

    # ----- Column widths ----------------------------------------------------
    ws.column_dimensions["A"].width = 26   # Child
    ws.column_dimensions["B"].width = 16   # Bunk
    for col_letter in [get_column_letter(c) for c in range(3, 11)]:
        ws.column_dimensions[col_letter].width = 5   # #1-#8
    for col_letter in [get_column_letter(c) for c in range(11, 16)]:
        ws.column_dimensions[col_letter].width = 4   # M T W R F
    ws.column_dimensions["P"].width = 6   # Age
    ws.column_dimensions["Q"].width = 6   # Grade
    ws.column_dimensions["R"].width = 14  # Total

    # Freeze panes below header
    ws.freeze_panes = "A3"


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

    # Left & Middle sections: per-bunk rows + per-camp subtotals
    mid_data_rows = {}  # camp -> (row, count)
    mid_row = 4

    for ci, camp in enumerate(config["camps"]):
        cn = camp["name"]
        camp_start_mid = mid_row

        for bi, bunk in enumerate(camp["bunks"]):
            bk = bunk["name"]
            alt = (data_row % 2 == 0)
            fill = ALT_FILL if alt else None

            _cell(ws, data_row, LEFT_C, cn,  font=BODY_FONT, fill=fill, align=LEFT,   border=THIN_BORDER)
            _cell(ws, data_row, LEFT_B, bk,  font=BODY_FONT, fill=fill, align=LEFT,   border=THIN_BORDER)
            _cell(ws, data_row, LEFT_N, bunk_count.get(bk, 0),
                  font=BODY_FONT, fill=fill, align=CENTER, border=THIN_BORDER)
            data_row += 1
            mid_row  += 1

        # Middle section camp subtotal
        _cell(ws, camp_start_mid, MID_C, cn,
              font=BODY_FONT, fill=LGREY_FILL, align=LEFT, border=THIN_BORDER)
        _cell(ws, camp_start_mid, MID_T, camp_count[cn],
              font=BODY_FONT, fill=LGREY_FILL, align=CENTER, border=THIN_BORDER)

    # Grand total rows
    _cell(ws, data_row, LEFT_C, "TOTAL", font=TOTAL_FONT, fill=TOTAL_FILL, align=LEFT, border=THIN_BORDER)
    _cell(ws, data_row, LEFT_B, None,    font=TOTAL_FONT, fill=TOTAL_FILL, border=THIN_BORDER)
    _cell(ws, data_row, LEFT_N, grand_total, font=TOTAL_FONT, fill=TOTAL_FILL, align=CENTER, border=THIN_BORDER)

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
        all_bunks_ordered.extend([b["name"] for b in camp["bunks"]])

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


def build_group_attendance_sheet(ws, campers: list, config: dict) -> None:
    """Build the single Data1 sheet for the Group Attendance report."""

    # Bunk sort order from config
    bunk_order = {}
    idx = 0
    for camp in config.get("camps", []):
        for bunk in camp.get("bunks", []):
            bunk_order[bunk["name"]] = idx
            idx += 1

    campers_sorted = sorted(
        campers,
        key=lambda c: (bunk_order.get(c["bunk"], 9999), c["name"])
    )

    # Group by bunk preserving sort order
    seen, groups = [], {}
    for c in campers_sorted:
        bk = c["bunk"]
        if bk not in groups:
            groups[bk] = []
            seen.append(bk)
        groups[bk].append(c)

    # ---- Styles ----
    hdr_font   = Font(name="Calibri", bold=True, color=WHITE, size=10)
    body_font  = Font(name="Calibri", size=10)
    bold_font  = Font(name="Calibri", bold=True, size=10)
    brand_fill = PatternFill("solid", fgColor=BRAND)
    alt_fill   = PatternFill("solid", fgColor=BRAND_ALT)
    total_fill = PatternFill("solid", fgColor=LIGHT_GREY)
    center     = Alignment(horizontal="center", vertical="center")
    left       = Alignment(horizontal="left",   vertical="center")

    def _c(r, col, val=None, font=None, fill=None, align=None, border=None):
        cell = ws.cell(row=r, column=col, value=val)
        if font:   cell.font   = font
        if fill:   cell.fill   = fill
        if align:  cell.alignment = align
        if border: cell.border  = border

    # ---- Header row ----
    headers = [None, "Bunk", "Camper", "MON", "TUES", "WED", "THURS", "FRI", "Enrolled"]
    for ci, h in enumerate(headers, 1):
        _c(1, ci, h, font=hdr_font if h else None,
           fill=brand_fill if h else None,
           align=center, border=THIN_BORDER if h else None)

    row = 2
    total_count = 0

    for bk in seen:
        group = groups[bk]
        count = len(group)
        total_count += count
        alt = (len(seen) % 2 == 0)   # track alternating at bunk level

        for i, camper in enumerate(group):
            fill = alt_fill if (row % 2 == 0) else None
            col_a = bk + " " if i == 0 else None
            _c(row, 1, col_a, font=body_font, fill=fill, align=left)
            _c(row, 2, bk,           font=body_font, fill=fill, align=left,   border=THIN_BORDER)
            _c(row, 3, camper["name"], font=body_font, fill=fill, align=left, border=THIN_BORDER)
            for ci in range(4, 9):   # MON–FRI blank
                _c(row, ci, None, fill=fill, border=THIN_BORDER)
            _c(row, 9, camper["enrolled"] or None, font=body_font, fill=fill,
               align=center, border=THIN_BORDER)
            row += 1

        # Subtotal row
        _c(row, 2, count, font=bold_font, fill=total_fill, align=center, border=THIN_BORDER)
        _c(row, 3, count, font=bold_font, fill=total_fill, align=center, border=THIN_BORDER)
        for ci in [1, 4, 5, 6, 7, 8, 9]:
            _c(row, ci, None, fill=total_fill, border=THIN_BORDER if ci != 1 else None)
        row += 1

    # Grand total
    _c(row, 2, total_count, font=bold_font, fill=PatternFill("solid", fgColor=DARK_GREY),
       align=center, border=THIN_BORDER)
    ws.cell(row=row, column=2).font = Font(name="Calibri", bold=True, color=WHITE, size=10)
    _c(row, 3, total_count, font=Font(name="Calibri", bold=True, color=WHITE, size=10),
       fill=PatternFill("solid", fgColor=DARK_GREY), align=center, border=THIN_BORDER)

    # Column widths
    ws.column_dimensions["A"].width = 16
    ws.column_dimensions["B"].width = 16
    ws.column_dimensions["C"].width = 26
    for col in ["D", "E", "F", "G", "H"]:
        ws.column_dimensions[col].width = 8
    ws.column_dimensions["I"].width = 12


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_report(file_bytes: bytes, report_type: str,
                   config: dict, job_id: str, output_dir: str) -> dict:

    supported = ("bunk_snapshot", "group_attendance")
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
        build_group_attendance_sheet(ws, campers, config)

        out_filename = f"Group Attendance {report_date.strftime('%m%d%Y')}.xlsx"
        out_path = os.path.join(output_dir, out_filename)
        wb.save(out_path)

        return {
            "success":  True,
            "message":  f"Processed {len(campers)} campers successfully.",
            "filename": out_filename,
            "rows":     len(campers),
        }
