"""
EL Reporting Center — Report Processor
---------------------------------------
This module handles the transformation of raw Elbow Lane Excel reports
into the formatted output required by camp administration.

Processing logic will be added once sample reports and desired outputs
are provided. The functions below define the expected interface.
"""

import os
import json
import uuid
from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side


def load_bunk_config(config_path: str) -> dict:
    """Load the bunk/camp configuration from JSON file."""
    with open(config_path, "r") as f:
        return json.load(f)


def save_bunk_config(config_path: str, config: dict) -> None:
    """Persist the bunk/camp configuration to JSON file."""
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)


def get_bunk_lookup(config: dict) -> dict:
    """
    Build a flat lookup dict mapping bunk name -> {number, camp}.
    e.g. {"Bunk 1": {"number": 1, "camp": "Pioneer"}, ...}
    """
    lookup = {}
    for camp in config.get("camps", []):
        for bunk in camp.get("bunks", []):
            lookup[bunk["name"]] = {
                "number": bunk["number"],
                "camp": camp["name"],
            }
    return lookup


def process_report(file_bytes: bytes, report_type: str, config: dict, job_id: str, output_dir: str) -> dict:
    """
    Main entry point for processing a raw Excel report.

    Parameters
    ----------
    file_bytes   : raw bytes of the uploaded .xlsx file
    report_type  : identifier string for which report format to apply
    config       : loaded bunk_config dict
    job_id       : unique job identifier (for output filename)
    output_dir   : directory to write the processed file

    Returns
    -------
    dict with keys:
        success  (bool)
        message  (str)
        filename (str)  — output filename on success
        rows     (int)  — number of rows processed
    """
    import io

    bunk_lookup = get_bunk_lookup(config)

    try:
        wb_in = load_workbook(filename=io.BytesIO(file_bytes))
    except Exception as e:
        return {"success": False, "message": f"Could not open Excel file: {e}"}

    # ------------------------------------------------------------------ #
    # TODO: Add report-type-specific transformation logic here once       #
    # sample files and desired outputs have been provided.                #
    # Each report type will have its own transformation function.         #
    # ------------------------------------------------------------------ #

    dispatch = {
        # "attendance":    _process_attendance,
        # "trip_roster":   _process_trip_roster,
        # "health_report": _process_health_report,
    }

    handler = dispatch.get(report_type)
    if handler is None:
        return {
            "success": False,
            "message": (
                f"Report type '{report_type}' is not yet configured. "
                "Please provide sample files to set up the transformation."
            ),
        }

    try:
        wb_out, rows_processed = handler(wb_in, bunk_lookup)
    except Exception as e:
        return {"success": False, "message": f"Processing error: {e}"}

    os.makedirs(output_dir, exist_ok=True)
    out_filename = f"report_{job_id}.xlsx"
    out_path = os.path.join(output_dir, out_filename)
    wb_out.save(out_path)

    return {
        "success": True,
        "message": f"Processed {rows_processed} rows successfully.",
        "filename": out_filename,
        "rows": rows_processed,
    }


# ------------------------------------------------------------------ #
# Shared styling helpers (reuse across report types)                  #
# ------------------------------------------------------------------ #

BRAND_FILL   = PatternFill("solid", fgColor="6D1F2F")
GOLD_FILL    = PatternFill("solid", fgColor="C9A84C")
HEADER_FONT  = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
BODY_FONT    = Font(name="Calibri", size=10)
CENTER_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)
LEFT_ALIGN   = Alignment(horizontal="left",   vertical="center", wrap_text=True)

THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)


def style_header_row(ws, row: int, num_cols: int) -> None:
    """Apply brand-color header styling to a row."""
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill = BRAND_FILL
        cell.font = HEADER_FONT
        cell.alignment = CENTER_ALIGN
        cell.border = THIN_BORDER


def style_data_row(ws, row: int, num_cols: int, alternate: bool = False) -> None:
    """Apply alternating-row body styling."""
    fill = PatternFill("solid", fgColor="F5E6E9") if alternate else PatternFill("solid", fgColor="FFFFFF")
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill = fill
        cell.font = BODY_FONT
        cell.alignment = LEFT_ALIGN
        cell.border = THIN_BORDER
