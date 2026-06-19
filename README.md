# EL Reporting Center

A web app for **Elbow Lane Day Camp** that turns a single camp-management **master sheet** into a full set of formatted, print-ready reports (Excel **and** Word labels) — plus a shared, editable **Payroll** attendance system.

The site runs as a single Flask app with an embedded single-page UI. Upload the master once and every report is generated from it; no need to re-import per report.

---

## How It Works

1. **Upload the master sheet once.** On the **Run Report** tab, drop in the camp master export (CSV/XLSX). It's auto-detected, de-duplicated, and saved on the server for reuse.
2. **Pick a report** (and, for week-specific reports, a **week**).
3. **Run** — the report builds in the background and downloads automatically.

Later runs don't need a re-upload — they use the saved master. You can re-upload any time the data changes, download the saved master, or update it from the UI.

---

## Reports

All camp reports are generated from the master sheet. Reports tagged **(week-specific)** filter/annotate to the week you select; the others use the full roster.

### Excel reports

| Report | Description |
|--------|-------------|
| **Bunk Snapshot** | Two sheets. *Report* — campers grouped by bunk (one bunk per page, bunk banner, week columns #1–#8, daily M–F, age, grade with K/PK/PS, per-bunk total). *Totals* — per-bunk, per-group, and per-week summary on a single portrait page. |
| **Group Attendance** *(week-specific)* | Campers grouped by bunk, one bunk per page, with Mon–Fri signing cells and an enrolled count. FT CIT campers are appended on their matched bunk page with their assigned area in parentheses. Week # and dates in the header. |
| **AM Extend** *(week-specific)* | Morning extended-care sign-in. Camper, Bunk, Time, Mon–Fri, Days/Week. Page header **"AM EXTENDED HOURS SIGN-IN"** + week # / dates on every page. |
| **PM Extend** *(week-specific)* | Afternoon extended-care sign-out. Two columns per day (Time / Initial). Page header **"PM EXTENDED HOURS SIGN-OUT"** + week # / dates. |
| **PM GRP Extend** *(week-specific)* | PM extended care grouped by group code, one group per page with a group banner and per-group total. Page header **"PM EXTENDED GROUP ATTENDANCE"** + week # / dates. |
| **Driver Totals** *(week-specific)* | Per-driver transportation sheet (driver name as page banner). Highlights the selected week; includes a Booster Required / Must-Walk legend footer. |

### Word label reports (Avery 5960)

| Report | Description |
|--------|-------------|
| **Inter** labels *(week-specific)* | Bunk / camper / days labels grouped by bunk. Full-week campers leave the days line blank. |
| **Junior** transport labels *(week-specific)* | Single `.docx` with three sections (Transport, PM Extend, Car Line), each starting on its own page. |

Numeric bunk sort is applied throughout (numbered bunks first, then PT CITs, FT CITs, with Staff Transport / unassigned last).

---

## Payroll

A shared, server-persisted staff attendance system on the **Payroll** tab. Changes save automatically and are visible across devices.

- **Editable grid** of all staff with daily check cells across the 8-week season, shown two weeks at a time (Weeks 1 & 2 … 7 & 8).
- **Tri-state day cells** — blank → ✓ (counts) → ✗ (doesn't count). The left **count** column totals the checks for the visible two-week block.
- **Add / delete staff**, and **click an Area cell to edit it inline**.
- **Filter** by area and **sort** by last name, area, or total.
- **Lock / Unlock** to freeze the sheet against edits.
- **BS** and **SP\MTC** extra columns (4-state ✓ / ✗ / ½ / N/A, never counted) on the Weeks 1 & 2 block only.
- **Totals** view — cumulative checks across all 8 weeks, with a "JC" tag for Junior Counselors.
- **Extended Staff** view — a blank, printable Mon–Fri check-in sheet for staff with AM/PM extended hours, with an **AM / PM shift** filter.
- **Print / PDF** (portrait) and **Excel export** that both mirror the current filtered/sorted view.

Staff data is seeded from `payroll_seed.json` and stored in `payroll.json` (server + S3).

---

## Other UI

- **Run Report** tab — report picker (camp reports + a **Labels** section), week selector, master-sheet management, recent reports, and a weather/calendar card.
- **Bunks & Camps** tab — edit the camp/bunk/group configuration inline (saved to `bunk_config.json`).
- Password gate for basic access control; pricing modal.

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main UI |
| `/api/process` | POST | Submit a report job (returns `job_id`) |
| `/api/status/<job_id>` | GET | Poll job progress/status |
| `/api/download/<job_id>` | GET | Download a completed report |
| `/api/recent` | GET | List recent reports |
| `/api/files/<filename>` | GET | Fetch a stored report file |
| `/api/master` | GET / DELETE | Saved master-sheet info / remove it |
| `/api/master/download` | GET | Download the saved master sheet |
| `/api/config` | GET / POST | Read / save bunk & camp configuration |
| `/api/payroll` | GET | Load payroll staff, checks, days, lock state |
| `/api/payroll/check` | POST | Set a day / extra-column cell state |
| `/api/payroll/staff` | POST | Add a staff member |
| `/api/payroll/staff/<sid>` | PATCH / DELETE | Edit (e.g. area) or remove a staff member |
| `/api/payroll/lock` | POST | Lock / unlock the payroll sheet |
| `/api/payroll/export` | GET | Server-built `.xlsx` of the current view |
| `/api/weather` | GET | Weather for the calendar card |
| `/health`, `/healthz` | GET | Health checks |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Flask 3.1 |
| Excel generation | openpyxl |
| Word / label generation | python-docx (Avery 5960) |
| Storage | AWS S3 via boto3 (optional) + local filesystem |
| XML parsing | lxml |
| Server | Gunicorn |
| Frontend | Embedded HTML / CSS / JS (single page) |
| Typography | Roboto Slab + DM Sans (Google Fonts) |

Brand styling uses the camp's burgundy (`#6D1F2F`).

---

## Storage

- **Local filesystem** — reports are written to `/outputs/` (most recent kept); the saved master, payroll, and config live alongside.
- **AWS S3 (optional)** — when configured, reports/master/payroll/config are mirrored to S3 with automatic cleanup. Downloads check S3 first, then fall back to local.
- Internal files (master, payroll, config) are hidden from the **Recent reports** list.

---

## Configuration

### Environment variables (all optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_S3_BUCKET` | S3 bucket name (enables S3 storage) | — |
| `AWS_S3_REGION` | AWS region | us-east-2 |
| `AWS_ACCESS_KEY_ID` | AWS access key | — |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | — |

Without S3 configured, everything is stored locally.

---

## Camp Structure

Default configuration (editable via the **Bunks & Camps** tab):

| Camp | Bunks | Groups |
|------|-------|--------|
| Junior | 7 bunks (01–07) | Jr1, Jr2, Jr3 |
| Inter | 7 bunks (08–14) | Jr3, Int1, Int2 |
| Senior | 10 bunks (15–24) | Int2, Sr1, Sr2, Up1 |
| Upper | 7 bunks (25–31) | Up1, Up2, CIT |
| FT CITs | 1 bunk (99) | CIT |

---

## Installation

```bash
pip install -r requirements.txt
python app.py
```

Runs at `http://localhost:5001`. Upload/output directories are created automatically.

---

## Dependencies

```
flask==3.1.3
openpyxl==3.1.5
gunicorn==21.2.0
boto3==1.35.0
lxml==5.3.0
python-docx==1.2.0
```
