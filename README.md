# EL Reporting Center

A report converter for **Elbow Lane Day Camp** that transforms CSV/XLSX camp management exports into formatted, print-ready Excel workbooks.

---

## Report Types

### 1. Bunk Snapshot
Two-sheet workbook with camper roster data:
- **Report sheet** — Campers grouped by bunk with week attendance columns (#1-#8), daily attendance (M-T-W-R-F), age, and grade. Subtotal row per bunk with page breaks. Landscape, print-ready.
- **Totals sheet** — Summary statistics per bunk, per camp, and per week.

### 2. Group Attendance
Single-sheet report with campers grouped by bunk. Each bunk gets its own page with signing cells for Mon-Fri and an enrolled count.

### 3. AM Extend
Morning extended care attendance. Extracts start times from enrollment strings. Columns: Camper, Bunk, Time, Mon-Fri, Days/Week. Sorted alphabetically. Portrait format.

### 4. PM Extend
Afternoon extended care attendance. Extracts pickup times. Two columns per day (Date + Time/Initial) for sign-out tracking. 14-column layout with Aptos Narrow font. Portrait format.

### 5. PM GRP Extend
Group-based PM attendance. Groups campers by group code from bunk configuration. Sorted by group order, then bunk number, then name. Subtotal count per group. Landscape format.

---

## Features

### File Processing
- Accepts **CSV and XLSX** uploads (drag-and-drop or file picker)
- **Background processing** with real-time progress updates
- Live progress log with color-coded messages (success/warning/error)
- Animated progress bar during conversion
- Download completed reports immediately

### Excel Output
- Print-optimized layouts (landscape or portrait per report type)
- Frozen headers, page breaks between groups, fitted column widths
- Branded styling with camp colors (#6D1F2F burgundy headers)
- Merged cells, rotated text, and custom fonts per report type

### Bunk & Camp Configuration
- Editable via the **Bunks & Camps** tab in the UI
- Supports 5 camps with 31 bunks and group codes
- Add/remove camps and bunks inline
- Configuration saved to `bunk_config.json` on the server

### Storage
- **Local filesystem** — reports saved to `/outputs/` directory (keeps 10 most recent)
- **AWS S3** (optional) — if configured, uploads reports to S3 with automatic cleanup
- Download checks S3 first, falls back to local
- Recent reports listed on the main page with download links

### UI
- Single-page app with two tabs: **Upload Report** and **Bunks & Camps**
- Responsive design with sticky header and tab bar
- Drag-and-drop file upload with validation
- Recent reports panel with timestamps and download links
- Summer 2026 calendar card with important camp dates
- Password gate for basic access control

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main UI |
| `/api/process` | POST | Submit file for conversion (returns job_id) |
| `/api/status/<job_id>` | GET | Poll job progress and status |
| `/api/download/<job_id>` | GET | Download completed report |
| `/api/recent` | GET | List 10 most recent reports |
| `/api/config` | GET | Retrieve bunk/camp configuration |
| `/api/config` | POST | Save bunk/camp configuration |
| `/health` | GET | Health check endpoint |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Flask 3.1 |
| Excel Generation | openpyxl |
| AWS Storage | boto3 (optional) |
| XML Parsing | lxml |
| Server | Gunicorn |
| Frontend | Embedded HTML/CSS/JS |
| Typography | Roboto Slab + DM Sans (Google Fonts) |

---

## Configuration

### Environment Variables (all optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_S3_BUCKET` | S3 bucket name (enables S3 storage) | — |
| `AWS_S3_REGION` | AWS region | us-east-2 |
| `AWS_ACCESS_KEY_ID` | AWS access key | — |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | — |

Without S3 configured, reports are stored locally only.

---

## Camp Structure

The default configuration includes 5 camps:

| Camp | Bunks | Groups |
|------|-------|--------|
| Junior | 7 bunks (01-07) | Jr1, Jr2, Jr3 |
| Inter | 7 bunks (08-14) | Jr3, Int1, Int2 |
| Senior | 10 bunks (15-24) | Int2, Sr1, Sr2, Up1 |
| Upper | 7 bunks (25-31) | Up1, Up2, CIT |
| FT CITs | 1 bunk (99) | CIT |

Editable via the Bunks & Camps tab in the UI.

---

## Installation

```bash
pip install -r requirements.txt
python app.py
```

Runs at `http://localhost:5001`. Upload and output directories are created automatically.

---

## Dependencies

```
flask==3.1.3
openpyxl==3.1.5
gunicorn==21.2.0
boto3==1.35.0
lxml==5.3.0
```
