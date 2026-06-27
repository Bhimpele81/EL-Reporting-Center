# Project Handoff — EL Reporting Center (+ PGAGolfPool)

Paste this file (or its key points) at the start of a new Claude Code session to restore context.
Last updated: 2026-06-19.

---

## Repos & where the code lives

| Repo | Purpose | Notes |
|------|---------|-------|
| **Bhimpele81/EL-Reporting-Center** | The camp reporting web app (this is the main project) | Flask single-file app. Deployed on **Render** at `https://el-reporting-center.onrender.com`. Local working copy used this session: `C:\Users\bhimpele\AppData\Local\Temp\EL-Reporting-Center`. |
| **Bhimpele81/PGAGolfPool** | A separate React golf pool tracker | Fixed a tee-time timezone/caching bug this session (commit `566be3a`). |

Push to `master` (EL-Reporting-Center) / `main` (PGAGolfPool); Render auto-deploys EL-Reporting-Center on push.

---

## EL Reporting Center — architecture

- **`app.py`** — Flask app. Serves a single-page UI via `render_template_string(HTML)` where `HTML = r"""..."""` (a **raw** string — so use single `\n` for JS newlines, and `\\` for a literal backslash). All HTML/CSS/JS is inline in that string.
- **`report_processor.py`** — all report builders (openpyxl for Excel, python-docx for Word labels) + the master-sheet parser.
- **Storage**: AWS S3 (boto3, optional) **plus** local files under `uploads/`. JSON state files: `bunk_config.json`, `current_master.dat`/`_meta.json`, `payroll.json`, `families.json`, `users.json`. These are hidden from the "Recent reports" list via `_PROTECTED_KEYS`.
- **Fonts**: Roboto Slab + DM Sans. Brand color `#6D1F2F` (burgundy).

### Tabs (left sidebar)
1. **Run Report** — pick a report (+ week for week-aware ones), Run → builds in background, auto-downloads. Shows the saved-master banner (with uploader). No master upload here anymore.
2. **Payroll** — shared staff attendance system (see below).
3. **Bunk Snapshot** *(nav tab `#tab-snap-nav`, with a green "NEW" badge; available to all signed-in users)* — on-screen version of the Bunk Snapshot report. Two sub-tabs: **Totals** (Bunk Totals, Group Totals, Group/Bunk Totals by Week) and **Bunks** (per-bunk roster: Child / #1–#8 / M T W R F / Age / Grade + per-bunk Total, with a camper search). Shows a "Data last updated …" line from the master meta. `GET /api/bunk-snapshot` (login-only) returns `{has_master, meta, report, totals}` computed by `report_processor.bunk_snapshot_data(campers, config)` (mirrors `build_report_sheet`/`build_totals_sheet` numbers without writing Excel). Client caches the last snapshot in `localStorage` (`el_snap_cache_v2`) keyed by the master's `uploaded_at`; on open it paints the cache instantly, then does a cheap `GET /api/master` timestamp check and only re-fetches the full snapshot when the master changed (or `loadBunkSnapshot(true)` is forced). No spinner/reload on normal opens.
4. **Families** *(nav tab `data-tab="families"`, green "NEW" badge; all signed-in users)* — type a camper/parent/family name → cards showing everything the system has for that family: camper(s) with bunk/age/grade and a per-week schedule (master default days, with `schedules.json` overrides applied), address, and contacts (primary/secondary parent + authorized pickups). `GET /api/families/full` groups `families.json` rows by family last name + address + zip + primary parent, joins each camper to the master (by `name||bunk`, falling back to name) for age/grade/enrollment/days, and returns `{families:[{name,key,search,campers,address,contacts}], weeks, has_families, has_master}`. Client filters by the `search` blob; lazy-loaded, `famDirLoaded` reset after a master or family-contacts upload. Each card has an **✎ Edit** button: edits the shared **contact info** (address, primary/secondary parent name/phone/email, 4 pickups) and each camper's **per-week schedule** (M–F day toggles). Save PATCHes every member record id in the family (contacts are shared by siblings) and POSTs `/api/schedules` only for weeks that changed. `primary_email`/`secondary_email` are now first-class `FAMILY_FIELDS` (with aliases); the API still auto-detects emails from legacy auto-captured columns so existing data is editable without re-import.
5. **Utilities** (renamed from "Bunks & Camps") — top→bottom: **Master Sheet** upload, **Camper Schedules**, **Family Contacts**, **Season Calendar**, **Bunks & Camps** config, and (admins only) **User Accounts**.

---

## Reports (all sourced from one uploaded master sheet)

Week-aware reports filter/annotate to a selected week (1–8): Group Attendance, AM/PM/GRP Extend, Driver Totals, Inter & Junior labels.

- **Bunk Snapshot** (Report + Totals sheets)
- **Group Attendance** — bunk-per-page, FT CIT lines appended with area in parens; week # + dates header
- **AM Extend** — page header `AM EXTENDED HOURS SIGN-IN` + week/dates
- **PM Extend** — page header `PM EXTENDED HOURS SIGN-OUT` + week/dates
- **PM GRP Extend** — group-per-page; page header `PM EXTENDED GROUP ATTENDANCE` + week/dates; non-attending days marked with a bold em dash (—); footer legend ✓ / O only (the "C = Confirmed Absent" legend was removed)
- **Driver Totals** — driver-name banner, week highlight, booster/walk legend footer
- **Labels (Word/Avery 5960)**: **Inter** labels, **Junior** transport labels (3 sections, page-boundary padded), **Mailing** labels (one per unique address from family contacts: Last / Address 1 / Address 2 / City, State Zip; uses **Family** field when present, else Last — sources `families.json`, deduped by address). Importer auto-captures unrecognized columns under a header slug (e.g. a "Family" column → `family` key) so new fields are kept, not dropped.

Master-sheet workflow: upload once (Utilities), auto-detected, de-duplicated, persisted, reused, week-filtered.

---

## Payroll (server-persisted, shared across devices — `payroll.json`)

- Editable grid of staff × 40 season days, shown two weeks at a time (Weeks 1&2 … 7&8). Seeded from `payroll_seed.json`.
- **Tri-state day cells**: blank → ✓ (counts) → ✗. Left count column totals the visible block.
- **BS** and **SP\MTC** extra columns (✓/✗/½/N/A, never counted) — **Weeks 1&2 block only**; same size/style as day cells with a left separator.
- **Add/delete staff**; **click an Area cell to edit it inline** (`PATCH /api/payroll/staff/<id>`).
- **Filter area** + **Sort by** (last/area/total) on the weeks grid. Both hidden in Extended Staff; **Sort by also hidden in Totals** (Totals always sorts by last name).
- **Lock/Unlock** freezes edits.
- **Totals** view — cumulative checks all 8 weeks, "JC" tag for Junior Counselors.
- **Extended Staff** view — blank printable Mon–Fri check-in sheet for AM/PM-extended staff, with an **AM/PM shift** filter; prints full-page-width.
- **Print/PDF** (portrait for all views now) + **Excel export** (`/api/payroll/export`) mirror the current filtered/sorted view.

---

## Utilities tab

- **Master Sheet** — drag/drop upload → `POST /api/master` (validates it's a master, saves, records uploader). Blue box shows `uploaded … by <user>`; if the username is an email, only the part before `@` is shown.
- **Family Contacts** — import a spreadsheet (`POST /api/families/import`; Replace vs Append), editable table (click any cell to edit inline), Add family form, per-row delete. Stored in `families.json`. **Schema now matches the real contact export** (`bills_master_contact_stuff.csv`): `last, first, bunk, p1_first/p1_last/p1_phone, p2_first/p2_last/p2_phone, address/city/state/zip, pu1..pu4 name+auth`. Header detection in `_FAMILY_ALIASES` + `_norm_header` (strips the `2026 >` prefix, collapses whitespace). Verified import = 570 families. **Reports that source this data are not built yet (planned).**
- **Camper Schedules** *(card `#sched-card` in Utilities, just below the Master Sheet section — available to all signed-in users)* — search a camper → per-week M/T/W/R/F day toggles. Edits are local until the **💾 Save schedule** button persists them (also auto-saves on Back / switching campers so nothing is lost). Stored in `schedules.json` (overrides keyed by `name||bunk` → `{week: "MWF"}`; `""` = attends no days that week). `process_report(schedule_overrides=...)` applies them to the selected week (replaces `days_sched`/`days`/`enrolled`) for week-aware reports. `/api/schedules` GET/POST require login only (no admin gate). POST accepts `week`+`days`, `clear`, or `replace` (full week map for Save). Saved overrides are **not** cleared on master upload — they persist and reapply as long as the camper's `name||bunk` still matches (dropped if a camper changes bunks); the card hint says so. Verified end-to-end for admin and non-admin users.
- **Season Calendar** — set the Monday that starts each of the 8 camp weeks (`season.json`). Drives both the report week #/date-range headers (`process_report(..., week_dates=...)` → `report_processor.set_week_dates`) and the Payroll day columns (`_payroll_days()` derives from each week's Monday). Range strings auto-format ("June 22 – 26" / "June 29 – July 3"). `/api/season` GET/POST.
- **Bunks & Camps** — existing camp/bunk/group config editor.
- **User Accounts** (admins only) — add user (with optional email → Copy/Email-it credentials via mailto), Reset PW, delete.

---

## Authentication (added this session)

- Real server-side auth: **per-user accounts** with hashed passwords (werkzeug), Flask signed-cookie sessions. `before_request` gates **all `/api/*`** except `/api/me`, `/api/login`, `/api/register`, `/api/logout`.
- **Single username field** = login ID *and* display name (no separate "name").
- **Self-registration** gated by a shared **ACCESS_CODE**; **first account created = admin**. Bill is the sole admin.
- Header shows the signed-in username (bold, clickable) + **Sign out**. Clicking the name opens a **Change Password** dialog (`POST /api/account/password`, verifies current password).
- Admin manages users from Utilities → User Accounts. `/api/users` GET/POST, `/api/users/<u>` PATCH (password/name/role)/DELETE. Self-demotion blocked.

### ⚠️ Render env vars to set (important)
- **`SECRET_KEY`** — long random string; signs session cookies and keeps logins stable across restarts/workers. Falls back to a default if unset (works but less secure).
- **`ACCESS_CODE`** — registration code (defaults to `trial`); change to something private.
- `AWS_S3_BUCKET` / `AWS_S3_REGION` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — optional S3.

---

## Conventions / gotchas

- **Don't change report/spreadsheet formatting unless explicitly asked** (standing user instruction).
- `HTML` is a **raw** Python string — JS newlines = single `\n`; literal backslash = `\\`.
- Test endpoints locally with Flask's test client (`app.app.test_client()`); deps installed in this env are flask/openpyxl/boto3/lxml/python-docx. Master detection: `bills_master_attempt (1).csv` parses as a master; `attendance__master_*.csv` does **not**.
- Windows: use `C:/...` paths with Python (not MSYS `/c/...`); Bash tool resets cwd between calls, so `cd` each time.
- Commits end with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## Recent commits (most recent first)

```
1340648 Payroll Totals: hide Sort by dropdown; always sort alphabetically
a56f144 Master blue box: strip @domain from uploader when username is an email
81e8fd1 Self-service password change (click your name)
faff0d8 Header: fixed height so envelope emoji doesn't make Support taller
e3a47c7 Header: match Sign out button size to Pricing/Support
a27a4a2 Admin: after creating a user, show credentials with Copy + Email actions
c3920b1 Accounts: collapse name + username into a single username field
43b0f3b Admin user management: create users, reset passwords, promote/demote
d398943 Add per-user login accounts; record master uploader; gate API behind sessions
db81d3e Rename Bunks & Camps -> Utilities; move master upload there; add Family Contacts; first-time notice
8867416 PM GRP Extend: make non-attending dash visible
31f73f1 PM GRP Extend: drop 'C = Confirmed Absent' legend; change box 'C' to '-'
6c16817 Payroll: Extended Staff print fills full page width
```

---

## Open / planned follow-ups

- **Family-contact reports** — build reports that source `families.json` (waiting on the real sample spreadsheet to finalize import column mapping + fields).
- Optional: disable self-registration (admin-only account creation); inline edit of username; trim `@domain` in the header too (currently only the master blue box does).
- Password resets are intentionally **admin-driven** (no email provider). Self-service email reset was declined.
