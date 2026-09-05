# Project Handoff: EL Reporting Center (UI branded "Operations Center")

Paste this file (or its key points) at the start of a new Claude Code session to restore context.
Last updated: 2026-09-05.

---

## What changed most recently (June 19 to September 5, 2026)

Read this first; the older sections below describe the app as it stood in June and a few
details there are superseded here.

**Environment / where the code lives**
- Work from the real clone, never a Temp folder. Windows: `C:\Users\bhimpele\Desktop\GitHub\EL-Reporting-Center`. Mac: `/Users/billhimpele/Documents/GitHub/EL-Reporting-Center`. (The old `AppData\Local\Temp` working copy was auto-cleaned mid-session and lost files and its `.git`; everything was already pushed, so nothing was lost.)
- Local `uploads/pricing.json` and `payroll.json` are stale snapshots. For customer-facing numbers, read the live app.

**Rebrand**: "Reporting Center" became **"Operations Center"** across the UI (header subtitle, page title, sign-in screen, email subjects). Repo name unchanged.

**Pizza tab (now the single Pizza tab, no badge)**: the original manual calculator was removed and the bunk-picker version promoted.
- Per lunch period and camper group (Minors, Majors, Inter, Senior, Upper, Specialists): a **Bunks** dropdown auto-fills campers (current-week enrollment from the master) and staff (Payroll bunk assignments); both stay editable.
- **Specialists** use an **Areas** dropdown built from Payroll `area` values (camp names, Support, Director, and Floater excluded); each area's head count feeds the staff field.
- **FT CITs** are returned as an Upper bunk (the separate "FT CITs" camp in the master), so they flow through the camper math at the Upper rate. The 5 CIT counselors (Payroll bunk "CITs") fold onto the first `PT CIT*` bunk, assigned once (no double count).
- **Additional Pies** section: direct entry for School, Office, Maintenance, Vendor.
- Rounding: each group rounds **up to the next whole pizza** (no half pies). A "This Week" reference column shows the current week and per-group counts.
- Minor/Major is a first-class per-bunk `division` on Junior-Camp bunks (Utilities > Bunks & Camps); Pizza reads it and falls back to the Munchkins/Rugrats name rule.
- `GET /api/pizza-bunk-counts` returns `{groups:{minors,majors,inter,senior,upper:[{bunk,count,staff}]}, spec_areas:[{area,staff}], week, has_master}`. Dropdowns are empty without a master sheet (expected on a fresh local instance).

**Payroll**
- Every day now cycles blank -> check -> half -> x (the `PR_HALF_DAYS` July-3-only gating was removed; half counts 0.5 everywhere it already did).
- Fixed Staff/Area column overlap on narrow windows: `.pr-hstaff` got a width and the weeks table a `min-width:960px`, so the existing `overflow-x:auto` wrapper scrolls instead of crushing columns.
- **Read-only accounts**: per-user `readonly` flag (User Accounts). Read-only users can view/print Payroll and Rate Settings but saves are blocked server-side (403) and the UI hides save/lock controls.
- **Sign-In Activity** (admin, Utilities): last 20 events with how (new account / sign-in / active session), logged once per day per user.

**Pricing / Rate Sheet**
- **All Rates PDF** button on the Rate Sheet: one portrait page with every program (2nd Grade+, Junior, FT CIT) by weeks and days, Final and Early Season, sibling columns = childcare sibling weekly rate x weeks (FT CIT capped at the regular rate), alternating week-group shading.
- New categories: 1st Year Childcare and 1st Year CC Sibling. Bold column headings and Tuition numbers (DM Sans 700/800 are now loaded so bold renders).
- Subscription modal collapsed to two plans: Starter $29.99 and Unlimited $99.99.

**Other**
- One-time end-of-season pop-up ("Congratulations on a great camp season, and have a great final week!") shows once per user (localStorage key `el_congrats_2027final_<username>`). Remove once the season is over.
- Weather tile: the Open-Meteo fetch got a User-Agent, a 12 s timeout, a 30-minute in-memory cache, and a stale-forecast fallback. **Still unresolved**: after deploying, Bill reported the tile still says the forecast is unavailable on Render, so Render is likely blocking or throttling outbound calls to open-meteo. Next step is a different weather source or a small proxy.
- FAQ was updated alongside every feature above (standing rule).

**External deliverables produced from this app's data (not in the repo)**
- 2027 Early Enrollment rates flyer (one-page PDF) and a WordPress-ready HTML embed for `elbowlanecamp.com/dates-and-rates-2027/`. Rates were taken from the live Rate Sheet **Tuition** column (Junior 5-day: 5,175 / 5,075 / 4,725 / 4,175 / 3,550; 2nd Grade+: 5,775 / 5,625 / 5,250 / 4,650 / 3,950 for 8 to 4 weeks). Lesson: the local pricing.json was stale and produced wrong numbers first.
- A staff payroll-cycle change notice graphic for Elbow Lane School (moving from Thu-Wed / Friday pay to Sun-Sat / Wednesday pay; transition paychecks Sep 4, Sep 18, Sep 23, Oct 7).


---

## Repos & where the code lives

| Repo | Purpose | Notes |
|------|---------|-------|
| **Bhimpele81/EL-Reporting-Center** | The camp reporting web app (this is the main project) | Flask single-file app. Deployed on **Render** at `https://el-reporting-center.onrender.com`. Work from the real clone (Windows `C:\Users\bhimpele\Desktop\GitHub\EL-Reporting-Center`, Mac `/Users/billhimpele/Documents/GitHub/EL-Reporting-Center`), never a Temp folder. |
| **Bhimpele81/PGAGolfPool** | A separate React golf pool tracker | Fixed a tee-time timezone/caching bug this session (commit `566be3a`). |

Push to `master` (EL-Reporting-Center) / `main` (PGAGolfPool); Render auto-deploys EL-Reporting-Center on push.

---

## EL Reporting Center: architecture

- **`app.py`**: Flask app. Serves a single-page UI via `render_template_string(HTML)` where `HTML = r"""..."""` (a **raw** string: so use single `\n` for JS newlines, and `\\` for a literal backslash). All HTML/CSS/JS is inline in that string.
- **`report_processor.py`**: all report builders (openpyxl for Excel, python-docx for Word labels) + the master-sheet parser.
- **Storage**: AWS S3 (boto3, optional) **plus** local files under `uploads/`. JSON state files: `bunk_config.json`, `current_master.dat`/`_meta.json`, `payroll.json`, `families.json`, `users.json`. These are hidden from the "Recent reports" list via `_PROTECTED_KEYS`.
- **Fonts**: Roboto Slab + DM Sans. Brand color `#6D1F2F` (burgundy).

### Tabs (left sidebar)
1. **Run Report**: pick a report (+ week for week-aware ones), Run → builds in background, auto-downloads. Shows the saved-master banner (with uploader). No master upload here anymore.
2. **Payroll**: shared staff attendance system (see below).
3. **Bunk Snapshot** *(nav tab `#tab-snap-nav`, with a green "NEW" badge; available to all signed-in users)*: on-screen version of the Bunk Snapshot report. Two sub-tabs: **Totals** (Bunk Totals, Group Totals, Group/Bunk Totals by Week) and **Bunks** (per-bunk roster: Child / #1–#8 / M T W R F / Age / Grade + per-bunk Total, with a camper search). Shows a "Data last updated …" line from the master meta. `GET /api/bunk-snapshot` (login-only) returns `{has_master, meta, report, totals}` computed by `report_processor.bunk_snapshot_data(campers, config)` (mirrors `build_report_sheet`/`build_totals_sheet` numbers without writing Excel). Client caches the last snapshot in `localStorage` (`el_snap_cache_v2`) keyed by the master's `uploaded_at`; on open it paints the cache instantly, then does a cheap `GET /api/master` timestamp check and only re-fetches the full snapshot when the master changed (or `loadBunkSnapshot(true)` is forced). No spinner/reload on normal opens.
4. **Families** *(nav tab `data-tab="families"`, green "NEW" badge; all signed-in users)*: type a camper/parent/family name → cards showing everything the system has for that family: camper(s) with bunk/age/grade and a per-week schedule (master default days, with `schedules.json` overrides applied), address, and contacts (primary/secondary parent + authorized pickups). `GET /api/families/full` groups `families.json` rows by family last name + address + zip + primary parent, joins each camper to the master (by `name||bunk`, falling back to name) for age/grade/enrollment/days, and returns `{families:[{name,key,search,campers,address,contacts}], weeks, has_families, has_master}`. Client filters by the `search` blob; lazy-loaded, `famDirLoaded` reset after a master or family-contacts upload. Each card has an **✎ Edit** button: edits the shared **contact info** (address, primary/secondary parent name/phone/email, 4 pickups) and each camper's **per-week schedule** (M–F day toggles). Save PATCHes every member record id in the family (contacts are shared by siblings) and POSTs `/api/schedules` only for weeks that changed. `primary_email`/`secondary_email` are now first-class `FAMILY_FIELDS` (with aliases); the API still auto-detects emails from legacy auto-captured columns so existing data is editable without re-import.
5. **Pricing** *(nav tab `#tab-pricing-nav`, green NEW badge; available to all signed-in users: everyone can edit, saves are shared/global)*: pricing module reading one editable config (`pricing.json`, `GET/POST /api/pricing`, `@admin_required`, seeded from `_DEFAULT_PRICING` = 2026 worksheet numbers, `_deep_fill` merges new keys). Four sub-tabs: **Calculator** (per-camper weeks/days/transport rows + Early-Signup vs Regular tier → itemized camp total; separate childcare block with 1-child vs 2-sibling combined weekly rate × weeks), **Explorer** (% increase + rounding $1/$25/$50/$100 → camp ES/Final + childcare tables with $/% diff vs current), **Rate Settings** (editable inputs for camp tiers, day multiplier, transport, childcare → Save), **Rate Sheet** (branded, Print/Save-PDF landscape via visibility-isolation print CSS). Full matrix for two programs derived by rule from the 2nd Grade+ base tuition: **2nd Grade+** (base) and **Junior Camp, PS-1st** (=90% of 2nd Grade+, round $25), each Early Season + Regular, columns by 5/4/3 days with Tuition / Sibling (10% off, round $1, 5-day only) / +Transportation (5-day 2-way weekly x weeks; sibling transport 10% off). Season toggle picks Current vs Proposed base. `pxSheetSeason`. Camp tuition = `tiers[ES|Final][weeks] × day_mult[days]` (day_mult all 1.0 for now: camp may add day rates later). Transport = weekly × numeric weeks. Childcare separate from camp. Rolled out to all users (no admin gate; all edits shared). FAQ section "Pricing" documents all four sub-tabs.
6. **Utilities** (renamed from "Bunks & Camps"): top→bottom: **Master Sheet** upload, **Camper Schedules**, **Family Contacts**, **Season Calendar**, **Bunks & Camps** config, and (admins only) **User Accounts**.

---

## Reports (all sourced from one uploaded master sheet)

Week-aware reports filter/annotate to a selected week (1–8): Group Attendance, AM/PM/GRP Extend, Driver Totals, Inter & Junior labels.

- **Bunk Snapshot** (Report + Totals sheets)
- **Group Attendance**: bunk-per-page, FT CIT lines appended with area in parens; week # + dates header
- **AM Extend**: page header `AM EXTENDED HOURS SIGN-IN` + week/dates
- **PM Extend**: page header `PM EXTENDED HOURS SIGN-OUT` + week/dates
- **PM GRP Extend**: group-per-page; page header `PM EXTENDED GROUP ATTENDANCE` + week/dates; non-attending days marked with a bold em dash (—); footer legend ✓ / O only (the "C = Confirmed Absent" legend was removed)
- **Driver Totals**: driver-name banner, week highlight, booster/walk legend footer
- **Upper** labels (`upper_labels`, week-specific): every camper in the **Upper** camp (`_group_bunks(config,"Upper")`) enrolled in the selected week (any # of days), one Avery 5960 label each: camper name (bold) with the bunk (number stripped via `_label_bunk`) on the line below. `build_upper_labels_docx`.
- **Labels (Word/Avery 5960)**: **Inter** labels, **Junior** transport labels (3 sections, page-boundary padded), **Mailing** labels (one per unique address from family contacts: Last / Address 1 / Address 2 / City, State Zip; uses **Family** field when present, else Last: sources `families.json`, deduped by address). Importer auto-captures unrecognized columns under a header slug (e.g. a "Family" column → `family` key) so new fields are kept, not dropped.

Master-sheet workflow: upload once (Utilities), auto-detected, de-duplicated, persisted, reused, week-filtered.

---

## Payroll (server-persisted, shared across devices: `payroll.json`)

- Editable grid of staff × 40 season days, shown two weeks at a time (Weeks 1&2 … 7&8). Seeded from `payroll_seed.json`.
- **Tri-state day cells**: blank → ✓ (counts) → ✗. Left count column totals the visible block.
- **BS** and **SP\MTC** extra columns (✓/✗/½/N/A, never counted): **Weeks 1&2 block only**; same size/style as day cells with a left separator.
- **Add/delete staff**; **click an Area cell to edit it inline** (`PATCH /api/payroll/staff/<id>`).
- **Filter area** + **Sort by** (last/area/total) on the weeks grid. Both hidden in Extended Staff; **Sort by also hidden in Totals** (Totals always sorts by last name).
- **Lock/Unlock** freezes edits.
- **Totals** view: cumulative checks all 8 weeks, "JC" tag for Junior Counselors.
- **Extended Staff** view: blank printable Mon–Fri check-in sheet for AM/PM-extended staff, with an **AM/PM shift** filter; prints full-page-width.
- **Holiday** view (`prHoliday`, 🎆 button right of Extended Staff): all staff with columns Name, BS, SP\MTC, then holiday-week days **Th 7/2 / Mon 7/6 / Fri 7/3** (resolved from `payroll.days` by m/d label; dark separators before 7/2 and 7/3). Editable; reuses shared `prClickDayCell`/`prClickXCell` so marks sync with the week tabs.
- **Half-day:** every day cycles blank→✓→½→✗ (the old July-3-only `PR_HALF_DAYS` gate was removed); `prCount`/`totalChecks` and the server export `cnt` count ½ as 0.5.
- **Print/PDF** (portrait for all views now) + **Excel export** (`/api/payroll/export`) mirror the current filtered/sorted view.

---

## Utilities tab

- **Master Sheet**: drag/drop upload → `POST /api/master` (validates it's a master, saves, records uploader). Blue box shows `uploaded … by <user>`; if the username is an email, only the part before `@` is shown.
- **Family Contacts**: import a spreadsheet (`POST /api/families/import`; Replace vs Append), editable table (click any cell to edit inline), Add family form, per-row delete. Stored in `families.json`. **Schema now matches the real contact export** (`bills_master_contact_stuff.csv`): `last, first, bunk, p1_first/p1_last/p1_phone, p2_first/p2_last/p2_phone, address/city/state/zip, pu1..pu4 name+auth`. Header detection in `_FAMILY_ALIASES` + `_norm_header` (strips the `2026 >` prefix, collapses whitespace). Verified import = 570 families. **Reports that source this data are not built yet (planned).**
- **Camper Schedules** *(card `#sched-card` in Utilities, just below the Master Sheet section: available to all signed-in users)*: search a camper → per-week M/T/W/R/F day toggles. Edits are local until the **💾 Save schedule** button persists them (also auto-saves on Back / switching campers so nothing is lost). Stored in `schedules.json` (overrides keyed by `name||bunk` → `{week: "MWF"}`; `""` = attends no days that week). `process_report(schedule_overrides=...)` applies them to the selected week (replaces `days_sched`/`days`/`enrolled`) for week-aware reports. `/api/schedules` GET/POST require login only (no admin gate). POST accepts `week`+`days`, `clear`, or `replace` (full week map for Save). Saved overrides are **not** cleared on master upload: they persist and reapply as long as the camper's `name||bunk` still matches (dropped if a camper changes bunks); the card hint says so. Verified end-to-end for admin and non-admin users.
- **Season Calendar**: set the Monday that starts each of the 8 camp weeks (`season.json`). Drives both the report week #/date-range headers (`process_report(..., week_dates=...)` → `report_processor.set_week_dates`) and the Payroll day columns (`_payroll_days()` derives from each week's Monday). Range strings auto-format ("June 22 – 26" / "June 29 – July 3"). `/api/season` GET/POST.
- **Bunks & Camps**: existing camp/bunk/group config editor.
- **User Accounts** (admins only): add user (with optional email → Copy/Email-it credentials via mailto), Reset PW, delete.

---

## Authentication (added this session)

- Real server-side auth: **per-user accounts** with hashed passwords (werkzeug), Flask signed-cookie sessions. `before_request` gates **all `/api/*`** except `/api/me`, `/api/login`, `/api/register`, `/api/logout`.
- **Single username field** = login ID *and* display name (no separate "name").
- **Self-registration** gated by a shared **ACCESS_CODE**; **first account created = admin**. Bill is the sole admin.
- Header shows the signed-in username (bold, clickable) + **Sign out**. Clicking the name opens a **Change Password** dialog (`POST /api/account/password`, verifies current password).
- Admin manages users from Utilities → User Accounts. `/api/users` GET/POST, `/api/users/<u>` PATCH (password/name/role)/DELETE. Self-demotion blocked.

### ⚠️ Render env vars to set (important)
- **`SECRET_KEY`**: long random string; signs session cookies and keeps logins stable across restarts/workers. Falls back to a default if unset (works but less secure).
- **`ACCESS_CODE`**: registration code (defaults to `trial`); change to something private.
- `AWS_S3_BUCKET` / `AWS_S3_REGION` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`: optional S3.

---

## Conventions / gotchas

- **Don't change report/spreadsheet formatting unless explicitly asked** (standing user instruction).
- `HTML` is a **raw** Python string: JS newlines = single `\n`; literal backslash = `\\`.
- Test endpoints locally with Flask's test client (`app.app.test_client()`); deps installed in this env are flask/openpyxl/boto3/lxml/python-docx. Master detection: `bills_master_attempt (1).csv` parses as a master; `attendance__master_*.csv` does **not**.
- Windows: use `C:/...` paths with Python (not MSYS `/c/...`); Bash tool resets cwd between calls, so `cd` each time.
- Commits end with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## Recent commits (most recent first)

```
c37dcf5 Weather: make the forecast tile resilient on the deployed host
8e1bd1d Payroll: allow every day to be marked as a half day
a46353a Add one-time end-of-season congratulations pop-up
18effda Payroll: fix Staff/Area column overlap on narrow windows
7b3bdf4 Pizza: remove the NEW badge from the tab
2490eef Pizza: robust PT CIT staff match; drop Floater from specialty areas
7383ec5 Pizza: FT CITs become an Upper bunk; route CIT counselors to a PT CIT bunk
e45946d Pizza: FT CIT count from master sheet; round each group up to a whole pie
03746f4 Pizza: FT CIT moves into Upper bunk dropdown; Specialists areas from Payroll
cf793b3 Pizza: add FT CIT to Upper and area breakdown to Specialists
feda3ad Pizza: replace School entry with Additional Pies (School/Office/Maintenance/Vendor)
e7dc720 Remove original Pizza module; promote bunk-picker version to sole Pizza tab
8b72dc2 Bunks & Camps: add a Minor/Major designation per Junior-Camp bunk; Pizza reads it
24105a0 Add Pizza Beta tab: per-group bunk multi-select that auto-fills current-week counts
515a8d2 All Rates PDF: fill in the sibling columns (childcare sibling weekly rate x weeks), FT CIT cap
b52bd25 Pricing modal: collapse to two plans, Starter $29.99 and Unlimited $99.99
579b396 Sign-In Activity: log a returning session once per DAY
d34d51f Rebrand: 'Reporting Center' -> 'Operations Center' across the UI
7ad9ab5 Accounts: add per-user read-only access to Payroll and Rate Settings
640f787 Rate Sheet: add 'All Rates PDF' button generating a master of every rate
5dba55d Pricing: add '1st Year Childcare / School (weekly)' rate category
f8a03dd Add admin Sign-In Activity log (last 20 sign-ins)
b1510c5 Add Pizza Order Calculator tab (from Jeanette's request)
```

---

## Open / planned follow-ups

- **Extended care**: built. `pricing.extended.{am,pm}` weekly-fee tables by time slot × days (editable on Rate Settings, seeded from the Extended Hours sheet). Calculator has per-camper **AM care** / **PM care** pickers (`c.am`/`c.pm`); fee = rate[slot][days] × weeks. Siblings (any camper after the first) get `assumptions.sibling_ext_disc` ($5) off each weekly fee, AM and PM ($10/wk with both).
- **CIT**: done. `Full-Time CIT` is a Camp-rate option in the Calculator: tuition = full 2nd Grade+ tuition × (1 − `assumptions.cit_disc_pct`/100), rounded to `round_tuition`. Default 33% off. (The old dormant `assumptions.addons` scaffolding was removed.)
- Pricing rules are config-driven in `pricing.assumptions` (no hardcoded values); Calculator + Rate Sheet both read them. Rules now include `junior_pct`, `sibling_disc_pct`, `cit_disc_pct`, `sibling_ext_disc`, `early_season_pct`, and the roundings. All edited in the **Derivation rules & rounding** section of **Rate Settings** (the separate Assumptions sub-tab was merged in).
- **Early Season is fully derived**, not entered: ES tuition = `early_season_pct`% (93) of the corresponding **Regular (Final)** cell, rounded to the nearest $25 (round-half-up). Only the Regular 5-day base is entered in Rate Settings; the ES tier tables (`camp.tiers.ES` etc.) are unused/ignored. Applies in the Rate Sheet, Calculator (ES rate selector), and Explorer preview.
- **Minicamp is flat**, not calculated: `pricing.camp.mini = {5:550, 4:485, 3:440}`, identical for Early Season and Regular; edited in the "Minicamp (flat rates)" table in Rate Settings.
- Junior 4/3-day derive from the Junior 5-day rate × day factor (single rounding) in BOTH the Rate Sheet and Calculator, not 90% of the 2nd-Grade day-rate (that double-rounded).
- **Family-contact reports**: build reports that source `families.json` (waiting on the real sample spreadsheet to finalize import column mapping + fields).
- Optional: disable self-registration (admin-only account creation); inline edit of username; trim `@domain` in the header too (currently only the master blue box does).
- Password resets are intentionally **admin-driven** (no email provider). Self-service email reset was declined.
