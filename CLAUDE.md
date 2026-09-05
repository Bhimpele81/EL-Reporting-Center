# EL-Reporting-Center: project notes for Claude Code

This file loads automatically at the start of every Claude Code session in this repo.
For the full architecture, feature inventory, and history, read `HANDOFF.md` (it is long and
detailed; the "What changed most recently" section at the top is the fastest catch-up).

## What this is
A Flask back-office web app for **Elbow Lane Day Camp** (Warrington, PA). It turns one uploaded
camp master sheet into print-ready Excel/Word reports, and hosts shared tools: Payroll attendance,
Camp Snapshot, Families, Pricing / Rate Sheet, a Pizza order calculator, and Utilities.
The UI is branded **"Operations Center"** (it was renamed from "Reporting Center"); the repo name
did not change.

- `app.py`: the whole app. Flask routes plus the entire single-page UI inline in one Python
  **raw** string `HTML = r"""..."""`. Because it is a raw string, JS regexes use single backslashes
  (`/^\s*\d+\s*/`, not `\\s`), a JS newline is `\n`, and a literal backslash is `\\`.
- `report_processor.py`: report builders (openpyxl, python-docx) and the master-sheet parser.
- State lives in JSON under `uploads/` and is mirrored to AWS S3 when configured (it is, on the
  deployed instance): `bunk_config.json`, `payroll.json`, `pricing.json`, `families.json`,
  `schedules.json`, `season.json`, `users.json`, `login_log.json`. `uploads/` is gitignored.
- Brand: burgundy `#6D1F2F`, fonts Roboto Slab (headings) + DM Sans (body), logo `logo.png`.

## Deploy and environment
- GitHub `Bhimpele81/EL-Reporting-Center`, branch **`master`**. Push and Render auto-deploys to
  `https://el-reporting-center.onrender.com`. Health check: `/healthz` returns `OK`.
- Render free tier **sleeps after ~15 min idle**; a cold start takes 30 to 60 s and makes the first
  requests (including login) fail or hang. That is not an auth bug. An UptimeRobot ping keeps it
  awake; when it naps anyway the ping had a gap. User accounts persist in S3 across redeploys and
  `SECRET_KEY` is stable, so restarts do not log people out.
- Render env vars: `SECRET_KEY`, `ACCESS_CODE` (registration code, default `trial`),
  `AWS_S3_BUCKET` / `AWS_S3_REGION` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.

## Working locally
- Run: `ACCESS_CODE=trial PORT=5001 python app.py` (debug reloader on). Register a throwaway user
  with the access code to get a session; delete test users from `uploads/users.json` afterward.
- Test endpoints with `app.app.test_client()` and a `session_transaction()` that sets `sess["user"]`.
- Always `python -m py_compile app.py` before committing.
- **Do not work out of a Temp folder.** A Windows Temp working copy was auto-cleaned mid-session
  (files and even `.git` vanished). Use the real clone: Windows `C:\Users\bhimpele\Desktop\GitHub\
  EL-Reporting-Center`, Mac `/Users/billhimpele/Documents/GitHub/EL-Reporting-Center`.
- The local `uploads/pricing.json` and `payroll.json` are **snapshots and go stale**. For anything
  customer-facing (rates, staff counts), read the live app's Rate Sheet / Payroll, never the local copy.

## Standing rules from Bill (follow without being asked)
- **Never use em dashes** anywhere: UI text, FAQ, code comments, commits, docs. Use commas, colons,
  parentheses, or separate sentences.
- **Keep the FAQ tab in sync.** Every user-facing feature you add, change, or rename gets its FAQ
  entry updated in the same commit (`<details class="faq">` blocks under `<div class="help-sub">`
  section headers in `app.py`). Bill treats the FAQ as the live user manual.
- **Do not change spreadsheet or report formatting** unless explicitly asked. Scope report-builder
  edits to exactly what was requested; ask before touching fonts, borders, fills, widths.
- **Never use the word "corpus."** Say "the files" or "the document set."
- Commit messages end with `Co-Authored-By: Claude <noreply@anthropic.com>`.
- Commit and push when a change is done and verified; report exactly what was verified.

## Key domain facts (easy to get wrong)
- Camp groups for pizza and snapshot: Junior (split into **Minors** = Munchkins/Rugrats bunks and
  **Majors** = the rest, or by the per-bunk `division` set in Utilities > Bunks & Camps), Inter,
  Senior, Upper, plus Specialists (staff only). Upper includes the **"30/31 PT CITs"** bunks; the
  **"FT CITs"** camp is separate in the master but is treated as an Upper bunk in the Pizza tab.
- The 5 CIT counselors are logged on Payroll under bunk **"CITs"**; the Pizza tab folds all 5 onto
  the first `PT CIT*` bunk (both PT CIT groups eat together).
- Payroll specialist areas come from the Payroll `area` field; camp names, Support, Director, and
  Floater are excluded from the Specialists dropdown.
- Early Season tuition is **derived** (93% of Regular, rounded to $25); only Regular is entered.
- Every payroll day cycles blank -> check -> half -> x (half counts 0.5). Marks save to the server.
