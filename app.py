"""
EL Reporting Center — Flask Application
-----------------------------------------
Drop-in Excel report converter for Elbow Lane Day Camp.
Shares the same design system as Transport Pro.
"""

import os
import io
import re
import csv
import json
import uuid
import threading
import urllib.request
from functools import wraps
from datetime import datetime, date, timedelta
try:
    from zoneinfo import ZoneInfo
    _EASTERN = ZoneInfo("America/New_York")
except Exception:
    _EASTERN = None
import boto3
from botocore.exceptions import ClientError
from flask import Flask, request, jsonify, send_file, render_template_string, session
from werkzeug.security import generate_password_hash, check_password_hash

from report_processor import process_report, load_bunk_config, save_bunk_config, is_master

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024  # 32 MB upload limit
# Stable secret for signed-cookie sessions. Set SECRET_KEY in the environment
# (esp. on multi-worker hosts); the fallback keeps sessions working otherwise.
app.secret_key = os.environ.get("SECRET_KEY", "el-reporting-center-default-secret-change-me")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=14)
# Shared code required to register a new account (augments per-user logins)
ACCESS_CODE = os.environ.get("ACCESS_CODE", "trial")

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "bunk_config.json")
UPLOAD_DIR  = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR  = os.path.join(BASE_DIR, "outputs")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# S3 setup (optional — falls back to local if env vars not set)
S3_BUCKET = os.environ.get("AWS_S3_BUCKET")
S3_REGION = os.environ.get("AWS_S3_REGION", "us-east-2")
_s3 = boto3.client(
    "s3",
    region_name=S3_REGION,
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
) if S3_BUCKET else None

def _s3_upload(local_path: str, filename: str) -> None:
    if _s3:
        _s3.upload_file(local_path, S3_BUCKET, filename)

def _s3_get_file(filename: str):
    """Download file from S3 into a BytesIO buffer and return it."""
    if not _s3:
        return None
    try:
        import io
        buf = io.BytesIO()
        _s3.download_fileobj(S3_BUCKET, filename, buf)
        buf.seek(0)
        return buf
    except ClientError:
        return None

def _s3_save_config(config: dict) -> None:
    """Save bunk config JSON to S3 so it persists across Render restarts."""
    if not _s3:
        return
    try:
        import io
        body = json.dumps(config, indent=2).encode("utf-8")
        _s3.put_object(Bucket=S3_BUCKET, Key="bunk_config.json", Body=body, ContentType="application/json")
    except ClientError as e:
        print(f"S3 config save failed: {e}")

def _s3_load_config() -> dict | None:
    """Load bunk config JSON from S3. Returns None if not found."""
    if not _s3:
        return None
    try:
        import io
        buf = io.BytesIO()
        _s3.download_fileobj(S3_BUCKET, "bunk_config.json", buf)
        buf.seek(0)
        return json.load(buf)
    except ClientError:
        return None

def _s3_list_recent(limit: int = 10) -> list:
    if not _s3:
        return []
    resp = _s3.list_objects_v2(Bucket=S3_BUCKET)
    # Only show generated report files — not internal config/master storage
    objects = [o for o in resp.get("Contents", [])
               if o["Key"].lower().endswith((".xlsx", ".docx", ".zip"))]
    objects.sort(key=lambda o: o["LastModified"], reverse=True)
    return objects[:limit]

def _s3_delete_old(keep: int = 10) -> None:
    if not _s3:
        return
    resp = _s3.list_objects_v2(Bucket=S3_BUCKET)
    # Never sweep the saved config or master sheet when pruning old outputs
    objects = sorted(
        [o for o in resp.get("Contents", []) if o["Key"] not in _PROTECTED_KEYS],
        key=lambda o: o["LastModified"], reverse=True,
    )
    for obj in objects[keep:]:
        try:
            _s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
        except ClientError:
            pass


# ---------------------------------------------------------------------------
# Saved master sheet — uploaded once, reused for every report until replaced
# ---------------------------------------------------------------------------

# Reports that take a camp-week selection
WEEK_AWARE_REPORTS = {"driver_totals", "group_attendance",
                      "am_extend", "pm_extend", "pm_grp_extend",
                      "inter_labels", "jr_transport_labels"}

MASTER_KEY        = "current_master.dat"
MASTER_META_KEY   = "current_master_meta.json"
LOCAL_MASTER      = os.path.join(UPLOAD_DIR, "current_master.dat")
LOCAL_MASTER_META = os.path.join(UPLOAD_DIR, "current_master_meta.json")
PAYROLL_KEY       = "payroll.json"
LOCAL_PAYROLL     = os.path.join(UPLOAD_DIR, "payroll.json")
SEED_PATH         = os.path.join(BASE_DIR, "payroll_seed.json")
WEEK1_MONDAY      = date(2026, 6, 22)   # first Monday of the 2026 season
FAMILIES_KEY      = "families.json"
LOCAL_FAMILIES    = os.path.join(UPLOAD_DIR, "families.json")
USERS_KEY         = "users.json"
LOCAL_USERS       = os.path.join(UPLOAD_DIR, "users.json")
SEASON_KEY        = "season.json"
LOCAL_SEASON      = os.path.join(UPLOAD_DIR, "season.json")
# Default season: Monday of each of the 8 camp weeks (2026)
_DEFAULT_SEASON_MONDAYS = ["2026-06-22", "2026-06-29", "2026-07-06", "2026-07-13",
                           "2026-07-20", "2026-07-27", "2026-08-03", "2026-08-10"]
# Fields stored for each family contact record (matches the contact master export)
FAMILY_FIELDS     = ["last", "first", "bunk",
                     "primary_first", "primary_last", "primary_phone",
                     "secondary_first", "secondary_last", "secondary_phone",
                     "address", "address2", "city", "state", "zip",
                     "pu1_name", "pu1_auth", "pu2_name", "pu2_auth",
                     "pu3_name", "pu3_auth", "pu4_name", "pu4_auth"]
_PROTECTED_KEYS   = {"bunk_config.json", MASTER_KEY, MASTER_META_KEY,
                     PAYROLL_KEY, FAMILIES_KEY, USERS_KEY, SEASON_KEY}


def _now_eastern_stamp() -> str:
    """Formatted Eastern-time timestamp, e.g. '6/19/2026 4:00 PM EDT'."""
    now = datetime.now(_EASTERN) if _EASTERN else datetime.now()
    fmt = "%#m/%#d/%Y %#I:%M %p %Z" if os.name == "nt" else "%-m/%-d/%Y %-I:%M %p %Z"
    return now.strftime(fmt).strip()


def _save_master(file_bytes: bytes, filename: str, uploaded_by: str = "") -> dict:
    """Persist the uploaded master sheet (S3 if configured, plus local copy)."""
    meta = {
        "filename":    filename or "master",
        "uploaded_at": _now_eastern_stamp(),
        "uploaded_by": uploaded_by or "",
        "size":        len(file_bytes),
    }
    try:
        with open(LOCAL_MASTER, "wb") as f:
            f.write(file_bytes)
        with open(LOCAL_MASTER_META, "w") as f:
            json.dump(meta, f)
    except Exception:
        pass
    if _s3:
        try:
            _s3.put_object(Bucket=S3_BUCKET, Key=MASTER_KEY, Body=file_bytes)
            _s3.put_object(Bucket=S3_BUCKET, Key=MASTER_META_KEY,
                           Body=json.dumps(meta).encode("utf-8"),
                           ContentType="application/json")
        except ClientError:
            pass
    return meta


def _load_master() -> bytes | None:
    """Return the saved master sheet bytes, or None if none stored."""
    if _s3:
        buf = _s3_get_file(MASTER_KEY)
        if buf:
            return buf.read()
    if os.path.exists(LOCAL_MASTER):
        try:
            with open(LOCAL_MASTER, "rb") as f:
                return f.read()
        except Exception:
            pass
    return None


def _load_master_meta() -> dict | None:
    """Return metadata about the saved master (filename, uploaded_at)."""
    if _s3:
        buf = _s3_get_file(MASTER_META_KEY)
        if buf:
            try:
                return json.load(buf)
            except Exception:
                pass
    if os.path.exists(LOCAL_MASTER_META):
        try:
            with open(LOCAL_MASTER_META) as f:
                return json.load(f)
        except Exception:
            pass
    return None

# ---------------------------------------------------------------------------
# Payroll attendance (staff roster + daily check-ins) — persisted like config
# ---------------------------------------------------------------------------

# --- Season calendar (editable 8-week Monday dates) ---

def _season_save(data: dict) -> None:
    try:
        with open(LOCAL_SEASON, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass
    if _s3:
        try:
            _s3.put_object(Bucket=S3_BUCKET, Key=SEASON_KEY,
                           Body=json.dumps(data).encode("utf-8"),
                           ContentType="application/json")
        except ClientError:
            pass


def _season_load() -> dict:
    data = None
    if _s3:
        buf = _s3_get_file(SEASON_KEY)
        if buf:
            try:
                data = json.load(buf)
            except Exception:
                data = None
    if data is None and os.path.exists(LOCAL_SEASON):
        try:
            with open(LOCAL_SEASON, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None
    if data is None:
        data = {}
    mondays = data.get("mondays") or []
    # Pad/trim to exactly 8, falling back to defaults for any blanks
    mondays = [(mondays[i] if i < len(mondays) and mondays[i] else _DEFAULT_SEASON_MONDAYS[i])
               for i in range(8)]
    return {"mondays": mondays}


def _season_mondays() -> list:
    out = []
    for iso in _season_load()["mondays"]:
        try:
            out.append(date.fromisoformat(iso))
        except (ValueError, TypeError):
            out.append(None)
    return out


def _week_range_str(monday: date) -> str:
    """'June 22 – 26' (same month) or 'June 29 – July 3' (spanning months)."""
    if monday is None:
        return ""
    fri = monday + timedelta(days=4)
    if monday.month == fri.month:
        return f"{monday.strftime('%B')} {monday.day} – {fri.day}"
    return f"{monday.strftime('%B')} {monday.day} – {fri.strftime('%B')} {fri.day}"


def _season_week_strings() -> list:
    """The 8 date-range strings used in report headers."""
    return [_week_range_str(m) for m in _season_mondays()]


def _payroll_days() -> list:
    """The 40 camp days (8 weeks x Mon-Fri) with day-of-week + m/d labels,
    derived from each week's Monday in the season calendar."""
    dows = ["MON", "TUES", "WED", "TH", "FRI"]
    mondays = _season_mondays()
    out = []
    for wk in range(8):
        base = mondays[wk] or (date.fromisoformat(_DEFAULT_SEASON_MONDAYS[wk]))
        for dow in range(5):
            d = base + timedelta(days=dow)
            out.append({"iso": d.isoformat(), "dow": dows[dow],
                        "md": f"{d.month}/{d.day}", "week": wk + 1})
    return out


def _payroll_save(data: dict) -> None:
    try:
        with open(LOCAL_PAYROLL, "w") as f:
            json.dump(data, f)
    except Exception:
        pass
    if _s3:
        try:
            _s3.put_object(Bucket=S3_BUCKET, Key=PAYROLL_KEY,
                           Body=json.dumps(data).encode("utf-8"),
                           ContentType="application/json")
        except ClientError:
            pass


def _payroll_load() -> dict:
    data = None
    if _s3:
        buf = _s3_get_file(PAYROLL_KEY)
        if buf:
            try:
                data = json.load(buf)
            except Exception:
                data = None
    if data is None and os.path.exists(LOCAL_PAYROLL):
        try:
            with open(LOCAL_PAYROLL) as f:
                data = json.load(f)
        except Exception:
            data = None
    if data is None:
        # First run — seed the roster from the bundled seed file
        try:
            with open(SEED_PATH) as f:
                seed = json.load(f)
            data = {"staff": seed.get("staff", []), "checks": {}}
        except Exception:
            data = {"staff": [], "checks": {}}
        _payroll_save(data)
    data.setdefault("staff", [])
    data.setdefault("checks", {})
    data.setdefault("locked", False)
    # Backfill 'bunk'/'title'/'ext' (added later) from the seed for staff missing them
    if any(("bunk" not in s or "title" not in s or "ext" not in s) for s in data["staff"]):
        try:
            with open(SEED_PATH) as f:
                seed_map = {(x["last"].lower(), x["first"].lower()): x
                            for x in json.load(f).get("staff", [])}
        except Exception:
            seed_map = {}
        for s in data["staff"]:
            sm = seed_map.get((s.get("last", "").lower(), s.get("first", "").lower()), {})
            if "bunk" not in s:
                s["bunk"] = sm.get("bunk", "")
            if "title" not in s:
                s["title"] = sm.get("title", "")
            if "ext" not in s:
                s["ext"] = sm.get("ext", "")
        _payroll_save(data)
    return data


# ---------------------------------------------------------------------------
# Family contacts — uploaded/imported once, then editable in the Utilities tab
# ---------------------------------------------------------------------------

def _families_save(data: dict) -> None:
    try:
        with open(LOCAL_FAMILIES, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass
    if _s3:
        try:
            _s3.put_object(Bucket=S3_BUCKET, Key=FAMILIES_KEY,
                           Body=json.dumps(data).encode("utf-8"),
                           ContentType="application/json")
        except ClientError:
            pass


def _families_load() -> dict:
    data = None
    if _s3:
        buf = _s3_get_file(FAMILIES_KEY)
        if buf:
            try:
                data = json.load(buf)
            except Exception:
                data = None
    if data is None and os.path.exists(LOCAL_FAMILIES):
        try:
            with open(LOCAL_FAMILIES, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None
    if data is None:
        data = {"families": []}
    data.setdefault("families", [])
    return data


def _family_next_id(families: list) -> str:
    nums = [int(x["id"][1:]) for x in families
            if str(x.get("id", "")).startswith("f") and str(x["id"])[1:].isdigit()]
    return "f" + str((max(nums) if nums else 0) + 1)


def _norm_header(h) -> str:
    """Lowercase, collapse whitespace, drop a leading '2026 >' prefix."""
    s = re.sub(r"\s+", " ", str(h or "").strip().lower())
    s = re.sub(r"^20\d\d\s*>\s*", "", s)   # strip '2026 >' / '2026 > ' prefixes
    return s.strip()


# Normalized header → canonical field (best-effort import mapping)
_FAMILY_ALIASES = {
    "last":     ["last name", "last", "camper last name", "camper last"],
    "first":    ["first name", "first", "camper first name", "camper first"],
    "bunk":     ["bunk name", "bunk"],
    "primary_first":   ["p1 first name", "parent 1 first name", "guardian 1 first name", "primary first name"],
    "primary_last":    ["p1 last name", "parent 1 last name", "guardian 1 last name", "primary last name"],
    "primary_phone":   ["p1 cell phone", "p1 phone", "parent 1 phone", "parent 1 cell phone", "primary phone"],
    "secondary_first": ["p2 first name", "parent 2 first name", "guardian 2 first name", "secondary first name"],
    "secondary_last":  ["p2 last name", "parent 2 last name", "guardian 2 last name", "secondary last name"],
    "secondary_phone": ["p2 cell phone", "p2 phone", "parent 2 phone", "parent 2 cell phone", "secondary phone"],
    "address":  ["primary family address 1", "primary family address", "address", "home address", "street"],
    "address2": ["primary family address 2", "address 2", "address line 2", "apt", "unit", "suite"],
    "city":     ["primary family city", "city"],
    "state":    ["primary family state", "state"],
    "zip":      ["primary family zip", "zip", "zip code", "postal code"],
    "pu1_name": ["authorized pick-up/emergency contact: 1 - first & last name", "pickup 1 name"],
    "pu1_auth": ["authorized pick-up/emergency contact: 1 - authorization", "pickup 1 authorization"],
    "pu2_name": ["authorized pick-up/emergency contact: 2 - first & last name", "pickup 2 name"],
    "pu2_auth": ["authorized pick-up/emergency contact: 2 - authorization", "pickup 2 authorization"],
    "pu3_name": ["authorized pick-up/emergency contact: 3 - first & last name", "pickup 3 name"],
    "pu3_auth": ["authorized pick-up/emergency contact: 3 - authorization", "pickup 3 authorization"],
    "pu4_name": ["authorized pick-up/emergency contact: 4 - first & last name", "pickup 4 name"],
    "pu4_auth": ["authorized pick-up/emergency contact: 4 - authorization", "pickup 4 authorization"],
}


def _families_from_rows(rows: list) -> list:
    """Map a header row + data rows into family records."""
    if not rows:
        return []
    hl = [_norm_header(h) for h in rows[0]]
    col_for = {}
    for field, aliases in _FAMILY_ALIASES.items():
        for a in aliases:
            if a in hl:
                col_for[field] = hl.index(a)
                break
    out = []
    for r in rows[1:]:
        rec = {}
        for field in FAMILY_FIELDS:
            ci = col_for.get(field)
            rec[field] = (str(r[ci]).strip() if (ci is not None and ci < len(r) and r[ci] is not None) else "")
        if any(rec.get(f) for f in FAMILY_FIELDS):
            out.append(rec)
    return out


def _read_spreadsheet_rows(file_bytes: bytes, filename: str) -> list:
    """Return a list of rows (each a list of cell values); first row = headers."""
    name = (filename or "").lower()
    if name.endswith((".xlsx", ".xlsm", ".xls")):
        from openpyxl import load_workbook
        wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb.active
        return [[c for c in row] for row in ws.iter_rows(values_only=True)]
    # CSV / TSV
    text = file_bytes.decode("utf-8-sig", errors="replace")
    delim = "\t" if (name.endswith(".tsv") or "\t" in text.split("\n", 1)[0]) else ","
    return [row for row in csv.reader(io.StringIO(text), delimiter=delim)]


# ---------------------------------------------------------------------------
# User accounts (login) — username + hashed password, server-side sessions
# ---------------------------------------------------------------------------

def _users_save(data: dict) -> None:
    try:
        with open(LOCAL_USERS, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass
    if _s3:
        try:
            _s3.put_object(Bucket=S3_BUCKET, Key=USERS_KEY,
                           Body=json.dumps(data).encode("utf-8"),
                           ContentType="application/json")
        except ClientError:
            pass


def _users_load() -> dict:
    data = None
    if _s3:
        buf = _s3_get_file(USERS_KEY)
        if buf:
            try:
                data = json.load(buf)
            except Exception:
                data = None
    if data is None and os.path.exists(LOCAL_USERS):
        try:
            with open(LOCAL_USERS, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None
    if data is None:
        data = {"users": []}
    data.setdefault("users", [])
    return data


def _find_user(username: str) -> dict | None:
    u = (username or "").strip().lower()
    return next((x for x in _users_load()["users"] if x.get("username", "").lower() == u), None)


def _current_user() -> dict | None:
    uname = session.get("user")
    return _find_user(uname) if uname else None


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if _current_user() is None:
            return jsonify({"error": "auth", "message": "Please sign in."}), 401
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        u = _current_user()
        if u is None:
            return jsonify({"error": "auth", "message": "Please sign in."}), 401
        if not u.get("is_admin"):
            return jsonify({"error": "forbidden", "message": "Admins only."}), 403
        return f(*args, **kwargs)
    return wrapper


# Paths reachable without a session (the page shell + auth endpoints)
_PUBLIC_PATHS = {"/", "/logo.png", "/health", "/healthz",
                 "/api/me", "/api/login", "/api/register", "/api/logout"}


@app.before_request
def _require_login():
    """Gate every /api/* route behind a valid session, except the auth ones."""
    p = request.path
    if p in _PUBLIC_PATHS or not p.startswith("/api/"):
        return None
    if _current_user() is None:
        return jsonify({"error": "auth", "message": "Please sign in."}), 401
    return None


# In-memory job store  {job_id: {status, progress, result}}
jobs: dict = {}
jobs_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------

def run_job(job_id: str, file_bytes: bytes, report_type: str, week_num: int = None) -> None:
    def log(msg: str, level: str = "info") -> None:
        with jobs_lock:
            jobs[job_id]["progress"].append({"msg": msg, "level": level})

    try:
        with jobs_lock:
            jobs[job_id]["status"] = "running"

        log("Loading bunk configuration…")
        config = _s3_load_config() or load_bunk_config(CONFIG_PATH)

        log(f"Processing report type: {report_type}…")
        result = process_report(file_bytes, report_type, config, job_id, OUTPUT_DIR,
                                week_num=week_num, week_dates=_season_week_strings())

        if result["success"]:
            log(result["message"], "ok")
            with jobs_lock:
                jobs[job_id]["status"]   = "done"
                jobs[job_id]["filename"] = result["filename"]
                jobs[job_id]["rows"]     = result.get("rows", 0)
            # Upload to S3 and clean up old files
            try:
                local_path = os.path.join(OUTPUT_DIR, result["filename"])
                _s3_upload(local_path, result["filename"])
                _s3_delete_old(keep=10)
            except Exception:
                pass
            # Keep only 10 most recent local files
            try:
                all_files = sorted(
                    [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".xlsx")],
                    key=lambda f: os.path.getmtime(os.path.join(OUTPUT_DIR, f)),
                    reverse=True
                )
                for old in all_files[10:]:
                    os.remove(os.path.join(OUTPUT_DIR, old))
            except Exception:
                pass
        else:
            log(result["message"], "err")
            with jobs_lock:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"]  = result["message"]

    except Exception as exc:
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(exc)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/logo.png")
def logo():
    path = os.path.join(BASE_DIR, "logo.png")
    if os.path.exists(path):
        return send_file(path, mimetype="image/png")
    return "", 404


# --- Bunk / Camp config ---

@app.route("/api/config", methods=["GET"])
def get_config():
    try:
        # Try S3 first (persists across Render restarts), fall back to local file
        config = _s3_load_config() or load_bunk_config(CONFIG_PATH)
        return jsonify(config)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/config", methods=["POST"])
def save_config():
    try:
        data = request.get_json(force=True)
        save_bunk_config(CONFIG_PATH, data)  # save locally as backup
        _s3_save_config(data)                # save to S3 for persistence
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _season_summary() -> dict:
    mondays = _season_mondays()
    start = _season_load()["mondays"][0]
    last_fri = (mondays[-1] + timedelta(days=4)) if mondays[-1] else None
    end_str = last_fri.strftime("%B %#d, %Y" if os.name == "nt" else "%B %-d, %Y") if last_fri else ""
    return {"start": start, "end": end_str,
            "weeks": [{"week": i + 1, "range": _week_range_str(m)} for i, m in enumerate(mondays)]}


@app.route("/api/season", methods=["GET"])
def api_season():
    return jsonify(_season_summary())


@app.route("/api/season", methods=["POST"])
def api_season_save():
    body = request.get_json(force=True, silent=True) or {}
    start = (body.get("start") or "").strip()
    try:
        d0 = date.fromisoformat(start)
    except (ValueError, TypeError):
        return jsonify({"error": "Enter a valid camp-start date."}), 400
    # Derive 8 consecutive Mondays from the start date
    clean = [(d0 + timedelta(days=7 * i)).isoformat() for i in range(8)]
    _season_save({"mondays": clean})
    return jsonify({"ok": True, **_season_summary()})


# --- Report processing ---

@app.route("/api/master", methods=["GET"])
def api_master():
    """Report whether a master sheet is currently saved (for the UI)."""
    meta = _load_master_meta()
    if meta and _load_master() is not None:
        return jsonify({"loaded": True, **meta})
    return jsonify({"loaded": False})


@app.route("/api/master", methods=["POST"])
def api_master_save():
    """Upload + save a master sheet directly (from the Utilities tab)."""
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "No file uploaded."}), 400
    file_bytes = f.read()
    if not is_master(file_bytes):
        return jsonify({"error": "That file doesn't look like a master sheet. "
                                 "Check that it has the expected camper columns."}), 400
    u = _current_user() or {}
    meta = _save_master(file_bytes, f.filename, uploaded_by=u.get("name") or u.get("username") or "")
    return jsonify({"loaded": True, **meta})


@app.route("/api/master/download")
def api_master_download():
    """Download the currently-saved master sheet (to edit and re-upload)."""
    data = _load_master()
    if data is None:
        return jsonify({"error": "No saved master sheet."}), 404
    meta = _load_master_meta() or {}
    fname = meta.get("filename") or "master.csv"
    return send_file(io.BytesIO(data), as_attachment=True, download_name=fname,
                     mimetype=_mime_for(fname))


@app.route("/api/master", methods=["DELETE"])
def api_master_clear():
    """Forget the saved master sheet."""
    for p in (LOCAL_MASTER, LOCAL_MASTER_META):
        try:
            os.remove(p)
        except OSError:
            pass
    if _s3:
        for key in (MASTER_KEY, MASTER_META_KEY):
            try:
                _s3.delete_object(Bucket=S3_BUCKET, Key=key)
            except ClientError:
                pass
    return jsonify({"loaded": False})


@app.route("/api/payroll", methods=["GET"])
def api_payroll():
    data = _payroll_load()
    return jsonify({"staff": data["staff"], "checks": data["checks"],
                    "days": _payroll_days(), "locked": data.get("locked", False)})


@app.route("/api/payroll/lock", methods=["POST"])
def api_payroll_lock():
    body = request.get_json(force=True, silent=True) or {}
    data = _payroll_load()
    data["locked"] = bool(body.get("locked"))
    _payroll_save(data)
    return jsonify({"locked": data["locked"]})


@app.route("/api/payroll/export")
def api_payroll_export():
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    view   = request.args.get("view", "weeks")
    sort   = request.args.get("sort", "last")
    extp   = request.args.get("extp", "ALL")
    sel_areas = [a for a in (request.args.get("areas", "") or "").split(",") if a]
    try:
        period = int(request.args.get("period", "0"))
    except (TypeError, ValueError):
        period = 0

    data = _payroll_load()
    checks = data["checks"]
    days_all = _payroll_days()
    staff = [s for s in data["staff"] if not sel_areas or s.get("area") in sel_areas]

    SYM = {"check": "✓", "x": "✗", "half": "½", "na": "N/A", True: "✓"}
    def namekey(s): return (s.get("last", "") + s.get("first", "")).lower()
    def cnt(sid, days):
        c = checks.get(sid, {})
        return sum(1 for d in days if c.get(d["iso"]) in ("check", True))
    def area_txt(s):
        return s.get("title") if (s.get("area") == "Support" and s.get("title")) else s.get("area", "")

    wb = Workbook(); ws = wb.active; ws.title = "Payroll"
    HDR = Font(bold=True, color="FFFFFF"); FILL = PatternFill("solid", fgColor="6D1F2F")
    CTR = Alignment(horizontal="center", vertical="center")
    LEFT = Alignment(horizontal="left", vertical="center")
    _t = Side(style="thin", color="CCCCCC"); BORD = Border(left=_t, right=_t, top=_t, bottom=_t)

    def header(cols):
        ws.append(cols)
        for c in ws[ws.max_row]:
            c.font = HDR; c.fill = FILL; c.alignment = CTR; c.border = BORD

    if view == "totals":
        staff.sort(key=(lambda s: (-cnt(s["id"], days_all), namekey(s))) if sort == "total"
                   else (lambda s: (s.get("area", "").lower(), namekey(s))) if sort == "area"
                   else namekey)
        header(["Staff", "Area", "Total Checks (all 8 weeks)"])
        for s in staff:
            jc = " (JC)" if "junior" in (s.get("title", "").lower()) else ""
            a = area_txt(s) + (f" — {s['bunk']}" if s.get("bunk") else "")
            ws.append([f"{s.get('last','')}, {s.get('first','')}{jc}", a, cnt(s["id"], days_all)])
            for c in ws[ws.max_row]:
                c.border = BORD; c.alignment = CTR if c.column == 3 else LEFT
        widths = {"A": 30, "B": 22, "C": 14}; fname = "Payroll_Totals.xlsx"
    elif view == "ext":
        def _shift_ok(e):
            e = e or ""
            if extp == "AM": return "AM" in e.upper()
            if extp == "PM": return "PM" in e.upper()
            return bool(e)
        staff = [s for s in staff if s.get("ext") and _shift_ok(s.get("ext"))]
        staff.sort(key=namekey)
        header(["Staff", "MON", "TUES", "WED", "THURS", "FRI"])
        for s in staff:
            ws.append([f"{s.get('last','')}, {s.get('first','')}", "", "", "", "", ""])
            ws.row_dimensions[ws.max_row].height = 26
            for c in ws[ws.max_row]:
                c.border = BORD; c.alignment = LEFT if c.column == 1 else CTR
        widths = {"A": 30, "B": 11, "C": 11, "D": 11, "E": 11, "F": 11}
        _sfx = {"AM": "_AM", "PM": "_PM"}.get(extp, "")
        fname = f"Extended_Staff{_sfx}.xlsx"
    else:  # weeks
        days = days_all[period * 10:period * 10 + 10]
        staff.sort(key=(lambda s: (-cnt(s["id"], days), namekey(s))) if sort == "total"
                   else (lambda s: (s.get("area", "").lower(), namekey(s))) if sort == "area"
                   else namekey)
        cols = ["#", "Staff", "Area"] + [f"{d['dow']} {d['md']}" for d in days]
        if period == 0:
            cols += ["BS", "SP\\MTC"]
        header(cols)
        for s in staff:
            c = checks.get(s["id"], {})
            row = [cnt(s["id"], days), f"{s.get('last','')}, {s.get('first','')}",
                   area_txt(s) + (f" / {s['bunk']}" if s.get("bunk") else "")]
            row += [SYM.get(c.get(d["iso"]), "") for d in days]
            if period == 0:
                row += [SYM.get(c.get(f"xtra:0:{cc}"), "") for cc in (1, 2)]
            ws.append(row)
            for cell in ws[ws.max_row]:
                cell.border = BORD; cell.alignment = LEFT if cell.column == 2 else CTR
        widths = {"A": 5, "B": 26, "C": 18}; fname = f"Payroll_Weeks_{period*2+1}_{period*2+2}.xlsx"

    for col, w in widths.items():
        ws.column_dimensions[col].width = w
    ws.page_setup.orientation = "landscape"
    ws.print_options.horizontalCentered = True

    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=fname, mimetype=_XLSX_MIME)


@app.route("/api/payroll/check", methods=["POST"])
def api_payroll_check():
    body = request.get_json(force=True, silent=True) or {}
    sid = str(body.get("id", "")); dt = str(body.get("date", "")); val = body.get("value")
    if not sid or not dt:
        return jsonify({"error": "missing id/date"}), 400
    data = _payroll_load()
    if data.get("locked"):
        return jsonify({"error": "locked"}), 403
    checks = data["checks"].setdefault(sid, {})
    if val in ("check", "x", "half", "na"):
        checks[dt] = val
    else:
        checks.pop(dt, None)   # blank / cleared
    _payroll_save(data)
    return jsonify({"ok": True})


@app.route("/api/payroll/staff", methods=["POST"])
def api_payroll_add():
    body = request.get_json(force=True, silent=True) or {}
    last = (body.get("last") or "").strip()
    first = (body.get("first") or "").strip()
    area = (body.get("area") or "").strip()
    if not last and not first:
        return jsonify({"error": "Name required."}), 400
    data = _payroll_load()
    if data.get("locked"):
        return jsonify({"error": "locked"}), 403
    nums = [int(s["id"][1:]) for s in data["staff"]
            if str(s.get("id", "")).startswith("s") and str(s["id"])[1:].isdigit()]
    entry = {"id": "s" + str((max(nums) if nums else 0) + 1),
             "last": last, "first": first, "area": area}
    data["staff"].append(entry)
    _payroll_save(data)
    return jsonify(entry)


@app.route("/api/payroll/staff/<sid>", methods=["PATCH"])
def api_payroll_edit(sid):
    body = request.get_json(force=True, silent=True) or {}
    data = _payroll_load()
    if data.get("locked"):
        return jsonify({"error": "locked"}), 403
    s = next((x for x in data["staff"] if x.get("id") == sid), None)
    if s is None:
        return jsonify({"error": "not found"}), 404
    if "area" in body:
        s["area"] = (body.get("area") or "").strip()
    _payroll_save(data)
    return jsonify(s)


@app.route("/api/payroll/staff/<sid>", methods=["DELETE"])
def api_payroll_del(sid):
    data = _payroll_load()
    if data.get("locked"):
        return jsonify({"error": "locked"}), 403
    data["staff"] = [s for s in data["staff"] if s.get("id") != sid]
    data["checks"].pop(sid, None)
    _payroll_save(data)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@app.route("/api/me", methods=["GET"])
def api_me():
    u = _current_user()
    any_users = bool(_users_load()["users"])
    if u is None:
        return jsonify({"authenticated": False, "has_users": any_users})
    return jsonify({"authenticated": True, "username": u["username"],
                    "name": u.get("name") or u["username"], "is_admin": bool(u.get("is_admin"))})


@app.route("/api/login", methods=["POST"])
def api_login():
    body = request.get_json(force=True, silent=True) or {}
    u = _find_user(body.get("username", ""))
    if u is None or not check_password_hash(u.get("pw_hash", ""), body.get("password", "")):
        return jsonify({"error": "Invalid username or password."}), 401
    session.permanent = True
    session["user"] = u["username"]
    return jsonify({"username": u["username"], "name": u.get("name") or u["username"],
                    "is_admin": bool(u.get("is_admin"))})


@app.route("/api/register", methods=["POST"])
def api_register():
    body = request.get_json(force=True, silent=True) or {}
    if (body.get("code") or "").strip() != ACCESS_CODE:
        return jsonify({"error": "Incorrect access code."}), 403
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400
    if len(password) < 4:
        return jsonify({"error": "Password must be at least 4 characters."}), 400
    data = _users_load()
    if any(x.get("username", "").lower() == username.lower() for x in data["users"]):
        return jsonify({"error": "That username is taken."}), 409
    entry = {"username": username, "name": username,
             "pw_hash": generate_password_hash(password),
             "is_admin": len(data["users"]) == 0}   # first account is the admin
    data["users"].append(entry)
    _users_save(data)
    session.permanent = True
    session["user"] = username
    return jsonify({"username": username, "name": entry["name"], "is_admin": entry["is_admin"]})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/account/password", methods=["POST"])
def api_account_password():
    """Let the signed-in user change their own password."""
    u = _current_user()
    if u is None:
        return jsonify({"error": "auth"}), 401
    body = request.get_json(force=True, silent=True) or {}
    current = body.get("current") or ""
    new = body.get("new") or ""
    if not check_password_hash(u.get("pw_hash", ""), current):
        return jsonify({"error": "Current password is incorrect."}), 403
    if len(new) < 4:
        return jsonify({"error": "New password must be at least 4 characters."}), 400
    data = _users_load()
    rec = next((x for x in data["users"] if x.get("username", "").lower() == u["username"].lower()), None)
    if rec is None:
        return jsonify({"error": "not found"}), 404
    rec["pw_hash"] = generate_password_hash(new)
    _users_save(data)
    return jsonify({"ok": True})


@app.route("/api/users", methods=["GET"])
@admin_required
def api_users():
    users = [{"username": x["username"], "name": x.get("name") or x["username"],
              "is_admin": bool(x.get("is_admin"))} for x in _users_load()["users"]]
    return jsonify({"users": users})


@app.route("/api/users", methods=["POST"])
@admin_required
def api_users_add():
    body = request.get_json(force=True, silent=True) or {}
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400
    if len(password) < 4:
        return jsonify({"error": "Password must be at least 4 characters."}), 400
    data = _users_load()
    if any(x.get("username", "").lower() == username.lower() for x in data["users"]):
        return jsonify({"error": "That username is taken."}), 409
    entry = {"username": username, "name": username,
             "pw_hash": generate_password_hash(password),
             "is_admin": bool(body.get("is_admin"))}
    data["users"].append(entry)
    _users_save(data)
    return jsonify({"username": username, "name": entry["name"], "is_admin": entry["is_admin"]})


@app.route("/api/users/<username>", methods=["PATCH"])
@admin_required
def api_users_edit(username):
    body = request.get_json(force=True, silent=True) or {}
    data = _users_load()
    u = next((x for x in data["users"] if x.get("username", "").lower() == username.lower()), None)
    if u is None:
        return jsonify({"error": "not found"}), 404
    new_u = (body.get("username") or "").strip()
    if new_u and new_u != u["username"]:
        if new_u.lower() != u["username"].lower() and \
           any(x.get("username", "").lower() == new_u.lower() for x in data["users"]):
            return jsonify({"error": "That username is taken."}), 409
        old = u["username"]
        if u.get("name", "") == old:   # name mirrors username in our model
            u["name"] = new_u
        u["username"] = new_u
        if session.get("user", "") == old:
            session["user"] = new_u
    if "name" in body:
        u["name"] = (body.get("name") or "").strip() or u["username"]
    if body.get("password"):
        if len(body["password"]) < 4:
            return jsonify({"error": "Password must be at least 4 characters."}), 400
        u["pw_hash"] = generate_password_hash(body["password"])
    if "is_admin" in body:
        me = _current_user()
        # Don't let an admin strip their own admin rights (avoid lockout)
        if not (me and me["username"].lower() == username.lower() and not body["is_admin"]):
            u["is_admin"] = bool(body["is_admin"])
    _users_save(data)
    return jsonify({"username": u["username"], "name": u.get("name"), "is_admin": bool(u.get("is_admin"))})


@app.route("/api/users/<username>", methods=["DELETE"])
@admin_required
def api_users_del(username):
    me = _current_user()
    if me and me["username"].lower() == username.lower():
        return jsonify({"error": "You can't delete your own account."}), 400
    data = _users_load()
    data["users"] = [x for x in data["users"] if x.get("username", "").lower() != username.lower()]
    _users_save(data)
    return jsonify({"ok": True})


@app.route("/api/families", methods=["GET"])
def api_families():
    return jsonify(_families_load())


@app.route("/api/families", methods=["POST"])
def api_families_add():
    body = request.get_json(force=True, silent=True) or {}
    data = _families_load()
    entry = {"id": _family_next_id(data["families"])}
    for f in FAMILY_FIELDS:
        entry[f] = (body.get(f) or "").strip()
    if not any(entry[f] for f in FAMILY_FIELDS):
        return jsonify({"error": "Enter at least one field."}), 400
    data["families"].append(entry)
    _families_save(data)
    return jsonify(entry)


@app.route("/api/families/<fid>", methods=["PATCH"])
def api_families_edit(fid):
    body = request.get_json(force=True, silent=True) or {}
    data = _families_load()
    fam = next((x for x in data["families"] if x.get("id") == fid), None)
    if fam is None:
        return jsonify({"error": "not found"}), 404
    for f in FAMILY_FIELDS:
        if f in body:
            fam[f] = (body.get(f) or "").strip()
    _families_save(data)
    return jsonify(fam)


@app.route("/api/families/<fid>", methods=["DELETE"])
def api_families_del(fid):
    data = _families_load()
    data["families"] = [x for x in data["families"] if x.get("id") != fid]
    _families_save(data)
    return jsonify({"ok": True})


@app.route("/api/families/import", methods=["POST"])
def api_families_import():
    f = request.files.get("file")
    if not f or not f.filename:
        return jsonify({"error": "No file uploaded."}), 400
    mode = request.form.get("mode", "replace")   # 'replace' or 'append'
    try:
        rows = _read_spreadsheet_rows(f.read(), f.filename)
        parsed = _families_from_rows(rows)
    except Exception as e:
        return jsonify({"error": f"Could not read file: {e}"}), 400
    if not parsed:
        return jsonify({"error": "No family rows found. Check that the sheet has a header row."}), 400
    data = _families_load()
    existing = [] if mode == "replace" else data["families"]
    out = list(existing)
    for rec in parsed:
        rec = {"id": _family_next_id(out), **rec}
        out.append(rec)
    data["families"] = out
    u = _current_user() or {}
    data["uploaded_at"] = _now_eastern_stamp()
    data["uploaded_by"] = u.get("name") or u.get("username") or ""
    data["filename"] = f.filename
    _families_save(data)
    return jsonify({"ok": True, "count": len(parsed), "total": len(out), "mode": mode})


@app.route("/api/process", methods=["POST"])
def api_process():
    excel_file  = request.files.get("excel_file")
    report_type = request.form.get("report_type", "").strip()

    if not report_type:
        return jsonify({"error": "No report type selected."}), 400

    # A new upload is used directly; if it's a master, save it for reuse.
    # With no upload, fall back to the previously saved master sheet.
    if excel_file and excel_file.filename:
        file_bytes = excel_file.read()
        if is_master(file_bytes):
            _save_master(file_bytes, excel_file.filename)
    else:
        file_bytes = _load_master()
        if not file_bytes:
            return jsonify({"error": "No file uploaded and no saved master sheet found. "
                                     "Upload a master sheet first."}), 400

    job_id     = uuid.uuid4().hex[:8]

    # Week-specific reports: Driver Totals highlights the week; the others
    # filter campers to those enrolled in the selected week.
    week_num = None
    if report_type in WEEK_AWARE_REPORTS:
        try:
            week_num = int(request.form.get("week_num", 0))
            if week_num < 1 or week_num > 8:
                week_num = None
        except (TypeError, ValueError):
            week_num = None

    with jobs_lock:
        jobs[job_id] = {"status": "queued", "progress": []}

    thread = threading.Thread(target=run_job,
                              args=(job_id, file_bytes, report_type),
                              kwargs={"week_num": week_num},
                              daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def api_status(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if job is None:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job)


_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

def _mime_for(filename: str) -> str:
    fn = filename.lower()
    if fn.endswith(".docx"):
        return _DOCX_MIME
    if fn.endswith(".zip"):
        return "application/zip"
    if fn.endswith(".csv"):
        return "text/csv"
    if fn.endswith(".xls"):
        return "application/vnd.ms-excel"
    return _XLSX_MIME


@app.route("/api/download/<job_id>")
def api_download(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if job is None or job.get("status") != "done":
        return jsonify({"error": "File not ready."}), 404
    filename = job["filename"]
    buf = _s3_get_file(filename)
    if buf:
        return send_file(buf, as_attachment=True, download_name=filename,
                         mimetype=_mime_for(filename))
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        return jsonify({"error": "Output file missing."}), 500
    return send_file(path, as_attachment=True, download_name=filename,
                     mimetype=_mime_for(filename))


@app.route("/api/files/<path:filename>")
def api_download_file(filename: str):
    safe = os.path.basename(filename)
    buf = _s3_get_file(safe)
    if buf:
        return send_file(buf, as_attachment=True, download_name=safe,
                         mimetype=_mime_for(safe))
    path = os.path.join(OUTPUT_DIR, safe)
    if not os.path.exists(path):
        return jsonify({"error": "File not found."}), 404
    return send_file(path, as_attachment=True, download_name=safe,
                     mimetype=_mime_for(safe))


@app.route("/api/recent")
def api_recent():
    try:
        if _s3:
            objects = _s3_list_recent(10)
            return jsonify([{
                "name":  o["Key"],
                "mtime": o["LastModified"].timestamp(),
                "url":   f"/api/files/{o['Key']}",
            } for o in objects])
        files = []
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(".xlsx"):
                fpath = os.path.join(OUTPUT_DIR, f)
                files.append({"name": f, "mtime": os.path.getmtime(fpath)})
        files.sort(key=lambda x: x["mtime"], reverse=True)
        return jsonify([{
            "name":  f["name"],
            "mtime": f["mtime"],
            "url":   f"/api/files/{f['name']}",
        } for f in files[:10]])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/weather")
def api_weather():
    """5-day forecast for Warrington, PA via Open-Meteo (no API key required)."""
    try:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=40.2479&longitude=-75.1330"
            "&daily=temperature_2m_max,temperature_2m_min,weathercode"
            "&temperature_unit=fahrenheit"
            "&timezone=America%2FNew_York"
            "&forecast_days=5"
        )
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
        daily = data["daily"]
        days  = []
        for i in range(5):
            days.append({
                "date":    daily["time"][i],
                "high":    round(daily["temperature_2m_max"][i]),
                "low":     round(daily["temperature_2m_min"][i]),
                "code":    daily["weathercode"][i],
            })
        return jsonify({"days": days})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health")
@app.route("/healthz")
def health():
    return "OK"


# ---------------------------------------------------------------------------
# Embedded HTML / CSS / JS
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<link rel="icon" type="image/png" href="/logo.png">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Elbow Lane — Reporting Center</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Roboto+Slab:wght@600;700;800&family=DM+Sans:ital,wght@0,300;0,400;0,500;0,600;1,400&display=swap" rel="stylesheet">
<style>
:root {
--brand: #6D1F2F;
--brand-dark: #4a1520;
--brand-mid: #9e3347;
--brand-light: #f5e6e9;
--gold: #c9a84c;
--gold-lt: #f0d98a;
--ink: #1a1018;
--mist: #f8f4f5;
--border: #e8dde0;
--success: #2d6a4f;
--warn: #b36a00;
--r: 12px;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'DM Sans',sans-serif;background:var(--mist);color:var(--ink);min-height:100vh}
header{background:var(--brand);color:#fff;padding:0 2rem;display:flex;align-items:center;gap:1.25rem;height:80px;box-shadow:0 2px 16px rgba(109,31,47,.35);position:sticky;top:0;z-index:200}
.h-nav{margin-left:auto;display:flex;align-items:center;gap:.6rem}
.h-support,.h-pricing{background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.3);color:#fff;font-size:.78rem;font-weight:600;letter-spacing:.05em;padding:.45rem 1rem;border-radius:6px;cursor:pointer;text-decoration:none;display:flex;align-items:center;gap:.4rem;line-height:1.1;height:34px;box-sizing:border-box;transition:background .18s}
.h-support:hover,.h-pricing:hover{background:rgba(255,255,255,.28)}
/* ---- First-time notice modal ---- */
#notice-overlay{position:fixed;inset:0;background:rgba(20,6,9,.72);backdrop-filter:blur(4px);z-index:10000;display:flex;align-items:center;justify-content:center;padding:1.5rem}
#notice-overlay.hidden{display:none}
#notice-box{background:#fff;border-radius:16px;padding:2rem 2rem 1.6rem;max-width:460px;width:94%;box-shadow:0 20px 60px rgba(0,0,0,.35);text-align:center}
#notice-box .notice-icon{font-size:2.2rem;margin-bottom:.5rem}
#notice-box h2{font-family:'Roboto Slab',serif;font-size:1.2rem;color:var(--brand);margin:0 0 .7rem}
#notice-box p{font-size:.9rem;color:#444;line-height:1.5;margin:0 0 .8rem}
#notice-box .notice-btn{margin-top:.4rem;padding:.6rem 1.6rem;background:var(--brand);color:#fff;border:none;border-radius:8px;font-family:'Roboto Slab',serif;font-weight:700;font-size:.85rem;letter-spacing:.03em;text-transform:uppercase;cursor:pointer}
#notice-box .notice-btn:hover{background:var(--brand-dark)}
/* ---- Change-password modal ---- */
#cpw-overlay{position:fixed;inset:0;background:rgba(20,6,9,.72);backdrop-filter:blur(4px);z-index:10001;display:flex;align-items:center;justify-content:center;padding:1.5rem}
#cpw-overlay.hidden{display:none}
#cpw-box{background:#fff;border-radius:14px;padding:1.8rem 1.8rem 1.5rem;max-width:380px;width:94%;box-shadow:0 20px 60px rgba(0,0,0,.35)}
#cpw-box h2{font-family:'Roboto Slab',serif;font-size:1.15rem;color:var(--brand);margin:0 0 .5rem}
/* ---- Pricing modal ---- */
#pricing-overlay{position:fixed;inset:0;background:rgba(20,6,9,.72);backdrop-filter:blur(4px);z-index:9999;display:flex;align-items:flex-start;justify-content:center;overflow-y:auto;padding:2rem 0}
#pricing-overlay.hidden{display:none}
#pricing-box{background:#fff;border-radius:16px;padding:2.4rem 2.2rem 2rem;max-width:820px;width:94%;box-shadow:0 20px 60px rgba(0,0,0,.35);position:relative;margin:auto}
#pricing-box .px-close{position:absolute;top:1rem;right:1.2rem;background:none;border:none;font-size:1.4rem;color:#bbb;cursor:pointer;line-height:1;transition:color .15s}
#pricing-box .px-close:hover{color:var(--brand)}
#pricing-box h2{font-family:'Roboto Slab',serif;font-size:1.4rem;color:var(--brand-dark);text-align:center;margin-bottom:.3rem}
#pricing-box .px-sub{text-align:center;font-size:.85rem;color:#888;margin-bottom:2rem}
.px-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:1.1rem}
.px-card{border:2px solid var(--border);border-radius:12px;padding:1.6rem 1.4rem;display:flex;flex-direction:column;gap:.6rem;position:relative;transition:border-color .2s,box-shadow .2s}
.px-card:hover{border-color:var(--brand-mid);box-shadow:0 6px 24px rgba(109,31,47,.1)}
.px-card.featured{border-color:var(--brand);box-shadow:0 6px 24px rgba(109,31,47,.15)}
.px-badge{position:absolute;top:-13px;left:50%;transform:translateX(-50%);background:var(--brand);color:#fff;font-size:.65rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;padding:.25rem .85rem;border-radius:20px;white-space:nowrap}
.px-tier{font-family:'Roboto Slab',serif;font-size:1rem;font-weight:700;color:var(--brand-dark);text-transform:uppercase;letter-spacing:.04em}
.px-price{font-family:'Roboto Slab',serif;font-size:2rem;font-weight:700;color:var(--brand)}
.px-price span{font-size:.85rem;font-weight:400;color:#999}
.px-desc{font-size:.82rem;color:#666;line-height:1.55;flex:1}
.px-features{list-style:none;display:flex;flex-direction:column;gap:.45rem;margin-top:.4rem}
.px-features li{font-size:.8rem;color:#555;display:flex;align-items:flex-start;gap:.5rem}
.px-features li::before{content:"✓";color:var(--brand);font-weight:700;flex-shrink:0}
.px-cta{margin-top:1rem;padding:.65rem 1rem;background:var(--brand);color:#fff;border:none;border-radius:8px;font-family:'Roboto Slab',serif;font-size:.82rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;cursor:pointer;transition:background .18s;text-align:center}
.px-cta:hover{background:var(--brand-dark)}
.px-card.featured .px-cta{background:var(--brand-dark)}
.px-note{text-align:center;font-size:.75rem;color:#aaa;margin-top:1.4rem}
@media(max-width:640px){.px-grid{grid-template-columns:1fr}.h-nav{gap:.4rem}}
.h-logo{width:60px;height:60px;flex-shrink:0;border-radius:50%;background-image:url("/logo.png");background-size:90%;background-position:center;background-repeat:no-repeat;background-color:var(--brand-dark)}
.h-title{font-family:'Roboto Slab',serif;font-size:1.25rem;font-weight:700;letter-spacing:.02em;text-transform:uppercase}
.h-sub{font-size:.72rem;opacity:.75;font-weight:400;margin-top:2px;letter-spacing:.08em;text-transform:uppercase}
.h-badge{margin-left:auto;background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.3);color:#fff;font-size:.68rem;font-family:'Roboto Slab',serif;font-weight:500;letter-spacing:.12em;text-transform:uppercase;padding:.35rem .9rem;border-radius:20px;white-space:nowrap}
.tab-bar{display:flex;background:#fff;border-bottom:2px solid var(--border);position:sticky;top:80px;z-index:100}
.tab{padding:.85rem 1.75rem;font-size:.82rem;font-weight:500;font-family:'Roboto Slab',serif;letter-spacing:.07em;text-transform:uppercase;color:#999;cursor:pointer;border-bottom:3px solid transparent;margin-bottom:-2px;transition:color .15s,border-color .15s;white-space:nowrap;display:flex;align-items:center;gap:.5rem}
.tab:hover{color:var(--brand-mid)}
.tab.active{color:var(--brand);border-bottom-color:var(--brand)}
.tab-badge{background:var(--brand);color:#fff;font-size:.65rem;font-weight:700;padding:.15rem .45rem;border-radius:10px;min-width:18px;text-align:center}
.container{max-width:960px;margin:0 auto;padding:2rem 1.5rem 4rem}
.tab-panel{display:none}.tab-panel.active{display:block}
.payroll-table{border-collapse:collapse;width:100%;font-size:.85rem}
.payroll-table th,.payroll-table td{border:1px solid #cfcfcf;padding:.35rem .4rem;text-align:center;vertical-align:middle}
.payroll-table td{height:42px}
.payroll-table thead th{background:var(--brand);color:#fff;font-weight:700;white-space:nowrap}
.payroll-table td.pr-name{text-align:left;font-weight:600;width:150px;white-space:normal;line-height:1.15}
.payroll-table td.pr-area{color:#555;width:86px;white-space:normal;line-height:1.15}
.payroll-table td.pr-area-edit{cursor:pointer}
.payroll-table td.pr-area-edit:hover{background:#f4eef0;outline:1px dashed var(--brand)}
.pr-area-input{width:80px;font-size:.8rem;padding:2px 3px;border:1px solid var(--brand);border-radius:4px;text-align:center}
.payroll-table th.pr-extday{width:74px;min-width:74px}
.fam-table{border-collapse:collapse;width:100%;font-size:.8rem}
.fam-table th,.fam-table td{border:1px solid #e2e2e2;padding:.35rem .5rem;text-align:left;vertical-align:top}
.fam-table thead th{background:var(--brand);color:#fff;font-weight:600;white-space:nowrap;font-size:.72rem;text-transform:uppercase;letter-spacing:.03em}
.fam-table td.fam-cell{cursor:pointer;min-width:70px}
.fam-table td.fam-cell:hover{background:#f4eef0;outline:1px dashed var(--brand)}
.fam-table .fam-del{cursor:pointer;border:none;background:none;color:#c0392b;font-size:.9rem;padding:0}
.usr-ic{cursor:pointer;border:none;background:none;color:#1A79BF;font-size:1rem;padding:0 .3rem;line-height:1}
.usr-ic:hover{color:var(--brand)}
.season-row{display:flex;align-items:center;gap:.7rem;padding:.3rem 0}
.season-row .sr-wk{font-weight:700;color:var(--brand);width:64px;font-size:.85rem}
.season-row input[type=date]{padding:.4rem .5rem;border:1px solid var(--border);border-radius:6px;font-size:.85rem}
.season-row .sr-range{font-size:.82rem;color:#666}
.payroll-table td.pr-count{font-weight:700;color:var(--brand);width:34px}
.payroll-table tbody tr:nth-child(even){background:#f4eef0}
.payroll-table td.pr-cell,.payroll-table td.pr-xcell{cursor:pointer;font-weight:800;font-size:1.6rem;user-select:none;line-height:1}
.payroll-table td.st-check{color:#2e7d32}
.payroll-table td.st-x{color:#c0392b}
.payroll-table td.st-half{color:#1A79BF}
.payroll-table td.st-na{color:#888;font-size:1.05rem;font-weight:700}
.payroll-table th.pr-day,.payroll-table td.pr-cell{width:42px;min-width:42px}
.payroll-table th.pr-extra{width:42px;min-width:42px;max-width:42px;background:#3f1119;color:#fff;white-space:nowrap;font-size:.6rem;letter-spacing:-.02em;padding-left:.12rem;padding-right:.12rem;line-height:1.1}
.payroll-table td.pr-xcell{width:42px;min-width:42px;max-width:42px}
.pr-xsep{border-left:2px solid #6d1f2f !important}
.payroll-table.pr-locked td.pr-cell,.payroll-table.pr-locked td.pr-xcell{cursor:not-allowed}
.payroll-table .pr-del{cursor:pointer;border:none;background:none;color:#c0392b;font-size:.95rem;padding:0}
.pr-week-sep{border-left:3px solid #6d1f2f !important}
.pr-period-btn{padding:.4rem .8rem;border:1px solid var(--brand);background:#fff;color:var(--brand);border-radius:8px;cursor:pointer;font-weight:600;font-size:.85rem}
.pr-period-btn.active{background:var(--brand);color:#fff}
.pr-period-btn.pr-sm{padding:.28rem .55rem;font-size:.72rem;font-weight:600}
.pr-input{padding:.45rem .6rem;border:1px solid var(--border);border-radius:8px;font-size:.85rem}
.pr-multi{position:relative;display:inline-block}
.pr-multi-btn{cursor:pointer;background:#fff;display:flex;align-items:center;gap:.5rem;min-width:120px;justify-content:space-between}
.pr-multi-btn .caret{font-size:.65rem;color:#888}
.pr-multi-menu{position:absolute;top:100%;left:0;margin-top:.25rem;background:#fff;border:1px solid var(--border);border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.15);min-width:170px;max-height:460px;overflow-y:auto;z-index:50;padding:.3rem}
.pr-multi-menu.hidden{display:none}
.pr-multi-menu label{display:flex;align-items:center;gap:.5rem;padding:.35rem .5rem;border-radius:6px;cursor:pointer;font-size:.85rem;color:#333;white-space:nowrap}
.pr-multi-menu label:hover{background:#f4eef0}
.pr-multi-menu .pr-multi-sep{border-top:1px solid #eee;margin:.25rem 0}
.payroll-table caption{caption-side:top;text-align:left;font-weight:700;font-size:1rem;padding:.3rem 0 .5rem;color:var(--brand)}
@media print {
  body * { visibility:hidden; }
  #payroll-table, #payroll-table * { visibility:visible; }
  #payroll-table { position:absolute; left:0; top:0; width:auto; font-size:9pt; }
  #payroll-table .pr-del { display:none; }
  /* keep maroon headers, row shading and symbol colors when printing */
  #payroll-table, #payroll-table * { -webkit-print-color-adjust:exact; print-color-adjust:exact; }
  /* Extended Staff sheet fills the page width (Staff ~30%, 5 day cols split the rest) */
  #payroll-table.pr-ext { width:100%; }
  #payroll-table.pr-ext .pr-extday { width:14%; }
}
.card{background:#fff;border:1px solid var(--border);border-radius:var(--r);padding:1.5rem 1.75rem;margin-bottom:1.1rem;box-shadow:0 1px 4px rgba(0,0,0,.04);transition:box-shadow .2s}
.card:hover{box-shadow:0 3px 12px rgba(109,31,47,.07)}
.card-hd{display:flex;align-items:center;gap:.7rem;margin-bottom:1.1rem}
.card-num{width:26px;height:26px;background:var(--brand);color:#fff;border-radius:50%;font-size:.75rem;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.card-title{font-family:'Roboto Slab',serif;font-size:1.05rem;font-weight:700;color:var(--brand-dark);letter-spacing:.01em;text-transform:uppercase}
.card-hint{font-size:.75rem;color:#999;margin-top:.15rem;font-weight:300}
label.lbl{display:block;font-size:.75rem;font-weight:600;color:var(--brand-dark);letter-spacing:.04em;text-transform:uppercase;margin-bottom:.4rem}
/* Drop zone */
.drop-zone{border:2px dashed var(--border);border-radius:var(--r);padding:1rem;text-align:center;cursor:pointer;transition:all .2s;background:var(--mist);position:relative}
.drop-zone:hover,.drop-zone.drag-over{border-color:var(--brand-mid);background:var(--brand-light)}
.drop-zone input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}
.drop-icon{font-size:2rem;margin-bottom:.4rem}
.drop-text{font-size:.88rem;color:#666}.drop-text strong{color:var(--brand)}
.drop-meta{font-size:.72rem;color:#bbb;margin-top:.3rem}
.file-chosen{display:none;align-items:center;gap:.7rem;padding:.65rem .9rem;background:#edfaf3;border:1px solid #a3d9b8;border-radius:8px;margin-top:.6rem;font-size:.83rem;color:var(--success);font-weight:500}
.file-chosen.visible{display:flex}
.file-chosen .rm{margin-left:auto;cursor:pointer;font-size:.9rem;color:#999;background:none;border:none;padding:0 .2rem}
/* Report type selector */
.report-types{display:flex;flex-wrap:wrap;gap:.6rem;margin-top:.5rem}
.rtype-section-hd{font-size:.8rem;font-weight:700;color:var(--brand);text-transform:uppercase;letter-spacing:.04em;margin:.3rem 0 .1rem}
.rtype-section-hd.labels{margin-top:1rem;padding-top:.8rem;border-top:1px solid #eee}
.rtype-btn{padding:.55rem 1.1rem;border:1.5px solid var(--border);border-radius:8px;background:#fff;color:#888;font-family:'Roboto Slab',serif;font-size:.78rem;font-weight:600;letter-spacing:.04em;text-transform:uppercase;cursor:pointer;transition:all .15s;white-space:nowrap}
.rtype-btn.active{background:var(--brand);border-color:var(--brand);color:#fff}
.rtype-btn:hover:not(.active){border-color:var(--brand-mid);color:var(--brand-mid)}
/* Run button */
.run-btn{width:100%;padding:.95rem 2rem;background:var(--brand);color:#fff;border:none;border-radius:var(--r);font-family:'Roboto Slab',serif;font-size:1.05rem;font-weight:700;letter-spacing:.02em;text-transform:uppercase;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:.65rem;transition:background .18s,transform .1s,box-shadow .18s;box-shadow:0 4px 14px rgba(109,31,47,.3);margin-top:1.25rem}
.run-btn:hover:not(:disabled){background:var(--brand-dark);box-shadow:0 6px 20px rgba(109,31,47,.4);transform:translateY(-1px)}
.run-btn:disabled{opacity:.55;cursor:not-allowed;transform:none;box-shadow:none}
/* Progress panel */
#prog-panel{display:none;background:#1a1018;border-radius:var(--r);padding:1.1rem 1.4rem;margin-top:1.1rem;border:1px solid #2d1e24}
#prog-panel.visible{display:block}
.prog-hd{display:flex;align-items:center;gap:.65rem;margin-bottom:.75rem;padding-bottom:.65rem;border-bottom:1px solid #2d1e24}
.prog-title{font-size:.82rem;font-weight:600;color:#e0d4d8;letter-spacing:.06em;text-transform:uppercase}
.spinner{width:15px;height:15px;border:2px solid rgba(255,255,255,.15);border-top-color:var(--gold);border-radius:50%;animation:spin .7s linear infinite;flex-shrink:0}
@keyframes spin{to{transform:rotate(360deg)}}
.pbar-wrap{background:rgba(255,255,255,.08);border-radius:4px;height:3px;margin-bottom:.65rem;overflow:hidden}
.pbar{height:100%;background:linear-gradient(90deg,var(--brand-mid),var(--gold));width:0%;transition:width .4s ease}
#log{font-family:monospace;font-size:.76rem;line-height:1.65;color:#c4b5bb;max-height:220px;overflow-y:auto}
#log .ok{color:#6fcf97}#log .warn{color:#f2c94c}#log .err{color:#eb5757}
/* Action bar */
.action-bar{display:flex;gap:.75rem;flex-wrap:wrap;margin-top:1.1rem}
.dl-btn{display:inline-flex;align-items:center;gap:.55rem;padding:.75rem 1.5rem;background:var(--gold);color:#1a1018;border-radius:8px;text-decoration:none;font-weight:700;font-size:.9rem;transition:background .15s,transform .1s;box-shadow:0 3px 10px rgba(201,168,76,.35);border:none;cursor:pointer}
.dl-btn:hover{background:var(--gold-lt);transform:translateY(-1px)}
/* Calendar card */
.cal-list{display:flex;flex-direction:column;gap:0}
.cal-row{display:flex;align-items:center;gap:1rem;padding:.6rem 0;border-bottom:1px solid var(--border)}
.cal-row:last-child{border-bottom:none}
.cal-dot{width:10px;height:10px;border-radius:50%;background:var(--brand);flex-shrink:0}
.cal-week .cal-dot{background:var(--gold)}
.cal-info{display:flex;align-items:baseline;gap:.75rem;flex-wrap:wrap}
.cal-date{font-size:.82rem;font-weight:700;color:var(--brand-dark);min-width:110px}
.cal-event{font-size:.85rem;color:#555}
.cal-week .cal-event{font-style:italic;color:var(--brand)}
/* Error card */
#error-card{display:none;background:#2d0d13;border:1px solid #6d1f2f;border-radius:var(--r);padding:1.1rem 1.4rem;margin-top:1.1rem;color:#f5c2cb;font-size:.85rem}
#error-card.visible{display:block}
#error-card strong{display:block;margin-bottom:.35rem;font-size:.95rem}
#recent-card{margin-top:1.4rem}
#recent-card .recent-hd{font-family:'Roboto Slab',serif;font-size:.85rem;font-weight:700;color:var(--brand);text-transform:uppercase;letter-spacing:.06em;margin-bottom:.7rem}
#recent-list{display:flex;flex-direction:column;gap:.45rem}
.recent-row{display:flex;align-items:center;justify-content:space-between;background:#faf7f7;border:1px solid #ecdcdf;border-radius:8px;padding:.55rem .9rem;gap:.75rem}
.recent-row:hover{background:#f5eeef}
.recent-info{flex:1;min-width:0}
.recent-name{font-size:.85rem;font-weight:600;color:#2d1018;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.recent-time{font-size:.75rem;color:#888;margin-top:.1rem}
.recent-dl{flex-shrink:0;padding:.35rem .85rem;background:var(--brand);color:#fff;border:none;border-radius:6px;font-size:.78rem;font-weight:600;text-decoration:none;cursor:pointer;transition:background .15s}
.recent-dl:hover{background:var(--brand-dark)}
#recent-empty{font-size:.82rem;color:#aaa;text-align:center;padding:.5rem 0}
/* ---- Config tab ---- */
.camp-block{background:#fff;border:1px solid var(--border);border-radius:var(--r);margin-bottom:1rem;overflow:hidden}
.camp-header{display:flex;align-items:center;gap:.75rem;padding:.8rem 1.1rem;background:var(--brand-light);border-bottom:1px solid var(--border)}
.camp-name-input{font-family:'Roboto Slab',serif;font-size:.95rem;font-weight:700;color:var(--brand-dark);border:none;background:transparent;letter-spacing:.02em;text-transform:uppercase;flex:1;outline:none;min-width:0}
.camp-name-input:focus{background:#fff;border-radius:4px;padding:0 .4rem}
.camp-rm{background:none;border:none;cursor:pointer;color:#bbb;font-size:1rem;padding:.2rem;transition:color .15s;flex-shrink:0}
.camp-rm:hover{color:var(--brand)}
.bunk-table{width:100%;border-collapse:collapse}
.bunk-table th{font-size:.7rem;font-weight:600;color:#999;letter-spacing:.05em;text-transform:uppercase;padding:.5rem .9rem;border-bottom:1px solid var(--border);text-align:left}
.bunk-table td{padding:.45rem .9rem;border-bottom:1px solid #f5f0f1;vertical-align:middle}
.bunk-table tr:last-child td{border-bottom:none}
.bunk-table tr:hover td{background:var(--mist)}
.bunk-input{border:1.5px solid var(--border);border-radius:6px;padding:.38rem .6rem;font-size:.82rem;font-family:'DM Sans',sans-serif;color:var(--ink);background:#fff;transition:border-color .15s;width:100%}
.bunk-input:focus{outline:none;border-color:var(--brand-mid)}
.bunk-num-input{width:70px}
.bunk-rm{background:none;border:none;cursor:pointer;color:#ccc;font-size:.95rem;padding:.2rem;transition:color .15s}
.bunk-rm:hover{color:var(--brand)}
.add-bunk-btn{display:flex;align-items:center;gap:.45rem;padding:.5rem .9rem;background:none;border:1.5px dashed var(--border);border-radius:8px;color:var(--brand-mid);font-size:.8rem;font-weight:600;cursor:pointer;transition:all .15s;margin:.6rem .9rem}
.add-bunk-btn:hover{border-color:var(--brand-mid);background:var(--brand-light)}
.add-camp-btn{display:flex;align-items:center;gap:.5rem;padding:.6rem 1.1rem;background:none;border:1.5px dashed var(--border);border-radius:8px;color:var(--brand-mid);font-size:.83rem;font-weight:600;cursor:pointer;transition:all .15s;width:100%;justify-content:center;margin-bottom:1rem}
.add-camp-btn:hover{border-color:var(--brand-mid);background:var(--brand-light)}
.save-config-btn{width:100%;padding:.85rem 2rem;background:var(--brand);color:#fff;border:none;border-radius:var(--r);font-family:'Roboto Slab',serif;font-size:1rem;font-weight:700;letter-spacing:.02em;text-transform:uppercase;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:.65rem;transition:background .18s,transform .1s,box-shadow .18s;box-shadow:0 4px 14px rgba(109,31,47,.3)}
.save-config-btn:hover{background:var(--brand-dark);box-shadow:0 6px 20px rgba(109,31,47,.4);transform:translateY(-1px)}
#save-msg{display:none;margin-top:.75rem;padding:.75rem 1.25rem;border-radius:8px;font-size:.95rem;font-weight:600;text-align:center;transition:opacity .6s ease}
#save-msg.ok{display:flex;align-items:center;justify-content:center;gap:.5rem;background:#edfaf3;border:1.5px solid #4caf82;color:#1e7d4a}
#save-msg.ok.fade-out{opacity:0}
#save-msg.err{display:block;background:#2d0d13;border:1px solid #6d1f2f;color:#f5c2cb}
/* Misc */
.section-title{font-family:'Roboto Slab',serif;font-size:.85rem;font-weight:700;color:var(--brand-dark);text-transform:uppercase;letter-spacing:.05em;margin-bottom:.65rem}
.empty-state{text-align:center;padding:3rem 2rem;color:#bbb}
.empty-state .empty-icon{font-size:2.5rem;margin-bottom:.75rem}
.empty-state p{font-size:.9rem;line-height:1.6}
/* Weather tile */
.wx-day{flex:1;min-width:80px;background:var(--mist);border:1px solid var(--border);border-radius:10px;padding:.65rem .5rem;text-align:center;display:flex;flex-direction:column;gap:.25rem;align-items:center}
.wx-dow{font-size:.7rem;font-weight:700;color:var(--brand);letter-spacing:.06em;text-transform:uppercase}
.wx-icon{font-size:1.6rem;line-height:1}
.wx-hi{font-size:.95rem;font-weight:700;color:var(--ink)}
.wx-lo{font-size:.78rem;color:#999}
.wx-desc{font-size:.65rem;color:#aaa;margin-top:.1rem}
/* Week selector buttons */
.week-btn{padding:.55rem 1.1rem;border:1.5px solid var(--border);border-radius:8px;background:#fff;color:#888;font-family:'Roboto Slab',serif;font-size:.78rem;font-weight:600;letter-spacing:.04em;text-transform:uppercase;cursor:pointer;transition:all .15s;white-space:nowrap}
.week-btn.active{background:var(--gold);border-color:var(--gold);color:#1a1018}
.week-btn:hover:not(.active){border-color:var(--gold);color:var(--gold)}
/* Responsive */
@media(max-width:640px){
.tab span:not(.tab-badge){display:none}
header{padding:0 1rem;gap:.75rem;height:64px}
.h-logo{width:46px;height:46px}
.h-title{font-size:1rem}
.h-sub{display:none}
.h-badge{display:none}
.container{padding:1rem .75rem 3rem}
.card{padding:1.1rem 1rem}
.rtype-btn{padding:.5rem .8rem;font-size:.72rem}
.run-btn{font-size:.95rem}
.bunk-table{font-size:.78rem}
.bunk-table th,.bunk-table td{padding:.4rem .6rem}
}
/* ---- Password modal ---- */
#pw-overlay{position:fixed;inset:0;background:rgba(20,6,9,.72);backdrop-filter:blur(4px);z-index:9999;display:flex;align-items:center;justify-content:center}
#pw-overlay.hidden{display:none}
#pw-box{background:#fff;border-radius:14px;padding:2.4rem 2.2rem 2rem;max-width:420px;width:90%;box-shadow:0 20px 60px rgba(0,0,0,.35);text-align:center}
#pw-box .pw-logo{width:72px;height:72px;margin:0 auto .9rem}#pw-box .pw-logo img{width:72px;height:72px;object-fit:contain;mix-blend-mode:multiply;display:block}
#pw-box h2{font-family:'Roboto Slab',serif;font-size:1.25rem;color:var(--brand);margin:0 0 .4rem}
#pw-box .pw-sub{font-size:.85rem;color:#555;margin:0 0 1.4rem;line-height:1.55}
#pw-box .pw-sub strong{color:var(--brand-dark)}
#pw-input-wrap{display:flex;gap:.5rem;margin-bottom:.6rem}
#pw-input{flex:1;padding:.7rem 1rem;border:1.5px solid #ddd;border-radius:8px;font-size:.95rem;outline:none;transition:border .18s}
#pw-input:focus{border-color:var(--brand)}
#pw-submit{padding:.7rem 1.2rem;background:var(--brand);color:#fff;border:none;border-radius:8px;font-weight:700;font-size:.95rem;cursor:pointer;transition:background .18s}
#pw-submit:hover{background:var(--brand-dark)}
#pw-error{font-size:.82rem;color:#c0392b;min-height:1.1rem;margin-top:.15rem}
.pw-field{margin-bottom:.6rem}
.pw-field input{width:100%;padding:.7rem 1rem;border:1.5px solid #ddd;border-radius:8px;font-size:.95rem;outline:none;transition:border .18s}
.pw-field input:focus{border-color:var(--brand)}
.pw-go{width:100%;padding:.75rem 1rem;background:var(--brand);color:#fff;border:none;border-radius:8px;font-weight:700;font-size:.95rem;cursor:pointer;transition:background .18s;margin-top:.3rem}
.pw-go:hover{background:var(--brand-dark)}
.pw-toggle{font-size:.82rem;color:#777;margin-top:.9rem}
.pw-toggle a{color:var(--brand);font-weight:600;cursor:pointer;text-decoration:underline}
.h-user{position:relative;display:flex;align-items:center;color:#fff}
.h-user-trigger{background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.25);color:#fff;font-size:.9rem;font-weight:700;letter-spacing:.02em;cursor:pointer;display:flex;align-items:center;gap:.4rem;padding:.4rem .8rem;height:34px;box-sizing:border-box;border-radius:6px;transition:background .18s}
.h-user-trigger:hover{background:rgba(255,255,255,.25)}
.h-user-trigger .caret{font-size:.65rem;opacity:.85}
.h-user-menu{position:absolute;top:100%;right:0;margin-top:.35rem;background:#fff;border:1px solid #e2e2e2;border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.18);min-width:175px;overflow:hidden;z-index:9998}
.h-user-menu.hidden{display:none}
.h-user-menu button{display:flex;align-items:center;gap:.55rem;width:100%;text-align:left;background:none;border:none;padding:.65rem .9rem;font-size:.85rem;color:#333;cursor:pointer}
.h-user-menu button:hover{background:#f4eef0;color:var(--brand)}
.h-user-menu .menu-sep{border-top:1px solid #eee}
</style>
</head>
<body>

<!-- Login / register gate -->
<div id="pw-overlay">
  <div id="pw-box">
    <div class="pw-logo"><img src="/logo.png" alt="Elbow Lane Day Camp"></div>
    <h2>Elbow Lane Reporting Center</h2>

    <div id="login-view">
      <p class="pw-sub">Sign in to continue.</p>
      <div class="pw-field"><input id="login-username" placeholder="Username" autocomplete="username"></div>
      <div class="pw-field"><input id="login-password" type="password" placeholder="Password" autocomplete="current-password"></div>
      <button id="login-btn" class="pw-go">Sign In</button>
      <div class="pw-toggle">No account yet? <a id="show-register">Create one</a></div>
    </div>

    <div id="register-view" style="display:none">
      <p class="pw-sub">Create your account. Your username is shown on reports you upload.</p>
      <div class="pw-field"><input id="reg-username" placeholder="Choose a username (e.g. your name)" autocomplete="username"></div>
      <div class="pw-field"><input id="reg-password" type="password" placeholder="Choose a password" autocomplete="new-password"></div>
      <div class="pw-field"><input id="reg-code" placeholder="Access code" autocomplete="off"></div>
      <button id="reg-btn" class="pw-go">Create Account &amp; Sign In</button>
      <div class="pw-toggle">Already have an account? <a id="show-login">Sign in</a></div>
    </div>

    <div id="pw-error"></div>
  </div>
</div>

<!-- Pricing modal -->
<div id="pricing-overlay" class="hidden">
  <div id="pricing-box">
    <button class="px-close" id="pricing-close">&#x2715;</button>
    <h2>Simple, Transparent Pricing</h2>
    <p class="px-sub">Choose the plan that fits your camp&rsquo;s needs. No contracts, cancel anytime.</p>
    <div class="px-grid">
      <div class="px-card">
        <div class="px-tier">Starter</div>
        <div class="px-price">$34.99<span>/mo</span></div>
        <p class="px-desc">Perfect for smaller camps or those just getting started with digital reporting.</p>
        <ul class="px-features">
          <li>20 reports per month</li>
          <li>All available report types</li>
          <li>Configurable bunks &amp; camps</li>
          <li>Print-ready Excel/Word output</li>
          <li>Email support</li>
        </ul>
        <button class="px-cta" onclick="document.getElementById('pricing-overlay').classList.add('hidden')">Get Started</button>
      </div>
      <div class="px-card featured">
        <div class="px-badge">Most Popular</div>
        <div class="px-tier">Pro</div>
        <div class="px-price">$49.99<span>/mo</span></div>
        <p class="px-desc">For active camps that run reports throughout the season on a regular basis.</p>
        <ul class="px-features">
          <li>50 reports per month</li>
          <li>All available report types</li>
          <li>Configurable bunks &amp; camps</li>
          <li>Print-ready Excel/Word output</li>
          <li>Recent reports history</li>
          <li>Priority email support</li>
        </ul>
        <button class="px-cta" onclick="document.getElementById('pricing-overlay').classList.add('hidden')">Get Started</button>
      </div>
      <div class="px-card">
        <div class="px-tier">Unlimited</div>
        <div class="px-price">$99.99<span>/mo</span></div>
        <p class="px-desc">Full access for camps that need unrestricted reporting all season long.</p>
        <ul class="px-features">
          <li>Unlimited reports</li>
          <li>All available report types</li>
          <li>Configurable bunks &amp; camps</li>
          <li>Print-ready Excel/Word output</li>
          <li>Recent reports history</li>
          <li>Priority support &amp; onboarding</li>
        </ul>
        <button class="px-cta" onclick="document.getElementById('pricing-overlay').classList.add('hidden')">Get Started</button>
      </div>
    </div>
    <p class="px-note">All plans include a 14-day free trial &mdash; no credit card required.</p>
  </div>
</div>

<!-- First-time "what's new" notice -->
<div id="notice-overlay" class="hidden">
  <div id="notice-box">
    <div class="notice-icon">📋</div>
    <h2>Heads up — new Utilities tab</h2>
    <p>The <strong>Bunks &amp; Camps</strong> tab is now <strong>Utilities</strong>. From now on, upload your <strong>Master Sheet</strong> from the <strong>Utilities</strong> tab (not the Run Report tab).</p>
    <p style="color:#777;font-size:.82rem">You can also import &amp; manage <strong>Family Contacts</strong> there. Bunks &amp; Camps settings moved to the bottom of that tab.</p>
    <button id="notice-ok" class="notice-btn">Got it</button>
  </div>
</div>

<!-- Change-my-password modal -->
<div id="cpw-overlay" class="hidden">
  <div id="cpw-box">
    <h2>Change Password</h2>
    <p style="font-size:.85rem;color:#666;margin:0 0 1rem">Update the password for <strong id="cpw-who"></strong>.</p>
    <div class="pw-field"><input id="cpw-current" type="password" placeholder="Current password" autocomplete="current-password"></div>
    <div class="pw-field"><input id="cpw-new" type="password" placeholder="New password" autocomplete="new-password"></div>
    <div class="pw-field"><input id="cpw-confirm" type="password" placeholder="Confirm new password" autocomplete="new-password"></div>
    <div id="cpw-error" style="font-size:.82rem;color:#c0392b;min-height:1.1rem;margin-bottom:.4rem"></div>
    <div style="display:flex;gap:.5rem;justify-content:flex-end">
      <button id="cpw-cancel" class="pr-period-btn pr-sm">Cancel</button>
      <button id="cpw-save" class="pw-go" style="width:auto;margin:0;padding:.55rem 1.2rem">Save</button>
    </div>
  </div>
</div>

<header>
  <div class="h-logo" role="img" aria-label="Elbow Lane Day Camp"></div>
  <div>
    <div class="h-title">Elbow Lane Day Camp</div>
    <div class="h-sub">Reporting Center</div>
  </div>
  <div class="h-nav">
    <span class="h-user" id="h-user" style="display:none">
      <button class="h-user-trigger" id="h-user-btn" title="Account"><span id="h-user-name"></span><span class="caret">▾</span></button>
      <div class="h-user-menu hidden" id="h-user-menu">
        <button id="menu-reset">🔑 Reset Password</button>
        <button id="menu-logout" class="menu-sep">↩ Sign Out</button>
      </div>
    </span>
    <button class="h-pricing" id="pricing-btn">$ Pricing</button>
    <a class="h-support" href="mailto:bhimpele@gmail.com?subject=EL%20Reporting%20Center%20Support">✉ Support</a>
  </div>
</header>

<div class="tab-bar">
  <div class="tab active" data-tab="upload">📂 <span>Run Report</span></div>
  <div class="tab" data-tab="payroll">🗓️ <span>Payroll</span></div>
  <div class="tab" data-tab="config">⚙️ <span>Utilities</span></div>
</div>

<div class="container">

<!-- ===== UPLOAD TAB ===== -->
<div class="tab-panel active" id="tab-upload">

  <div class="card">
    <div class="card-hd">
      <div>
        <div class="card-title">Select Report Type</div>
        <div class="card-hint">Choose the report you want to run</div>
      </div>
    </div>
    <div class="rtype-section-hd">📊 Reports</div>
    <div class="report-types" id="report-types">
        <button class="rtype-btn active" data-rtype="bunk_snapshot">Bunk Snapshot</button>
        <button class="rtype-btn" data-rtype="group_attendance">Group Attendance</button>
        <button class="rtype-btn" data-rtype="am_extend">AM Extend</button>
        <button class="rtype-btn" data-rtype="pm_extend">PM Extend</button>
        <button class="rtype-btn" data-rtype="pm_grp_extend">PM GRP Extend</button>
        <button class="rtype-btn" data-rtype="driver_totals">Driver Totals</button>
    </div>
    <div class="rtype-section-hd labels">🏷️ Labels</div>
    <div class="report-types">
        <button class="rtype-btn" data-rtype="inter_labels">Inter</button>
        <button class="rtype-btn" data-rtype="jr_transport_labels">Junior</button>
    </div>
  </div>

  <!-- Week selector — only visible when Driver Totals is selected -->
  <div class="card" id="week-card" style="display:none">
    <div class="card-hd">
      <div>
        <div class="card-title">Select Camp Week</div>
        <div class="card-hint">Only campers enrolled in the selected week are included. (The Driver Totals Report instead highlights that week's campers in yellow.)</div>
      </div>
    </div>
    <div style="display:flex;gap:.6rem;flex-wrap:wrap;margin-top:.25rem">
      <button class="week-btn active" data-week="1">Week 1</button>
      <button class="week-btn" data-week="2">Week 2</button>
      <button class="week-btn" data-week="3">Week 3</button>
      <button class="week-btn" data-week="4">Week 4</button>
      <button class="week-btn" data-week="5">Week 5</button>
      <button class="week-btn" data-week="6">Week 6</button>
      <button class="week-btn" data-week="7">Week 7</button>
      <button class="week-btn" data-week="8">Week 8</button>
    </div>
  </div>

  <!-- Saved-master banner: shows which data the report will use -->
  <div id="master-banner" style="display:none;align-items:center;gap:.6rem;padding:.6rem .85rem;background:#eef4fb;border:1px solid #b9d2ec;border-radius:8px;margin:0 0 .8rem;font-size:.83rem;color:#1A79BF;font-weight:500">
    <span>📋</span>
    <span id="master-banner-text" style="flex:1">—</span>
    <a id="master-download" href="/api/master/download" style="cursor:pointer;font-size:.75rem;color:#1A79BF;background:#fff;border:1px solid #b9d2ec;border-radius:6px;padding:.2rem .55rem;text-decoration:none">⬇ Download</a>
    <button id="master-clear" style="cursor:pointer;font-size:.75rem;color:#777;background:#fff;border:1px solid #ccd;border-radius:6px;padding:.2rem .55rem">Clear</button>
  </div>

  <button class="run-btn" id="run-btn" disabled>
    <span id="run-icon">⚙️</span>
    <span id="run-label">Run Report</span>
  </button>

  <div id="prog-panel">
    <div class="prog-hd">
      <div class="spinner" id="spinner"></div>
      <span class="prog-title" id="prog-title">Processing report…</span>
    </div>
    <div class="pbar-wrap"><div class="pbar" id="pbar"></div></div>
    <div id="log"></div>
  </div>

  <div class="action-bar" id="action-bar" style="display:none">
    <a class="dl-btn" id="dl-link" href="#" download>⬇ Download Report</a>
  </div>

  <!-- (Master sheet is now uploaded/managed from the Utilities tab.) -->

  <div id="error-card">
    <strong>⚠ Processing Error</strong>
    <span id="error-msg"></span>
  </div>

  <!-- ===== WEATHER TILE ===== -->
  <div class="card" id="weather-card" style="margin-top:2.5rem">
    <div class="card-hd" style="margin-bottom:.75rem">
      <span class="card-num">🌤</span>
      <div>
        <div class="card-title">5-Day Forecast — Warrington, PA</div>
      </div>
    </div>
    <div id="weather-body" style="display:flex;gap:.5rem;flex-wrap:wrap">
      <div style="color:#bbb;font-size:.82rem">Loading forecast…</div>
    </div>
  </div>

  <div class="card" id="calendar-card" style="margin-top:1.1rem">
    <div class="card-hd">
      <span class="card-num">📅</span>
      <div>
        <div class="card-title">Important Dates — Summer 2026</div>
      </div>
    </div>
    <div class="cal-list">
      <div class="cal-row cal-week">
        <div class="cal-dot"></div>
        <div class="cal-info">
          <div class="cal-date">Week of June 15</div>
          <div class="cal-event">Minicamp #1</div>
        </div>
      </div>
      <div class="cal-row">
        <div class="cal-dot"></div>
        <div class="cal-info">
          <div class="cal-date">June 22</div>
          <div class="cal-event">First Day of Camp</div>
        </div>
      </div>
      <div class="cal-row">
        <div class="cal-dot"></div>
        <div class="cal-info">
          <div class="cal-date">July 7</div>
          <div class="cal-event">Camp Pictures</div>
        </div>
      </div>
      <div class="cal-row">
        <div class="cal-dot"></div>
        <div class="cal-info">
          <div class="cal-date">July 20</div>
          <div class="cal-event">Olde Tyme Country Fair</div>
        </div>
      </div>
      <div class="cal-row">
        <div class="cal-dot"></div>
        <div class="cal-info">
          <div class="cal-date">August 4</div>
          <div class="cal-event">Family Fun Night</div>
        </div>
      </div>
      <div class="cal-row">
        <div class="cal-dot"></div>
        <div class="cal-info">
          <div class="cal-date">August 14</div>
          <div class="cal-event">Last Day of Camp</div>
        </div>
      </div>
      <div class="cal-row cal-week">
        <div class="cal-dot"></div>
        <div class="cal-info">
          <div class="cal-date">Week of August 17</div>
          <div class="cal-event">Minicamp #2</div>
        </div>
      </div>
    </div>
  </div>

  <div id="recent-card" class="card">
    <div class="recent-hd">Recent Reports</div>
    <div id="recent-list"><div id="recent-empty">No reports yet.</div></div>
  </div>

</div><!-- /tab-upload -->

<!-- ===== UTILITIES TAB ===== -->
<div class="tab-panel" id="tab-config">

  <!-- Master sheet upload -->
  <div class="card">
    <div class="card-hd">
      <div>
        <div class="card-title">Master Sheet</div>
        <div class="card-hint">Upload the camper master sheet. It's saved on the server and used for every report — re-upload here whenever the camper data changes.</div>
      </div>
    </div>
    <div id="master-status" style="display:none;align-items:center;gap:.6rem;padding:.6rem .85rem;background:#eef4fb;border:1px solid #b9d2ec;border-radius:8px;margin:0 0 .8rem;font-size:.83rem;color:#1A79BF;font-weight:500">
      <span>📋</span>
      <span id="master-status-text" style="flex:1">—</span>
      <a id="master-status-dl" href="/api/master/download" style="cursor:pointer;font-size:.75rem;color:#1A79BF;background:#fff;border:1px solid #b9d2ec;border-radius:6px;padding:.2rem .55rem;text-decoration:none">⬇ Download</a>
      <button id="master-status-clear" style="cursor:pointer;font-size:.75rem;color:#777;background:#fff;border:1px solid #ccd;border-radius:6px;padding:.2rem .55rem">Clear</button>
    </div>
    <div class="drop-zone" id="master-drop">
      <input type="file" id="master-file" accept=".csv,.xlsx,.xls">
      <div class="drop-icon">📊</div>
      <div class="drop-text"><strong>Click to choose</strong> or drag &amp; drop the master sheet</div>
      <div class="drop-meta">Accepted formats: .csv, .xlsx, .xls</div>
    </div>
    <div id="master-msg" style="font-size:.82rem;margin-top:.5rem"></div>
  </div>

  <!-- Family contacts -->
  <div class="card">
    <div class="card-hd">
      <div>
        <div class="card-title">Family Contacts</div>
        <div class="card-hint">Import the camper family-contact spreadsheet. It's stored on the server and used to source family-contact reports &amp; labels (not displayed here). Re-import to refresh.</div>
      </div>
    </div>
    <div class="drop-zone" id="fam-drop" style="padding:.75rem">
      <input type="file" id="fam-file" accept=".csv,.xlsx,.xls,.tsv">
      <div class="drop-icon" style="font-size:1.4rem">📇</div>
      <div class="drop-text"><strong>Click to choose</strong> or drag &amp; drop a family contact spreadsheet</div>
      <div class="drop-meta">Columns are auto-detected (last/first, bunk, parents, address, pickups)</div>
    </div>
    <div style="display:flex;align-items:center;gap:1rem;margin:.5rem 0 .2rem;font-size:.8rem;color:#666">
      <label><input type="radio" name="fam-import-mode" value="replace" checked> Replace all</label>
      <label><input type="radio" name="fam-import-mode" value="append"> Add to existing</label>
      <span id="fam-msg" style="margin-left:auto"></span>
    </div>
    <div id="fam-status" style="display:none;align-items:center;gap:.6rem;padding:.6rem .85rem;background:#eef4fb;border:1px solid #b9d2ec;border-radius:8px;margin-top:.6rem;font-size:.83rem;color:#1A79BF;font-weight:500">
      <span>📇</span>
      <span id="fam-status-text" style="flex:1">—</span>
    </div>
  </div>

  <!-- Season calendar: just the first day of camp -->
  <div class="card">
    <div class="card-hd">
      <div>
        <div class="card-title">Season Calendar</div>
        <div class="card-hint">Set the first day of camp (Week 1 Monday). The 8 camp weeks are calculated from it and used for report week #/date ranges and the Payroll day columns.</div>
      </div>
    </div>
    <div style="display:flex;align-items:center;gap:.8rem;flex-wrap:wrap">
      <label style="font-size:.85rem;color:#555">Camp starts:
        <input type="date" id="season-start" style="padding:.4rem .5rem;border:1px solid var(--border);border-radius:6px;font-size:.85rem;margin-left:.4rem">
      </label>
      <button class="pr-period-btn" id="season-save">💾 Save</button>
      <span id="season-msg" style="font-size:.82rem;color:#777"></span>
    </div>
    <div id="season-summary" style="font-size:.82rem;color:#666;margin-top:.6rem"></div>
  </div>

  <!-- Bunks & Camps (rarely change once the season starts) -->
  <div class="card">
    <div class="card-hd">
      <span class="card-num" style="background:var(--gold);color:#1a1018">★</span>
      <div>
        <div class="card-title">Bunks &amp; Camps</div>
        <div class="card-hint">Manage bunk names, their numbers, and the camp group they belong to. Changes are saved to the server and used when processing all future reports.</div>
      </div>
    </div>

    <div id="camp-list"><!-- rendered by JS --></div>

    <button class="add-camp-btn" id="add-camp-btn">＋ Add Camp Group</button>

    <button class="save-config-btn" id="save-config-btn">💾 Save Configuration</button>
    <div id="save-msg"></div>
  </div>

  <!-- User accounts (admins only) -->
  <div class="card" id="users-card" style="display:none">
    <div class="card-hd">
      <div>
        <div class="card-title">User Accounts</div>
        <div class="card-hint">People sign in with their own username &amp; password. New users create an account with the shared access code on the sign-in screen. As an admin you can remove accounts here.</div>
      </div>
    </div>
    <div style="overflow-x:auto">
      <table class="fam-table" id="users-table"></table>
    </div>
    <div style="display:flex;gap:.5rem;flex-wrap:wrap;align-items:center;margin-top:.8rem;padding-top:.8rem;border-top:1px solid #eee">
      <strong style="font-size:.85rem;color:#555">Add user:</strong>
      <input class="pr-input" id="usr-username" placeholder="Username" style="width:140px">
      <input class="pr-input" id="usr-password" placeholder="Password" style="width:120px">
      <input class="pr-input" id="usr-email" placeholder="Their email (optional)" style="width:170px">
      <label style="font-size:.8rem;color:#666"><input type="checkbox" id="usr-admin"> Admin</label>
      <button class="pr-period-btn" id="usr-add">＋ Add User</button>
      <span id="usr-msg" style="font-size:.82rem;color:#777"></span>
    </div>
    <div id="usr-result" style="display:none;margin-top:.7rem;padding:.8rem .9rem;background:#edfaf3;border:1px solid #a3d9b8;border-radius:8px">
      <div style="font-size:.82rem;color:#2e7d32;font-weight:600;margin-bottom:.4rem">✓ Account created — share these credentials:</div>
      <pre id="usr-creds" style="font-size:.82rem;background:#fff;border:1px solid #d7ecdf;border-radius:6px;padding:.6rem .7rem;margin:0 0 .5rem;white-space:pre-wrap;word-break:break-word"></pre>
      <div style="display:flex;gap:.5rem;flex-wrap:wrap">
        <button class="pr-period-btn pr-sm" id="usr-copy">📋 Copy</button>
        <a class="pr-period-btn pr-sm" id="usr-email-link" href="#" style="text-decoration:none">✉ Email it</a>
        <span id="usr-copy-msg" style="font-size:.8rem;color:#777;align-self:center"></span>
      </div>
    </div>
  </div>

</div><!-- /tab-config -->

<!-- ===== PAYROLL TAB ===== -->
<div class="tab-panel" id="tab-payroll">
  <div class="card">
    <div class="card-hd">
      <div>
        <div class="card-title">Payroll — Staff Attendance</div>
        <div class="card-hint">Check each day a staff member is present. The count on the left totals the checks for the two-week period. Changes save automatically.</div>
      </div>
    </div>

    <div id="payroll-periods" style="display:flex;gap:.5rem;flex-wrap:wrap;margin:.3rem 0 .9rem"></div>

    <div style="display:flex;gap:1rem;flex-wrap:wrap;align-items:center;margin:0 0 .8rem;font-size:.82rem;color:#555">
      <span id="pr-area-filter" style="display:flex;align-items:center;gap:.45rem">Filter area:
        <span class="pr-multi" id="pr-area-wrap">
          <button type="button" class="pr-input pr-multi-btn" id="pr-area-btn">All areas <span class="caret">▾</span></button>
          <div class="pr-multi-menu hidden" id="pr-area-menu"></div>
        </span></span>
      <label>Sort by:
        <select id="pr-sort" class="pr-input" onchange="renderPayroll()">
          <option value="last">Last name</option>
          <option value="area">Area</option>
          <option value="total">Total (high to low)</option>
        </select></label>
      <label id="pr-ext-period-wrap" style="display:none">Shift:
        <select id="pr-ext-period" class="pr-input" onchange="prExtPeriod=this.value;renderPayroll()">
          <option value="ALL">AM &amp; PM</option>
          <option value="AM">AM only</option>
          <option value="PM">PM only</option>
        </select></label>
    </div>

    <div style="display:flex;gap:.5rem;justify-content:flex-end;margin:0">
      <button id="pr-export" class="pr-period-btn pr-sm">⬇ Excel</button>
      <button id="pr-print" class="pr-period-btn pr-sm">🖨 Print / PDF</button>
      <button id="pr-lock" class="pr-period-btn pr-sm">🔓 Unlocked</button>
    </div>

    <div style="overflow-x:auto;margin-top:-1.7rem">
      <table class="payroll-table" id="payroll-table"></table>
    </div>

    <div style="display:flex;gap:.5rem;flex-wrap:wrap;align-items:center;margin-top:1.1rem;padding-top:.9rem;border-top:1px solid #eee">
      <strong style="font-size:.85rem;color:#555">Add staff:</strong>
      <input class="pr-input" id="pr-last"  placeholder="Last name"  style="width:140px">
      <input class="pr-input" id="pr-first" placeholder="First name" style="width:140px">
      <input class="pr-input" id="pr-area"  placeholder="Area"       style="width:140px">
      <button class="pr-period-btn" id="pr-add">＋ Add Staff</button>
      <span id="pr-msg" style="font-size:.82rem;color:#777"></span>
    </div>
  </div>
</div><!-- /tab-payroll -->

</div><!-- /container -->

<script>
// ─────────────────────────────────────────────
// Tab switching
// ─────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    if (tab.dataset.tab === 'payroll') renderPayroll();
  });
});

// ─────────────────────────────────────────────
// Upload tab state
// ─────────────────────────────────────────────
let selectedReportType = 'bunk_snapshot';
let selectedWeek = 1;
let currentJobId = null;
let pollTimer = null;
let lastLineCount = 0;
let masterLoaded = false;

// Saved master sheet status — reflected on the Run Report banner AND the
// Utilities "Master Sheet" card.
async function loadMaster() {
  try {
    const res = await fetch('/api/master');
    const d = await res.json();
    masterLoaded = !!d.loaded;
    const banner = document.getElementById('master-banner');
    const status = document.getElementById('master-status');
    if (masterLoaded) {
      // If the username is an email, show only the part before the @
      const uploader = (d.uploaded_by || '').replace(/@.*$/, '');
      const by = uploader ? ` by <strong>${uploader}</strong>` : '';
      // Reformat "6/19/2026 4:00 PM EDT" -> "6/19/2026 @ 4:00 PM"
      let when = (d.uploaded_at || '').replace(/\s*[A-Z]{2,4}\s*$/, '')
                                      .replace(/\s+(\d{1,2}:\d{2}\s*[AP]M)/i, ' @ $1');
      const stamp = when ? ` &middot; uploaded on ${when}${by}` : '';
      document.getElementById('master-banner-text').innerHTML =
        `Using saved master: <strong>${d.filename || 'master sheet'}</strong>${stamp}` +
        `. Reports and Labels will use this data until an updated file is uploaded.`;
      banner.style.display = 'flex';
      if (status) {
        document.getElementById('master-status-text').innerHTML =
          `Current master: <strong>${d.filename || 'master sheet'}</strong>${stamp}`;
        status.style.display = 'flex';
      }
    } else {
      // No master saved — prompt the user to upload one in the Utilities tab
      document.getElementById('master-banner-text').innerHTML =
        `No master sheet saved yet. Upload one in the <strong>Utilities</strong> tab to run reports.`;
      document.getElementById('master-download').style.display = 'none';
      document.getElementById('master-clear').style.display = 'none';
      banner.style.display = 'flex';
      if (status) status.style.display = 'none';
    }
    // Restore the banner buttons when a master is present
    if (masterLoaded) {
      document.getElementById('master-download').style.display = '';
      document.getElementById('master-clear').style.display = '';
    }
  } catch(e) { masterLoaded = false; }
  updateRunBtn();
}

async function clearMaster() {
  try { await fetch('/api/master', {method: 'DELETE'}); } catch(e) {}
  loadMaster();
}
document.getElementById('master-clear').addEventListener('click', clearMaster);
document.getElementById('master-status-clear').addEventListener('click', clearMaster);

// Upload a master sheet from the Utilities tab
const masterDrop = document.getElementById('master-drop');
const masterFile = document.getElementById('master-file');
async function uploadMaster(f) {
  const msg = document.getElementById('master-msg');
  if (!f) return;
  msg.style.color = '#666'; msg.textContent = 'Uploading…';
  const fd = new FormData(); fd.append('file', f);
  try {
    const res = await fetch('/api/master', {method: 'POST', body: fd});
    const d = await res.json();
    if (!res.ok || d.error) { msg.style.color = '#c0392b'; msg.textContent = d.error || 'Upload failed.'; return; }
    msg.style.color = '#2e7d32'; msg.textContent = '✓ Master sheet saved.';
    loadMaster();
  } catch(e) { msg.style.color = '#c0392b'; msg.textContent = 'Network error: ' + e.message; }
}
masterDrop.addEventListener('dragover', e => { e.preventDefault(); masterDrop.classList.add('drag-over'); });
masterDrop.addEventListener('dragleave', () => masterDrop.classList.remove('drag-over'));
masterDrop.addEventListener('drop', e => {
  e.preventDefault(); masterDrop.classList.remove('drag-over');
  if (e.dataTransfer.files[0]) uploadMaster(e.dataTransfer.files[0]);
});
masterFile.addEventListener('change', e => { if (e.target.files[0]) uploadMaster(e.target.files[0]); });

function updateRunBtn() {
  document.getElementById('run-btn').disabled = !(masterLoaded && selectedReportType);
}

// Reports that use a camp-week selection
const WEEK_AWARE = ['driver_totals','group_attendance','am_extend','pm_extend','pm_grp_extend','inter_labels','jr_transport_labels'];

// Report type buttons
document.querySelectorAll('.rtype-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.rtype-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    selectedReportType = btn.dataset.rtype;
    document.getElementById('week-card').style.display =
      WEEK_AWARE.includes(selectedReportType) ? '' : 'none';
    updateRunBtn();
  });
});

// Week selector buttons
document.querySelectorAll('.week-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.week-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    selectedWeek = parseInt(btn.dataset.week, 10);
  });
});


// Run button
document.getElementById('run-btn').addEventListener('click', async () => {
  if (!masterLoaded || !selectedReportType) return;
  startProcessing();

  const fd = new FormData();   // always runs from the saved master
  fd.append('report_type', selectedReportType);
  if (WEEK_AWARE.includes(selectedReportType)) fd.append('week_num', selectedWeek);

  try {
    const res  = await fetch('/api/process', {method: 'POST', body: fd});
    const data = await res.json();
    if (!res.ok || data.error) { showError(data.error || 'Server error'); return; }
    currentJobId  = data.job_id;
    lastLineCount = 0;
    pollTimer     = setInterval(pollStatus, 1200);
  } catch(err) {
    showError('Network error: ' + err.message);
  }
});

function startProcessing() {
  document.getElementById('run-btn').disabled = true;
  document.getElementById('run-label').textContent = 'Processing…';
  document.getElementById('run-icon').textContent = '⏳';
  document.getElementById('prog-panel').classList.add('visible');
  document.getElementById('action-bar').style.display = 'none';
  document.getElementById('error-card').classList.remove('visible');
  document.getElementById('log').innerHTML = '';
  document.getElementById('pbar').style.width = '10%';
  document.getElementById('prog-title').textContent = 'Processing report…';
  document.getElementById('spinner').style.display = '';
}

async function pollStatus() {
  try {
    const res  = await fetch(`/api/status/${currentJobId}`);
    const data = await res.json();

    // Append new log lines
    const lines = data.progress || [];
    for (let i = lastLineCount; i < lines.length; i++) {
      const entry = lines[i];
      const div   = document.createElement('div');
      div.className = entry.level === 'ok' ? 'ok' : entry.level === 'err' ? 'err' : entry.level === 'warn' ? 'warn' : '';
      div.textContent = entry.msg;
      document.getElementById('log').appendChild(div);
    }
    lastLineCount = lines.length;
    document.getElementById('log').scrollTop = 999999;

    // Progress bar heuristic
    const pct = Math.min(10 + lastLineCount * 25, 90);
    document.getElementById('pbar').style.width = pct + '%';

    if (data.status === 'done') {
      clearInterval(pollTimer);
      document.getElementById('pbar').style.width = '100%';
      document.getElementById('spinner').style.animation = 'none';
      document.getElementById('spinner').style.borderTopColor = '#6fcf97';
      document.getElementById('prog-title').textContent = 'Complete! Downloading…';
      document.getElementById('run-btn').disabled = false;
      document.getElementById('run-label').textContent = 'Run Report';
      document.getElementById('run-icon').textContent = '⚙️';

      const dlLink = document.getElementById('dl-link');
      dlLink.href  = `/api/download/${currentJobId}`;
      document.getElementById('action-bar').style.display = 'flex';
      // Auto-download (button stays as a fallback if the browser blocks it)
      try { dlLink.click(); } catch(e) {}
      loadRecent();
      loadMaster();   // a freshly uploaded master is now saved for reuse
    }

    if (data.status === 'error') {
      clearInterval(pollTimer);
      showError(data.error || 'Unknown error');
    }
  } catch(err) {
    clearInterval(pollTimer);
    showError('Network error while polling: ' + err.message);
  }
}

function showError(msg) {
  document.getElementById('error-msg').textContent = msg;
  document.getElementById('error-card').classList.add('visible');
  document.getElementById('prog-panel').classList.remove('visible');
  document.getElementById('run-btn').disabled = false;
  document.getElementById('run-label').textContent = 'Run Report';
  document.getElementById('run-icon').textContent = '⚙️';
}

// ─────────────────────────────────────────────
// Config tab
// ─────────────────────────────────────────────
let campConfig = {camps: []};

async function loadConfig() {
  try {
    const res  = await fetch('/api/config');
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    campConfig = data;
    renderCamps();
  } catch(e) {
    document.getElementById('camp-list').innerHTML =
      `<div style="padding:1rem;color:#c0392b;font-size:.85rem">⚠ Could not load configuration: ${e.message}</div>`;
  }
}

function renderCamps() {
  const list = document.getElementById('camp-list');
  list.innerHTML = '';
  // Sort camps by their lowest bunk number; camps with no bunks go to the end
  const sorted = [...campConfig.camps].map((camp, ci) => ({ camp, ci }))
    .sort((a, b) => {
      const minA = a.camp.bunks.length ? Math.min(...a.camp.bunks.map(b => b.number)) : Infinity;
      const minB = b.camp.bunks.length ? Math.min(...b.camp.bunks.map(b => b.number)) : Infinity;
      return minA - minB;
    });
  sorted.forEach(({ camp, ci }) => {
    const block = document.createElement('div');
    block.className = 'camp-block';
    block.innerHTML = `
      <div class="camp-header">
        <input class="camp-name-input" value="${escHtml(camp.name)}" placeholder="Camp Name"
          oninput="campConfig.camps[${ci}].name = this.value">
        <button class="camp-rm" title="Remove camp" onclick="removeCamp(${ci})">✕</button>
      </div>
      <table class="bunk-table">
        <thead>
          <tr>
            <th>Bunk Name</th>
            <th style="width:70px">Number</th>
            <th style="width:100px">Grp</th>
            <th style="width:36px"></th>
          </tr>
        </thead>
        <tbody id="bunk-body-${ci}">
          ${[...camp.bunks].sort((a,b) => a.number - b.number).map((b, bi) => bunkRow(ci, camp.bunks.indexOf(b), b)).join('')}
        </tbody>
      </table>
      <button class="add-bunk-btn" onclick="addBunk(${ci})">＋ Add Bunk</button>
    `;
    list.appendChild(block);
  });
}

function bunkRow(ci, bi, b) {
  return `<tr id="bunk-${ci}-${bi}">
    <td><input class="bunk-input" value="${escHtml(b.name)}" placeholder="Bunk name"
      oninput="campConfig.camps[${ci}].bunks[${bi}].name = this.value"></td>
    <td><input class="bunk-input bunk-num-input" type="number" min="0" value="${b.number}"
      oninput="campConfig.camps[${ci}].bunks[${bi}].number = parseInt(this.value)||0"></td>
    <td><input class="bunk-input" value="${escHtml(b.grp||'')}" placeholder="Grp"
      oninput="campConfig.camps[${ci}].bunks[${bi}].grp = this.value"></td>
    <td><button class="bunk-rm" title="Remove bunk" onclick="removeBunk(${ci},${bi})">✕</button></td>
  </tr>`;
}

function addCamp() {
  campConfig.camps.push({name: 'New Camp', bunks: []});
  renderCamps();
}

function removeCamp(ci) {
  campConfig.camps.splice(ci, 1);
  renderCamps();
}

function addBunk(ci) {
  campConfig.camps[ci].bunks.push({name: '', number: 0, grp: ''});
  renderCamps();
  // Focus the new bunk name input
  const rows = document.querySelectorAll(`#bunk-body-${ci} tr`);
  if (rows.length) rows[rows.length-1].querySelector('input')?.focus();
}

function removeBunk(ci, bi) {
  campConfig.camps[ci].bunks.splice(bi, 1);
  renderCamps();
}

document.getElementById('add-camp-btn').addEventListener('click', addCamp);

document.getElementById('save-config-btn').addEventListener('click', async () => {
  const msg = document.getElementById('save-msg');
  msg.className = '';
  msg.style.display = 'none';
  try {
    const res  = await fetch('/api/config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(campConfig),
    });
    const data = await res.json();
    if (data.ok) {
      msg.innerHTML   = '<span style="font-size:1.2rem">✔</span> Configuration saved successfully.';
      msg.className   = 'ok';
      msg.style.display = '';
      msg.style.opacity = '1';
      clearTimeout(msg._fadeTimer);
      msg._fadeTimer = setTimeout(() => {
        msg.classList.add('fade-out');
        setTimeout(() => { msg.style.display = 'none'; msg.className = ''; }, 650);
      }, 3000);
    } else {
      msg.textContent = '⚠ ' + (data.error || 'Save failed.');
      msg.className   = 'err';
    }
  } catch(e) {
    msg.textContent = '⚠ Network error: ' + e.message;
    msg.className   = 'err';
  }
});

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ---- Recent Reports ----
async function loadRecent() {
  const list = document.getElementById('recent-list');
  try {
    const res   = await fetch('/api/recent');
    const files = await res.json();
    if (!files.length) {
      list.innerHTML = '<div id="recent-empty">No reports yet.</div>';
      return;
    }
    list.innerHTML = files.map(f => {
      const d   = new Date(f.mtime * 1000);
      const fmt = d.toLocaleDateString('en-US', {month:'short',day:'numeric',year:'numeric'})
                + ' ' + d.toLocaleTimeString('en-US', {hour:'numeric',minute:'2-digit'});
      const name = f.name.replace(/\.xlsx$/i, '');
      return `<div class="recent-row">
        <div class="recent-info">
          <div class="recent-name" title="${escHtml(f.name)}">${escHtml(name)}</div>
          <div class="recent-time">${fmt}</div>
        </div>
        <a class="recent-dl" href="${escHtml(f.url)}" download="${escHtml(f.name)}">⬇ Download</a>
      </div>`;
    }).join('');
  } catch(e) {
    list.innerHTML = '<div id="recent-empty">Could not load recent reports.</div>';
  }
}

// ─────────────────────────────────────────────
// Weather tile
// ─────────────────────────────────────────────
const WX_ICONS = {
  0:'☀️', 1:'🌤️', 2:'⛅', 3:'☁️',
  45:'🌫️', 48:'🌫️',
  51:'🌦️', 53:'🌦️', 55:'🌧️',
  61:'🌧️', 63:'🌧️', 65:'🌧️',
  71:'🌨️', 73:'🌨️', 75:'❄️',
  77:'🌨️',
  80:'🌦️', 81:'🌦️', 82:'🌧️',
  85:'🌨️', 86:'❄️',
  95:'⛈️', 96:'⛈️', 99:'⛈️',
};
const WX_DESC = {
  0:'Clear', 1:'Mostly clear', 2:'Partly cloudy', 3:'Overcast',
  45:'Fog', 48:'Icy fog',
  51:'Light drizzle', 53:'Drizzle', 55:'Heavy drizzle',
  61:'Light rain', 63:'Rain', 65:'Heavy rain',
  71:'Light snow', 73:'Snow', 75:'Heavy snow', 77:'Snow grains',
  80:'Showers', 81:'Showers', 82:'Heavy showers',
  85:'Snow showers', 86:'Heavy snow showers',
  95:'Thunderstorm', 96:'T-storm / hail', 99:'T-storm / hail',
};
const DAYS_SHORT = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];

async function loadWeather() {
  const body = document.getElementById('weather-body');
  try {
    const res  = await fetch('/api/weather');
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    body.innerHTML = data.days.map(d => {
      const dt   = new Date(d.date + 'T12:00:00');
      const dow  = DAYS_SHORT[dt.getDay()];
      const icon = WX_ICONS[d.code] || '🌡️';
      const desc = WX_DESC[d.code]  || '';
      return `<div class="wx-day">
        <div class="wx-dow">${dow}</div>
        <div class="wx-icon">${icon}</div>
        <div class="wx-hi">${d.high}°</div>
        <div class="wx-lo">${d.low}°</div>
        <div class="wx-desc">${desc}</div>
      </div>`;
    }).join('');
  } catch(e) {
    body.innerHTML = `<div style="color:#aaa;font-size:.82rem">Forecast unavailable</div>`;
  }
}

// ─────────────────────────────────────────────
// Payroll tab
// ─────────────────────────────────────────────
let payroll = {staff: [], checks: {}, days: []};
let prPeriod = 0;   // 0..3 → weeks 1&2, 3&4, 5&6, 7&8
let prTotals = false;   // when true, show the cumulative Totals view
let prExt = false;      // when true, show the blank Extended Staff sheet
let prExtPeriod = 'ALL';// Extended Staff AM/PM filter: ALL | AM | PM
let prAreas = [];       // selected areas to filter by ([] = all areas)

// True if a staff member passes the current area filter
function prAreaMatch(s) { return prAreas.length === 0 || prAreas.includes(s.area || ''); }

// Build the multi-select area dropdown (button label + checkbox menu)
function renderAreaFilter(areas) {
  prAreas = prAreas.filter(a => areas.includes(a));   // drop areas that no longer exist
  const btn = document.getElementById('pr-area-btn');
  btn.firstChild.textContent =
    (prAreas.length === 0 ? 'All areas'
     : prAreas.length === 1 ? prAreas[0]
     : prAreas.length + ' areas') + ' ';
  const menu = document.getElementById('pr-area-menu');
  menu.innerHTML =
    `<label><input type="checkbox" id="pr-area-all" ${prAreas.length === 0 ? 'checked' : ''}> All areas</label>` +
    `<div class="pr-multi-sep"></div>` +
    areas.map(a => `<label><input type="checkbox" class="pr-area-cb" value="${a}" ${prAreas.includes(a) ? 'checked' : ''}> ${a}</label>`).join('');
  menu.querySelector('#pr-area-all').addEventListener('change', () => { prAreas = []; renderPayroll(); });
  menu.querySelectorAll('.pr-area-cb').forEach(cb => {
    cb.addEventListener('change', () => {
      prAreas = Array.from(menu.querySelectorAll('.pr-area-cb:checked')).map(x => x.value);
      renderPayroll();
    });
  });
}

// Open/close the area dropdown (set up once)
(function() {
  const btn = document.getElementById('pr-area-btn');
  const menu = document.getElementById('pr-area-menu');
  if (!btn || !menu) return;
  btn.addEventListener('click', e => { e.stopPropagation(); menu.classList.toggle('hidden'); });
  menu.addEventListener('click', e => e.stopPropagation());
  document.addEventListener('click', () => menu.classList.add('hidden'));
})();

async function loadPayroll() {
  try {
    const res = await fetch('/api/payroll');
    payroll = await res.json();
    renderPayroll();
  } catch(e) { /* ignore */ }
}

function prPeriodDays() {
  return payroll.days.slice(prPeriod * 10, prPeriod * 10 + 10);
}

function cellState(id, iso) {
  const v = (payroll.checks[id] || {})[iso];
  if (v === true || v === 'check') return 'check';   // legacy true == check
  if (v === 'x') return 'x';
  return '';
}

function prCount(id) {
  // Only checkmarks count toward the total (X marks do not)
  return prPeriodDays().reduce((n, d) => n + (cellState(id, d.iso) === 'check' ? 1 : 0), 0);
}

function xtraState(id, key) {
  const v = (payroll.checks[id] || {})[key];
  return ['check','x','half','na'].includes(v) ? v : '';
}

function symFor(st) {
  return st === 'check' ? '✓' : st === 'x' ? '✗' : st === 'half' ? '½' : st === 'na' ? 'N/A' : '';
}

function renderPayroll() {
  // period buttons
  const pb = document.getElementById('payroll-periods');
  pb.innerHTML = '';
  for (let p = 0; p < 4; p++) {
    const b = document.createElement('button');
    b.className = 'pr-period-btn' + ((!prTotals && !prExt && p === prPeriod) ? ' active' : '');
    b.textContent = `Weeks ${p*2+1} & ${p*2+2}`;
    b.onclick = () => { prPeriod = p; prTotals = false; prExt = false; renderPayroll(); };
    pb.appendChild(b);
  }
  const tb = document.createElement('button');   // Totals view, slightly separated
  tb.className = 'pr-period-btn' + (prTotals ? ' active' : '');
  tb.textContent = '🧮 Totals';
  tb.style.marginLeft = '1.4rem';
  tb.onclick = () => { prTotals = true; prExt = false; renderPayroll(); };
  pb.appendChild(tb);
  const eb = document.createElement('button');   // Extended Staff blank sheet
  eb.className = 'pr-period-btn' + (prExt ? ' active' : '');
  eb.textContent = '👤 Extended Staff';
  eb.style.marginLeft = '.5rem';
  eb.onclick = () => { prExt = true; prTotals = false; renderPayroll(); };
  pb.appendChild(eb);
  // area filter (multi-select dropdown)
  const areas = [...new Set(payroll.staff.map(s => s.area).filter(Boolean))].sort();
  renderAreaFilter(areas);
  const sortKey = document.getElementById('pr-sort').value;

  // Lock button + add-staff controls reflect lock state (both views)
  document.getElementById('pr-lock').textContent = payroll.locked ? '🔒 Locked' : '🔓 Unlocked';
  ['pr-last','pr-first','pr-area','pr-add'].forEach(id => {
    const el = document.getElementById(id); if (el) el.disabled = payroll.locked;
  });

  // AM/PM shift selector is only relevant to the Extended Staff sheet;
  // the area filter is not used there, so hide it.
  const extWrap = document.getElementById('pr-ext-period-wrap');
  if (extWrap) extWrap.style.display = prExt ? '' : 'none';
  document.getElementById('pr-ext-period').value = prExtPeriod;
  const areaWrap = document.getElementById('pr-area-filter');
  if (areaWrap) areaWrap.style.display = prExt ? 'none' : 'flex';
  const sortWrap = document.getElementById('pr-sort').closest('label');
  if (sortWrap) sortWrap.style.display = (prExt || prTotals) ? 'none' : '';

  if (prExt) { renderExtTable('ALL', prExtPeriod); return; }
  if (prTotals) { renderTotalsTable('last'); return; }

  // table
  const days = prPeriodDays();
  let staff = payroll.staff.filter(prAreaMatch);
  staff.sort((a,b) => {
    if (sortKey === 'total') {
      const d = prCount(b.id) - prCount(a.id);
      if (d) return d;
    } else if (sortKey === 'area') {
      const c = (a.area||'').toLowerCase().localeCompare((b.area||'').toLowerCase());
      if (c) return c;
    }
    return (a.last+a.first).toLowerCase().localeCompare((b.last+b.first).toLowerCase());
  });
  const showExtra = prPeriod === 0;   // BS / SP\\MTC columns only on the Weeks 1 & 2 block
  let html = `<caption>${payrollTitle()}</caption><thead><tr><th>#</th><th>Staff</th><th>Area</th>`;
  days.forEach((d,i) => {
    const cls = 'pr-day' + (i === 5 ? ' pr-week-sep' : '');
    html += `<th class="${cls}">${d.dow}<br>${d.md}</th>`;
  });
  if (showExtra) html += '<th class="pr-extra pr-xsep">BS</th><th class="pr-extra">SP\\MTC</th>';
  html += '<th></th></tr></thead><tbody>';
  staff.forEach(s => {
    const c = payroll.checks[s.id] || {};
    html += `<tr data-id="${s.id}">`;
    html += `<td class="pr-count" id="cnt-${s.id}">${prCount(s.id)}</td>`;
    html += `<td class="pr-name">${s.last}, ${s.first}</td>`;
    const areaTxt = (s.area === 'Support' && s.title) ? s.title : (s.area || '');
    const bunkLine = s.bunk ? `<br><small style="color:#888;font-weight:400">${s.bunk}</small>` : '';
    html += `<td class="pr-area pr-area-edit" data-id="${s.id}" title="Click to edit area">${areaTxt}${bunkLine}</td>`;
    days.forEach((d,i) => {
      const st = cellState(s.id, d.iso);
      const sym = st === 'check' ? '✓' : st === 'x' ? '✗' : '';
      const cls = 'pr-cell st-' + (st || 'none') + (i === 5 ? ' pr-week-sep' : '');
      html += `<td class="${cls}" data-id="${s.id}" data-date="${d.iso}">${sym}</td>`;
    });
    if (showExtra) {
      for (let cc = 1; cc <= 2; cc++) {
        const key = `xtra:${prPeriod}:${cc}`;
        const xs = xtraState(s.id, key);
        const xcls = 'pr-xcell st-' + (xs || 'none') + (cc === 1 ? ' pr-xsep' : '');
        html += `<td class="${xcls}" data-id="${s.id}" data-key="${key}">${symFor(xs)}</td>`;
      }
    }
    html += `<td><button class="pr-del" data-id="${s.id}" title="Remove">✕</button></td>`;
    html += '</tr>';
  });
  html += '</tbody>';
  const tbl = document.getElementById('payroll-table');
  tbl.innerHTML = html;
  tbl.className = 'payroll-table' + (payroll.locked ? ' pr-locked' : '');

  // Day cells: blank -> ✓ (counts) -> ✗ -> blank
  tbl.querySelectorAll('td.pr-cell').forEach(cell => {
    cell.addEventListener('click', async () => {
      if (payroll.locked) return;
      const id = cell.dataset.id, dt = cell.dataset.date;
      const cur = cellState(id, dt);
      const next = cur === '' ? 'check' : cur === 'check' ? 'x' : '';
      payroll.checks[id] = payroll.checks[id] || {};
      if (next) payroll.checks[id][dt] = next; else delete payroll.checks[id][dt];
      cell.textContent = symFor(next);
      cell.classList.remove('st-check','st-x','st-none');
      cell.classList.add('st-' + (next || 'none'));
      document.getElementById('cnt-' + id).textContent = prCount(id);
      try { await fetch('/api/payroll/check', {method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({id, date: dt, value: next})}); } catch(e) {}
    });
  });

  // Extra columns: blank -> ✓ -> ✗ -> ½ -> N/A -> blank  (never counted)
  const xorder = ['', 'check', 'x', 'half', 'na'];
  tbl.querySelectorAll('td.pr-xcell').forEach(cell => {
    cell.addEventListener('click', async () => {
      if (payroll.locked) return;
      const id = cell.dataset.id, key = cell.dataset.key;
      const next = xorder[(xorder.indexOf(xtraState(id, key)) + 1) % xorder.length];
      payroll.checks[id] = payroll.checks[id] || {};
      if (next) payroll.checks[id][key] = next; else delete payroll.checks[id][key];
      cell.textContent = symFor(next);
      cell.classList.remove('st-check','st-x','st-half','st-na','st-none');
      cell.classList.add('st-' + (next || 'none'));
      try { await fetch('/api/payroll/check', {method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({id, date: key, value: next})}); } catch(e) {}
    });
  });

  tbl.querySelectorAll('.pr-del').forEach(btn => {
    btn.addEventListener('click', async () => {
      if (payroll.locked) return;
      const id = btn.dataset.id;
      const s = payroll.staff.find(x => x.id === id);
      if (!confirm(`Remove ${s ? s.last + ', ' + s.first : 'this staff member'}?`)) return;
      payroll.staff = payroll.staff.filter(x => x.id !== id);
      delete payroll.checks[id];
      renderPayroll();
      try { await fetch('/api/payroll/staff/' + id, {method:'DELETE'}); } catch(e) {}
    });
  });

  // Click an Area cell to edit it inline
  tbl.querySelectorAll('td.pr-area-edit').forEach(td => {
    td.addEventListener('click', () => {
      if (payroll.locked || td.querySelector('input')) return;
      const id = td.dataset.id;
      const s = payroll.staff.find(x => x.id === id);
      if (!s) return;
      const orig = s.area || '';
      td.innerHTML = `<input class="pr-area-input" value="${orig.replace(/"/g,'&quot;')}">`;
      const inp = td.querySelector('input');
      inp.focus(); inp.select();
      let done = false;
      const commit = async (save) => {
        if (done) return; done = true;
        const val = inp.value.trim();
        if (save && val !== orig) {
          s.area = val;
          try { await fetch('/api/payroll/staff/' + id, {method:'PATCH',
                headers:{'Content-Type':'application/json'}, body: JSON.stringify({area: val})}); } catch(e) {}
        }
        renderPayroll();
      };
      inp.addEventListener('keydown', e => {
        if (e.key === 'Enter')  { e.preventDefault(); commit(true);  }
        if (e.key === 'Escape') { e.preventDefault(); commit(false); }
      });
      inp.addEventListener('blur', () => commit(true));
    });
  });
}

// Lock / unlock toggle
document.getElementById('pr-lock').addEventListener('click', async () => {
  const next = !payroll.locked;
  payroll.locked = next;
  renderPayroll();
  try { await fetch('/api/payroll/lock', {method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({locked: next})}); } catch(e) {}
});

// ---- Totals tab (cumulative checks across all weeks) ----
function totalChecks(id) {
  const c = payroll.checks[id] || {};
  return payroll.days.reduce((n, d) => n + ((c[d.iso] === 'check' || c[d.iso] === true) ? 1 : 0), 0);
}
function isJC(s) { return (s.title || '').toLowerCase().includes('junior'); }

// Extended Staff — blank printable check-in sheet (only AM/PM-extended staff)
function renderExtTable(filterArea, extPeriod) {
  extPeriod = extPeriod || 'ALL';
  const matchShift = e => extPeriod === 'ALL'
    || (extPeriod === 'AM' && /AM/i.test(e))
    || (extPeriod === 'PM' && /PM/i.test(e));
  const staff = payroll.staff
    .filter(s => s.ext && matchShift(s.ext) && (filterArea === 'ALL' || s.area === filterArea))
    .sort((a,b) => (a.last+a.first).toLowerCase().localeCompare((b.last+b.first).toLowerCase()));
  const shiftLbl = extPeriod === 'AM' ? 'AM' : extPeriod === 'PM' ? 'PM' : 'AM & PM';
  let html = `<caption>Extended Staff (${shiftLbl}) — daily check-in (${staff.length})</caption>` +
    '<thead><tr><th>Staff</th>' +
    ['MON','TUES','WED','THURS','FRI'].map(d => `<th class="pr-extday">${d}</th>`).join('') +
    '</tr></thead><tbody>';
  staff.forEach(s => {
    html += `<tr><td class="pr-name">${s.last}, ${s.first}</td>` +
            '<td></td><td></td><td></td><td></td><td></td></tr>';
  });
  html += '</tbody>';
  const tbl = document.getElementById('payroll-table');
  tbl.innerHTML = html; tbl.className = 'payroll-table pr-ext';
}

// Totals view (rendered into the same Payroll table when the Totals button is on)
function renderTotalsTable(sortKey) {
  let staff = payroll.staff.filter(prAreaMatch)
                           .map(s => ({...s, _total: totalChecks(s.id)}));
  staff.sort((a,b) => {
    if (sortKey === 'total') return b._total - a._total ||
      (a.last+a.first).toLowerCase().localeCompare((b.last+b.first).toLowerCase());
    if (sortKey === 'area') {
      const c = (a.area||'').toLowerCase().localeCompare((b.area||'').toLowerCase());
      if (c) return c;
    }
    return (a.last+a.first).toLowerCase().localeCompare((b.last+b.first).toLowerCase());
  });

  let html = `<caption>${payrollTitle()}</caption><thead><tr><th>Staff</th><th>Area</th><th>Total Checks<br><small style="font-weight:400">(all 8 weeks)</small></th></tr></thead><tbody>`;
  staff.forEach(s => {
    const jc = isJC(s) ? ' <small style="color:#1A79BF;font-weight:700">JC</small>' : '';
    const areaTxt = (s.area === 'Support' && s.title) ? s.title : (s.area || '');
    const bunkLine = s.bunk ? `<br><small style="color:#888;font-weight:400">${s.bunk}</small>` : '';
    html += `<tr><td class="pr-name">${s.last}, ${s.first}${jc}</td>` +
            `<td class="pr-area">${areaTxt}${bunkLine}</td>` +
            `<td class="pr-count">${s._total}</td></tr>`;
  });
  html += '</tbody>';
  const tbl = document.getElementById('payroll-table');
  tbl.innerHTML = html;
  tbl.className = 'payroll-table';
}

document.getElementById('pr-add').addEventListener('click', async () => {
  const last = document.getElementById('pr-last').value.trim();
  const first = document.getElementById('pr-first').value.trim();
  const area = document.getElementById('pr-area').value.trim();
  const msg = document.getElementById('pr-msg');
  if (!last && !first) { msg.textContent = 'Enter a name.'; return; }
  msg.textContent = 'Adding…';
  try {
    const res = await fetch('/api/payroll/staff', {method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({last, first, area})});
    const entry = await res.json();
    payroll.staff.push(entry);
    document.getElementById('pr-last').value = '';
    document.getElementById('pr-first').value = '';
    document.getElementById('pr-area').value = '';
    msg.textContent = '';
    renderPayroll();
  } catch(e) { msg.textContent = 'Error adding staff.'; }
});

function payrollTitle() {
  let t = prTotals ? 'Payroll Totals — All 8 Weeks'
                   : `Payroll — Weeks ${prPeriod*2+1} & ${prPeriod*2+2}`;
  if (prAreas.length) t += '  —  ' + prAreas.join(', ');
  return t;
}

// Print / save-as-PDF (prints exactly what's on screen, filtered/sorted).
// Narrow views (Extended Staff, Totals) print portrait; the wide grid lands­cape.
document.getElementById('pr-print').addEventListener('click', () => {
  // All payroll views print portrait — the grid only needs ~8" of width, so
  // landscape just wastes space and fits fewer rows per page.
  let st = document.getElementById('pr-print-orient');
  if (!st) { st = document.createElement('style'); st.id = 'pr-print-orient'; document.head.appendChild(st); }
  st.textContent = `@media print{@page{size:portrait;margin:.4in}}`;
  window.print();
});

// Export the current (filtered/sorted) view to a real .xlsx (server-built)
document.getElementById('pr-export').addEventListener('click', () => {
  const view = prTotals ? 'totals' : prExt ? 'ext' : 'weeks';
  const areas = encodeURIComponent(prExt ? '' : prAreas.join(','));
  const sort = document.getElementById('pr-sort').value;
  window.location = `/api/payroll/export?view=${view}&period=${prPeriod}&areas=${areas}&sort=${sort}&extp=${prExtPeriod}`;
});

// ---- Family contacts ----
let families = [];
const famEsc = s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

async function loadFamilies() {
  let d = {};
  try {
    const res = await fetch('/api/families');
    d = await res.json();
    families = d.families || [];
  } catch(e) { families = []; }
  const box = document.getElementById('fam-status');
  const txt = document.getElementById('fam-status-text');
  if (!box) return;
  if (families.length) {
    const uploader = (d.uploaded_by || '').replace(/@.*$/, '');
    let when = (d.uploaded_at || '').replace(/\s*[A-Z]{2,4}\s*$/, '')
                                    .replace(/\s+(\d{1,2}:\d{2}\s*[AP]M)/i, ' @ $1');
    txt.innerHTML = `<strong>${families.length}</strong> family records stored` +
      (when ? ` &middot; uploaded on ${when}` : '') +
      (uploader ? ` by <strong>${uploader}</strong>` : '') + '.';
    box.style.display = 'flex';
  } else {
    box.style.display = 'none';
  }
}

const famDrop = document.getElementById('fam-drop');
const famFile = document.getElementById('fam-file');
async function importFamilies(f) {
  const msg = document.getElementById('fam-msg');
  if (!f) return;
  const mode = (document.querySelector('input[name="fam-import-mode"]:checked') || {}).value || 'replace';
  msg.style.color = '#666'; msg.textContent = 'Importing…';
  const fd = new FormData(); fd.append('file', f); fd.append('mode', mode);
  try {
    const res = await fetch('/api/families/import', {method:'POST', body: fd});
    const d = await res.json();
    if (!res.ok || d.error) { msg.style.color = '#c0392b'; msg.textContent = d.error || 'Import failed.'; return; }
    msg.style.color = '#2e7d32'; msg.textContent = `✓ Imported ${d.count} (${d.total} total).`;
    loadFamilies();
  } catch(e) { msg.style.color = '#c0392b'; msg.textContent = 'Network error: ' + e.message; }
}
famDrop.addEventListener('dragover', e => { e.preventDefault(); famDrop.classList.add('drag-over'); });
famDrop.addEventListener('dragleave', () => famDrop.classList.remove('drag-over'));
famDrop.addEventListener('drop', e => {
  e.preventDefault(); famDrop.classList.remove('drag-over');
  if (e.dataTransfer.files[0]) importFamilies(e.dataTransfer.files[0]);
});
famFile.addEventListener('change', e => { if (e.target.files[0]) { importFamilies(e.target.files[0]); e.target.value=''; } });

// ---- Season calendar (single start date) ----
let seasonStart = '';

async function loadSeason() {
  try {
    const res = await fetch('/api/season');
    const d = await res.json();
    seasonStart = d.start || '';
    const inp = document.getElementById('season-start');
    if (inp) inp.value = seasonStart;
    renderSeasonSummary(d);
  } catch(e) {}
}

function renderSeasonSummary(d) {
  const el = document.getElementById('season-summary');
  if (!el) return;
  const w1 = (d.weeks && d.weeks[0] && d.weeks[0].range) || '';
  el.innerHTML = (w1 && d.end)
    ? `8 weeks: <strong>Week 1</strong> ${w1} &hellip; through <strong>${d.end}</strong>.`
    : '';
}

document.getElementById('season-save').addEventListener('click', async () => {
  const msg = document.getElementById('season-msg');
  const start = document.getElementById('season-start').value;
  if (!start) { msg.style.color = '#c0392b'; msg.textContent = 'Pick the first day of camp.'; return; }
  // Warn only if the start actually changed — Payroll checks are keyed to dates
  if (seasonStart && start !== seasonStart && !confirm(
      'Changing the start date will re-date the Payroll day columns. Any attendance ' +
      'checks already entered are tied to the old dates and will NOT carry over to the ' +
      'new ones.\n\nThis is safe before the season starts. Continue?')) {
    return;
  }
  msg.style.color = '#666'; msg.textContent = 'Saving…';
  try {
    const res = await fetch('/api/season', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({start})});
    const d = await res.json();
    if (!res.ok || d.error) { msg.style.color = '#c0392b'; msg.textContent = d.error || 'Could not save.'; return; }
    seasonStart = d.start || start;
    renderSeasonSummary(d);
    msg.style.color = '#2e7d32'; msg.textContent = '✓ Saved. Reports & Payroll now use these dates.';
    loadPayroll();   // refresh payroll day columns
  } catch(e) { msg.style.color = '#c0392b'; msg.textContent = 'Network error: ' + e.message; }
});

// Load all data for the app (only after the user is signed in)
function loadAllData() {
  loadConfig();
  loadRecent();
  loadWeather();
  loadMaster();
  loadPayroll();
  loadFamilies();
  loadUsers();
  loadSeason();
}

// ---- First-time "Utilities" notice (shows once per browser, after sign-in) ----
function maybeShowNotice() {
  const KEY = 'el_seen_utilities_notice_v1';
  const overlay = document.getElementById('notice-overlay');
  const close = () => { overlay.classList.add('hidden'); try { localStorage.setItem(KEY, '1'); } catch(e) {} };
  try {
    if (localStorage.getItem(KEY)) return;
  } catch(e) {}
  overlay.classList.remove('hidden');
  document.getElementById('notice-ok').onclick = close;
  overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
}

// ---- Pricing modal ----
(function() {
  const overlay = document.getElementById('pricing-overlay');
  document.getElementById('pricing-btn').addEventListener('click', () => overlay.classList.remove('hidden'));
  document.getElementById('pricing-close').addEventListener('click', () => overlay.classList.add('hidden'));
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.classList.add('hidden'); });
})();

// ---- Authentication (per-user login accounts) ----
let currentUser = null;

async function loadUsers() {
  // Admin-only; the endpoint 403s for non-admins, in which case we hide the card.
  const card = document.getElementById('users-card');
  if (!currentUser || !currentUser.is_admin) { if (card) card.style.display = 'none'; return; }
  try {
    const res = await fetch('/api/users');
    if (!res.ok) { card.style.display = 'none'; return; }
    const d = await res.json();
    const tbl = document.getElementById('users-table');
    let h = '<thead><tr><th>Username</th><th>Role</th><th></th></tr></thead><tbody>';
    (d.users || []).forEach(u => {
      const isMe = currentUser && u.username.toLowerCase() === currentUser.username.toLowerCase();
      h += `<tr><td>${famEsc(u.username)}${isMe ? ' (you)' : ''}</td>` +
           `<td>${u.is_admin ? 'Admin' : 'User'}</td>` +
           `<td style="white-space:nowrap"><button class="usr-ic usr-rename" data-u="${famEsc(u.username)}" title="Rename">✎</button> ` +
           `<button class="pr-period-btn pr-sm usr-pw" data-u="${famEsc(u.username)}">Reset PW</button> ` +
           `${isMe ? '' : `<button class="pr-del usr-del" data-u="${famEsc(u.username)}" title="Remove">✕</button>`}</td></tr>`;
    });
    h += '</tbody>';
    tbl.innerHTML = h;
    card.style.display = '';
    tbl.querySelectorAll('.usr-del').forEach(btn => {
      btn.addEventListener('click', async () => {
        const un = btn.dataset.u;
        if (!confirm(`Remove user "${un}"?`)) return;
        try { await fetch('/api/users/' + encodeURIComponent(un), {method:'DELETE'}); } catch(e) {}
        loadUsers();
      });
    });
    tbl.querySelectorAll('.usr-pw').forEach(btn => {
      btn.addEventListener('click', async () => {
        const un = btn.dataset.u;
        const pw = prompt(`New password for "${un}" (min 4 characters):`);
        if (!pw) return;
        const r = await fetch('/api/users/' + encodeURIComponent(un), {method:'PATCH',
          headers:{'Content-Type':'application/json'}, body: JSON.stringify({password: pw})});
        const dd = await r.json();
        alert(r.ok && !dd.error ? `Password reset for ${un}.` : (dd.error || 'Could not reset password.'));
      });
    });
    tbl.querySelectorAll('.usr-rename').forEach(btn => {
      btn.addEventListener('click', async () => {
        const un = btn.dataset.u;
        const nu = prompt(`New username for "${un}":`, un);
        if (!nu || nu.trim() === un) return;
        const r = await fetch('/api/users/' + encodeURIComponent(un), {method:'PATCH',
          headers:{'Content-Type':'application/json'}, body: JSON.stringify({username: nu.trim()})});
        const dd = await r.json();
        if (!r.ok || dd.error) { alert(dd.error || 'Could not rename.'); return; }
        // If we renamed ourselves, update the header chip
        if (currentUser && currentUser.username.toLowerCase() === un.toLowerCase()) {
          currentUser.username = dd.username;
          document.getElementById('h-user-name').textContent = dd.username;
        }
        loadUsers();
      });
    });
  } catch(e) { card.style.display = 'none'; }
}

document.getElementById('usr-add').addEventListener('click', async () => {
  const msg = document.getElementById('usr-msg');
  const username = document.getElementById('usr-username').value.trim();
  const password = document.getElementById('usr-password').value;
  const email    = document.getElementById('usr-email').value.trim();
  const body = { username, password, is_admin: document.getElementById('usr-admin').checked };
  if (!username || !password) { msg.style.color = '#c0392b'; msg.textContent = 'Username and password required.'; return; }
  try {
    const res = await fetch('/api/users', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
    const d = await res.json();
    if (!res.ok || d.error) { msg.style.color = '#c0392b'; msg.textContent = d.error || 'Could not add user.'; return; }
    msg.style.color = '#2e7d32'; msg.textContent = `✓ Added ${d.username}.`;

    // Build a shareable credentials message + Copy / Email actions
    const url = window.location.origin;
    const message =
      `Elbow Lane Reporting Center — your login\n` +
      `Site: ${url}\n` +
      `Username: ${username}\n` +
      `Password: ${password}\n\n` +
      `Sign in at the link above. Keep this private.`;
    document.getElementById('usr-creds').textContent = message;
    const subject = 'Your Elbow Lane Reporting Center login';
    document.getElementById('usr-email-link').href =
      `mailto:${encodeURIComponent(email)}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(message)}`;
    document.getElementById('usr-copy-msg').textContent = '';
    document.getElementById('usr-result').style.display = '';

    ['usr-username','usr-password','usr-email'].forEach(id => document.getElementById(id).value = '');
    document.getElementById('usr-admin').checked = false;
    loadUsers();
  } catch(e) { msg.style.color = '#c0392b'; msg.textContent = 'Network error: ' + e.message; }
});

document.getElementById('usr-copy').addEventListener('click', async () => {
  const text = document.getElementById('usr-creds').textContent;
  const cm = document.getElementById('usr-copy-msg');
  try {
    await navigator.clipboard.writeText(text);
    cm.textContent = 'Copied!';
  } catch(e) {
    // Fallback: select the text for manual copy
    const r = document.createRange(); r.selectNodeContents(document.getElementById('usr-creds'));
    const sel = window.getSelection(); sel.removeAllRanges(); sel.addRange(r);
    cm.textContent = 'Press Ctrl/Cmd+C to copy.';
  }
  setTimeout(() => { cm.textContent = ''; }, 4000);
});

(function() {
  const overlay  = document.getElementById('pw-overlay');
  const errEl    = document.getElementById('pw-error');
  const loginView = document.getElementById('login-view');
  const regView   = document.getElementById('register-view');

  function showApp(user) {
    currentUser = user;
    overlay.classList.add('hidden');
    document.getElementById('h-user').style.display = 'flex';
    document.getElementById('h-user-name').textContent = user.name || user.username;
    loadAllData();
    maybeShowNotice();
  }

  document.getElementById('show-register').addEventListener('click', () => {
    loginView.style.display = 'none'; regView.style.display = ''; errEl.textContent = '';
  });
  document.getElementById('show-login').addEventListener('click', () => {
    regView.style.display = 'none'; loginView.style.display = ''; errEl.textContent = '';
  });

  async function doLogin() {
    errEl.textContent = '';
    const body = {
      username: document.getElementById('login-username').value.trim(),
      password: document.getElementById('login-password').value,
    };
    if (!body.username || !body.password) { errEl.textContent = 'Enter your username and password.'; return; }
    try {
      const res = await fetch('/api/login', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
      const d = await res.json();
      if (!res.ok || d.error) { errEl.textContent = d.error || 'Sign-in failed.'; return; }
      showApp(d);
    } catch(e) { errEl.textContent = 'Network error: ' + e.message; }
  }

  async function doRegister() {
    errEl.textContent = '';
    const body = {
      username: document.getElementById('reg-username').value.trim(),
      password: document.getElementById('reg-password').value,
      code:     document.getElementById('reg-code').value.trim(),
    };
    if (!body.username || !body.password) { errEl.textContent = 'Choose a username and password.'; return; }
    try {
      const res = await fetch('/api/register', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
      const d = await res.json();
      if (!res.ok || d.error) { errEl.textContent = d.error || 'Could not create account.'; return; }
      showApp(d);
    } catch(e) { errEl.textContent = 'Network error: ' + e.message; }
  }

  document.getElementById('login-btn').addEventListener('click', doLogin);
  document.getElementById('reg-btn').addEventListener('click', doRegister);
  document.getElementById('login-password').addEventListener('keydown', e => { if (e.key === 'Enter') doLogin(); });
  document.getElementById('reg-code').addEventListener('keydown', e => { if (e.key === 'Enter') doRegister(); });

  // Account dropdown (click your name)
  const userMenu = document.getElementById('h-user-menu');
  function closeMenu() { userMenu.classList.add('hidden'); }
  document.getElementById('h-user-btn').addEventListener('click', e => {
    e.stopPropagation();
    userMenu.classList.toggle('hidden');
  });
  document.addEventListener('click', () => closeMenu());
  document.getElementById('menu-logout').addEventListener('click', async () => {
    closeMenu();
    try { await fetch('/api/logout', {method:'POST'}); } catch(e) {}
    location.reload();
  });

  // Reset Password menu item → change-password dialog
  const cpw = document.getElementById('cpw-overlay');
  const cpwErr = document.getElementById('cpw-error');
  function openCpw() {
    if (!currentUser) return;
    document.getElementById('cpw-who').textContent = currentUser.username;
    ['cpw-current','cpw-new','cpw-confirm'].forEach(id => document.getElementById(id).value = '');
    cpwErr.textContent = '';
    cpw.classList.remove('hidden');
    document.getElementById('cpw-current').focus();
  }
  function closeCpw() { cpw.classList.add('hidden'); }
  document.getElementById('menu-reset').addEventListener('click', () => { closeMenu(); openCpw(); });
  document.getElementById('cpw-cancel').addEventListener('click', closeCpw);
  cpw.addEventListener('click', e => { if (e.target === cpw) closeCpw(); });
  document.getElementById('cpw-save').addEventListener('click', async () => {
    const current = document.getElementById('cpw-current').value;
    const nw = document.getElementById('cpw-new').value;
    const conf = document.getElementById('cpw-confirm').value;
    cpwErr.textContent = '';
    if (!current || !nw) { cpwErr.textContent = 'Fill in all fields.'; return; }
    if (nw.length < 4) { cpwErr.textContent = 'New password must be at least 4 characters.'; return; }
    if (nw !== conf) { cpwErr.textContent = 'New passwords do not match.'; return; }
    try {
      const res = await fetch('/api/account/password', {method:'POST',
        headers:{'Content-Type':'application/json'}, body: JSON.stringify({current, new: nw})});
      const d = await res.json();
      if (!res.ok || d.error) { cpwErr.textContent = d.error || 'Could not change password.'; return; }
      closeCpw();
      alert('Password changed.');
    } catch(e) { cpwErr.textContent = 'Network error: ' + e.message; }
  });

  // On load: if already signed in, go straight in; otherwise show the gate.
  (async () => {
    try {
      const res = await fetch('/api/me');
      const d = await res.json();
      if (d.authenticated) { showApp(d); return; }
      // No accounts yet → default to the create-account view
      if (!d.has_users) { loginView.style.display = 'none'; regView.style.display = ''; }
    } catch(e) {}
    document.getElementById('login-username').focus();
  })();
})();
</script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
