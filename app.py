"""
EL Reporting Center — Flask Application
-----------------------------------------
Drop-in Excel report converter for Elbow Lane Day Camp.
Shares the same design system as Transport Pro.
"""

import os
import io
import json
import uuid
import threading
import urllib.request
from datetime import datetime, date, timedelta
try:
    from zoneinfo import ZoneInfo
    _EASTERN = ZoneInfo("America/New_York")
except Exception:
    _EASTERN = None
import boto3
from botocore.exceptions import ClientError
from flask import Flask, request, jsonify, send_file, render_template_string

from report_processor import process_report, load_bunk_config, save_bunk_config, is_master

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024  # 32 MB upload limit

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
_PROTECTED_KEYS   = {"bunk_config.json", MASTER_KEY, MASTER_META_KEY, PAYROLL_KEY}


def _save_master(file_bytes: bytes, filename: str) -> dict:
    """Persist the uploaded master sheet (S3 if configured, plus local copy)."""
    now = datetime.now(_EASTERN) if _EASTERN else datetime.now()
    fmt = "%#m/%#d/%Y %#I:%M %p %Z" if os.name == "nt" else "%-m/%-d/%Y %-I:%M %p %Z"
    meta = {
        "filename":    filename or "master",
        "uploaded_at": now.strftime(fmt).strip(),
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

def _payroll_days() -> list:
    """The 40 camp days (8 weeks x Mon-Fri) with day-of-week + m/d labels."""
    dows = ["MON", "TUES", "WED", "TH", "FRI"]
    out = []
    for i in range(40):
        wk, dow = divmod(i, 5)
        d = WEEK1_MONDAY + timedelta(days=wk * 7 + dow)
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
    # Backfill 'bunk'/'title' (added later) from the seed for staff missing them
    if any(("bunk" not in s or "title" not in s) for s in data["staff"]):
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
        _payroll_save(data)
    return data


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
                                week_num=week_num)

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


# --- Report processing ---

@app.route("/api/master", methods=["GET"])
def api_master():
    """Report whether a master sheet is currently saved (for the UI)."""
    meta = _load_master_meta()
    if meta and _load_master() is not None:
        return jsonify({"loaded": True, **meta})
    return jsonify({"loaded": False})


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


@app.route("/api/payroll/staff/<sid>", methods=["DELETE"])
def api_payroll_del(sid):
    data = _payroll_load()
    if data.get("locked"):
        return jsonify({"error": "locked"}), 403
    data["staff"] = [s for s in data["staff"] if s.get("id") != sid]
    data["checks"].pop(sid, None)
    _payroll_save(data)
    return jsonify({"ok": True})


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
.h-support,.h-pricing{background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.3);color:#fff;font-size:.78rem;font-weight:600;letter-spacing:.05em;padding:.45rem 1rem;border-radius:6px;cursor:pointer;text-decoration:none;display:flex;align-items:center;gap:.4rem;transition:background .18s}
.h-support:hover,.h-pricing:hover{background:rgba(255,255,255,.28)}
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
.payroll-table td.pr-count{font-weight:700;color:var(--brand);width:34px}
.payroll-table tbody tr:nth-child(even){background:#f4eef0}
.payroll-table td.pr-cell{cursor:pointer;font-weight:800;font-size:1.6rem;user-select:none;line-height:1}
.payroll-table td.pr-cell.st-check{color:#2e7d32}
.payroll-table td.pr-cell.st-x{color:#c0392b}
.payroll-table td.pr-cell.st-half{color:#1A79BF;font-size:1.1rem;font-weight:700}
.payroll-table td.pr-cell.st-na{color:#888;font-size:.95rem;font-weight:700}
.payroll-table th.pr-day,.payroll-table td.pr-cell{width:42px;min-width:42px}
.payroll-table th.pr-extra{width:58px;min-width:58px;background:#3f1119;color:#fff}
.payroll-table td.pr-xcell{width:58px;min-width:58px;background:#f3e7ea}
.payroll-table.pr-locked td.pr-cell,.payroll-table.pr-locked td.pr-xcell{cursor:not-allowed}
.payroll-table .pr-del{cursor:pointer;border:none;background:none;color:#c0392b;font-size:.95rem;padding:0}
.pr-week-sep{border-left:3px solid #6d1f2f !important}
.pr-period-btn{padding:.4rem .8rem;border:1px solid var(--brand);background:#fff;color:var(--brand);border-radius:8px;cursor:pointer;font-weight:600;font-size:.85rem}
.pr-period-btn.active{background:var(--brand);color:#fff}
.pr-input{padding:.45rem .6rem;border:1px solid var(--border);border-radius:8px;font-size:.85rem}
.payroll-table caption{caption-side:top;text-align:left;font-weight:700;font-size:1rem;padding:.3rem 0 .5rem;color:var(--brand)}
@media print {
  body * { visibility:hidden; }
  #payroll-table, #payroll-table * { visibility:visible; }
  #payroll-table { position:absolute; left:0; top:0; width:auto; font-size:9pt; }
  #payroll-table .pr-del { display:none; }
  @page { size:landscape; margin:.4in; }
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
</style>
</head>
<body>

<!-- Password gate -->
<div id="pw-overlay">
  <div id="pw-box">
    <div class="pw-logo"><img src="/logo.png" alt="Elbow Lane Day Camp"></div>
    <h2>Elbow Lane Reporting Center</h2>
    <p class="pw-sub">You have <strong>trial access</strong> to this reporting center at no cost.<br>Enter your access code to continue.</p>
    <div id="pw-input-wrap">
      <input id="pw-input" type="password" placeholder="Enter access code" autocomplete="off">
      <button id="pw-submit">Enter</button>
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

<header>
  <div class="h-logo" role="img" aria-label="Elbow Lane Day Camp"></div>
  <div>
    <div class="h-title">Elbow Lane Day Camp</div>
    <div class="h-sub">Reporting Center</div>
  </div>
  <div class="h-nav">
    <button class="h-pricing" id="pricing-btn">$ Pricing</button>
    <a class="h-support" href="mailto:bhimpele@gmail.com?subject=EL%20Reporting%20Center%20Support">✉ Support</a>
  </div>
</header>

<div class="tab-bar">
  <div class="tab active" data-tab="upload">📂 <span>Run Report</span></div>
  <div class="tab" data-tab="payroll">🗓️ <span>Payroll</span></div>
  <div class="tab" data-tab="config">⚙️ <span>Bunks &amp; Camps</span></div>
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

  <!-- Update master sheet (optional — only when the data changes) -->
  <div class="card" style="margin-top:1.25rem">
    <div class="card-hd">
      <div>
        <div class="card-title">Update Master Sheet</div>
        <div class="card-hint">Only needed when the camper data changes — upload a new master and it replaces the saved one for every report. (Original per-report exports still work as well.)</div>
      </div>
    </div>
    <div class="drop-zone" id="drop-zone">
      <input type="file" id="excel-file" accept=".csv,.xlsx,.xls">
      <div class="drop-icon">📊</div>
      <div class="drop-text"><strong>Click to choose</strong> or drag &amp; drop the master sheet</div>
      <div class="drop-meta">Accepted formats: .csv, .xlsx, .xls</div>
    </div>
    <div class="file-chosen" id="file-chosen">
      <span>✅</span>
      <span id="file-name">—</span>
      <button class="rm" id="remove-file">✕</button>
    </div>
  </div>

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

<!-- ===== CONFIG TAB ===== -->
<div class="tab-panel" id="tab-config">

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
      <label>Filter area:
        <select id="pr-filter-area" class="pr-input" onchange="renderPayroll()"></select></label>
      <label>Sort by:
        <select id="pr-sort" class="pr-input" onchange="renderPayroll()">
          <option value="last">Last name</option>
          <option value="area">Area</option>
          <option value="total">Total (high to low)</option>
        </select></label>
      <button id="pr-export" class="pr-period-btn" style="margin-left:auto">⬇ Excel</button>
      <button id="pr-print" class="pr-period-btn">🖨 Print / PDF</button>
      <button id="pr-lock" class="pr-period-btn">🔓 Unlocked</button>
    </div>

    <div style="overflow-x:auto">
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
let excelFile = null;
let selectedReportType = 'bunk_snapshot';
let selectedWeek = 1;
let currentJobId = null;
let pollTimer = null;
let lastLineCount = 0;
let masterLoaded = false;

// Saved master sheet status
async function loadMaster() {
  try {
    const res = await fetch('/api/master');
    const d = await res.json();
    masterLoaded = !!d.loaded;
    const banner = document.getElementById('master-banner');
    if (masterLoaded) {
      document.getElementById('master-banner-text').innerHTML =
        `Using saved master: <strong>${d.filename || 'master sheet'}</strong>` +
        (d.uploaded_at ? ` &middot; uploaded ${d.uploaded_at}` : '') +
        `. Reports and Labels will use this data until an updated file is uploaded.`;
      banner.style.display = 'flex';
    } else {
      banner.style.display = 'none';
    }
  } catch(e) { masterLoaded = false; }
  updateRunBtn();
}

document.getElementById('master-clear').addEventListener('click', async () => {
  try { await fetch('/api/master', {method: 'DELETE'}); } catch(e) {}
  loadMaster();
});

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

// Drop zone
const dropZone = document.getElementById('drop-zone');
const fileInput = document.getElementById('excel-file');

dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
dropZone.addEventListener('drop', e => {
  e.preventDefault();
  dropZone.classList.remove('drag-over');
  const f = e.dataTransfer.files[0];
  if (f && (f.name.endsWith('.csv') || f.name.endsWith('.xlsx') || f.name.endsWith('.xls'))) setFile(f);
});
fileInput.addEventListener('change', e => {
  if (e.target.files[0]) setFile(e.target.files[0]);
});
document.getElementById('remove-file').addEventListener('click', e => {
  e.stopPropagation();
  clearFile();
});

function setFile(f) {
  excelFile = f;
  document.getElementById('file-name').textContent = f.name;
  document.getElementById('file-chosen').classList.add('visible');
  dropZone.querySelector('.drop-icon').textContent = '✅';
  updateRunBtn();
}

function clearFile() {
  excelFile = null;
  fileInput.value = '';
  document.getElementById('file-chosen').classList.remove('visible');
  dropZone.querySelector('.drop-icon').textContent = '📊';
  updateRunBtn();
}

function updateRunBtn() {
  document.getElementById('run-btn').disabled = !((excelFile || masterLoaded) && selectedReportType);
}

// Run button
document.getElementById('run-btn').addEventListener('click', async () => {
  if (!(excelFile || masterLoaded) || !selectedReportType) return;
  startProcessing();

  const fd = new FormData();
  if (excelFile) fd.append('excel_file', excelFile);   // omit to reuse saved master
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
    b.className = 'pr-period-btn' + ((!prTotals && p === prPeriod) ? ' active' : '');
    b.textContent = `Weeks ${p*2+1} & ${p*2+2}`;
    b.onclick = () => { prPeriod = p; prTotals = false; renderPayroll(); };
    pb.appendChild(b);
  }
  const tb = document.createElement('button');   // Totals view, slightly separated
  tb.className = 'pr-period-btn' + (prTotals ? ' active' : '');
  tb.textContent = '🧮 Totals';
  tb.style.marginLeft = '1.4rem';
  tb.onclick = () => { prTotals = true; renderPayroll(); };
  pb.appendChild(tb);
  // area filter dropdown (preserve current selection)
  const fsel = document.getElementById('pr-filter-area');
  const areas = [...new Set(payroll.staff.map(s => s.area).filter(Boolean))].sort();
  const cur = fsel.value || 'ALL';
  fsel.innerHTML = '<option value="ALL">All areas</option>' +
    areas.map(a => `<option value="${a}">${a}</option>`).join('');
  fsel.value = (cur === 'ALL' || areas.includes(cur)) ? cur : 'ALL';

  const filterArea = fsel.value;
  const sortKey = document.getElementById('pr-sort').value;

  // Lock button + add-staff controls reflect lock state (both views)
  document.getElementById('pr-lock').textContent = payroll.locked ? '🔒 Locked' : '🔓 Unlocked';
  ['pr-last','pr-first','pr-area','pr-add'].forEach(id => {
    const el = document.getElementById(id); if (el) el.disabled = payroll.locked;
  });

  if (prTotals) { renderTotalsTable(filterArea, sortKey); return; }

  // table
  const days = prPeriodDays();
  let staff = payroll.staff.filter(s => filterArea === 'ALL' || s.area === filterArea);
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
  if (showExtra) html += '<th class="pr-extra">BS</th><th class="pr-extra">SP\\MTC</th>';
  html += '<th></th></tr></thead><tbody>';
  staff.forEach(s => {
    const c = payroll.checks[s.id] || {};
    html += `<tr data-id="${s.id}">`;
    html += `<td class="pr-count" id="cnt-${s.id}">${prCount(s.id)}</td>`;
    html += `<td class="pr-name">${s.last}, ${s.first}</td>`;
    const areaTxt = (s.area === 'Support' && s.title) ? s.title : (s.area || '');
    const bunkLine = s.bunk ? `<br><small style="color:#888;font-weight:400">${s.bunk}</small>` : '';
    html += `<td class="pr-area">${areaTxt}${bunkLine}</td>`;
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
        html += `<td class="pr-xcell st-${xs || 'none'}" data-id="${s.id}" data-key="${key}">${symFor(xs)}</td>`;
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

// Totals view (rendered into the same Payroll table when the Totals button is on)
function renderTotalsTable(filterArea, sortKey) {
  let staff = payroll.staff.filter(s => filterArea === 'ALL' || s.area === filterArea)
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
  const fa = document.getElementById('pr-filter-area').value;
  if (fa && fa !== 'ALL') t += '  —  ' + fa;
  return t;
}

// Print / save-as-PDF (prints exactly what's on screen, filtered/sorted)
document.getElementById('pr-print').addEventListener('click', () => window.print());

// Export the current (filtered/sorted) table to an .xls Excel file
document.getElementById('pr-export').addEventListener('click', () => {
  const tbl = document.getElementById('payroll-table').cloneNode(true);
  if (!prTotals) {  // drop the delete (✕) column in the weeks grid
    tbl.querySelectorAll('tr').forEach(tr => { if (tr.lastElementChild) tr.lastElementChild.remove(); });
  }
  tbl.querySelectorAll('button').forEach(b => b.remove());
  const html = '<html xmlns:o="urn:schemas-microsoft-com:office:office" ' +
    'xmlns:x="urn:schemas-microsoft-com:office:excel"><head><meta charset="utf-8"></head><body>' +
    tbl.outerHTML + '</body></html>';
  const blob = new Blob(['﻿' + html], {type: 'application/vnd.ms-excel'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = payrollTitle().replace(/[^\w]+/g, '_').replace(/^_|_$/g, '') + '.xls';
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(a.href);
});

// Boot
loadConfig();
loadRecent();
loadWeather();
loadMaster();
loadPayroll();

// ---- Pricing modal ----
(function() {
  const overlay = document.getElementById('pricing-overlay');
  document.getElementById('pricing-btn').addEventListener('click', () => overlay.classList.remove('hidden'));
  document.getElementById('pricing-close').addEventListener('click', () => overlay.classList.add('hidden'));
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.classList.add('hidden'); });
})();

// ---- Password gate ----
(function() {
  const overlay  = document.getElementById('pw-overlay');
  const input    = document.getElementById('pw-input');
  const btn      = document.getElementById('pw-submit');
  const errEl    = document.getElementById('pw-error');
  const KEY      = 'el_rc_auth';
  const TTL_MS   = 3 * 60 * 60 * 1000; // 3 hours

  // Skip gate if still within the 3-hour window
  const saved = localStorage.getItem(KEY);
  if (saved && (Date.now() - parseInt(saved, 10)) < TTL_MS) {
    overlay.classList.add('hidden');
    return;
  }

  function attempt() {
    if (input.value.trim().toLowerCase() === 'trial') {
      localStorage.setItem(KEY, Date.now().toString());
      overlay.classList.add('hidden');
    } else {
      errEl.textContent = 'Incorrect access code. Please try again.';
      input.value = '';
      input.focus();
    }
  }

  btn.addEventListener('click', attempt);
  input.addEventListener('keydown', e => { if (e.key === 'Enter') attempt(); });
  input.focus();
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
