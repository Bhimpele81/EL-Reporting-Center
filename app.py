"""
EL Reporting Center — Flask Application
-----------------------------------------
Drop-in Excel report converter for Elbow Lane Day Camp.
Shares the same design system as Transport Pro.
"""

import os
import json
import uuid
import threading
from flask import Flask, request, jsonify, send_file, render_template_string

from report_processor import process_report, load_bunk_config, save_bunk_config

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

# In-memory job store  {job_id: {status, progress, result}}
jobs: dict = {}
jobs_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------

def run_job(job_id: str, file_bytes: bytes, report_type: str) -> None:
    def log(msg: str, level: str = "info") -> None:
        with jobs_lock:
            jobs[job_id]["progress"].append({"msg": msg, "level": level})

    try:
        with jobs_lock:
            jobs[job_id]["status"] = "running"

        log("Loading bunk configuration…")
        config = load_bunk_config(CONFIG_PATH)

        log(f"Processing report type: {report_type}…")
        result = process_report(file_bytes, report_type, config, job_id, OUTPUT_DIR)

        if result["success"]:
            log(result["message"], "ok")
            with jobs_lock:
                jobs[job_id]["status"]   = "done"
                jobs[job_id]["filename"] = result["filename"]
                jobs[job_id]["rows"]     = result.get("rows", 0)
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
        config = load_bunk_config(CONFIG_PATH)
        return jsonify(config)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/config", methods=["POST"])
def save_config():
    try:
        data = request.get_json(force=True)
        save_bunk_config(CONFIG_PATH, data)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- Report processing ---

@app.route("/api/process", methods=["POST"])
def api_process():
    excel_file  = request.files.get("excel_file")
    report_type = request.form.get("report_type", "").strip()

    if not excel_file:
        return jsonify({"error": "No file uploaded."}), 400
    if not report_type:
        return jsonify({"error": "No report type selected."}), 400

    file_bytes = excel_file.read()
    job_id = uuid.uuid4().hex[:8]

    with jobs_lock:
        jobs[job_id] = {"status": "queued", "progress": []}

    thread = threading.Thread(target=run_job, args=(job_id, file_bytes, report_type), daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def api_status(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if job is None:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job)


@app.route("/api/download/<job_id>")
def api_download(job_id: str):
    with jobs_lock:
        job = jobs.get(job_id)
    if job is None or job.get("status") != "done":
        return jsonify({"error": "File not ready."}), 404
    path = os.path.join(OUTPUT_DIR, job["filename"])
    if not os.path.exists(path):
        return jsonify({"error": "Output file missing."}), 500
    return send_file(path, as_attachment=True, download_name=job["filename"])


@app.route("/healthz")
def health():
    return "ok"


# ---------------------------------------------------------------------------
# Embedded HTML / CSS / JS
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🏕️</text></svg>">
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
.card{background:#fff;border:1px solid var(--border);border-radius:var(--r);padding:1.5rem 1.75rem;margin-bottom:1.1rem;box-shadow:0 1px 4px rgba(0,0,0,.04);transition:box-shadow .2s}
.card:hover{box-shadow:0 3px 12px rgba(109,31,47,.07)}
.card-hd{display:flex;align-items:center;gap:.7rem;margin-bottom:1.1rem}
.card-num{width:26px;height:26px;background:var(--brand);color:#fff;border-radius:50%;font-size:.75rem;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.card-title{font-family:'Roboto Slab',serif;font-size:1.05rem;font-weight:700;color:var(--brand-dark);letter-spacing:.01em;text-transform:uppercase}
.card-hint{font-size:.75rem;color:#999;margin-top:.15rem;font-weight:300}
label.lbl{display:block;font-size:.75rem;font-weight:600;color:var(--brand-dark);letter-spacing:.04em;text-transform:uppercase;margin-bottom:.4rem}
/* Drop zone */
.drop-zone{border:2px dashed var(--border);border-radius:var(--r);padding:1.75rem;text-align:center;cursor:pointer;transition:all .2s;background:var(--mist);position:relative}
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
/* Error card */
#error-card{display:none;background:#2d0d13;border:1px solid #6d1f2f;border-radius:var(--r);padding:1.1rem 1.4rem;margin-top:1.1rem;color:#f5c2cb;font-size:.85rem}
#error-card.visible{display:block}
#error-card strong{display:block;margin-bottom:.35rem;font-size:.95rem}
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
#save-msg{display:none;margin-top:.75rem;padding:.6rem 1rem;border-radius:8px;font-size:.83rem;font-weight:500;text-align:center}
#save-msg.ok{display:block;background:#edfaf3;border:1px solid #a3d9b8;color:var(--success)}
#save-msg.err{display:block;background:#2d0d13;border:1px solid #6d1f2f;color:#f5c2cb}
/* Misc */
.section-title{font-family:'Roboto Slab',serif;font-size:.85rem;font-weight:700;color:var(--brand-dark);text-transform:uppercase;letter-spacing:.05em;margin-bottom:.65rem}
.empty-state{text-align:center;padding:3rem 2rem;color:#bbb}
.empty-state .empty-icon{font-size:2.5rem;margin-bottom:.75rem}
.empty-state p{font-size:.9rem;line-height:1.6}
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
</style>
</head>
<body>
<header>
  <div class="h-logo" role="img" aria-label="Elbow Lane Day Camp"></div>
  <div>
    <div class="h-title">Elbow Lane Day Camp</div>
    <div class="h-sub">Reporting Center</div>
  </div>
  <span class="h-badge">Report Tools</span>
</header>

<div class="tab-bar">
  <div class="tab active" data-tab="upload">📂 <span>Upload Report</span></div>
  <div class="tab" data-tab="config">⚙️ <span>Bunks &amp; Camps</span></div>
</div>

<div class="container">

<!-- ===== UPLOAD TAB ===== -->
<div class="tab-panel active" id="tab-upload">

  <div class="card">
    <div class="card-hd">
      <span class="card-num">1</span>
      <div>
        <div class="card-title">Select Report Type</div>
        <div class="card-hint">Choose the type of report you are converting</div>
      </div>
    </div>
    <div class="report-types" id="report-types">
      <!-- Populated dynamically — add report types here as they are configured -->
      <button class="rtype-btn active" data-rtype="placeholder">Sample Report</button>
    </div>
    <div style="margin-top:.75rem;font-size:.75rem;color:#aaa;">
      Additional report types will appear here once transformation logic is configured.
    </div>
  </div>

  <div class="card">
    <div class="card-hd">
      <span class="card-num">2</span>
      <div>
        <div class="card-title">Upload Raw Excel File</div>
        <div class="card-hint">Drop the raw report exported from your camp management system</div>
      </div>
    </div>
    <div class="drop-zone" id="drop-zone">
      <input type="file" id="excel-file" accept=".xlsx,.xls">
      <div class="drop-icon">📊</div>
      <div class="drop-text"><strong>Click to choose</strong> or drag &amp; drop your Excel file</div>
      <div class="drop-meta">Accepted formats: .xlsx, .xls</div>
    </div>
    <div class="file-chosen" id="file-chosen">
      <span>✅</span>
      <span id="file-name">—</span>
      <button class="rm" id="remove-file">✕</button>
    </div>
  </div>

  <button class="run-btn" id="run-btn" disabled>
    <span id="run-icon">⚙️</span>
    <span id="run-label">Convert Report</span>
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
    <a class="dl-btn" id="dl-link" href="#" download>⬇ Download Converted Report</a>
  </div>

  <div id="error-card">
    <strong>⚠ Processing Error</strong>
    <span id="error-msg"></span>
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
  });
});

// ─────────────────────────────────────────────
// Upload tab state
// ─────────────────────────────────────────────
let excelFile = null;
let selectedReportType = 'placeholder';
let currentJobId = null;
let pollTimer = null;
let lastLineCount = 0;

// Report type buttons
document.querySelectorAll('.rtype-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.rtype-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    selectedReportType = btn.dataset.rtype;
    updateRunBtn();
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
  if (f && (f.name.endsWith('.xlsx') || f.name.endsWith('.xls'))) setFile(f);
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
  document.getElementById('run-btn').disabled = !(excelFile && selectedReportType);
}

// Run button
document.getElementById('run-btn').addEventListener('click', async () => {
  if (!excelFile || !selectedReportType) return;
  startProcessing();

  const fd = new FormData();
  fd.append('excel_file', excelFile);
  fd.append('report_type', selectedReportType);

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
      document.getElementById('prog-title').textContent = 'Complete!';
      document.getElementById('run-btn').disabled = false;
      document.getElementById('run-label').textContent = 'Convert Report';
      document.getElementById('run-icon').textContent = '⚙️';

      const dlLink = document.getElementById('dl-link');
      dlLink.href  = `/api/download/${currentJobId}`;
      document.getElementById('action-bar').style.display = 'flex';
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
  document.getElementById('run-label').textContent = 'Convert Report';
  document.getElementById('run-icon').textContent = '⚙️';
}

// ─────────────────────────────────────────────
// Config tab
// ─────────────────────────────────────────────
let campConfig = {camps: []};

async function loadConfig() {
  try {
    const res  = await fetch('/api/config');
    campConfig = await res.json();
    renderCamps();
  } catch(e) {
    console.error('Could not load config', e);
  }
}

function renderCamps() {
  const list = document.getElementById('camp-list');
  list.innerHTML = '';
  campConfig.camps.forEach((camp, ci) => {
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
            <th style="width:90px">Number</th>
            <th style="width:36px"></th>
          </tr>
        </thead>
        <tbody id="bunk-body-${ci}">
          ${camp.bunks.map((b, bi) => bunkRow(ci, bi, b)).join('')}
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
  campConfig.camps[ci].bunks.push({name: '', number: 0});
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
      msg.textContent = '✅ Configuration saved successfully.';
      msg.className   = 'ok';
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

// Boot
loadConfig();
</script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
