"""
Crée 2 jobs sur cron-job.org :
  - Scan toutes les 30 minutes (h:00 et h:30)
  - Rapport horaire toutes les heures (h:00)
Stdlib uniquement, aucune dépendance.
"""

import json
import urllib.request
import urllib.error

# ── Config ─────────────────────────────────────────────────────────────────────

CRONJOB_API_KEY = "REMPLACE_PAR_TA_CLE_CRONJOB"   # cron-job.org → Settings → API key
GITHUB_TOKEN    = "REMPLACE_PAR_TON_GITHUB_TOKEN"  # GitHub PAT avec scope "workflow"

GITHUB_USER   = "apagescargo"
GITHUB_REPO   = "pokemon-vinted-veille"
WORKFLOW_FILE = "veille.yml"

DISPATCH_URL = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"

GITHUB_HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "Content-Type": "application/json",
}

# ── Jobs à créer ───────────────────────────────────────────────────────────────

JOBS = [
    {
        "title": "Veille Pokémon — scan 30min",
        "body":  json.dumps({"ref": "main", "inputs": {"rapport_horaire": "false"}}),
        "schedule": {
            "timezone": "Europe/Paris",
            "hours":    [-1],
            "minutes":  [0, 30],
            "wdays":    [-1],
            "mdays":    [-1],
            "months":   [-1],
        },
    },
    {
        "title": "Veille Pokémon — rapport horaire",
        "body":  json.dumps({"ref": "main", "inputs": {"rapport_horaire": "true"}}),
        "schedule": {
            "timezone": "Europe/Paris",
            "hours":    [-1],
            "minutes":  [0],
            "wdays":    [-1],
            "mdays":    [-1],
            "months":   [-1],
        },
    },
]

# ── Création des jobs ──────────────────────────────────────────────────────────

def creer_job(job_def: dict):
    payload = json.dumps({
        "job": {
            "url":           DISPATCH_URL,
            "title":         job_def["title"],
            "enabled":       True,
            "saveResponses": True,
            "requestMethod": 1,
            "extendedData": {
                "headers": GITHUB_HEADERS,
                "body":    job_def["body"],
            },
            "schedule": job_def["schedule"],
        }
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.cron-job.org/jobs",
        data=payload,
        method="PUT",
        headers={
            "Authorization": f"Bearer {CRONJOB_API_KEY}",
            "Content-Type":  "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req) as resp:
            body   = json.loads(resp.read())
            job_id = body.get("jobId")
            print(f"  ✅ '{job_def['title']}' — ID : {job_id}")
            print(f"     https://console.cron-job.org/jobs/{job_id}")
    except urllib.error.HTTPError as e:
        print(f"  ❌ '{job_def['title']}' — Erreur {e.code} : {e.read().decode()}")
    except Exception as e:
        print(f"  ❌ '{job_def['title']}' — {e}")


print("Création des jobs cron-job.org...\n")
for j in JOBS:
    creer_job(j)
print("\nTerminé !")
