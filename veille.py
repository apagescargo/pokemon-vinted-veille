import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from functools import lru_cache

# ── Config ─────────────────────────────────────────────────────────────────────

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
BLACKLIST_FILE      = os.path.join(os.path.dirname(__file__), "blacklist.json")

MOTS_CLES          = [q.strip() for q in os.getenv("MOTS_CLES", "gros lot cartes pokemon,lot vrac pokemon,collection pokemon,pokemon,lot cartes pokemon,reverses pokemon").split(",") if q.strip()]
MOTS_EXCLUS        = [m.strip().lower() for m in os.getenv("MOTS_EXCLUS", "").split(",") if m.strip()]
SEUIL_MAX          = float(os.getenv("SEUIL_MAX", "0.04"))
MIN_CARTES         = int(os.getenv("MIN_CARTES", "300"))
ANCIENNETE_MINUTES = int(os.getenv("ANCIENNETE_MINUTES", "60"))
RAPPORT_HORAIRE    = os.getenv("RAPPORT_HORAIRE", "false").lower() == "true"

VINTED_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.vinted.fr/",
    "Origin":          "https://www.vinted.fr",
    "Sec-Fetch-Dest":  "empty",
    "Sec-Fetch-Mode":  "cors",
    "Sec-Fetch-Site":  "same-origin",
}

_thread_local     = threading.local()
_NOTIFY_LOCK      = threading.Lock()
_COUNTER_LOCK     = threading.Lock()
_EXECUTOR_QUERIES = ThreadPoolExecutor(max_workers=6)
_EXECUTOR_ITEMS   = ThreadPoolExecutor(max_workers=32)

_CARD_REF = re.compile(r"\d+/\d+")
_PATTERNS = [
    re.compile(r"(\d+)\s*cartes?",        re.I),
    re.compile(r"lot\s+(?:de\s+)?(\d+)",  re.I),
    re.compile(r"(\d+)\s+(?:pok[eé]mon)", re.I),
    re.compile(r"(?<![a-zA-Z])x\s*(\d+)", re.I),
    re.compile(r"(\d+)\s*pcs?",           re.I),
]
_LD_RE = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.DOTALL)

# ── Session HTTP par thread ────────────────────────────────────────────────────

def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=8, pool_maxsize=8,
            max_retries=Retry(total=2, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503]),
        )
        s.mount("https://", adapter)
        s.headers.update(VINTED_HEADERS)
        try:
            s.get("https://www.vinted.fr/", timeout=8)
        except Exception:
            pass
        _thread_local.session = s
    return _thread_local.session

# ── Blacklist ──────────────────────────────────────────────────────────────────

def load_blacklist() -> set:
    try:
        with open(BLACKLIST_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_blacklist(bl: set):
    with open(BLACKLIST_FILE, "w") as f:
        json.dump(list(bl), f)

# ── Scraping ───────────────────────────────────────────────────────────────────

def scrape_page(query: str, page: int) -> list[dict]:
    try:
        resp = get_session().get(
            "https://www.vinted.fr/api/v2/catalog/items",
            params={"search_text": query, "per_page": 96, "page": page, "order": "newest_first"},
            timeout=12,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    items = []
    for item in data.get("items", []):
        photos    = item.get("photos", [])
        price     = item.get("price", 0)
        items.append({
            "id":         item["id"],
            "titre":      item.get("title", ""),
            "titre_low":  item.get("title", "").lower(),
            "prix":       float(price.get("amount", 0) if isinstance(price, dict) else price),
            "url":        f"https://www.vinted.fr/items/{item['id']}",
            "photo":      photos[0].get("url", "") if photos else "",
            "created_at": photos[0].get("high_resolution", {}).get("timestamp") if photos else None,
        })
    return items

def scrape_all_pages(query: str) -> list[dict]:
    futures  = [_EXECUTOR_ITEMS.submit(scrape_page, query, p) for p in range(1, 11)]
    all_items = []
    for fut in as_completed(futures):
        all_items.extend(fut.result())
    return all_items

# ── Analyse ────────────────────────────────────────────────────────────────────

def _count_cards(texte: str) -> int:
    t = _CARD_REF.sub("", texte)
    for pat in _PATTERNS:
        m = pat.search(t)
        if m:
            val = int(m.group(1))
            if 1 <= val <= 10_000:
                return val
    return 0

@lru_cache(maxsize=2000)
def fetch_description(item_id: int) -> str:
    try:
        with get_session().get(
            f"https://www.vinted.fr/items/{item_id}",
            headers={"Accept": "text/html,application/xhtml+xml", "Sec-Fetch-Mode": "navigate"},
            timeout=10, stream=True,
        ) as resp:
            resp.raise_for_status()
            buf = ""
            for chunk in resp.iter_content(chunk_size=4096, decode_unicode=True):
                buf += chunk
                if "application/ld+json" in buf:
                    m = _LD_RE.search(buf)
                    if m:
                        return json.loads(m.group(1)).get("description", "")
                if len(buf) > 30_000:
                    break
    except Exception:
        pass
    return ""

def analyser_item(titre: str, id_: int, prix: float) -> tuple[int, int]:
    if MIN_CARTES > 0 and (prix / MIN_CARTES) > SEUIL_MAX:
        return id_, -1

    nb = _count_cards(titre)
    if nb:
        return id_, nb

    desc = fetch_description(id_)
    if desc:
        nb = _count_cards(titre + " " + desc)

    return id_, nb

# ── Discord ────────────────────────────────────────────────────────────────────

def _age(ts) -> str:
    if ts is None:
        return "date inconnue"
    try:
        s = int((datetime.now(timezone.utc) - datetime.fromtimestamp(ts, tz=timezone.utc)).total_seconds())
        if s < 60:    return f"il y a {s}s"
        if s < 3600:  return f"il y a {s // 60}min"
        if s < 86400: return f"il y a {s // 3600}h{(s % 3600) // 60:02d}"
        return f"il y a {s // 86400}j"
    except Exception:
        return "date inconnue"

def envoyer_alerte(r: dict) -> bool:
    if not DISCORD_WEBHOOK_URL:
        return False
    embed = {
        "title":  r["titre"][:256],
        "url":    r["url"],
        "color":  0x28a745,
        "fields": [
            {"name": "💰 Prix",       "value": f"{r['prix']:.2f} €",         "inline": True},
            {"name": "🃏 Cartes",     "value": str(r["nb_cartes"]),           "inline": True},
            {"name": "📉 €/carte",    "value": f"{r['cout_unitaire']:.3f} €", "inline": True},
            {"name": "🕐 Ancienneté", "value": _age(r["created_at"]),         "inline": True},
        ],
        "thumbnail": {"url": r["photo"]} if r.get("photo") else {},
    }
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]}, timeout=8)
        return resp.status_code in (200, 204)
    except Exception:
        return False

def envoyer_rapport(total_analyses: int, trouves: int):
    if not DISCORD_WEBHOOK_URL:
        return
    embed = {
        "title": "📊 Rapport horaire — Veille Pokémon",
        "color": 0x5865f2,
        "fields": [
            {"name": "🔍 Annonces analysées", "value": str(total_analyses), "inline": True},
            {"name": "✅ Affaires trouvées",   "value": str(trouves),        "inline": True},
            {"name": "⚙️ Critères",            "value": f"≥{MIN_CARTES} cartes · ≤{SEUIL_MAX}€/carte", "inline": False},
            {"name": "🕐 Heure",               "value": datetime.now(timezone.utc).strftime("%H:%M UTC"), "inline": True},
        ],
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]}, timeout=8)
    except Exception:
        pass

# ── Scan principal ─────────────────────────────────────────────────────────────

def scan():
    blacklist      = load_blacklist()
    deja_notifies  = blacklist.copy()
    maintenant     = datetime.now(timezone.utc)
    limite         = timedelta(minutes=ANCIENNETE_MINUTES) if ANCIENNETE_MINUTES > 0 else None
    trouves        = 0
    total_analyses = 0
    nouveaux_ids   = set()  # IDs à ajouter à la blacklist en fin de scan

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Démarrage — {len(MOTS_CLES)} mots-clés | seuil={SEUIL_MAX}€ | min={MIN_CARTES} cartes | ancienneté={ANCIENNETE_MINUTES}min")

    def scan_query(query: str) -> list[dict]:
        items = scrape_all_pages(query)
        a_analyser = []
        for a in items:
            if a["id"] in deja_notifies:
                continue
            if limite and a["created_at"]:
                if (maintenant - datetime.fromtimestamp(a["created_at"], tz=timezone.utc)) > limite:
                    continue
            if MOTS_EXCLUS and any(m in a["titre_low"] for m in MOTS_EXCLUS):
                continue
            if MIN_CARTES > 0 and a["prix"] / MIN_CARTES > SEUIL_MAX:
                continue
            a_analyser.append(a)

        with _COUNTER_LOCK:
            nonlocal total_analyses
            total_analyses += len(a_analyser)

        futures    = {_EXECUTOR_ITEMS.submit(analyser_item, a["titre"], a["id"], a["prix"]): a for a in a_analyser}
        resultats  = []
        for fut in as_completed(futures):
            a      = futures[fut]
            _, nb  = fut.result()
            if nb >= MIN_CARTES:
                cout = a["prix"] / nb
                if cout <= SEUIL_MAX:
                    resultats.append({**a, "nb_cartes": nb, "cout_unitaire": cout})
        return resultats

    futures_q = {_EXECUTOR_QUERIES.submit(scan_query, q): q for q in MOTS_CLES}
    for fut in as_completed(futures_q):
        query = futures_q[fut]
        for r in fut.result():
            with _NOTIFY_LOCK:
                if r["id"] in deja_notifies:
                    continue
                deja_notifies.add(r["id"])

            if envoyer_alerte(r):
                nouveaux_ids.add(r["id"])
                trouves += 1
                print(f"  ✅ [{query}] {r['titre'][:60]} — {r['nb_cartes']} cartes @ {r['cout_unitaire']:.3f}€/c")
            else:
                with _NOTIFY_LOCK:
                    deja_notifies.discard(r["id"])

    # Sauvegarde blacklist en une seule écriture
    if nouveaux_ids:
        save_blacklist(blacklist | nouveaux_ids)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Terminé — {trouves} affaire(s) | {total_analyses} annonces analysées")

    if RAPPORT_HORAIRE:
        envoyer_rapport(total_analyses, trouves)

if __name__ == "__main__":
    scan()
