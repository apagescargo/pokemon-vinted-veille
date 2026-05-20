import streamlit as st
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import google.genai as genai
import re
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from functools import lru_cache

load_dotenv()

GEMINI_API_KEY      = os.getenv("GEMINI_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
BLACKLIST_FILE      = os.path.join(os.path.dirname(__file__), "blacklist.json")
MOTS_EXCLUS_FILE    = os.path.join(os.path.dirname(__file__), "mots_exclus.json")

if GEMINI_API_KEY:
    _gemini_client = genai.Client(api_key=GEMINI_API_KEY)
else:
    _gemini_client = None

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

LIMITES = {
    "Moins de 30min": timedelta(minutes=30),
    "Moins de 1h":    timedelta(hours=1),
    "Moins de 4h":    timedelta(hours=4),
    "Moins de 24h":   timedelta(hours=24),
    "Cette semaine":  timedelta(weeks=1),
    "Tout":           None,
}

MOTS_CLES_DEFAUT = (
    # Génériques
    "pokemon\n"
    "lot pokemon\n"
    "vrac pokemon\n"
    "collection pokemon\n"
    "reverses pokemon\n"
    # Écarlate et Violet (série actuelle)
    "lot ecarlate violet\n"
    "lot EV pokemon\n"
    # Épée et Bouclier
    "lot epee bouclier\n"
    "lot EB pokemon\n"
    # Soleil et Lune
    "lot soleil lune\n"
    "lot SL pokemon\n"
    # XY
    "lot XY pokemon\n"
    # Noir et Blanc
    "lot noir blanc\n"
    "lot NB pokemon\n"
    # Diamant et Perle / Platine / HGSS
    "lot diamant perle\n"
    "lot platine pokemon\n"
    "lot HGSS pokemon"
)

# Trois pools séparés — aucun nesting dans le même pool
_EXECUTOR_QUERIES = ThreadPoolExecutor(max_workers=5)   # 1 thread par mot-clé
_EXECUTOR_PAGES   = ThreadPoolExecutor(max_workers=20)  # scraping pages Vinted (I/O réseau pur)
_EXECUTOR_ITEMS   = ThreadPoolExecutor(max_workers=16)  # analyse items (regex + fetch description)
_DIAG_LOCK        = threading.Lock()                    # Fix thread-safety diag list
_NOTIFY_LOCK      = threading.Lock()                    # Fix double-envoi Discord

# ── Fix #2 : session par thread (thread-safe) ──────────────────────────────────
_thread_local = threading.local()

def _make_session() -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=8,
        pool_maxsize=8,
        max_retries=Retry(total=2, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503]),
    )
    s.mount("https://", adapter)
    s.headers.update(VINTED_HEADERS)
    try:
        s.get("https://www.vinted.fr/", timeout=8)
    except Exception:
        pass
    return s

def get_session() -> requests.Session:
    """Une session par thread — thread-safe, pas de corruption de cookies."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = _make_session()
    return _thread_local.session


# ── Blacklist ──────────────────────────────────────────────────────────────────

def load_blacklist() -> set:
    try:
        with open(BLACKLIST_FILE) as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()

def save_blacklist(bl: set):
    with open(BLACKLIST_FILE, "w") as f:
        json.dump(list(bl), f)

def load_mots_exclus_permanents() -> list[str]:
    try:
        with open(MOTS_EXCLUS_FILE) as f:
            return json.load(f)
    except Exception:
        return []

def save_mots_exclus_permanents(mots: list[str]):
    with open(MOTS_EXCLUS_FILE, "w") as f:
        json.dump(mots, f)


# ── Vinted scraping ────────────────────────────────────────────────────────────

def _parse_items(data: dict) -> tuple[list[dict], int]:
    total_pages = data.get("pagination", {}).get("total_pages", 1)
    items = []
    for item in data.get("items", []):
        photos     = item.get("photos", [])
        photo_url  = photos[0].get("url", "") if photos else ""
        created_at = photos[0].get("high_resolution", {}).get("timestamp") if photos else None
        price      = item.get("price", 0)
        items.append({
            "id":         item.get("id"),
            "titre":      item.get("title", ""),
            # Fix #5 : titre lowercasé une seule fois ici, pas à chaque filtre
            "titre_low":  item.get("title", "").lower(),
            "prix":       float(price.get("amount", 0) if isinstance(price, dict) else price),
            "url":        f"https://www.vinted.fr/items/{item.get('id')}",
            "photo":      photo_url,
            "created_at": created_at,
        })
    return items, total_pages

@st.cache_data(ttl=60, show_spinner=False)
def _scrape_page_cached(query: str, page: int) -> tuple[list[dict], int]:
    try:
        resp = get_session().get(
            "https://www.vinted.fr/api/v2/catalog/items",
            params={"search_text": query, "per_page": 96, "page": page, "order": "newest_first"},
            timeout=12,
        )
        resp.raise_for_status()
        return _parse_items(resp.json())
    except Exception:
        return [], 0

def scrape_all_pages(query: str) -> list[dict]:
    """Scrape les pages en parallèle via _EXECUTOR_PAGES (pool dédié réseau).
    On sonde d'abord la page 1 pour connaître total_pages, puis on lance les suivantes."""
    MAX_PROBE = 10  # Vinted plafonne à 10 pages × 96 = 960 items max

    futures = {
        _EXECUTOR_PAGES.submit(_scrape_page_cached, query, p): p
        for p in range(1, MAX_PROBE + 1)
    }

    all_items   = []
    total_pages = 1
    p1_done     = False

    for fut in as_completed(futures):
        items, tp = fut.result()
        if not p1_done and futures[fut] == 1:
            total_pages = tp
            p1_done = True
        if items:
            all_items.extend(items)

    # Ne garder que les items des pages ≤ total_pages réel
    # (les pages au-delà retournent [] donc pas de pollution)
    return all_items


# ── Analyse ────────────────────────────────────────────────────────────────────

_CARD_REF = re.compile(r"\d+/\d+")
_PATTERNS = [
    re.compile(r"(\d+)\s*cartes?",        re.I),
    re.compile(r"lot\s+(?:de\s+)?(\d+)",  re.I),
    re.compile(r"(\d+)\s+(?:pok[eé]mon)", re.I),
    re.compile(r"(?<![a-zA-Z])x\s*(\d+)", re.I),
    re.compile(r"(\d+)\s*pcs?",           re.I),
]

def _regex(texte: str) -> int:
    t = _CARD_REF.sub("", texte)
    for pat in _PATTERNS:
        m = pat.search(t)
        if m:
            val = int(m.group(1))
            if 1 <= val <= 10000:
                return val
    return 0

_LD_RE = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.DOTALL)

@lru_cache(maxsize=2000)
def fetch_description(item_id: int) -> str:
    """Fix #6 : lecture en streaming — on s'arrête dès qu'on a le bloc ld+json
    sans télécharger les 80-150 Ko du HTML complet."""
    try:
        with get_session().get(
            f"https://www.vinted.fr/items/{item_id}",
            headers={"Accept": "text/html,application/xhtml+xml", "Sec-Fetch-Mode": "navigate"},
            timeout=10,
            stream=True,
        ) as resp:
            resp.raise_for_status()
            buf = ""
            for chunk in resp.iter_content(chunk_size=4096, decode_unicode=True):
                buf += chunk
                # Dès qu'on a le marqueur de fin du bloc JSON-LD, on arrête
                if 'application/ld+json' in buf:
                    m = _LD_RE.search(buf)
                    if m:
                        return json.loads(m.group(1)).get("description", "")
                # Sécurité : si on dépasse 30 Ko sans trouver le bloc, on abandonne
                if len(buf) > 150_000:
                    break
    except Exception:
        pass
    return ""

def _analyser_item(titre: str, id_: int, prix: float,
                   cache: dict, seuil_max: float, min_cartes: int) -> tuple[int, int]:
    if id_ in cache:
        return id_, cache[id_]

    # Fix #1 : si même avec min_cartes le prix unitaire dépasse le seuil → skip total
    if min_cartes > 0 and (prix / min_cartes) > seuil_max:
        return id_, -1  # -1 = "trop cher sans même compter les cartes"

    # Phase 1 : regex titre seul (0 I/O)
    nb = _regex(titre)
    if nb:
        return id_, nb

    # Phase 2 : scrape description + regex
    desc = fetch_description(id_)
    if desc:
        nb = _regex(titre + " " + desc)
    if nb:
        return id_, nb

    # Phase 3 : Gemini
    if _gemini_client and desc:
        try:
            r = _gemini_client.models.generate_content(
                model="gemini-2.0-flash",
                contents=(
                    "Extrait le nombre total de cartes Pokémon dans ce titre et cette description Vinted. "
                    "Réponds UNIQUEMENT avec un entier. Si impossible, réponds 0.\n"
                    f"Titre: {titre}\nDescription: {desc[:500]}"
                ),
            )
            nb = int(r.text.strip())
        except Exception:
            pass

    return id_, nb

def analyse_batch(annonces: list[dict], cache: dict,
                  seuil_max: float, min_cartes: int) -> dict[int, int]:
    a_faire = [a for a in annonces if a["id"] not in cache]
    results = {a["id"]: cache[a["id"]] for a in annonces if a["id"] in cache}
    if not a_faire:
        return results

    futures = {
        _EXECUTOR_ITEMS.submit(_analyser_item, a["titre"], a["id"], a["prix"], cache, seuil_max, min_cartes): a["id"]
        for a in a_faire
    }
    for fut in as_completed(futures):
        id_, nb      = fut.result()
        results[id_] = nb
        if nb != -1:           # ne pas cacher les "trop cher" — le seuil peut changer
            cache[id_] = nb
    return results


# ── Discord ────────────────────────────────────────────────────────────────────

def envoyer_discord(r: dict) -> bool:
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
            {"name": "🕐 Ancienneté", "value": anciennete(r["created_at"]),   "inline": True},
        ],
        "thumbnail": {"url": r["photo"]} if r.get("photo") else {},
    }
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]}, timeout=8)
        return resp.status_code in (200, 204)
    except Exception:
        return False


# ── Helpers ────────────────────────────────────────────────────────────────────

def anciennete(ts) -> str:
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

def ts_to_dt(ts) -> datetime | None:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc) if isinstance(ts, (int, float)) else None
    except Exception:
        return None


# ── Scanner un mot-clé ─────────────────────────────────────────────────────────

def scanner_query(query: str, seuil_max: float, min_cartes: int, limite: timedelta | None,
                  cache: dict, blacklist: set, deja_notifies: set,
                  mots_exclus: list = None, diag: list = None) -> list[dict]:

    maintenant = datetime.now(timezone.utc)
    all_items  = scrape_all_pages(query)

    # Pré-filtre O(N) CPU pur — utilise titre_low précalculé (fix #5)
    a_analyser, exclu_date, exclu_bl, exclu_mots, exclu_prix = [], 0, 0, 0, 0
    for a in all_items:
        if a["id"] in blacklist or a["id"] in deja_notifies:
            exclu_bl += 1; continue
        pub = ts_to_dt(a["created_at"])
        if limite and pub and (maintenant - pub) > limite:
            exclu_date += 1; continue
        if mots_exclus and any(m in a["titre_low"] for m in mots_exclus):
            exclu_mots += 1; continue
        if a["prix"] <= PRIX_MIN:
            exclu_prix += 1; continue
        # Fix #1 précoce : prix unitaire impossible même avec min_cartes
        if min_cartes > 0 and a["prix"] / min_cartes > seuil_max:
            exclu_prix += 1; continue
        a_analyser.append(a)

    # Timestamps min/max sur les annonces effectivement analysées (après filtres)
    ts_valides = [a["created_at"] for a in a_analyser if a["created_at"]]
    ts_min = min(ts_valides) if ts_valides else None
    ts_max = max(ts_valides) if ts_valides else None

    nb_map = analyse_batch(a_analyser, cache, seuil_max, min_cartes)

    nouveaux, nb0, trop_peu, trop_cher = [], 0, 0, 0
    for a in a_analyser:
        nb = nb_map.get(a["id"], 0)
        if nb <= 0:               nb0 += 1
        elif nb < min_cartes:     trop_peu += 1
        else:
            cout = a["prix"] / nb
            if cout <= seuil_max:
                nouveaux.append({**a, "nb_cartes": nb, "cout_unitaire": cout})
            else:
                trop_cher += 1

    if diag is not None:
        with _DIAG_LOCK:
            diag.append({
                "query":      query,
                "scrappees":  len(all_items),
                "analysees":  len(a_analyser),
                "exclu_bl":   exclu_bl,
                "exclu_date": exclu_date,
                "exclu_mots": exclu_mots,
                "exclu_prix": exclu_prix,
                "nb0":        nb0,
                "trop_peu":   trop_peu,
                "trop_cher":  trop_cher,
                "affaires":   len(nouveaux),
                "ts_min":     ts_min,
                "ts_max":     ts_max,
            })
    return nouveaux


PRIX_MIN = 1.0  # annonces à 1€ ou moins → ignorées dans tous les cas

# ── Catégorisation ─────────────────────────────────────────────────────────────

_CATEGORIES = {
    "📦 Lot de cartes":        ["lot", "vrac", "bulk", "collection", "tas"],
    "🏆 Cartes gradées":        ["psa", "bgs", "cgc", "ace", "gradé", "grade", "slabbed"],
    "✨ Cartes rares":          ["holo", "full art", "secret rare", "gold", "rainbow",
                                 " ex ", "ex ", " gx", "gx ", "vmax", "vstar", "v-union"],
    "🃏 Carte à l'unité":      ["kaart", "carta ", "carte ", "deutsch", "italiano", "español",
                                 "near mint", "mint condition", "nm/m", "single",
                                 " ita", " eng ", "promo", "gallery"],
    "🎁 Boosters / Display":   ["booster", "display", "étui", " eb ", " sv "],
    "📗 Classeurs":             ["classeur", "binder", "portfolio", "album"],
    "🎮 Jeux vidéo":            [" ds ", "switch", "gba", "gameboy", "jeu vidéo", "jeux video"],
    "🧸 Peluches / Figurines":  ["peluche", "figurine", "statue", "plush", "knuffel", "rugzak",
                                  "backpack", "sac à dos", "mug", "t-shirt", "t shirt", "tshirt", "poster"],
    "🗃️ Coffrets / Decks":     ["coffret", "deck", "starter", "dresseur"],
    "🎒 Goodies / Accessoires": ["pin ", "badge", "porte-monnaie", "portefeuille",
                                  "stylo", "trousse", "casquette"],
}

# Numéro de set : "095/094", "82/111", "s11a 073", "#152", "sv-p 291"
_SET_NUMBER_RE = re.compile(
    r"(?:\d{2,3}/\d{2,3}"       # 082/111
    r"|[a-z]{1,4}-?[a-z]?\s*\d{3}"  # s11a 073, sv-p 291, svp 212
    r"|#\d{2,3})"                # #51, #152
)

LOT_MIN_CARTES = 100  # en dessous → "Petits lots / singles"

def categoriser(titre_low: str, nb_cartes: int = 0) -> str:
    for cat, mots in _CATEGORIES.items():
        if any(m in titre_low for m in mots):
            if cat == "📦 Lot de cartes" and 0 < nb_cartes < LOT_MIN_CARTES:
                return "🃏 Petits lots / singles"
            return cat
    # Numéro de set détecté (ex: 015/165) → carte à l'unité
    if _SET_NUMBER_RE.search(titre_low):
        return "🃏 Carte à l'unité"
    return "❓ Autre"


# ── UI ─────────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="Pokemon Vinted Deals", page_icon="🃏", layout="wide")
    st.title("🃏 Meilleures affaires Pokémon sur Vinted")

    if not GEMINI_API_KEY:
        st.info("ℹ️ Pas de clé Gemini — extraction par regex + HTML uniquement.")

    for k, v in [("analyse_cache", {}), ("resultats", []), ("deja_notifies", set()),
                 ("veille_active", False), ("veille_log", []), ("analyse_history", []),
                 ("veille_runs", [])]:
        if k not in st.session_state:
            st.session_state[k] = v

    # Fix #4 : une seule lecture disque par rerun
    blacklist        = load_blacklist()
    mots_exclus_perm = load_mots_exclus_permanents()

    with st.sidebar:
        st.header("🔍 Paramètres")
        mots_cles_txt   = st.text_area("Mots-clés (un par ligne)", value=MOTS_CLES_DEFAUT, height=100)
        queries         = [q.strip() for q in mots_cles_txt.splitlines() if q.strip()]
        mots_exclus_txt = st.text_input("Mots exclus du titre (virgule)", value="japonaise,lotto,one piece,pyjama,japanese,giapponese,assassin,cartas,brinquedos")
        # Fusion sidebar + permanents (dédoublonnés)
        mots_exclus = list({m.strip().lower() for m in mots_exclus_txt.split(",") if m.strip()} | set(mots_exclus_perm))
        if mots_exclus_perm:
            st.caption(f"🚫 Permanents : {', '.join(mots_exclus_perm)}")
        seuil_max       = st.slider("Seuil max €/carte", 0.01, 0.50, 0.04, 0.01, format="%.2f€")
        min_cartes      = st.slider("Cartes minimum", 10, 4000, 300, 10)
        filtre_date     = st.selectbox("Ancienneté max", list(LIMITES), index=1)
        nb_resultats    = st.slider("Résultats max", 1, 20, 5)

        st.divider()
        st.subheader("⏰ Mode veille")
        intervalle = st.select_slider("Intervalle", options=[1, 2, 5, 10, 15, 30], value=5, format_func=lambda x: f"{x} min")
        veille_on  = st.toggle("Activer la veille", value=st.session_state["veille_active"])
        if veille_on != st.session_state["veille_active"]:
            st.session_state["veille_active"] = veille_on
            if veille_on:
                st.session_state["veille_log"] = []
            st.rerun()

        st.divider()
        nb_cache = len(st.session_state["analyse_cache"])
        nb_desc  = fetch_description.cache_info().currsize
        st.caption(f"🚫 {len(blacklist)} masquée(s) · 🧠 {nb_cache} titres · 📄 {nb_desc} descriptions · 📨 {len(st.session_state['deja_notifies'])} notifiée(s)")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ Blacklist", use_container_width=True):
                save_blacklist(set())
                st.rerun()
        with col2:
            if st.button("🔄 Reset", use_container_width=True):
                st.cache_data.clear()
                fetch_description.cache_clear()
                for k in ["analyse_cache", "resultats", "deja_notifies", "veille_log"]:
                    st.session_state[k] = {} if k == "analyse_cache" else []
                st.rerun()

        lancer = st.button("🚀 Lancer la recherche", use_container_width=True, type="primary")

    tab1, tab2 = st.tabs(["🔍 Recherche & Veille", "📊 Analyse du marché"])

    # ── Onglet Analyse ─────────────────────────────────────────────────────────
    with tab2:
        st.info("⏸️ Analyse du marché désactivée temporairement (optimisation en cours).")
        # @st.fragment(run_every=60)
        def _onglet_analyse():
            with st.spinner("Chargement des 100 dernières annonces…"):
                items = scrape_all_pages("pokemon")[:100]

            if not items:
                st.warning("Impossible de charger les annonces.")
                return

            # Appliquer les mots exclus et la règle prix > 1€
            items = [a for a in items if a["prix"] > PRIX_MIN]
            if mots_exclus:
                items = [a for a in items if not any(m in a["titre_low"] for m in mots_exclus)]

            # Catégorisation avec nb cartes pour distinguer lots/singles
            for a in items:
                nb = _regex(a["titre"])
                a["nb_cartes_detect"] = nb
                a["categorie"] = categoriser(a["titre_low"], nb)

            # Comptage par catégorie
            from collections import Counter
            counts = Counter(a["categorie"] for a in items)

            # Prix moyen/carte pour les vrais lots (≥100 cartes)
            lots = []
            for a in items:
                if a["categorie"] == "📦 Lot de cartes" and a["nb_cartes_detect"] > 0:
                    lots.append(round(a["prix"] / a["nb_cartes_detect"], 4))

            prix_moyen = round(sum(lots) / len(lots), 4) if lots else None

            # Sauvegarde snapshot historique
            history = st.session_state["analyse_history"]
            history.append({
                "ts":          datetime.now().strftime("%H:%M:%S"),
                "prix_moyen":  prix_moyen,
                "nb_lots":     len(lots),
                "total":       len(items),
            })
            st.session_state["analyse_history"] = history[-30:]  # 30 points max

            # ── Affichage ─────────────────────────────────────────────────────
            st.markdown(f"### 📊 {len(items)} annonces analysées · mis à jour à {datetime.now().strftime('%H:%M:%S')}")

            # Métriques
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📦 Lots détectés", len(lots))
            with col2:
                if prix_moyen:
                    prev = history[-2]["prix_moyen"] if len(history) >= 2 and history[-2]["prix_moyen"] else None
                    delta = round(prix_moyen - prev, 4) if prev else None
                    st.metric("📉 Prix moyen/carte (lots)", f"{prix_moyen:.4f} €",
                              delta=f"{delta:+.4f} €" if delta is not None else None,
                              delta_color="inverse")
                else:
                    st.metric("📉 Prix moyen/carte (lots)", "—")
            with col3:
                st.metric("🏷️ Catégories détectées", len(counts))

            st.divider()

            # Répartition par catégorie
            col_cat, col_evo = st.columns([1, 1])

            with col_cat:
                st.markdown("#### 📂 Répartition par catégorie")
                total = sum(counts.values())
                for cat, n in sorted(counts.items(), key=lambda x: -x[1]):
                    pct = n / total * 100
                    st.markdown(f"**{cat}** — {n} annonces ({pct:.0f}%)")
                    st.progress(pct / 100)

            with col_evo:
                st.markdown("#### 📈 Évolution prix/carte (lots)")
                if len(history) >= 2:
                    valid = [(h["ts"], h["prix_moyen"]) for h in history if h["prix_moyen"] is not None]
                    if len(valid) >= 2:
                        import pandas as pd
                        df = pd.DataFrame(valid, columns=["Heure", "€/carte"])
                        df = df.set_index("Heure")
                        st.line_chart(df)
                    else:
                        st.info("En attente de données… (1 min)")
                else:
                    st.info("En attente du 2ème scan… (1 min)")

            st.divider()

            # Liste des lots
            st.markdown("#### 📦 Détail des lots (triés par €/carte)")
            lots_avec_nb, lots_sans_nb = [], []
            for a in items:
                if a["categorie"] == "📦 Lot de cartes":
                    nb = a["nb_cartes_detect"]
                    if nb >= LOT_MIN_CARTES:
                        lots_avec_nb.append((a, nb, round(a["prix"] / nb, 4)))
                    else:
                        lots_sans_nb.append(a)
            lots_avec_nb.sort(key=lambda x: x[2])

            if lots_avec_nb:
                for a, nb, pu in lots_avec_nb[:15]:
                    st.markdown(
                        f"[{a['titre'][:70]}]({a['url']}) — "
                        f"💰 {a['prix']:.2f}€ · 🃏 {nb} cartes · 📉 **{pu:.4f}€/carte**"
                    )
            else:
                st.info("Aucun lot avec nombre de cartes ≥100 détecté dans le titre.")

            if lots_sans_nb:
                # Tentative de récupération depuis la description
                futures_desc = {
                    _EXECUTOR_PAGES.submit(fetch_description, a["id"]): a
                    for a in lots_sans_nb
                }
                recuperes = []
                toujours_inconnus = []
                for fut in as_completed(futures_desc):
                    a    = futures_desc[fut]
                    desc = fut.result()
                    nb   = _regex(a["titre"] + " " + desc) if desc else 0
                    if nb >= LOT_MIN_CARTES:
                        recuperes.append((a, nb, round(a["prix"] / nb, 4)))
                    else:
                        toujours_inconnus.append((a, desc))

                if recuperes:
                    recuperes.sort(key=lambda x: x[2])
                    st.markdown("**Lots récupérés via description :**")
                    for a, nb, pu in recuperes:
                        lots_avec_nb.append((a, nb, round(a["prix"] / nb, 4)))
                        st.markdown(
                            f"[{a['titre'][:70]}]({a['url']}) — "
                            f"💰 {a['prix']:.2f}€ · 🃏 {nb} cartes · 📉 **{pu:.4f}€/carte**"
                        )

                if toujours_inconnus:
                    with st.expander(f"⚠️ {len(toujours_inconnus)} lot(s) sans nb de cartes (titre + description)"):
                        for a, desc in toujours_inconnus:
                            st.markdown(f"**[{a['titre'][:80]}]({a['url']})** — 💰 {a['prix']:.2f}€")
                            if desc:
                                st.caption(desc[:200])

            st.divider()

            # Échantillon "Autre" pour affinage des catégories
            st.markdown("#### ❓ Échantillon — catégorie Autre (pour affinage)")
            autres = [a for a in items if a["categorie"] == "❓ Autre"][:5]
            if autres:
                for a in autres:
                    st.markdown(f"- [{a['titre']}]({a['url']}) — 💰 {a['prix']:.2f}€")

            st.caption(f"Prochain rafraîchissement dans 1 min")

        _onglet_analyse()

    # ── Onglet Recherche & Veille ──────────────────────────────────────────────
    with tab1:
        if st.session_state["veille_active"]:
            col_status, col_stop = st.columns([4, 1])
            with col_status:
                st.success(f"🟢 Veille active — scan toutes les {intervalle} min sur {len(queries)} mot(s)-clé(s)")
            with col_stop:
                if st.button("⏹️ Arrêter", type="primary", use_container_width=True):
                    st.session_state["veille_active"] = False
                    st.rerun()

            @st.fragment(run_every=intervalle * 60)
            def _veille_fragment():
                cache         = st.session_state["analyse_cache"]
                deja_notifies = st.session_state["deja_notifies"]
                limite        = LIMITES[filtre_date]
                logs          = st.session_state["veille_log"]
                runs          = st.session_state["veille_runs"]
                diag_run      = []
                affaires_run  = 0
                heure_run     = datetime.now().strftime("%H:%M:%S")

                with st.spinner("Scan en cours…"):
                    futures_veille = {
                        _EXECUTOR_QUERIES.submit(scanner_query, q, seuil_max, min_cartes, limite,
                                                 cache, blacklist, deja_notifies, mots_exclus, diag_run): q
                        for q in queries
                    }
                    for fut in as_completed(futures_veille):
                        for r in fut.result():
                            with _NOTIFY_LOCK:
                                if r["id"] in deja_notifies:
                                    continue
                                deja_notifies.add(r["id"])
                            ok = envoyer_discord(r)
                            if ok:
                                bl = load_blacklist(); bl.add(r["id"]); save_blacklist(bl)
                                affaires_run += 1
                                logs.insert(0, f"✅ {datetime.now().strftime('%H:%M:%S')} — **{r['titre'][:50]}** ({r['nb_cartes']} cartes, {r['cout_unitaire']:.3f}€/c)")
                            else:
                                with _NOTIFY_LOCK:
                                    deja_notifies.discard(r["id"])

                # Résumé du run
                total_scrap = sum(d["scrappees"] for d in diag_run)
                total_anal  = sum(d["analysees"] for d in diag_run)
                run_entry   = {
                    "heure":    heure_run,
                    "scrap":    total_scrap,
                    "anal":     total_anal,
                    "affaires": affaires_run,
                }
                runs.insert(0, run_entry)
                st.session_state["veille_runs"] = runs[:20]  # 20 runs max
                st.session_state["veille_log"]  = logs[:50]

                # ── Métriques du dernier run ───────────────────────────────────
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("🕐 Dernier scan", heure_run)
                c2.metric("📡 Scrappées",    total_scrap)
                c3.metric("🔍 Analysées",    total_anal)
                c4.metric("✅ Affaires",     affaires_run)

                # ── Historique des runs ────────────────────────────────────────
                if len(runs) > 1:
                    with st.expander(f"📈 Historique des {len(runs)} derniers runs", expanded=False):
                        header = "| Heure | Scrappées | Analysées | Affaires |"
                        sep    = "|---|---:|---:|---:|"
                        rows   = [
                            f"| `{run['heure']}` | {run['scrap']} | {run['anal']} "
                            f"| {'✅ ' + str(run['affaires']) if run['affaires'] else '—'} |"
                            for run in runs
                        ]
                        st.markdown("\n".join([header, sep] + rows))

                st.divider()
                st.markdown("### 📋 Journal de veille")
                for l in (logs[:10] if logs else ["_Aucune affaire trouvée pour l'instant…_"]):
                    st.markdown(l)
                st.caption(f"Prochain scan dans {intervalle} min")

            _veille_fragment()

        else:
            # ── Recherche manuelle ─────────────────────────────────────────────
            if lancer:
                if not queries:
                    st.warning("Ajoutez au moins un mot-clé.")
                else:
                    cache  = st.session_state["analyse_cache"]
                    limite = LIMITES[filtre_date]
                    diag   = []

                    with st.spinner(f"Scan de {len(queries)} mot(s)-clé(s) en parallèle…"):
                        futures_search = {
                            _EXECUTOR_QUERIES.submit(scanner_query, q, seuil_max, min_cartes, limite,
                                                     cache, blacklist, set(), mots_exclus, diag): q
                            for q in queries
                        }
                        resultats = []
                        for fut in as_completed(futures_search):
                            for r in fut.result():
                                if not any(x["id"] == r["id"] for x in resultats):
                                    resultats.append(r)

                    st.session_state["resultats"] = resultats
                    st.session_state["diag"]      = diag

            # ── Diagnostic ────────────────────────────────────────────────────
            if "diag" in st.session_state and st.session_state["diag"]:
                diag_data = st.session_state["diag"]
                with st.expander("🔬 Dernière analyse — détail par mot-clé", expanded=True):
                    def _fmt_ts(ts):
                        if ts is None: return "—"
                        from zoneinfo import ZoneInfo
                        return datetime.fromtimestamp(ts, tz=ZoneInfo("Europe/Paris")).strftime("%H:%M")

                    header = "| Mot-clé | Scrappées | Analysées | Affaires | 🕐 Analysée la + ancienne | 🕐 Analysée la + récente |"
                    sep    = "|---|---:|---:|---:|---:|---:|"
                    rows   = [
                        f"| `{d['query']}` | {d['scrappees']} | {d['analysees']} "
                        f"| {'✅ ' + str(d['affaires']) if d['affaires'] else '—'} "
                        f"| {_fmt_ts(d.get('ts_min'))} | {_fmt_ts(d.get('ts_max'))} |"
                        for d in diag_data
                    ]
                    total_s = sum(d["scrappees"] for d in diag_data)
                    total_a = sum(d["analysees"] for d in diag_data)
                    total_f = sum(d["affaires"]  for d in diag_data)
                    rows.append(f"| **TOTAL** | **{total_s}** | **{total_a}** | **{'✅ ' + str(total_f) if total_f else '—'}** | | |")
                    st.markdown("\n".join([header, sep] + rows))
                    st.caption("Heures en heure française (Europe/Paris)")
                    with st.expander("détail des exclusions"):
                        for d in diag_data:
                            st.markdown(
                                f"**`{d['query']}`** — exclus : "
                                f"blacklist={d['exclu_bl']} · date={d['exclu_date']} · "
                                f"mots={d['exclu_mots']} · prix={d['exclu_prix']} | "
                                f"nb inconnu={d['nb0']} · trop peu={d['trop_peu']} · trop cher={d['trop_cher']}"
                            )

            # ── Résultats ──────────────────────────────────────────────────────
            if not st.session_state["resultats"]:
                st.info("Configurez vos filtres et cliquez sur **🚀 Lancer la recherche**, ou activez la **veille automatique**.")
            else:
                resultats = [r for r in st.session_state["resultats"] if r["id"] not in blacklist]
                resultats.sort(key=lambda x: x["created_at"] or 0, reverse=True)

                st.markdown(f"### 📊 {len(resultats)} bonne(s) affaire(s) — plus récentes en premier")
                if not resultats:
                    st.warning("Aucune annonce ne correspond aux critères.")
                else:
                    for r in resultats:
                        col_photo, col_info, col_action = st.columns([1, 4, 1])
                        with col_photo:
                            if r["photo"]:
                                st.image(r["photo"], width=120)
                        with col_info:
                            st.markdown(
                                f"""<div style="border:2px solid #28a745;background:#f0fff4;border-radius:8px;padding:12px;">
                                    <span style="color:#888;font-size:.85em">🕐 {anciennete(r['created_at'])}</span><br>
                                    <b>{r['titre']}</b><br>
                                    💰 <b>{r['prix']:.2f} €</b> &nbsp;|&nbsp;
                                    🃏 <b>{r['nb_cartes']} cartes</b> &nbsp;|&nbsp;
                                    📉 <b>{r['cout_unitaire']:.3f} €/carte</b><br>
                                    <a href="{r['url']}" target="_blank">🔗 Voir l'annonce</a>
                                </div>""",
                                unsafe_allow_html=True,
                            )
                        with col_action:
                            st.markdown("<br>", unsafe_allow_html=True)
                            if st.button("📨 Discord", key=f"discord_{r['id']}", use_container_width=True):
                                ok = envoyer_discord(r)
                                if ok:
                                    bl = load_blacklist(); bl.add(r["id"]); save_blacklist(bl)
                                    st.session_state["deja_notifies"].add(r["id"])
                                    st.toast("✅ Envoyé et masqué !")
                                    st.rerun()
                                else:
                                    st.toast("❌ Échec de l'envoi.")
                            if st.button("🚫 Masquer", key=f"hide_{r['id']}", use_container_width=True):
                                bl = load_blacklist(); bl.add(r["id"]); save_blacklist(bl)
                                st.rerun()
                            mot_key = f"mot_exclu_{r['id']}"
                            mot = st.text_input("", placeholder="mot à exclure", key=mot_key,
                                                label_visibility="collapsed")
                            if st.button("➕ Exclure", key=f"exclu_{r['id']}", use_container_width=True):
                                mot_clean = mot.strip().lower()
                                if mot_clean:
                                    perm = load_mots_exclus_permanents()
                                    if mot_clean not in perm:
                                        perm.append(mot_clean)
                                        save_mots_exclus_permanents(perm)
                                    st.toast(f"✅ « {mot_clean} » ajouté aux exclusions permanentes")
                                    st.rerun()
                        st.divider()


if __name__ == "__main__":
    main()
