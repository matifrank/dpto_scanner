# scripts/zonaprop_to_sheets.py
import os
import re
import json
import time
import random
import hashlib
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Config
# =========================
SHEET_ID = os.getenv("SHEET_ID")  # your sheet id
SITEMAP_INDEX = "https://www.zonaprop.com.ar/sitemaps_https.xml"
ROBOTS_URL = "https://www.zonaprop.com.ar/robots.txt"

MAX_USD = int(os.getenv("MAX_USD", "121000"))
MAX_EXP = int(os.getenv("MAX_EXP", "120000"))
MIN_AMB = int(os.getenv("MIN_AMB", "2"))
MAX_NEW_URLS_PER_RUN = int(os.getenv("MAX_NEW_URLS_PER_RUN", "120"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "1.2"))

ZONAS_OK = {z.strip().lower() for z in os.getenv(
    "ZONAS_OK",
    "olivos,villa urquiza,coghlan,colegiales,belgrano,vicente lopez,vicente lópez"
).split(",")}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TAB_MASTER = "MASTER"
TAB_REVISAR = "REVISAR"
TAB_LOG = "LOG"


# =========================
# Camouflage HTTP
# =========================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    "Referer": "https://www.zonaprop.com.ar/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch(url: str, timeout: int = 25) -> str:
    """
    Best-effort fetch with:
    - session cookies
    - retries
    - exponential-ish backoff + jitter
    """
    last = None
    for attempt in range(1, 7):
        try:
            r = SESSION.get(url, timeout=timeout)
            # debug line in logs (helps)
            print(f"GET {url} -> {r.status_code}")

            if r.status_code in (403, 429):
                # backoff + jitter
                sleep = (5 * attempt) + random.uniform(0.2, 1.8)
                time.sleep(sleep)
                last = f"HTTP {r.status_code}"
                continue

            r.raise_for_status()
            return r.text
        except Exception as e:
            last = repr(e)
            time.sleep(2 * attempt + random.uniform(0.2, 1.0))

    raise RuntimeError(f"Fetch failed for {url}. Last={last}")


def warmup():
    """
    Warm up session by hitting robots + homepage (sometimes helps cookies/WAF).
    """
    try:
        _ = fetch(ROBOTS_URL)
        time.sleep(1.0)
    except Exception as e:
        print("Warmup robots failed:", e)

    try:
        _ = fetch("https://www.zonaprop.com.ar/")
        time.sleep(1.0)
    except Exception as e:
        print("Warmup homepage failed:", e)


# =========================
# Sitemap discovery
# =========================
def parse_sitemap(xml_text: str):
    root = ET.fromstring(xml_text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    if root.tag.endswith("sitemapindex"):
        locs = []
        for sm in root.findall("sm:sitemap", ns):
            loc = sm.find("sm:loc", ns).text.strip()
            locs.append(loc)
        return "index", locs

    if root.tag.endswith("urlset"):
        urls = []
        for u in root.findall("sm:url", ns):
            loc = u.find("sm:loc", ns).text.strip()
            urls.append(loc)
        return "urlset", urls

    return "unknown", []


def sitemaps_from_robots(robots_text: str):
    # lines like: Sitemap: https://...
    sm = []
    for line in robots_text.splitlines():
        if line.lower().startswith("sitemap:"):
            sm.append(line.split(":", 1)[1].strip())
    return sm


def get_urls_from_sitemaps(limit_sitemaps: int = 8):
    """
    Try:
      1) sitemap index
      2) fallback: robots.txt sitemaps
    """
    # Primary
    try:
        idx = fetch(SITEMAP_INDEX)
        kind, sitemaps = parse_sitemap(idx)
        if kind == "index" and sitemaps:
            candidates = [s for s in sitemaps if "sitemap" in s.lower()]
            return collect_urls_from_sitemaps(candidates[:limit_sitemaps])
    except Exception as e:
        print("SITEMAP_INDEX failed:", e)

    # Fallback: robots.txt
    robots = fetch(ROBOTS_URL)
    sms = sitemaps_from_robots(robots)
    if not sms:
        raise RuntimeError("No sitemaps found in robots.txt fallback")

    return collect_urls_from_sitemaps(sms[:limit_sitemaps])


def collect_urls_from_sitemaps(sitemap_urls):
    urls = set()
    for sm_url in sitemap_urls:
        try:
            xml = fetch(sm_url)
            kind, u = parse_sitemap(xml)
            if kind == "urlset":
                urls.update(u)
        except Exception as e:
            print("Sitemap fetch failed:", sm_url, e)
        time.sleep(1.0)
    return sorted(urls)


def normalize_url(url: str) -> str:
    # strip query params
    return url.split("?")[0].strip()


# =========================
# Parsing listing
# =========================
def extract_metrics(html: str):
    soup = BeautifulSoup(html, "lxml")
    title = (soup.title.text.strip() if soup.title else "")[:180]
    text = soup.get_text(" ", strip=True).lower()

    # ambientes
    amb = None
    m = re.search(r"(\d+)\s+ambiente", text)
    if m:
        try:
            amb = int(m.group(1))
        except:
            pass

    # expensas
    exp = None
    m = re.search(r"expensas\s*\$?\s*([\d\.\,]+)", text)
    if m:
        s = m.group(1).replace(".", "").replace(",", "")
        if s.isdigit():
            exp = int(s)

    # precio USD
    price = None
    m = re.search(r"u\$s\s*([\d\.\,]+)", text) or re.search(r"usd\s*([\d\.\,]+)", text)
    if m:
        s = m.group(1).replace(".", "").replace(",", "")
        if s.isdigit():
            price = int(s)

    # barrio/localidad: tokens
    barrio = ""
    for z in ZONAS_OK:
        if z in text:
            barrio = z
            break

    # tipo
    tipo = "PH" if " ph " in f" {text} " else "Depto"

    # balcón/patio/terraza
    balcon_patio = "S" if ("balcón" in text or "balcon" in text or "patio" in text or "terraza" in text) else "N"

    return {
        "title": title,
        "ambientes": amb,
        "price_usd": price,
        "expensas_ars": exp,
        "barrio": barrio,
        "tipo": tipo,
        "balcon_patio": balcon_patio,
    }


def strict_ok(d):
    """
    STRICT to MASTER:
    - barrio detectado
    - amb >= 2 (conocido)
    - price <= 121k (conocido)
    - expensas: si falta => REVISAR (como pediste)
    """
    if not d["barrio"]:
        return False, "Falta barrio"
    if d["ambientes"] is None or d["ambientes"] < MIN_AMB:
        return False, "Ambientes no cumple / falta"
    if d["price_usd"] is None or d["price_usd"] > MAX_USD:
        return False, "Precio no cumple / falta"
    if d["expensas_ars"] is None:
        return False, "Faltan expensas (REVISAR)"
    if d["expensas_ars"] > MAX_EXP:
        return False, "Expensas > tope"
    return True, "OK"


# =========================
# Google Sheets
# =========================
def connect_sheet():
    sa_json = os.getenv("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Missing secret GCP_SA_JSON")

    info = json.loads(sa_json)
    print("Using service account:", info.get("client_email"))
    print("Target sheet id:", SHEET_ID)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    def get_or_create(title, headers):
        try:
            ws = sh.worksheet(title)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=title, rows=2000, cols=len(headers) + 5)
            ws.append_row(headers)
        return ws

    master_headers = [
        "url", "portal", "barrio", "tipo", "ambientes", "price_usd", "expensas_ars",
        "balcon_patio", "apto_credito", "title", "first_seen", "last_seen",
        "status", "price_min", "price_max", "exp_min"
    ]
    revisar_headers = [
        "url", "portal", "barrio_detectado", "tipo", "ambientes", "price_usd", "expensas_ars",
        "balcon_patio", "title", "reason", "first_seen", "last_seen"
    ]
    log_headers = ["ts_utc", "new_urls_checked", "added_master", "added_revisar", "errors"]

    ws_master = get_or_create(TAB_MASTER, master_headers)
    ws_revisar = get_or_create(TAB_REVISAR, revisar_headers)
    ws_log = get_or_create(TAB_LOG, log_headers)
    return ws_master, ws_revisar, ws_log


def load_existing_urls(ws):
    vals = ws.col_values(1)  # url column
    return set(v.strip() for v in vals[1:] if v.strip())


def upsert_master(ws, url, d, ts):
    urls = ws.col_values(1)
    try:
        idx = urls.index(url)
        row = idx + 1

        current_price = d["price_usd"]
        current_exp = d["expensas_ars"]

        def to_int(x):
            try:
                return int(str(x).strip())
            except:
                return None

        pmin = to_int(ws.cell(row, 14).value)
        pmax = to_int(ws.cell(row, 15).value)
        emin = to_int(ws.cell(row, 16).value)

        pmin = current_price if pmin is None else min(pmin, current_price)
        pmax = current_price if pmax is None else max(pmax, current_price)
        emin = current_exp if emin is None else min(emin, current_exp)

        ws.update(f"F{row}:G{row}", [[current_price, current_exp]])
        ws.update(f"L{row}:P{row}", [[ts, "Activo", pmin, pmax, emin]])
        return "UPDATED"
    except ValueError:
        ws.append_row([
            url, "Zonaprop", d["barrio"], d["tipo"], d["ambientes"], d["price_usd"], d["expensas_ars"],
            d["balcon_patio"], "Validar", d["title"],
            ts, ts, "Activo",
            d["price_usd"], d["price_usd"], d["expensas_ars"]
        ])
        return "NEW"


def upsert_revisar(ws, url, d, ts, reason):
    urls = ws.col_values(1)
    try:
        idx = urls.index(url)
        row = idx + 1
        ws.update(f"L{row}:L{row}", [[ts]])   # last_seen
        ws.update(f"J{row}:J{row}", [[reason]])
        return "UPDATED"
    except ValueError:
        ws.append_row([
            url, "Zonaprop", d["barrio"], d["tipo"], d["ambientes"], d["price_usd"], d["expensas_ars"],
            d["balcon_patio"], d["title"], reason, ts, ts
        ])
        return "NEW"


# =========================
# Telegram
# =========================
def telegram_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured (missing secrets).")
        return
    api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        r = requests.post(api, json=payload, timeout=20)
        print("Telegram status:", r.status_code)
    except Exception as e:
        print("Telegram send failed:", e)


# =========================
# Main
# =========================
def main():
    if not SHEET_ID:
        raise RuntimeError("Missing SHEET_ID env")

    ts = now_utc_iso()

    warmup()

    ws_master, ws_revisar, ws_log = connect_sheet()
    existing_master = load_existing_urls(ws_master)
    existing_revisar = load_existing_urls(ws_revisar)

    # Get candidate URLs
    all_urls = get_urls_from_sitemaps()
    print("Total URLs from sitemap sample:", len(all_urls))

    # Only URLs not already present
    new_urls = []
    for u in all_urls:
        u0 = normalize_url(u)
        if u0 not in existing_master and u0 not in existing_revisar:
            new_urls.append(u0)
        if len(new_urls) >= MAX_NEW_URLS_PER_RUN:
            break

    print("New URLs to check:", len(new_urls))

    added_master = 0
    added_revisar = 0
    errors = 0
    telegram_items = []

    for u in new_urls:
        try:
            html = fetch(u)
            d = extract_metrics(html)
            ok, reason = strict_ok(d)

            if ok:
                res = upsert_master(ws_master, u, d, ts)
                if res == "NEW":
                    added_master += 1
                    telegram_items.append(
                        f"- {d['barrio'].title()} | {d['tipo']} | {d['ambientes']} amb | "
                        f"USD {d['price_usd']} | Exp {d['expensas_ars']} | {u}"
                    )
            else:
                upsert_revisar(ws_revisar, u, d, ts, reason)
                added_revisar += 1

            time.sleep(SLEEP_SEC)
        except Exception as e:
            errors += 1
            print("Error on URL:", u, e)
            time.sleep(SLEEP_SEC)

    ws_log.append_row([ts, len(new_urls), added_master, added_revisar, errors])

    # Telegram
    if telegram_items:
        msg = "🏠 Nuevas oportunidades (Zonaprop) — OK filtros\n" + "\n".join(telegram_items[:10])
        if len(telegram_items) > 10:
            msg += f"\n… y {len(telegram_items) - 10} más en el Sheet."
        telegram_send(msg)
    else:
        telegram_send("📭 Zonaprop — sin nuevas oportunidades OK en esta corrida.")

    print(f"Done. checked={len(new_urls)} master+={added_master} revisar+={added_revisar} errors={errors}")


if __name__ == "__main__":
    main()
