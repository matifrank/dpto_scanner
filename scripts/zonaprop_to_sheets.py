import os, re, json, time, hashlib
from datetime import datetime, timezone
import requests
from xml.etree import ElementTree as ET
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials


SITEMAP_INDEX = "https://www.zonaprop.com.ar/sitemaps_https.xml"
UA = os.getenv("UA", "Mozilla/5.0 (property-tracker/1.0)")
HEADERS = {"User-Agent": UA}

SHEET_ID = os.getenv("SHEET_ID")
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

# Sheets tabs (create if missing)
TAB_MASTER = "MASTER"
TAB_REVISAR = "REVISAR"
TAB_LOG = "LOG"


def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch(url, timeout=25):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_sitemap(xml_text):
    root = ET.fromstring(xml_text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    if root.tag.endswith("sitemapindex"):
        return "index", [sm.find("sm:loc", ns).text.strip() for sm in root.findall("sm:sitemap", ns)]
    if root.tag.endswith("urlset"):
        return "urlset", [u.find("sm:loc", ns).text.strip() for u in root.findall("sm:url", ns)]
    return "unknown", []


def get_urls_from_sitemaps(limit_sitemaps=8):
    idx = fetch(SITEMAP_INDEX)
    kind, sitemaps = parse_sitemap(idx)
    if kind != "index":
        raise RuntimeError("Sitemap index inesperado")

    candidates = [s for s in sitemaps if "sitemap" in s.lower()]
    urls = set()

    for sm in candidates[:limit_sitemaps]:
        xml = fetch(sm)
        k, u = parse_sitemap(xml)
        if k == "urlset":
            urls.update(u)
        time.sleep(1.0)

    return sorted(urls)


def normalize_url(url: str) -> str:
    # removes tracking params if any (best effort)
    return url.split("?")[0].strip()


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


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

    # barrio/localidad: buscamos tokens
    barrio = ""
    for z in ZONAS_OK:
        if z in text:
            barrio = z
            break

    # tipo (heurística)
    tipo = "PH" if " ph " in f" {text} " else "Depto"

    # balcón/patio (heurística)
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
    “Rigurosamente” para enviar a MASTER:
    - barrio detectado y dentro de tu lista
    - ambientes conocido y >= 2
    - precio conocido y <= 121k
    - expensas: si está, <=120k; si NO está -> REVISAR
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


def connect_sheet():
    sa_json = os.getenv("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Falta secret GCP_SA_JSON")

    info = json.loads(sa_json)
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
            ws = sh.add_worksheet(title=title, rows=2000, cols=len(headers)+5)
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
    # assumes header in row 1, url in col 1
    vals = ws.col_values(1)
    return set(v.strip() for v in vals[1:] if v.strip())


def upsert_master(ws, url, d, ts):
    # naive approach: find row by url (small-scale ok)
    # For big scale, cache url->row.
    urls = ws.col_values(1)
    try:
        idx = urls.index(url)
        row = idx + 1
        # update last_seen + current price/exp, update min/max
        current_price = d["price_usd"]
        current_exp = d["expensas_ars"]

        price_min_cell = ws.cell(row, 14).value  # price_min
        price_max_cell = ws.cell(row, 15).value
        exp_min_cell = ws.cell(row, 16).value

        def to_int(x):
            try:
                return int(str(x).strip())
            except:
                return None

        pmin = to_int(price_min_cell)
        pmax = to_int(price_max_cell)
        emin = to_int(exp_min_cell)

        pmin = current_price if pmin is None else min(pmin, current_price)
        pmax = current_price if pmax is None else max(pmax, current_price)
        emin = current_exp if emin is None else min(emin, current_exp)

        ws.update(f"L{row}:P{row}", [[ts, "Activo", pmin, pmax, emin]])
        ws.update(f"F{row}:G{row}", [[current_price, current_exp]])
        return "UPDATED"
    except ValueError:
        # insert
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
        ws.update(f"L{row}:L{row}", [[ts]])
        ws.update(f"J{row}:J{row}", [[reason]])
        return "UPDATED"
    except ValueError:
        ws.append_row([
            url, "Zonaprop", d["barrio"], d["tipo"], d["ambientes"], d["price_usd"], d["expensas_ars"],
            d["balcon_patio"], d["title"], reason, ts, ts
        ])
        return "NEW"


def telegram_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    requests.post(url, json=payload, timeout=20)


def main():
    ts = now_utc_iso()
    ws_master, ws_revisar, ws_log = connect_sheet()

    existing_master = load_existing_urls(ws_master)
    existing_revisar = load_existing_urls(ws_revisar)

    all_urls = get_urls_from_sitemaps()
    # nuevas = URLs que no estén ni en master ni en revisar
    new_urls = []
    for u in all_urls:
        u0 = normalize_url(u)
        if u0 not in existing_master and u0 not in existing_revisar:
            new_urls.append(u0)
        if len(new_urls) >= MAX_NEW_URLS_PER_RUN:
            break

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
                    telegram_items.append(f"- {d['barrio'].title()} | {d['tipo']} | {d['ambientes']} amb | USD {d['price_usd']} | Exp {d['expensas_ars']} | {u}")
            else:
                # REVISAR
                upsert_revisar(ws_revisar, u, d, ts, reason)
                added_revisar += 1

            time.sleep(SLEEP_SEC)
        except Exception:
            errors += 1
            time.sleep(SLEEP_SEC)

    ws_log.append_row([ts, len(new_urls), added_master, added_revisar, errors])

    # Telegram summary
    if telegram_items:
        msg = "🏠 Nuevas oportunidades (Zonaprop) — filtros OK\n" + "\n".join(telegram_items[:10])
        if len(telegram_items) > 10:
            msg += f"\n… y {len(telegram_items)-10} más en el Sheet."
        telegram_send(msg)
    else:
        telegram_send("📭 Zonaprop — sin nuevas oportunidades que cumplan filtros OK en esta corrida.")

    print(f"Checked new: {len(new_urls)} | master+ {added_master} | revisar+ {added_revisar} | errors {errors}")


if __name__ == "__main__":
    if not SHEET_ID:
        raise RuntimeError("Falta SHEET_ID env")
    main()
