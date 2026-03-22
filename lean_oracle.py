"""
lean_oracle.py — Andy Solty Oracle Suite  (v3 — Service Account + Eastern Time)

Combines:
  • Market Oracle  — Dynamic Universe from Google Sheets "Solty 100 List"
                     Liquidity Gate · Trend Anchor · Manifesto Score · Zombie Penalty
                     Tiered Badging (Elite / Strong / Moderate / Speculative)
                     Batched Twelve Data API calls (groups of 20)
  • Becht Family Legacy — Historic Archive · Heritage History · Trip Logistics & Itinerary

Run:  streamlit run lean_oracle.py

Secrets required in .streamlit/secrets.toml:
  TWELVE_DATA_API_KEY = "..."

  [gcp_service_account]
  type = "service_account"
  project_id = "..."
  private_key_id = "..."
  private_key = "-----BEGIN RSA PRIVATE KEY-----\\n...\\n-----END RSA PRIVATE KEY-----\\n"
  client_email = "...@....iam.gserviceaccount.com"
  client_id = "..."
  auth_uri = "https://accounts.google.com/o/oauth2/auth"
  token_uri = "https://oauth2.googleapis.com/token"
  auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
  client_x509_cert_url = "..."

Sheet layout for "Solty 100 List" (Sheet ID: 1fWKc0DmaaTPWOJQ2Jp3yP5KaCoqrjOjrHAA7RaZNfGo):
  Col A  — Ticker
  Col C  — Market Scope   (global / north_american / domestic)
  Col D  — Sub-Sector     (descriptive string)
  Col N  — Zombie Penalty (0, -8, -12, or -20)
"""

import time
import requests
import gspread
import pytz
import streamlit as st
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials as SACredentials

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

API_KEY  = st.secrets.get("TWELVE_DATA_API_KEY", "")
BASE_URL = "https://api.twelvedata.com"
CACHE_TTL_SECONDS = 15 * 60
BATCH_SIZE = 20          # Twelve Data safe batch window

EASTERN = pytz.timezone("US/Eastern")

# Hardcoded target spreadsheet
ORACLE_SHEET_ID = "1fWKc0DmaaTPWOJQ2Jp3yP5KaCoqrjOjrHAA7RaZNfGo"

# Badge thresholds
BADGE_ELITE      = 90
BADGE_STRONG     = 75
BADGE_MODERATE   = 60
# below 60 → Speculative / Watch

# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS — service account auth
# ══════════════════════════════════════════════════════════════════════════════

SHEET_TITLE = "Legacy & Logistics — Becht 2026 Trip"

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


@st.cache_resource(show_spinner=False)
def _get_gspread_client() -> gspread.Client:
    creds = SACredentials.from_service_account_info(
        st.secrets["connections.gsheets"], scopes=_SCOPES
    )
    return gspread.authorize(creds)


def _get_or_create_worksheet(sh: gspread.Spreadsheet, title: str, rows: int = 200, cols: int = 10):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


@st.cache_resource(show_spinner=False)
def get_legacy_sheet() -> gspread.Spreadsheet:
    """
    Open (or create) the Legacy & Logistics spreadsheet.
    Uses the oracle sheet ID so both universes live in one file,
    but falls back to creating a new sheet if needed.
    """
    gc = _get_gspread_client()
    try:
        sh = gc.open_by_key(ORACLE_SHEET_ID)
    except Exception:
        sh = gc.create(SHEET_TITLE)
    _ensure_worksheets(sh)
    return sh


def get_sheet_url() -> str:
    return f"https://docs.google.com/spreadsheets/d/{ORACLE_SHEET_ID}"


def _ensure_worksheets(sh: gspread.Spreadsheet):
    # LegacyNotes
    ws_notes = _get_or_create_worksheet(sh, "LegacyNotes")
    if not ws_notes.get_all_values():
        ws_notes.append_row(["Timestamp", "Author", "Subject", "Story"])

    for tab, header in [
        ("Itinerary",    ["City", "Date", "Activity", "Vibe", "Weather", "Notes"]),
        ("Artifacts",    ["Timestamp", "Name", "Description", "Backstory", "Photo Link"]),
        ("ReviewerLogs", ["Timestamp", "Author", "Type", "Note"]),
    ]:
        ws = _get_or_create_worksheet(sh, tab)
        if not ws.get_all_values():
            ws.append_row(header)


# ── "Solty 100 List" Universe Pull ───────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_universe_from_sheet() -> list[dict]:
    """
    Fetch the dynamic universe from the 'Solty 100 List' worksheet.

    Actual sheet columns (0-indexed):
      0  (A) — Ticker
      1  (B) — Company Name   (ignored)
      2  (C) — Market Scope   (global / north_american / domestic)
      3  (D) — Sub-Sector
      13 (N) — Zombie Penalty (0, -8, -12, or -20)

    We fetch the explicit range A:N to guarantee all rows are returned
    even when columns E–M are sparse or empty.
    """
    try:
        gc = _get_gspread_client()
        sh = gc.open_by_key(ORACLE_SHEET_ID)
        ws = sh.worksheet("Solty 100 List")
        # Explicitly fetch A through N — this avoids gspread silently
        # truncating rows when trailing columns are empty
        rows = ws.get("A:N", value_render_option="UNFORMATTED_VALUE")
    except gspread.WorksheetNotFound:
        st.error("'Solty 100 List' worksheet not found in the spreadsheet.")
        return []
    except Exception as e:
        st.error(f"Could not load universe from sheet: {e}")
        return []

    if not rows:
        st.error("'Solty 100 List' returned no data.")
        return []

    _HEADER_TOKENS = {"ticker", "symbol", "name", "#", "no", "stock", "company"}

    universe      = []
    skipped_blank = 0
    skipped_hdr   = 0

    for raw_row in rows:
        # Pad to 14 columns so index 13 (Col N) always exists
        padded = list(raw_row) + [""] * max(0, 14 - len(raw_row))

        ticker = str(padded[0]).strip().upper()

        if not ticker:
            skipped_blank += 1
            continue
        if ticker.lower() in _HEADER_TOKENS:
            skipped_hdr += 1
            continue

        # ── Market Scope → tier (Col C, index 2) ─────────────────────────────
        scope = str(padded[2]).strip().lower()
        if "global" in scope:
            tier = "global"
        elif "north" in scope or scope == "na":
            tier = "north_american"
        elif "domestic" in scope or scope in ("ca", "cdn", "canada"):
            tier = "domestic"
        else:
            tier = "domestic"

        # ── Sub-Sector (Col D, index 3) ──────────────────────────────────────
        sub = str(padded[3]).strip() or "—"

        # ── Zombie Penalty (Col N, index 13) ─────────────────────────────────
        penalty_raw = str(padded[13]).strip().replace("−", "-").replace("(", "-").replace(")", "")
        try:
            zombie_penalty = int(float(penalty_raw)) if penalty_raw else 0
        except ValueError:
            zombie_penalty = 0
        zombie_penalty = max(-20, min(0, zombie_penalty))

        universe.append({
            "symbol":         ticker,
            "exchange":       _infer_exchange(ticker),
            "tier":           tier,
            "sub":            sub,
            "zombie_penalty": zombie_penalty,
        })

    loaded = len(universe)
    if loaded == 0:
        st.error(f"Universe parsed 0 tickers from {len(rows)} raw rows. Check sheet structure.")

    return universe


@st.cache_data(ttl=300, show_spinner=False)
def _load_raw_sheet_rows() -> list[list]:
    """Return raw rows from col A only — used for the debug diagnostic."""
    try:
        gc = _get_gspread_client()
        sh = gc.open_by_key(ORACLE_SHEET_ID)
        ws = sh.worksheet("Solty 100 List")
        return ws.col_values(1)   # Column A, all values
    except Exception:
        return []


# ── Comprehensive TSX symbol list ─────────────────────────────────────────────
# Covers all major TSX-listed equities likely to appear in a Canadian investor
# universe.  Any symbol in this set gets the :TSX suffix when querying Twelve Data.
# US-listed symbols (even those with identical tickers) are NOT in this set.
_KNOWN_TSX = {
    # Banks & Financials
    "RY","TD","BNS","BMO","CM","NA","CWB","EQB","IGM","IFC","MFC","SLF",
    "GWO","POW","FFH","X","ECN","HCG","LB",
    # Energy — Integrated & Midstream
    "ENB","TRP","PPL","KEY","IPL","GEI","PIF","PKI",
    # Energy — E&P / Oil Sands
    "CNQ","SU","CVE","OVV","IMO","MEG","PEY","ERF","BTE","ARX","TOU",
    "WCP","CPG","TVE","SGY","NVA","CR","VII","NXE",
    # Energy — Uranium
    "CCO","DML","NXE","UEX","FCU","ISO","LAM",
    # Pipelines / Utilities
    "FTS","AQN","EMA","H","CU","ALA","NPI","INE","CPX","BLX","ACI",
    "RNW","VIBE","ATCO","ACO",
    # Industrials / Rail / Transport
    "CP","CNR","TIH","STN","WSP","BDT","CAE","MDA","NFI","TFI","TFII",
    "CHRW","AC","WJA","CHR",
    # Technology
    "SHOP","CSU","ENGH","KXS","DSG","DCBO","BBTV","LSPD","NVEI","TOI",
    "ALYA","CWAN","DND","MTLO","GIB","MG","BB","TELO",
    # Bitcoin / Crypto Mining
    "HUT","BITF","MARA","RIOT","CBIT","DMGI",
    # Consumer / Retail
    "ATD","L","MRU","WN","EMP","DOL","CTC","GOOS","QSR","MTY","RECP",
    "SIQ","PBH","HBC","DLCG",
    # Materials / Mining / Gold
    "ABX","AEM","KL","AGI","K","IMG","EDV","OR","FR","FM","LUN","HBM",
    "CS","TECK","WPM","NGT","MAG","DPM","CG","CIA","RIO","ELD","SMF",
    "SVM","NG","AR","GUY","BTO","MUX","IAU","OSK","NGT",
    # Real Estate
    "REI","HR","SRU","CHP","CRT","NWH","D","AAR","AP","AX","BPY",
    "GRT","KMP","MEQ","MRG","NVU","PLZ","PRV","SMU","TCN","TNT",
    # Telecom / Media
    "BCE","T","RCI","SJR","MBT","QBR","TVA",
    # Infrastructure / Brookfield
    "BAM","BIP","BEP","BBU","BIPC","BEPC","BN",
    # Healthcare / Biotech
    "CLS","WELL","DND","SIA","TLRY","ACB","WEED","APHA","OGI","CRON",
    # Other / Diversified
    "TRI","WCN","GFL","TRP","CCL","ITP","PLC","ACQ","BYD","PZA",
    "OTEX","PHO","AIF","ECN","PSD","FCR","FSV","GWO","HLF","MX",
    "POW","PWF","QSP","SAP","SCL","SNC","TA","TCL","TDG","TRZ",
    "WFG","ZZZ","YRI","NTR","AGU","RUS","WPK",
}


def _infer_exchange(symbol: str) -> str:
    """
    Return 'TSX' if the symbol is a known Canadian-listed ticker,
    otherwise 'NASDAQ' (covers NYSE and NASDAQ — exchange is confirmed
    from the Twelve Data API response and displayed in the UI).
    """
    return "TSX" if symbol in _KNOWN_TSX else "NASDAQ"


def td_symbol(ticker: dict) -> str:
    return f"{ticker['symbol']}:TSX" if ticker["exchange"] == "TSX" else ticker["symbol"]


# ── Sheet read / write helpers (cached) ──────────────────────────────────────

def _safe_df(records, columns):
    return pd.DataFrame(records) if records else pd.DataFrame(columns=columns)


@st.cache_data(ttl=30, show_spinner=False)
def load_notes():
    try:
        sh = get_legacy_sheet()
        return _safe_df(sh.worksheet("LegacyNotes").get_all_records(),
                        ["Timestamp","Author","Subject","Story"])
    except Exception as e:
        st.warning(f"Could not load notes: {e}")
        return pd.DataFrame(columns=["Timestamp","Author","Subject","Story"])


@st.cache_data(ttl=30, show_spinner=False)
def load_itinerary():
    try:
        sh = get_legacy_sheet()
        return _safe_df(sh.worksheet("Itinerary").get_all_records(),
                        ["City","Date","Activity","Vibe","Weather","Notes"])
    except Exception as e:
        st.warning(f"Could not load itinerary: {e}")
        return pd.DataFrame(columns=["City","Date","Activity","Vibe","Weather","Notes"])


@st.cache_data(ttl=30, show_spinner=False)
def load_artifacts():
    try:
        sh = get_legacy_sheet()
        return _safe_df(sh.worksheet("Artifacts").get_all_records(),
                        ["Timestamp","Name","Description","Backstory","Photo Link"])
    except Exception:
        return pd.DataFrame(columns=["Timestamp","Name","Description","Backstory","Photo Link"])


@st.cache_data(ttl=30, show_spinner=False)
def load_reviewer_logs():
    try:
        sh = get_legacy_sheet()
        return _safe_df(sh.worksheet("ReviewerLogs").get_all_records(),
                        ["Timestamp","Author","Type","Note"])
    except Exception:
        return pd.DataFrame(columns=["Timestamp","Author","Type","Note"])


def _now_et() -> str:
    return datetime.now(EASTERN).isoformat()


def save_note(author, subject, story):
    sh = get_legacy_sheet()
    sh.worksheet("LegacyNotes").append_row([_now_et(), author, subject, story])
    load_notes.clear()

def save_artifact(name, desc, backstory, photo_link=""):
    sh = get_legacy_sheet()
    sh.worksheet("Artifacts").append_row([_now_et(), name, desc, backstory, photo_link])
    load_artifacts.clear()

def save_activity(city, date_str, activity, vibe, weather, notes):
    sh = get_legacy_sheet()
    sh.worksheet("Itinerary").append_row([city, date_str, activity, vibe, weather, notes])
    load_itinerary.clear()

def overwrite_itinerary(df):
    sh = get_legacy_sheet()
    ws = sh.worksheet("Itinerary")
    ws.clear()
    ws.update([df.columns.tolist()] + df.fillna("").values.tolist())
    load_itinerary.clear()

def save_reviewer_log(author, log_type, note):
    sh = get_legacy_sheet()
    sh.worksheet("ReviewerLogs").append_row([_now_et(), author, log_type, note])
    load_reviewer_logs.clear()


# ══════════════════════════════════════════════════════════════════════════════
# MARKET ORACLE — API HELPERS (BATCHED)
# ══════════════════════════════════════════════════════════════════════════════

def safe_float(val) -> float:
    try:
        return float(val) if val not in (None, "", "N/A") else 0.0
    except (TypeError, ValueError):
        return 0.0


def fetch_batch_quotes(tickers: list[dict]) -> dict:
    """
    Fetch quotes in batches of BATCH_SIZE.
    Returns {symbol: quote_dict} keyed by plain symbol (e.g. "RY" not "RY:TSX").

    On a 429 rate-limit response, waits and retries up to 3 times before
    giving up on that batch (rather than aborting all remaining batches).
    """
    result  = {}
    batches = [tickers[i:i + BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        if batch_idx > 0:
            time.sleep(1.5)

        symbols_str = ",".join(td_symbol(t) for t in batch)
        url = f"{BASE_URL}/quote?symbol={symbols_str}&apikey={API_KEY}"

        data = None
        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=45)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                if attempt == 2:
                    st.warning(f"Quote API error on batch {batch_idx + 1} after 3 attempts: {e}")
                continue

            if isinstance(data, dict) and data.get("code") == 429:
                wait = 15 * (attempt + 1)
                time.sleep(wait)
                data = None
                continue
            break   # successful response

        if data is None:
            continue   # skip this batch, move to next — don't abort everything

        if len(batch) == 1:
            q = data if isinstance(data, dict) else {}
            if q.get("close") and not q.get("code"):
                result[batch[0]["symbol"]] = q
            else:
                td_key = td_symbol(batch[0])
                nested = data.get(td_key) or data.get(batch[0]["symbol"], {})
                if isinstance(nested, dict) and nested.get("close") and not nested.get("code"):
                    result[batch[0]["symbol"]] = nested
        else:
            for ticker in batch:
                td_key = td_symbol(ticker)
                plain  = ticker["symbol"]
                q = data.get(td_key) or data.get(plain, {})
                if isinstance(q, dict) and q.get("close") and not q.get("code"):
                    result[plain] = q

    return result


def fetch_sma50(td_sym: str) -> float | None:
    url = (
        f"{BASE_URL}/sma?symbol={td_sym}"
        f"&interval=1day&time_period=50&outputsize=1&apikey={API_KEY}"
    )
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=15)
            data = resp.json()
            if data.get("code") == 429:
                time.sleep(15 * (attempt + 1))
                continue
            values = data.get("values", [])
            if values:
                return safe_float(values[0].get("sma"))
            return None
        except Exception:
            if attempt == 2:
                return None
            time.sleep(2)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MANIFESTO SCORING ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def technical_score(
    above_dma:   bool,
    pct_vs_dma:  float | None,
    volume:      float,
    avg_volume:  float,
    change_pct:  float,
    tier:        str,
) -> int:
    """
    Pure technical score (0–100) before zombie penalty.
    """
    score = 0

    # ── Trend Anchor (max 30) ─────────────────────────────────────────────────
    if above_dma:
        score += 25
        if pct_vs_dma is not None and pct_vs_dma > 5:
            score += 5
    elif pct_vs_dma is not None and pct_vs_dma >= -1:
        score += 15

    # ── Sector Leadership (max 25) ───────────────────────────────────────────
    if tier == "global":
        score += 25
    elif tier == "north_american":
        score += 17
    elif tier == "domestic":
        score += 10

    # ── Volume Strength (max 20) ─────────────────────────────────────────────
    if avg_volume > 0:
        ratio = volume / avg_volume
        if   ratio > 2:   score += 20
        elif ratio > 1.5: score += 15
        elif ratio > 1:   score += 10
        else:             score += 5
    elif volume > 500_000:
        score += 10

    # ── Price Momentum (max 15) ──────────────────────────────────────────────
    if   change_pct > 3: score += 15
    elif change_pct > 1: score += 10
    elif change_pct > 0: score += 5

    return max(0, min(100, score))


def manifesto_score(
    above_dma:      bool,
    pct_vs_dma:     float | None,
    volume:         float,
    avg_volume:     float,
    change_pct:     float,
    tier:           str,
    zombie_penalty: int,
) -> tuple[int, int]:
    """
    Returns (final_score, tech_score).
    final_score = tech_score + zombie_penalty  (penalty is ≤ 0)
    """
    tech  = technical_score(above_dma, pct_vs_dma, volume, avg_volume, change_pct, tier)
    final = max(0, min(100, tech + zombie_penalty))
    return final, tech


def badge_for_score(score: int) -> tuple[str, str]:
    """Returns (label, colour_hex)."""
    if score >= BADGE_ELITE:
        return "🏆 Elite",       "#22c55e"   # green
    elif score >= BADGE_STRONG:
        return "💪 Strong",      "#86efac"   # light green
    elif score >= BADGE_MODERATE:
        return "📊 Moderate",    "#fde047"   # yellow
    else:
        return "👁️ Speculative", "#9ca3af"   # gray


# ══════════════════════════════════════════════════════════════════════════════
# SCAN ENGINE
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def run_scan(exchange_filter: str) -> tuple[list[dict], list[dict], list[dict], int]:
    """
    Returns (passed, near_miss, watchlist, total_scanned).

    Tiers:
      passed    — above 50-DMA (or within −1%), score ≥ 60
      near_miss — above 50-DMA, score 40–59
      watchlist — failed trend gate OR score < 40
    """
    universe = load_universe_from_sheet()
    if exchange_filter != "ALL":
        universe = [t for t in universe if t["exchange"] == exchange_filter]

    quotes        = fetch_batch_quotes(universe)
    liquidity_ok  = []
    rejected_liq  = []

    # ── Liquidity Gate ────────────────────────────────────────────────────────
    for ticker in universe:
        q = quotes.get(ticker["symbol"])

        # ── No quote returned (API miss / unrecognised symbol) ────────────────
        if not q:
            rejected_liq.append({
                "symbol":         ticker["symbol"],
                "exchange":       ticker["exchange"],
                "sub":            ticker["sub"],
                "price":          0.0,
                "volume":         0,
                "50-DMA":         None,
                "% vs DMA":       None,
                "status":         "⚠️ NO DATA",
                "reason":         "No quote returned from Twelve Data — check symbol/exchange",
                "score":          None,
                "tech":           None,
                "badge":          ("—", "#6b7280"),
                "zombie_penalty": ticker["zombie_penalty"],
                "change%":        0.0,
            })
            continue

        price  = safe_float(q.get("close"))
        volume = safe_float(q.get("volume"))
        if price < 2.0 or volume < 100_000:
            reasons = []
            if price < 2.0:      reasons.append(f"Price (${price:.2f}) below $2.00")
            if volume < 100_000: reasons.append(f"Volume ({int(volume):,}) below 100 k")
            rejected_liq.append({
                "symbol":         ticker["symbol"],
                "exchange":       ticker["exchange"],
                "sub":            ticker["sub"],
                "price":          price,
                "volume":         int(volume),
                "50-DMA":         None,
                "% vs DMA":       None,
                "status":         "❌ LIQUIDITY",
                "reason":         "; ".join(reasons),
                "score":          None,
                "tech":           None,
                "badge":          ("—", "#6b7280"),
                "zombie_penalty": ticker["zombie_penalty"],
                "change%":        0.0,
            })
            continue
        liquidity_ok.append((ticker, q, price, volume))

    # ── Trend + Score ─────────────────────────────────────────────────────────
    passed, near_miss, watchlist = [], [], []

    for ticker, q, price, volume in liquidity_ok:
        avg_vol    = safe_float(q.get("average_volume"))
        change_pct = safe_float(q.get("percent_change"))
        sma50      = fetch_sma50(td_symbol(ticker))
        time.sleep(0.12)

        if sma50 is None or sma50 == 0:
            pct_vs_dma, above_dma = None, False
        else:
            pct_vs_dma = ((price - sma50) / sma50) * 100
            above_dma  = pct_vs_dma >= -1.0

        final, tech = manifesto_score(
            above_dma, pct_vs_dma, volume, avg_vol,
            change_pct, ticker["tier"], ticker["zombie_penalty"]
        )

        row = {
            "symbol":         ticker["symbol"],
            "exchange":       ticker["exchange"],
            "sub":            ticker["sub"],
            "tier":           ticker["tier"],
            "price":          price,
            "volume":         int(volume),
            "50-DMA":         round(sma50, 2) if sma50 else None,
            "% vs DMA":       round(pct_vs_dma, 2) if pct_vs_dma is not None else None,
            "score":          final,
            "tech":           tech,
            "zombie_penalty": ticker["zombie_penalty"],
            "change%":        round(change_pct, 2),
            "badge":          badge_for_score(final),
        }

        if above_dma:
            row["status"] = "✅ PASSED"
            row["reason"] = ""
            if final >= BADGE_MODERATE:
                passed.append(row)
            else:
                near_miss.append(row)   # score < 60 but above DMA — near miss
        else:
            row["status"] = "📉 TREND FAIL"
            row["reason"] = (
                f"Price (${price:.2f}) below 50-DMA (${sma50:.2f}) [{pct_vs_dma:.2f}%]"
                if pct_vs_dma is not None else "50-DMA unavailable"
            )
            watchlist.append(row)

    # also push liquidity failures into watchlist
    watchlist.extend(rejected_liq)

    passed.sort(   key=lambda r: r["score"] or 0,            reverse=True)
    near_miss.sort(key=lambda r: r["score"] or 0,            reverse=True)
    watchlist.sort(key=lambda r: r.get("% vs DMA") or -999,  reverse=True)

    return passed, near_miss, watchlist, len(universe)


# ══════════════════════════════════════════════════════════════════════════════
# BECHT FAMILY LEGACY — DATA
# ══════════════════════════════════════════════════════════════════════════════

LANDMARKS = [
    {
        "name": "Herengracht 172",
        "label": "Huis Bartolotti — Becht Publishing Office",
        "year": "Built 1617",
        "architect": "Hendrick de Keyser",
        "inscription": "Ingenio et Assiduo Labore",
        "inscription_translation": "By Ingenuity and Diligent Labor",
        "description": (
            "Built in 1617 by Hendrick de Keyser — the most celebrated architect of the "
            "Dutch Golden Age — Huis Bartolotti is one of Amsterdam's finest canal houses, "
            "situated on the Golden Bend of the Herengracht. The Latin inscription on its "
            "facade, *Ingenio et Assiduo Labore* (\u201cBy Ingenuity and Diligent Labor\u201d), feels "
            "almost written for a publishing house. That the Becht family chose this address "
            "for their publishing business speaks to both their ambition and their deep roots "
            "in Amsterdam's intellectual and commercial life."
        ),
        "maps": "https://maps.google.com/?q=Herengracht+172,+Amsterdam",
    },
    {
        "name": "Koningslaan 70",
        "label": "Mother's Childhood Home — Nazi Occupation Site",
        "year": "WWII Requisition",
        "architect": None,
        "inscription": None,
        "inscription_translation": None,
        "description": (
            "Koningslaan sits at the edge of Vondelpark in Oud-Zuid, one of Amsterdam's "
            "grandest residential streets. During the occupation, the Nazi Sicherheitsdienst "
            "(SD) and senior German officials systematically requisitioned these villas. "
            "Andy's mother's memory of being forced out is part of the broader *vorderingen* "
            "(requisitions) that displaced hundreds of Amsterdam families. Standing here is "
            "a direct encounter with that history."
        ),
        "maps": "https://maps.google.com/?q=Koningslaan+70,+Amsterdam",
    },
]

HERITAGE_IMAGES = {"clock_full": "", "clock_face": "", "guilder_coin": ""}

def get_drive_url(url: str) -> str:
    if "drive.google.com" in url and "/file/d/" in url:
        file_id = url.split("/file/d/")[1].split("/")[0]
        return f"https://drive.google.com/uc?id={file_id}"
    return url

VIBES          = ["Outdoor/Heritage", "Coffee/Food", "Legacy Visit"]
WEATHER_OPTIONS= ["Either", "☀️ Sunny Day", "🌧️ Rain Day"]
VIBE_ICONS     = {"Outdoor/Heritage": "🌿", "Coffee/Food": "☕", "Legacy Visit": "🏛️"}
CITIES         = ["Amsterdam", "Tuscany", "Rome"]


# ══════════════════════════════════════════════════════════════════════════════
# STREAMLIT APP
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Solty Oracle Suite",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── SIDEBAR ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 📡 Solty Oracle Suite")
    st.divider()

    app_mode = st.radio(
        "Navigate",
        ["📡  Market Oracle", "🧳  Becht Family Legacy"],
        label_visibility="collapsed",
    )
    st.divider()

    if "Legacy" in app_mode:
        st.markdown("### 🧳 Becht Legacy\n##### Andy & Sue · Europe 2026")
        legacy_view = st.radio(
            "View",
            [
                "🏛️  Historic Legacy Archive",
                "🌍  Europe Heritage & History",
                "🗺️  Trip Logistics & Itinerary",
            ],
            label_visibility="collapsed",
        )
        st.divider()
        st.success("📋 Google Sheet connected")
        st.link_button("Open Shared Sheet", get_sheet_url(), use_container_width=True)
        st.divider()
        st.caption("🇳🇱 Amsterdam · 🇮🇹 Tuscany · 🇮🇹 Rome · 🇭🇺 Budapest")

    else:
        st.markdown("### 📡 Market Oracle")
        exchange = st.selectbox("Exchange", ["ALL", "NASDAQ", "NYSE", "TSX"])
        top_n    = st.number_input("Top N results", min_value=1, max_value=100, value=15)
        st.divider()

        if st.button("🔄 Refresh scan", type="secondary", use_container_width=True):
            run_scan.clear()
            load_universe_from_sheet.clear()
            st.rerun()
        if st.button("🔄 Reload Universe", type="secondary", use_container_width=True):
            load_universe_from_sheet.clear()
            st.rerun()
        st.divider()
        st.caption("Dynamic Universe · Manifesto Score · Zombie Penalty · Batched API")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW A — MARKET ORACLE
# ══════════════════════════════════════════════════════════════════════════════

if "Oracle" in app_mode:

    st.title("📡 Andy Solty Market Oracle")
    st.caption("Dynamic Universe from Google Sheets · Manifesto Scoring Engine · Tiered Badging")

    if not API_KEY:
        st.error("❌ TWELVE_DATA_API_KEY environment variable is not set.")
        st.stop()

    with st.spinner(f"Loading universe & scanning {exchange}…"):
        passed, near_miss, watchlist, total = run_scan(exchange)

    scanned_at = datetime.now(EASTERN).strftime("%H:%M:%S ET")
    total_outcomes = len(passed) + len(near_miss) + len(watchlist)
    st.markdown(
        f"**📋 Loaded from sheet:** {total} tickers &nbsp;|&nbsp; "
        f"**📡 API outcomes:** {total_outcomes} "
        f"( ✅ {len(passed)} Passed &nbsp;· "
        f"🟡 {len(near_miss)} Near Miss &nbsp;· "
        f"👁️ {len(watchlist)} Watchlist ) &nbsp;|&nbsp; "
        f"**🕐** {scanned_at} *(cached 15 min)*"
    )
    if total_outcomes < total:
        st.warning(
            f"⚠️ **{total - total_outcomes} ticker(s) got no response from Twelve Data.** "
            "Open the 🔬 Universe Diagnostic below to identify which ones, "
            "then check the 👁️ Watchlist for ⚠️ NO DATA entries."
        )

    # ── Badge legend ──────────────────────────────────────────────────────────
    with st.expander("🏅 Badge Guide & Scoring Breakdown"):
        col_b1, col_b2, col_b3, col_b4 = st.columns(4)
        with col_b1:
            st.markdown(
                "<div style='background:#22c55e;color:white;padding:8px 12px;border-radius:8px;text-align:center'>"
                "🏆 Elite<br><small>Score 90–100</small></div>", unsafe_allow_html=True)
        with col_b2:
            st.markdown(
                "<div style='background:#86efac;color:#166534;padding:8px 12px;border-radius:8px;text-align:center'>"
                "💪 Strong<br><small>Score 75–89</small></div>", unsafe_allow_html=True)
        with col_b3:
            st.markdown(
                "<div style='background:#fde047;color:#713f12;padding:8px 12px;border-radius:8px;text-align:center'>"
                "📊 Moderate<br><small>Score 60–74</small></div>", unsafe_allow_html=True)
        with col_b4:
            st.markdown(
                "<div style='background:#9ca3af;color:white;padding:8px 12px;border-radius:8px;text-align:center'>"
                "👁️ Speculative<br><small>Score &lt; 60</small></div>", unsafe_allow_html=True)

        st.divider()
        st.markdown("""
| Component | Max pts | Rules |
|---|---|---|
| **Trend Anchor** | 30 | 25 pts above 50-DMA; +5 bonus >5% above; 15 pts within −1% |
| **Sector Leadership** | 25 | Global = 25 · North American = 17 · Domestic = 10 |
| **Volume Strength** | 20 | >2× avg = 20 · >1.5× = 15 · >1× = 10 · else = 5 |
| **Price Momentum** | 15 | >3% day = 15 · >1% = 10 · >0% = 5 |
| **Zombie Penalty** | 0/−8/−12/−20 | From Column N of Solty 100 List sheet |

**Final Score = Technical Score + Zombie Penalty**  
**Liquidity Gate**: Price ≥ $2.00 · Volume ≥ 100,000  
**Trend Anchor**: Price ≥ 50-DMA (tolerance: within −1%)
""")


    # ── Helper: render tile grid ──────────────────────────────────────────────

    def _tile(r: dict):
        badge_label, badge_colour = r["badge"]
        zp = r.get("zombie_penalty", 0)
        zp_str = f" ({zp:+d})" if zp else ""
        dma_str = (
            f"+{r['% vs DMA']:.1f}% vs DMA" if r.get("% vs DMA") and r["% vs DMA"] >= 0
            else (f"{r['% vs DMA']:.1f}% vs DMA" if r.get("% vs DMA") is not None else "DMA n/a")
        )
        with st.container(border=True):
            c1, c2, c3 = st.columns([2, 3, 1])
            with c1:
                st.markdown(
                    f"<span style='font-size:1.25rem;font-weight:700'>{r['symbol']}</span>"
                    f"<br><small style='color:#6b7280'>{r['exchange']}</small>",
                    unsafe_allow_html=True,
                )
            with c2:
                st.caption(r.get("sub", "—"))
                st.markdown(
                    f"`${r['price']:.2f}` &nbsp; `{dma_str}` &nbsp; `chg {r.get('change%',0):+.1f}%`"
                )
                if zp:
                    st.caption(f"Zombie penalty: {zp:+d} pts  |  Tech: {r['tech']}")
            with c3:
                st.markdown(
                    f"<div style='background:{badge_colour};padding:6px 8px;border-radius:8px;"
                    f"text-align:center;font-weight:600;font-size:0.78rem'>"
                    f"{badge_label}<br>"
                    f"<span style='font-size:1.1rem'>{r['score']}</span></div>",
                    unsafe_allow_html=True,
                )


    # ── Universe Diagnostic ───────────────────────────────────────────────────
    with st.expander(f"🔬 Universe Diagnostic — {total} tickers loaded from sheet"):
        raw_col_a = _load_raw_sheet_rows()
        raw_universe = load_universe_from_sheet()

        d1, d2, d3 = st.columns(3)
        d1.metric("Rows in Col A (sheet)", len(raw_col_a))
        d2.metric("Tickers parsed", len(raw_universe))
        d3.metric("Sent to API (this filter)", total)

        st.divider()

        # Show everything in Col A so you can see exactly what the sheet returns
        st.markdown("**Raw Column A values** (everything the sheet returned):")
        col_a_df = pd.DataFrame({"Row": range(1, len(raw_col_a)+1), "Col A value": raw_col_a})
        st.dataframe(col_a_df, use_container_width=True, hide_index=True, height=300)

        st.divider()

        # Show the parsed universe
        if raw_universe:
            st.markdown("**Parsed universe** (tickers that made it through the parser):")
            diag_df = pd.DataFrame(raw_universe)[["symbol","exchange","tier","sub","zombie_penalty"]]
            st.dataframe(diag_df, use_container_width=True, hide_index=True, height=300)

            # Highlight any discrepancy
            raw_tickers    = {str(v).strip().upper() for v in raw_col_a if str(v).strip()}
            parsed_tickers = {r["symbol"] for r in raw_universe}
            _HEADER_TOKENS = {"ticker", "symbol", "name", "#", "no", "stock", "company"}
            raw_tickers   -= _HEADER_TOKENS
            missing        = raw_tickers - parsed_tickers
            if missing:
                st.warning(
                    f"⚠️ **{len(missing)} ticker(s) in Col A were NOT parsed** — "
                    f"likely filtered as header/blank rows: `{sorted(missing)}`"
                )
            else:
                st.success("✅ All non-header Col A values were successfully parsed as tickers.")
        else:
            st.warning("No tickers loaded — check sheet connection and column layout.")

    # ── PASSED ────────────────────────────────────────────────────────────────
    st.subheader(f"✅ Passed — Top {min(top_n, len(passed))} of {len(passed)}")
    if passed:
        for r in passed[:top_n]:
            _tile(r)
    else:
        st.info("No stocks passed both gates in this scan.")


    # ── NEAR MISS ─────────────────────────────────────────────────────────────
    st.subheader(f"🟡 Near Miss ({len(near_miss)} stocks)")
    st.caption("Above 50-DMA but Manifesto Score 40–59. Worth watching — one strong session away.")
    if near_miss:
        for r in near_miss[:top_n]:
            _tile(r)
    else:
        st.info("No near-miss stocks in this scan.")


    # ── WATCHLIST ─────────────────────────────────────────────────────────────
    with st.expander(f"👁️ Watchlist / Rejected ({len(watchlist)} stocks)"):
        if watchlist:
            no_data    = [r for r in watchlist if r.get("status") == "⚠️ NO DATA"]
            liq_fail   = [r for r in watchlist if r.get("status") == "❌ LIQUIDITY"]
            trend_fail = [r for r in watchlist if r.get("status") == "📉 TREND FAIL"]

            if no_data:
                st.markdown(
                    f"**⚠️ No Quote Returned — {len(no_data)} ticker(s).** "
                    "These symbols returned no data from Twelve Data. "
                    "Check for wrong symbol spelling or missing exchange suffix in the sheet."
                )
                st.dataframe(
                    pd.DataFrame([{
                        "Symbol":   r["symbol"],
                        "Exchange": r["exchange"],
                        "Sub":      r.get("sub","—"),
                        "Reason":   r.get("reason",""),
                    } for r in no_data]),
                    use_container_width=True, hide_index=True,
                )
                st.divider()

            combined = trend_fail + liq_fail
            if combined:
                df_w = pd.DataFrame([{
                    "Symbol":   r["symbol"],
                    "Exchange": r["exchange"],
                    "Sub":      r.get("sub","—"),
                    "Price":    r.get("price", 0.0),
                    "50-DMA":   r.get("50-DMA"),
                    "% vs DMA": r.get("% vs DMA"),
                    "Score":    r.get("score"),
                    "Status":   r.get("status",""),
                    "Reason":   r.get("reason",""),
                } for r in combined])
                st.dataframe(
                    df_w.style.format({
                        "Price":    "${:.2f}",
                        "50-DMA":   lambda v: f"${v:.2f}" if v else "—",
                        "% vs DMA": lambda v: f"{v:.2f}%" if v is not None else "—",
                        "Score":    lambda v: f"{int(v)}" if v is not None else "—",
                    }),
                    use_container_width=True, hide_index=True,
                )
        else:
            st.write("All tickers passed the gates — impressive universe quality!")


    # ── Reviewer Log ──────────────────────────────────────────────────────────
    st.divider()
    st.subheader("📝 Reviewer Log")
    st.caption("Private market notes — saved to the ReviewerLogs tab in Google Sheets.")

    log_col_a, log_col_b = st.columns(2)
    with log_col_a:
        log_author = st.selectbox("Author", ["Andy"], key="log_author")
    with log_col_b:
        log_type = st.selectbox(
            "Note Type",
            ["Market Observation","Conviction Review","Research Note","Trade Idea","Other"],
            key="log_type",
        )
    log_note = st.text_area(
        "Note",
        placeholder=(
            "Record anything worth keeping:\n"
            "- Why a specific ticker is or isn't in the feed today\n"
            "- A macro observation that changes the picture\n"
            "- A sector rotation you're tracking\n"
            "- A trade idea or conviction change…"
        ),
        height=130,
        key="log_note",
    )
    if st.button("💾 Save to Reviewer Log", type="primary"):
        if log_note.strip():
            try:
                save_reviewer_log(log_author, log_type, log_note.strip())
                st.success("Log entry saved to Google Sheets!")
                st.rerun()
            except Exception as e:
                st.error(f"Could not save: {e}")
        else:
            st.warning("Please write something before saving.")

    with st.expander("📄 Past Reviewer Logs"):
        if st.button("🔄 Refresh", key="rl_refresh"):
            load_reviewer_logs.clear()
            st.rerun()
        logs_df = load_reviewer_logs()
        if not logs_df.empty:
            for _, row in logs_df.iloc[::-1].iterrows():
                with st.container(border=True):
                    c1, c2 = st.columns([1, 4])
                    with c1:
                        st.markdown(f"**{row.get('Author','?')}**")
                        st.caption(row.get("Type",""))
                        ts = row.get("Timestamp","")
                        if ts:
                            try:
                                st.caption(datetime.fromisoformat(ts).strftime("%b %d, %Y %H:%M"))
                            except Exception:
                                st.caption(str(ts)[:16])
                    with c2:
                        st.write(row.get("Note",""))
        else:
            st.info("No log entries yet.")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW B — BECHT FAMILY LEGACY
# ══════════════════════════════════════════════════════════════════════════════

else:
    st.title("🧳 Becht Legacy & Logistics 2026")
    st.caption("Andy & Sue · Europe Trip")

    view = legacy_view

    # ══════════════════════════════════════════════════════════════════════════
    # VIEW 1 — HISTORIC LEGACY ARCHIVE
    # ══════════════════════════════════════════════════════════════════════════

    if "Archive" in view:
        st.title("🏛️ Historic Legacy Archive")
        st.caption("🇳🇱  Becht Family · Amsterdam Heritage Trail")

        tab_landmarks, tab_gallery, tab_history = st.tabs([
            "📍 Family Landmarks",
            "🖼️ Artifact Gallery",
            "📜 Family History",
        ])

        with tab_landmarks:
            st.subheader("📍 Family Landmarks")
            st.write("Key Amsterdam addresses connected to the Becht family. Visit these on your heritage walk.")
            st.divider()

            col1, col2 = st.columns(2)
            for i, lm in enumerate(LANDMARKS):
                with [col1, col2][i]:
                    with st.container(border=True):
                        st.markdown(f"### 🇳🇱 {lm['name']}")
                        st.markdown(f"**{lm['label']}**")
                        st.divider()
                        if lm.get("year"):
                            c_l, c_r = st.columns([1, 2])
                            with c_l:
                                st.caption("BUILT")
                                st.markdown(f"**{lm['year']}**")
                            if lm.get("architect"):
                                with c_r:
                                    st.caption("ARCHITECT")
                                    st.markdown(f"**{lm['architect']}**")
                        if lm.get("inscription"):
                            st.caption("INSCRIPTION")
                            st.markdown(f"> *{lm['inscription']}*  \n> \u201c{lm['inscription_translation']}\u201d")
                        st.caption("SIGNIFICANCE")
                        st.write(lm["description"])
                        st.link_button("📍 Open in Google Maps", lm["maps"], use_container_width=True, type="primary")

            st.divider()
            with st.container(border=True):
                st.markdown("### 🗺️ Heritage Walk Route")
                st.caption("SUGGESTED ORDER")
                st.write(
                    "Start at **Herengracht 172** (canal-side, morning light is perfect), "
                    "walk south along the canals to **Koningslaan 70**, "
                    "then continue to Vondelpark for lunch."
                )
                st.link_button(
                    "🗺️ Plan Route in Google Maps",
                    "https://maps.google.com/maps/dir/Herengracht+172,+Amsterdam/Koningslaan+70,+Amsterdam",
                    use_container_width=True,
                )

        with tab_gallery:
            st.subheader("🖼️ Artifact Gallery")
            st.write("Family heirlooms from the Becht collection. Data pulled live from the **Artifacts** tab in Google Sheets.")

            if st.button("🔄 Refresh", key="gallery_refresh"):
                load_artifacts.clear()
                st.rerun()

            art_df = load_artifacts()
            if art_df.empty:
                st.info("No artifacts in the sheet yet. Add one using the form below.")
            else:
                for _, row in art_df.iterrows():
                    name       = str(row.get("Name","")).strip()
                    description= str(row.get("Description","")).strip()
                    backstory  = str(row.get("Backstory","")).strip()
                    photo_link = str(row.get("Photo Link","")).strip()
                    with st.container(border=True):
                        img_col, text_col = st.columns([1, 2])
                        with img_col:
                            if photo_link:
                                try:
                                    st.image(get_drive_url(photo_link), use_container_width=True)
                                except Exception:
                                    st.warning("Could not load image — check the Drive link.")
                            else:
                                st.info("No photo linked.\nAdd a Google Drive link in the sheet's **Photo Link** column.")
                        with text_col:
                            st.markdown(f"#### {name}")
                            if description:
                                st.caption(description)
                            st.divider()
                            if backstory:
                                st.write(backstory)
                            else:
                                st.caption("No backstory recorded yet.")

            st.divider()
            with st.expander("➕ Add New Artifact"):
                art_name  = st.text_input("Artifact Name", placeholder="e.g., Becht Family Bible")
                art_desc  = st.text_input("Brief Description", placeholder="e.g., 19th century, currently with Uncle Jan")
                art_link  = st.text_input("Photo Link (Google Drive share URL)", placeholder="https://drive.google.com/file/d/…")
                art_story = st.text_area("Backstory", height=100)
                if st.button("💾 Save Artifact to Sheet", type="primary"):
                    if art_name.strip():
                        try:
                            save_artifact(art_name.strip(), art_desc.strip(), art_story.strip(), art_link.strip())
                            st.success("Artifact saved to Google Sheets!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not save: {e}")
                    else:
                        st.warning("Please enter an artifact name.")

        with tab_history:
            st.subheader("📜 Family History")
            st.write("Record stories, memories, and research notes about the Becht family.")

            with st.expander("➕ Add a New Story", expanded=True):
                col_a, col_b = st.columns(2)
                with col_a:
                    author  = st.selectbox("Author", ["Andy","Sue"])
                with col_b:
                    subject = st.selectbox("About", [
                        "Andre Becht (Grandfather)", "Herman Becht (Great-Grandfather)",
                        "Becht Family — General", "Amsterdam Observations", "Other",
                    ])
                story = st.text_area(
                    "Story / Memory / Note",
                    placeholder="Write a memory, research finding, family story, or observation here…",
                    height=160,
                )
                if st.button("💾 Save Story", type="primary", use_container_width=True):
                    if story.strip():
                        try:
                            save_note(author, subject, story.strip())
                            st.success("Story saved to Google Sheets!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not save: {e}")
                    else:
                        st.warning("Please write something before saving.")

            st.divider()
            col_r, col_f = st.columns([1, 3])
            with col_r:
                if st.button("🔄 Refresh"):
                    load_notes.clear()
                    st.rerun()
            with col_f:
                filter_sub = st.selectbox("Filter", [
                    "All Subjects","Andre Becht (Grandfather)","Herman Becht (Great-Grandfather)",
                    "Becht Family — General","Amsterdam Observations","Other",
                ], label_visibility="collapsed")

            notes_df = load_notes()
            if filter_sub != "All Subjects":
                notes_df = notes_df[notes_df.get("Subject", pd.Series()) == filter_sub]

            if not notes_df.empty:
                for _, row in notes_df.iloc[::-1].iterrows():
                    with st.container(border=True):
                        c1, c2 = st.columns([1, 4])
                        with c1:
                            st.markdown(f"**{row.get('Author','?')}**")
                            st.caption(row.get("Subject",""))
                            ts = row.get("Timestamp","")
                            if ts:
                                try:
                                    st.caption(datetime.fromisoformat(ts).strftime("%b %d, %Y"))
                                except Exception:
                                    st.caption(str(ts)[:10])
                        with c2:
                            st.write(row.get("Story",""))
            else:
                st.info("No stories yet — add your first note above!")

            st.divider()
            st.subheader("🔍 City Archives & Research Links")
            with st.container(border=True):
                st.markdown("#### Search for H.J.W. Becht")
                search_term = st.text_input("Search keyword", value="H.J.W. Becht")
                search_url  = f"https://archief.amsterdam/indexen/search?search={search_term.replace(' ','+')}"
                c1, c2 = st.columns(2)
                with c1:
                    st.link_button("🔍 Search Amsterdam Stadsarchief", search_url, use_container_width=True, type="primary")
                with c2:
                    st.link_button("🏛️ Browse Full Archive", "https://archief.amsterdam", use_container_width=True)

            st.divider()
            cols = st.columns(3)
            links = [
                ("🏠 Herengracht 172", "Historical ownership records",
                 "https://archief.amsterdam/indexen/search?search=Herengracht+172"),
                ("📚 Becht Publishing", "Dutch publishers & booksellers records",
                 "https://archief.amsterdam/indexen/search?search=Becht+uitgeverij"),
                ("🗺️ Zandvoort Records", "Zandvoort compensation & property records",
                 "https://archief.amsterdam/indexen/search?search=Zandvoort+Becht"),
            ]
            for col, (label, desc, url) in zip(cols, links):
                with col:
                    with st.container(border=True):
                        st.markdown(f"**{label}**")
                        st.caption(desc)
                        st.link_button("Search →", url, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # VIEW 2 — EUROPE HERITAGE & HISTORY  (unchanged content, kept intact)
    # ══════════════════════════════════════════════════════════════════════════

    elif "Heritage" in view:
        st.title("🌍 Europe Heritage & History")
        st.caption("🇳🇱 Becht Family · 🇩🇪 Publishing Dynasty · 🇭🇺 Hungarian Roots — A Family Chronicle")

        tab_becht, tab_publishing, tab_hungary = st.tabs([
            "🏚️ The Becht Family",
            "📚 Publishing Legacy",
            "🇭🇺 Budapest & Hungarian Heritage",
        ])

        with tab_becht:
            st.subheader("🏚️ The Becht Family — Dutch Roots & History")
            with st.container(border=True):
                st.markdown("### 🌳 Family Chronicle")
                st.markdown("""
**H.J.W. Becht — Founder of the Publishing House**

The Becht name is inseparable from Dutch intellectual and cultural life. H.J.W. Becht
established the family publishing house in Amsterdam during the late 19th century,
choosing the prestigious Herengracht as their address — a deliberate statement of
the family's ambitions in the city's commercial and cultural world.

The motto carved into Huis Bartolotti at Herengracht 172 — *Ingenio et Assiduo Labore*,
"By Ingenuity and Diligent Labor" — reads as if written for a publishing family.
                """)

            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                with st.container(border=True):
                    st.markdown("#### 👴 Herman Becht — Great-Grandfather")
                    st.markdown("""
Herman Becht built the publishing house into one of Amsterdam's distinguished imprints.
Under his stewardship, Becht Publishing grew from a local enterprise into a respected
name in Dutch cultural publishing.
                    """)
                    st.link_button("🔍 Search Stadsarchief for Herman Becht",
                        "https://archief.amsterdam/indexen/search?search=Herman+Becht",
                        use_container_width=True)

            with col2:
                with st.container(border=True):
                    st.markdown("#### 👨 André Becht — Grandfather")
                    st.markdown("""
André Becht guided the publishing house through some of the most turbulent decades
in Dutch history, including the Nazi occupation and postwar rebuilding of Dutch cultural life.
He was formally recognised by the Dutch state around 1973, evidenced by the *'s Rijks Munt Utrecht*
presentation coin now in the family's possession in Oakville, Ontario.
                    """)
                    st.link_button("🏅 Search Lintjes.nl for André Becht",
                        "https://lintjes.nl/decorandi?q=Becht",
                        use_container_width=True)

            st.divider()
            with st.container(border=True):
                st.markdown("### 🏠 The Koningslaan Years — World War II")
                st.markdown("""
Koningslaan sits along the edge of Vondelpark in Oud-Zuid. In 1940, Nazi Germany occupied
the Netherlands. The German Sicherheitsdienst (SD) systematically requisitioned the finest
villas in Amsterdam. The Becht family were forced to leave. Your mother lived this.
Post-war *Rechtsherstel* sought to compensate families; the grandfather clock is the
family's physical connection to this reckoning.
                """)
                st.link_button("📍 Koningslaan 70 on Google Maps",
                    "https://maps.google.com/?q=Koningslaan+70,+Amsterdam",
                    use_container_width=True)

        with tab_publishing:
            st.subheader("📚 Becht Publishing House — A Dutch Cultural Legacy")
            with st.container(border=True):
                st.markdown("### 🏢 Herengracht 172 — The Publishing Address")
                st.markdown("""
Huis Bartolotti was designed by Hendrick de Keyser, the most celebrated architect of the
Dutch Golden Age, and situated on the *Golden Bend* of the Herengracht.
The inscription *Ingenio et Assiduo Labore* resonates deeply for a publishing family.
                """)
                c1, c2 = st.columns(2)
                with c1:
                    st.link_button("📍 Open in Google Maps",
                        "https://maps.google.com/?q=Herengracht+172,+Amsterdam",
                        use_container_width=True, type="primary")
                with c2:
                    st.link_button("🏛️ Huis Bartolotti — Wikipedia",
                        "https://en.wikipedia.org/wiki/Huis_Bartolotti",
                        use_container_width=True)

            st.divider()
            with st.container(border=True):
                st.markdown("### 📖 What Becht Published")
                st.markdown("""
Becht Publishing built its reputation across Art & Architecture Books, Natural History & Science,
Dutch-language Literary Works, and Travel & Geography. The full catalogue is preserved in
the **Koninklijke Bibliotheek** (Royal Library) in The Hague.
                """)
                st.link_button("📚 Browse Becht Titles in KB Catalogue",
                    "https://opc.kb.nl/DB=1/LNG=NE/CMD?ACT=SRCHA&IKT=4&SRT=YOP&TRM=becht",
                    use_container_width=True, type="primary")

            st.divider()
            with st.container(border=True):
                st.markdown("### 🏅 State Recognition — The 1973 Royal Honour")
                r1, r2, r3 = st.columns(3)
                with r1:
                    st.link_button("🥇 Lintjes.nl Registry",
                        "https://lintjes.nl/decorandi?q=Becht",
                        use_container_width=True, type="primary")
                with r2:
                    st.link_button("📰 Delpher Newspapers",
                        "https://www.delpher.nl/nl/kranten/results?query=Andr%C3%A9+Becht&facets%5Bperiode%5D%5B%5D=2%7C20e+eeuw%7C1970-1979%7C",
                        use_container_width=True)
                with r3:
                    st.link_button("🏛️ Nationaal Archief",
                        "https://www.nationaalarchief.nl/onderzoeken/zoeken?q=Becht",
                        use_container_width=True)

        with tab_hungary:
            st.subheader("🇭🇺 Budapest & Hungarian Heritage — Future Trip")
            st.info("**Planning ahead:** This section is dedicated to your mother's Hungarian heritage and will grow as you prepare for a future Budapest trip.")

            with st.container(border=True):
                st.markdown("### 🌉 The Two Threads — Dutch and Hungarian")
                st.markdown("""
**The Dutch Thread** — the Becht family, Amsterdam, the publishing house, the canal houses of Oud-Zuid.

**The Hungarian Thread** — your mother's Hungarian heritage, rooted in Budapest, with its own
rich history of empire, revolution, occupation, and survival.
                """)

            st.divider()
            with st.container(border=True):
                st.markdown("### 🔗 Connecting to the Solty History")
                arch_col1, arch_col2 = st.columns(2)
                with arch_col1:
                    st.link_button("🏛️ Budapest Főváros Levéltára", "https://www.bparchiv.hu",
                        use_container_width=True, type="primary")
                    st.link_button("🔍 FamilySearch — Hungary Records",
                        "https://www.familysearch.org/en/wiki/Hungary_Genealogy",
                        use_container_width=True)
                with arch_col2:
                    st.link_button("📚 Arcanum — Hungarian Digital Archives",
                        "https://arcanum.com/en/", use_container_width=True)
                    st.link_button("🕍 Jewish Museum Budapest",
                        "https://www.jewishmuseum.hu/en/", use_container_width=True)

            st.divider()
            with st.expander("➕ Add a Budapest Heritage Note", expanded=False):
                bud_author  = st.selectbox("Author", ["Andy","Sue"], key="bud_author")
                bud_subject = st.selectbox("Subject", [
                    "Hungarian Family History","Solty History in Budapest",
                    "Budapest Addresses / Locations","Hungarian Heritage — General",
                    "Future Trip Planning","Other",
                ], key="bud_subject")
                bud_note = st.text_area(
                    "Note / Memory / Research Finding",
                    placeholder="Add anything you know about the Hungarian heritage…",
                    height=150, key="bud_note",
                )
                if st.button("💾 Save Budapest Note", type="primary", use_container_width=True, key="save_bud"):
                    if bud_note.strip():
                        try:
                            save_note(bud_author, f"🇭🇺 {bud_subject}", bud_note.strip())
                            st.success("Note saved to Google Sheets!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not save: {e}")
                    else:
                        st.warning("Please write something before saving.")

    # ══════════════════════════════════════════════════════════════════════════
    # VIEW 3 — TRIP LOGISTICS & ITINERARY
    # ══════════════════════════════════════════════════════════════════════════

    else:
        st.title("🗺️ Trip Logistics & Itinerary")
        st.caption("🇳🇱 Amsterdam · 🇮🇹 Tuscany · 🇮🇹 Rome — 2026")

        col_hdr, col_sync = st.columns([5, 1])
        with col_sync:
            if st.button("🔄 Sync"):
                load_itinerary.clear()
                st.rerun()

        itin_df = load_itinerary()
        for col in ["City","Date","Activity","Vibe","Weather","Notes"]:
            if col not in itin_df.columns:
                itin_df[col] = ""

        tab_ams, tab_tus, tab_rome, tab_add, tab_edit = st.tabs([
            "🇳🇱 Amsterdam", "🌿 Tuscany", "🏛️ Rome", "➕ Add Activity", "✏️ Edit All",
        ])

        def render_city(city_key: str, flag: str):
            city_df = itin_df[itin_df["City"].astype(str).str.lower() == city_key.lower()].copy()
            if city_df.empty:
                st.info(f"No activities planned for {flag} {city_key} yet. Use the **➕ Add Activity** tab.")
                return
            sunny = city_df[city_df["Weather"].astype(str).str.contains("Sunny", na=False)]
            rainy = city_df[city_df["Weather"].astype(str).str.contains("Rain",  na=False)]
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Activities", len(city_df))
            m2.metric("☀️ Best in Sun",  len(sunny))
            m3.metric("🌧️ Rain-Proof",   len(rainy))
            st.divider()
            vibe_filter    = st.selectbox("Filter by Vibe",    ["All Vibes"]+VIBES,                         key=f"vibe_{city_key}")
            weather_filter = st.selectbox("Filter by Weather", ["All Weather","☀️ Sunny Day","🌧️ Rain Day","Either"], key=f"wx_{city_key}")
            display_df = city_df.copy()
            if vibe_filter    != "All Vibes":    display_df = display_df[display_df["Vibe"]    == vibe_filter]
            if weather_filter != "All Weather":  display_df = display_df[display_df["Weather"] == weather_filter]
            for _, row in display_df.iterrows():
                wx_str   = str(row.get("Weather",""))
                wx_icon  = "☀️" if "Sunny" in wx_str else "🌧️" if "Rain" in wx_str else "🌤️"
                vibe_icon= VIBE_ICONS.get(str(row.get("Vibe","")), "📍")
                with st.container(border=True):
                    c_left, c_right = st.columns([4, 1])
                    with c_left:
                        st.markdown(f"**{vibe_icon} {row.get('Activity','Activity')}**")
                        if str(row.get("Date","")):
                            st.caption(f"📅 {row['Date']}")
                        if row.get("Notes"):
                            st.write(str(row["Notes"]))
                    with c_right:
                        st.markdown(f"### {wx_icon}")
                        st.caption(str(row.get("Vibe","")))

        with tab_ams:
            st.subheader("🇳🇱 Amsterdam")
            render_city("Amsterdam","🇳🇱")

        with tab_tus:
            st.subheader("🌿 Tuscany")
            render_city("Tuscany","🌿")

        with tab_rome:
            st.subheader("🏛️ Rome")
            render_city("Rome","🏛️")

        with tab_add:
            st.subheader("➕ Add New Activity")
            with st.form("add_activity_form", clear_on_submit=True):
                c1, c2 = st.columns(2)
                with c1:
                    city     = st.selectbox("City", CITIES)
                    activity = st.text_input("Activity Name",
                        placeholder="e.g., Rijksmuseum · Pasta class in Siena · Colosseum")
                    vibe     = st.selectbox("Vibe", VIBES)
                with c2:
                    act_date = st.date_input("Date (optional)", value=None)
                    weather  = st.selectbox("Best Weather Condition", WEATHER_OPTIONS)
                    who      = st.multiselect("Who's going?", ["Andy","Sue","Both"], default=["Both"])
                    notes    = st.text_area("Notes / Details",
                        placeholder="Address, booking info, tips, reservation time…", height=90)
                submitted = st.form_submit_button("➕ Add to Itinerary", type="primary", use_container_width=True)

            if submitted:
                if activity.strip():
                    try:
                        date_str = str(act_date) if act_date else ""
                        note_str = notes.strip()
                        if who and "Both" not in who:
                            note_str = f"[{'/'.join(who)}] {note_str}".strip()
                        save_activity(city, date_str, activity.strip(), vibe, weather, note_str)
                        st.success(f"✅ Added **{activity}** to the {city} itinerary!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not save: {e}")
                else:
                    st.warning("Please enter an activity name.")

        with tab_edit:
            st.subheader("✏️ Edit Full Itinerary")
            st.caption("Make inline changes below, then click **Save All** to sync to Google Sheets.")
            if not itin_df.empty:
                edited = st.data_editor(
                    itin_df,
                    column_config={
                        "City":    st.column_config.SelectboxColumn("City",    options=CITIES),
                        "Vibe":    st.column_config.SelectboxColumn("Vibe",    options=VIBES),
                        "Weather": st.column_config.SelectboxColumn("Weather", options=WEATHER_OPTIONS),
                    },
                    num_rows="dynamic",
                    use_container_width=True,
                    key="itin_editor",
                )
                if st.button("💾 Save All Changes", type="primary"):
                    try:
                        overwrite_itinerary(edited)
                        st.success("Itinerary saved to Google Sheets!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not save: {e}")
            else:
                st.info("No activities yet. Add some in the **➕ Add Activity** tab.")
