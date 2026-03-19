"""
lean_oracle.py — Andy Solty Oracle Suite (standalone, single file)

Combines:
  • Market Oracle  — Liquidity Gate · Trend Anchor · 100-pt Confidence Score · Zombie Penalty
  • Becht Family Legacy — Historic Archive · Heritage History · Trip Logistics & Itinerary

Run:  streamlit run lean_oracle.py
Env:  TWELVE_DATA_API_KEY must be set
      Google Sheets integration via Replit Connectors (REPLIT_CONNECTORS_HOSTNAME)
"""

import os
import time
import requests
import gspread
import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path
from google.oauth2.credentials import Credentials

# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS — inlined helper (replaces sheets_helper.py)
# ══════════════════════════════════════════════════════════════════════════════

SHEET_ID_PATH = Path("artifacts/legacy-trip/.trip_sheet_id")
SHEET_TITLE = "Legacy & Logistics — Becht 2026 Trip"


def _get_access_token() -> str:
    hostname = os.environ.get("REPLIT_CONNECTORS_HOSTNAME")
    if not hostname:
        raise RuntimeError(
            "REPLIT_CONNECTORS_HOSTNAME not set — Google Sheets not wired."
        )
    repl_identity = os.environ.get("REPL_IDENTITY")
    web_repl_renewal = os.environ.get("WEB_REPL_RENEWAL")
    if repl_identity:
        x_token = f"repl {repl_identity}"
    elif web_repl_renewal:
        x_token = f"depl {web_repl_renewal}"
    else:
        raise RuntimeError(
            "No Replit identity token found (REPL_IDENTITY / WEB_REPL_RENEWAL)."
        )
    resp = requests.get(
        f"https://{hostname}/api/v2/connection?include_secrets=true&connector_names=google-sheet",
        headers={"Accept": "application/json", "X-Replit-Token": x_token},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    items = data.get("items", [])
    if not items:
        raise RuntimeError(
            "Google Sheet not connected. Set up the integration in Replit."
        )
    settings = items[0].get("settings", {})
    token = settings.get("access_token") or settings.get("oauth", {}).get(
        "credentials", {}
    ).get("access_token")
    if not token:
        raise RuntimeError("Access token missing from Google Sheet connection.")
    return token


def _get_gspread_client() -> gspread.Client:
    return gspread.authorize(Credentials(token=_get_access_token()))


def _get_or_create_worksheet(
    sh: gspread.Spreadsheet, title: str, rows: int = 200, cols: int = 10
):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
        return ws


def get_or_create_sheet():
    gc = _get_gspread_client()
    if SHEET_ID_PATH.exists():
        sheet_id = SHEET_ID_PATH.read_text().strip()
        if sheet_id:
            try:
                sh = gc.open_by_key(sheet_id)
                # Ensure all expected worksheets exist (idempotent)
                _ensure_worksheets(sh)
                return sh, sheet_id
            except Exception:
                pass
    sh = gc.create(SHEET_TITLE)
    sheet_id = sh.id
    SHEET_ID_PATH.write_text(sheet_id)
    _ensure_worksheets(sh, created_fresh=True)
    return sh, sheet_id


def _ensure_worksheets(sh: gspread.Spreadsheet, created_fresh: bool = False):
    if created_fresh:
        ws0 = sh.get_worksheet(0)
        ws0.update_title("LegacyNotes")
        ws0.append_row(["Timestamp", "Author", "Subject", "Story"])
    else:
        ws_notes = _get_or_create_worksheet(sh, "LegacyNotes")
        if ws_notes.row_count == 1 and not ws_notes.get_all_values():
            ws_notes.append_row(["Timestamp", "Author", "Subject", "Story"])

    for tab, header in [
        ("Itinerary", ["City", "Date", "Activity", "Vibe", "Weather", "Notes"]),
        ("Artifacts", ["Timestamp", "Name", "Description", "Backstory", "Photo Link"]),
        ("ReviewerLogs", ["Timestamp", "Author", "Type", "Note"]),
    ]:
        ws = _get_or_create_worksheet(sh, tab)
        if not ws.get_all_values():
            ws.append_row(header)


def get_sheet_url() -> str | None:
    if SHEET_ID_PATH.exists():
        sid = SHEET_ID_PATH.read_text().strip()
        if sid:
            return f"https://docs.google.com/spreadsheets/d/{sid}"
    return None


# ── Sheet read helpers (cached) ───────────────────────────────────────────────


@st.cache_data(ttl=30, show_spinner=False)
def load_notes():
    try:
        sh, _ = get_or_create_sheet()
        records = sh.worksheet("LegacyNotes").get_all_records()
        return (
            pd.DataFrame(records)
            if records
            else pd.DataFrame(columns=["Timestamp", "Author", "Subject", "Story"])
        )
    except Exception as e:
        st.warning(f"Could not load notes: {e}")
        return pd.DataFrame(columns=["Timestamp", "Author", "Subject", "Story"])


@st.cache_data(ttl=30, show_spinner=False)
def load_itinerary():
    try:
        sh, _ = get_or_create_sheet()
        records = sh.worksheet("Itinerary").get_all_records()
        return (
            pd.DataFrame(records)
            if records
            else pd.DataFrame(
                columns=["City", "Date", "Activity", "Vibe", "Weather", "Notes"]
            )
        )
    except Exception as e:
        st.warning(f"Could not load itinerary: {e}")
        return pd.DataFrame(
            columns=["City", "Date", "Activity", "Vibe", "Weather", "Notes"]
        )


@st.cache_data(ttl=30, show_spinner=False)
def load_artifacts():
    try:
        sh, _ = get_or_create_sheet()
        records = sh.worksheet("Artifacts").get_all_records()
        return (
            pd.DataFrame(records)
            if records
            else pd.DataFrame(
                columns=["Timestamp", "Name", "Description", "Backstory", "Photo Link"]
            )
        )
    except Exception:
        return pd.DataFrame(
            columns=["Timestamp", "Name", "Description", "Backstory", "Photo Link"]
        )


@st.cache_data(ttl=30, show_spinner=False)
def load_reviewer_logs():
    try:
        sh, _ = get_or_create_sheet()
        records = sh.worksheet("ReviewerLogs").get_all_records()
        return (
            pd.DataFrame(records)
            if records
            else pd.DataFrame(columns=["Timestamp", "Author", "Type", "Note"])
        )
    except Exception:
        return pd.DataFrame(columns=["Timestamp", "Author", "Type", "Note"])


# ── Sheet write helpers ───────────────────────────────────────────────────────


def save_note(author, subject, story):
    sh, _ = get_or_create_sheet()
    sh.worksheet("LegacyNotes").append_row(
        [datetime.now().isoformat(), author, subject, story]
    )
    load_notes.clear()


def save_artifact(name, desc, backstory, photo_link=""):
    sh, _ = get_or_create_sheet()
    sh.worksheet("Artifacts").append_row(
        [datetime.now().isoformat(), name, desc, backstory, photo_link]
    )
    load_artifacts.clear()


def save_activity(city, date_str, activity, vibe, weather, notes):
    sh, _ = get_or_create_sheet()
    sh.worksheet("Itinerary").append_row(
        [city, date_str, activity, vibe, weather, notes]
    )
    load_itinerary.clear()


def overwrite_itinerary(df):
    sh, _ = get_or_create_sheet()
    ws = sh.worksheet("Itinerary")
    ws.clear()
    ws.update([df.columns.tolist()] + df.fillna("").values.tolist())
    load_itinerary.clear()


def save_reviewer_log(author, log_type, note):
    sh, _ = get_or_create_sheet()
    sh.worksheet("ReviewerLogs").append_row(
        [datetime.now().isoformat(), author, log_type, note]
    )
    load_reviewer_logs.clear()


# ══════════════════════════════════════════════════════════════════════════════
# MARKET ORACLE — CONFIG, UNIVERSE & SCORING
# ══════════════════════════════════════════════════════════════════════════════

API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")
BASE_URL = "https://api.twelvedata.com"
CACHE_TTL_SECONDS = 15 * 60

UNIVERSE = [
    # TSX
    {
        "symbol": "RY",
        "exchange": "TSX",
        "sector": "Financials",
        "tier": "north_american",
        "sub": "Retail Banking",
    },
    {
        "symbol": "TD",
        "exchange": "TSX",
        "sector": "Financials",
        "tier": "north_american",
        "sub": "Retail Banking",
    },
    {
        "symbol": "ENB",
        "exchange": "TSX",
        "sector": "Energy",
        "tier": "north_american",
        "sub": "Midstream Infrastructure",
    },
    {
        "symbol": "CNQ",
        "exchange": "TSX",
        "sector": "Energy",
        "tier": "domestic",
        "sub": "Oil Sands E&P",
    },
    {
        "symbol": "BMO",
        "exchange": "TSX",
        "sector": "Financials",
        "tier": "north_american",
        "sub": "Retail Banking",
    },
    {
        "symbol": "BNS",
        "exchange": "TSX",
        "sector": "Financials",
        "tier": "north_american",
        "sub": "Retail Banking",
    },
    {
        "symbol": "CP",
        "exchange": "TSX",
        "sector": "Industrials",
        "tier": "north_american",
        "sub": "Rail Transportation",
    },
    {
        "symbol": "CNR",
        "exchange": "TSX",
        "sector": "Industrials",
        "tier": "north_american",
        "sub": "Rail Transportation",
    },
    {
        "symbol": "SU",
        "exchange": "TSX",
        "sector": "Energy",
        "tier": "domestic",
        "sub": "Integrated Oil & Gas",
    },
    {
        "symbol": "TRI",
        "exchange": "TSX",
        "sector": "Industrials",
        "tier": "global",
        "sub": "Business Information",
    },
    {
        "symbol": "SHOP",
        "exchange": "TSX",
        "sector": "Technology",
        "tier": "global",
        "sub": "E-Commerce Platform",
    },
    {
        "symbol": "OVV",
        "exchange": "TSX",
        "sector": "Energy",
        "tier": "north_american",
        "sub": "E&P Oil & Gas",
    },
    {
        "symbol": "HUT",
        "exchange": "TSX",
        "sector": "Technology",
        "tier": "domestic",
        "sub": "Bitcoin Mining",
    },
    {
        "symbol": "ATD",
        "exchange": "TSX",
        "sector": "Consumer Staples",
        "tier": "north_american",
        "sub": "Convenience Retail",
    },
    # NASDAQ
    {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "global",
        "sub": "Consumer Electronics",
    },
    {
        "symbol": "MSFT",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "global",
        "sub": "Enterprise Software & Cloud",
    },
    {
        "symbol": "NVDA",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "global",
        "sub": "AI Semiconductors",
    },
    {
        "symbol": "AMZN",
        "exchange": "NASDAQ",
        "sector": "Consumer Discretionary",
        "tier": "global",
        "sub": "E-Commerce & Cloud",
    },
    {
        "symbol": "META",
        "exchange": "NASDAQ",
        "sector": "Communication Services",
        "tier": "global",
        "sub": "Social Media",
    },
    {
        "symbol": "GOOGL",
        "exchange": "NASDAQ",
        "sector": "Communication Services",
        "tier": "global",
        "sub": "Search & Advertising",
    },
    {
        "symbol": "TSLA",
        "exchange": "NASDAQ",
        "sector": "Consumer Discretionary",
        "tier": "global",
        "sub": "Electric Vehicles",
    },
    {
        "symbol": "AVGO",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "global",
        "sub": "Semiconductors",
    },
    {
        "symbol": "COST",
        "exchange": "NASDAQ",
        "sector": "Consumer Staples",
        "tier": "global",
        "sub": "Warehouse Retail",
    },
    {
        "symbol": "AMD",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "global",
        "sub": "Semiconductors",
    },
    {
        "symbol": "NFLX",
        "exchange": "NASDAQ",
        "sector": "Communication Services",
        "tier": "global",
        "sub": "Streaming Media",
    },
    {
        "symbol": "MSTR",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "domestic",
        "sub": "Bitcoin Treasury",
    },
    {
        "symbol": "PLTR",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "global",
        "sub": "AI & Data Analytics",
    },
    {
        "symbol": "PANW",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "global",
        "sub": "Cybersecurity",
    },
    {
        "symbol": "CRWD",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "global",
        "sub": "Cybersecurity",
    },
    {
        "symbol": "HON",
        "exchange": "NASDAQ",
        "sector": "Industrials",
        "tier": "global",
        "sub": "Industrial Automation",
    },
    {
        "symbol": "STRC",
        "exchange": "NASDAQ",
        "sector": "Technology",
        "tier": "domestic",
        "sub": "Bitcoin Treasury",
    },
    # NYSE
    {
        "symbol": "ORCL",
        "exchange": "NYSE",
        "sector": "Technology",
        "tier": "global",
        "sub": "Enterprise Database & Cloud",
    },
    {
        "symbol": "CCJ",
        "exchange": "NYSE",
        "sector": "Energy",
        "tier": "global",
        "sub": "Uranium Mining",
    },
    {
        "symbol": "V",
        "exchange": "NYSE",
        "sector": "Financials",
        "tier": "global",
        "sub": "Payment Networks",
    },
    {
        "symbol": "XOM",
        "exchange": "NYSE",
        "sector": "Energy",
        "tier": "global",
        "sub": "Integrated Oil Major",
    },
    {
        "symbol": "LLY",
        "exchange": "NYSE",
        "sector": "Healthcare",
        "tier": "global",
        "sub": "Pharmaceuticals",
    },
    {
        "symbol": "UNH",
        "exchange": "NYSE",
        "sector": "Healthcare",
        "tier": "north_american",
        "sub": "Managed Healthcare",
    },
    {
        "symbol": "CAT",
        "exchange": "NYSE",
        "sector": "Industrials",
        "tier": "global",
        "sub": "Heavy Equipment",
    },
    {
        "symbol": "WMT",
        "exchange": "NYSE",
        "sector": "Consumer Staples",
        "tier": "global",
        "sub": "Mass Market Retail",
    },
]

ZOMBIE_SYMBOLS = {"MSTR", "STRC", "HUT", "MARA", "SNOW", "OKLO", "SMR", "DNN"}


def td_symbol(ticker: dict) -> str:
    return (
        f"{ticker['symbol']}:TSX" if ticker["exchange"] == "TSX" else ticker["symbol"]
    )


def safe_float(val) -> float:
    try:
        return float(val) if val not in (None, "", "N/A") else 0.0
    except (TypeError, ValueError):
        return 0.0


def fetch_batch_quotes(tickers: list[dict]) -> dict:
    symbols_str = ",".join(td_symbol(t) for t in tickers)
    url = f"{BASE_URL}/quote?symbol={symbols_str}&apikey={API_KEY}"
    try:
        resp = requests.get(url, timeout=30)
        data = resp.json()
    except Exception as e:
        st.error(f"Quote API error: {e}")
        return {}
    if isinstance(data, dict) and data.get("code") == 429:
        st.warning(
            "⚠️ Twelve Data rate limit hit — try again later or upgrade your plan."
        )
        return {}
    result = {}
    if len(tickers) == 1:
        q = data if isinstance(data, dict) else {}
        if q.get("close") and not q.get("code"):
            result[tickers[0]["symbol"]] = q
    else:
        for ticker in tickers:
            key = td_symbol(ticker)
            q = data.get(key, {})
            if q.get("close") and not q.get("code"):
                result[ticker["symbol"]] = q
    return result


def fetch_sma50(td_sym: str) -> float | None:
    url = (
        f"{BASE_URL}/sma?symbol={td_sym}"
        f"&interval=1day&time_period=50&outputsize=1&apikey={API_KEY}"
    )
    try:
        resp = requests.get(url, timeout=15)
        data = resp.json()
        if data.get("code") == 429:
            return None
        values = data.get("values", [])
        if values:
            return safe_float(values[0].get("sma"))
    except Exception:
        pass
    return None


def confidence_score(
    above_dma: bool,
    pct_vs_dma: float | None,
    volume: float,
    avg_volume: float,
    change_pct: float,
    tier: str,
    is_zombie: bool,
) -> int:
    score = 0
    if above_dma:
        score += 25
        if pct_vs_dma is not None and pct_vs_dma > 5:
            score += 5
    elif pct_vs_dma is not None and pct_vs_dma >= -1:
        score += 15
    if tier == "global":
        score += 25
    elif tier == "north_american":
        score += 17
    elif tier == "domestic":
        score += 10
    if avg_volume > 0:
        ratio = volume / avg_volume
        if ratio > 2:
            score += 20
        elif ratio > 1.5:
            score += 15
        elif ratio > 1:
            score += 10
        else:
            score += 5
    elif volume > 500_000:
        score += 10
    if change_pct > 3:
        score += 15
    elif change_pct > 1:
        score += 10
    elif change_pct > 0:
        score += 5
    if is_zombie:
        score -= 20
    return max(0, min(100, score))


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def run_scan(exchange_filter: str) -> tuple[list[dict], list[dict], int]:
    universe = (
        UNIVERSE
        if exchange_filter == "ALL"
        else [t for t in UNIVERSE if t["exchange"] == exchange_filter]
    )
    quotes = fetch_batch_quotes(universe)
    passed, rejected = [], []
    liquidity_ok = []

    for ticker in universe:
        q = quotes.get(ticker["symbol"])
        if not q:
            continue
        price = safe_float(q.get("close"))
        volume = safe_float(q.get("volume"))
        if price < 2.0 or volume < 100_000:
            reasons = []
            if price < 2.0:
                reasons.append(f"Price (${price:.2f}) below $2.00 minimum")
            if volume < 100_000:
                reasons.append(f"Volume ({int(volume):,}) below 100,000 minimum")
            rejected.append(
                {
                    "symbol": ticker["symbol"],
                    "exchange": ticker["exchange"],
                    "sector": ticker["sector"],
                    "price": price,
                    "volume": int(volume),
                    "50-DMA": None,
                    "% vs DMA": None,
                    "score": None,
                    "status": "❌ LIQUIDITY",
                    "zombie": False,
                    "reason": "; ".join(reasons),
                }
            )
            continue
        liquidity_ok.append((ticker, q, price, volume))

    for ticker, q, price, volume in liquidity_ok:
        avg_vol = safe_float(q.get("average_volume"))
        change_pct = safe_float(q.get("percent_change"))
        is_zombie = ticker["symbol"] in ZOMBIE_SYMBOLS
        sma50 = fetch_sma50(td_symbol(ticker))
        time.sleep(0.12)

        if sma50 is None or sma50 == 0:
            pct_vs_dma, above_dma = None, False
        else:
            pct_vs_dma = ((price - sma50) / sma50) * 100
            above_dma = pct_vs_dma >= -1.0

        score = confidence_score(
            above_dma,
            pct_vs_dma,
            volume,
            avg_vol,
            change_pct,
            ticker["tier"],
            is_zombie,
        )
        row = {
            "symbol": ticker["symbol"],
            "exchange": ticker["exchange"],
            "sector": ticker["sector"],
            "sub": ticker["sub"],
            "tier": ticker["tier"],
            "price": price,
            "volume": int(volume),
            "50-DMA": round(sma50, 2) if sma50 else None,
            "% vs DMA": round(pct_vs_dma, 2) if pct_vs_dma is not None else None,
            "score": score,
            "zombie": is_zombie,
            "change%": round(change_pct, 2),
        }
        if above_dma:
            row["status"] = "✅ PASSED" + (" ⚠️ ZOMBIE" if is_zombie else "")
            row["reason"] = ""
            passed.append(row)
        else:
            row["status"] = "📉 TREND FAIL"
            row["reason"] = (
                f"Price (${price:.2f}) below 50-DMA (${sma50:.2f}) [{pct_vs_dma:.2f}%]"
                if pct_vs_dma is not None
                else "50-DMA unavailable"
            )
            rejected.append(row)

    passed.sort(key=lambda r: r["score"], reverse=True)
    rejected.sort(
        key=lambda r: (
            0 if r["status"] == "📉 TREND FAIL" else 1,
            -(r["% vs DMA"] or -999),
        )
    )
    return passed, rejected, len(universe)


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
            "(SD) and senior German officials systematically requisitioned these villas "
            "— they were the most luxurious in the city. Andy's mother's memory of being "
            "forced out is part of the broader *vorderingen* (requisitions) that displaced "
            "hundreds of Amsterdam families. Standing here is a direct encounter with "
            "that history."
        ),
        "maps": "https://maps.google.com/?q=Koningslaan+70,+Amsterdam",
    },
]

HERITAGE_IMAGES = {
    "clock_full": "",
    "clock_face": "",
    "guilder_coin": "",
}


def get_drive_url(url: str) -> str:
    if "drive.google.com" in url and "/file/d/" in url:
        file_id = url.split("/file/d/")[1].split("/")[0]
        return f"https://drive.google.com/uc?id={file_id}"
    return url


VIBES = ["Outdoor/Heritage", "Coffee/Food", "Legacy Visit"]
WEATHER_OPTIONS = ["Either", "☀️ Sunny Day", "🌧️ Rain Day"]
VIBE_ICONS = {"Outdoor/Heritage": "🌿", "Coffee/Food": "☕", "Legacy Visit": "🏛️"}
CITIES = ["Amsterdam", "Tuscany", "Rome"]

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
        try:
            sheet_url = get_sheet_url()
            if sheet_url:
                st.success("📋 Google Sheet synced")
                st.link_button("Open Shared Sheet", sheet_url, use_container_width=True)
            else:
                st.info("📋 Sheet created on first save")
        except Exception:
            st.info("📋 Sheet created on first save")
        st.divider()
        st.caption("🇳🇱 Amsterdam · 🇮🇹 Tuscany · 🇮🇹 Rome · 🇭🇺 Budapest")

    else:
        st.markdown("### 📡 Market Oracle")
        exchange = st.selectbox("Exchange", ["ALL", "NASDAQ", "NYSE", "TSX"])
        top_n = st.number_input("Top N results", min_value=1, max_value=40, value=10)
        st.divider()
        if st.button("🔄 Refresh scan", type="secondary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.divider()
        st.caption("Liquidity Gate · Trend Anchor · 100-pt Score · Zombie Penalty")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW A — MARKET ORACLE
# ══════════════════════════════════════════════════════════════════════════════

if "Oracle" in app_mode:
    st.title("📡 Andy Solty Market Oracle")
    st.caption(
        "Liquidity Gate · Trend Anchor · 100-pt Confidence Score · Zombie Penalty"
    )

    if not API_KEY:
        st.error("TWELVE_DATA_API_KEY environment variable is not set.")
        st.stop()

    with st.spinner(f"Scanning {exchange} universe via Twelve Data…"):
        passed, rejected, total = run_scan(exchange)

    scanned_at = datetime.now().strftime("%H:%M:%S")
    st.markdown(
        f"**Scanned:** {total} tickers &nbsp;|&nbsp; "
        f"**Passed:** {len(passed)} &nbsp;|&nbsp; "
        f"**Rejected:** {len(rejected)} &nbsp;|&nbsp; "
        f"**As of:** {scanned_at} &nbsp;*(cached 15 min)*"
    )

    # ── Conviction Feed ───────────────────────────────────────────────────────
    st.subheader(f"🏆 Conviction Feed — Top {min(top_n, len(passed))}")

    if passed:
        top = passed[:top_n]
        df_pass = pd.DataFrame(
            [
                {
                    "Rank": i + 1,
                    "Symbol": r["symbol"],
                    "Exchange": r["exchange"],
                    "Sector": r["sub"],
                    "Price": r["price"],
                    "50-DMA": r["50-DMA"],
                    "% vs DMA": r["% vs DMA"],
                    "Volume": r["volume"],
                    "Chg %": r["change%"],
                    "Score": r["score"],
                    "Zombie": "⚠️" if r["zombie"] else "",
                }
                for i, r in enumerate(top)
            ]
        )

        st.dataframe(
            df_pass.style.format(
                {
                    "Price": "${:.2f}",
                    "50-DMA": lambda v: f"${v:.2f}" if v else "—",
                    "% vs DMA": lambda v: f"+{v:.2f}%"
                    if v and v >= 0
                    else (f"{v:.2f}%" if v else "—"),
                    "Volume": "{:,.0f}",
                    "Chg %": "{:+.2f}%",
                    "Score": "{:.0f}",
                }
            )
            .background_gradient(subset=["Score"], cmap="YlGn")
            .applymap(lambda v: "color: #e05" if v == "⚠️" else "", subset=["Zombie"]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No stocks passed both gates in this scan.")

    # ── Rejected / Near Misses ────────────────────────────────────────────────
    with st.expander(f"📋 Rejected / Near Misses ({len(rejected)} stocks)"):
        if rejected:
            df_rej = pd.DataFrame(
                [
                    {
                        "Symbol": r["symbol"],
                        "Exchange": r["exchange"],
                        "Price": r["price"],
                        "50-DMA": r.get("50-DMA"),
                        "% vs DMA": r.get("% vs DMA"),
                        "Volume": r["volume"],
                        "Status": r["status"],
                        "Reason": r.get("reason", ""),
                    }
                    for r in rejected
                ]
            )
            st.dataframe(
                df_rej.style.format(
                    {
                        "Price": "${:.2f}",
                        "50-DMA": lambda v: f"${v:.2f}" if v else "—",
                        "% vs DMA": lambda v: f"{v:.2f}%" if v is not None else "—",
                        "Volume": "{:,.0f}",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.write("All tickers passed.")

    # ── Scoring legend ────────────────────────────────────────────────────────
    with st.expander("ℹ️ Scoring breakdown"):
        st.markdown("""
| Component | Max pts | Rules |
|---|---|---|
| **Trend Anchor** | 30 | 25 pts above 50-DMA; +5 bonus if >5% above; 15 pts if within −1% |
| **Sector Leadership** | 25 | Global = 25 · North American = 17 · Domestic = 10 |
| **Volume Strength** | 20 | >2× avg = 20 · >1.5× = 15 · >1× = 10 · else = 5 |
| **Price Momentum** | 15 | >3% day = 15 · >1% = 10 · >0% = 5 |
| **Market Cap** | 10 | >$100B = 10 · >$10B = 7 · >$1B = 4 *(not scored in lean mode)* |
| **Zombie Penalty** | −20 | Structurally negative GAAP EPS |

**Liquidity Gate** (must pass both): Price ≥ $2.00 · Volume ≥ 100,000  
**Trend Anchor**: Price ≥ 50-DMA (tolerance: within −1%)
""")

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
            [
                "Market Observation",
                "Conviction Review",
                "Research Note",
                "Trade Idea",
                "Other",
            ],
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
        rl_col1, rl_col2 = st.columns([1, 5])
        with rl_col1:
            if st.button("🔄 Refresh", key="rl_refresh"):
                load_reviewer_logs.clear()
                st.rerun()
        logs_df = load_reviewer_logs()
        if not logs_df.empty:
            for _, row in logs_df.iloc[::-1].iterrows():
                with st.container(border=True):
                    c1, c2 = st.columns([1, 4])
                    with c1:
                        st.markdown(f"**{row.get('Author', '?')}**")
                        st.caption(row.get("Type", ""))
                        ts = row.get("Timestamp", "")
                        if ts:
                            try:
                                st.caption(
                                    datetime.fromisoformat(ts).strftime(
                                        "%b %d, %Y %H:%M"
                                    )
                                )
                            except Exception:
                                st.caption(str(ts)[:16])
                    with c2:
                        st.write(row.get("Note", ""))
        else:
            st.info("No log entries yet.")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW B — BECHT FAMILY LEGACY
# ══════════════════════════════════════════════════════════════════════════════

else:
    st.title("🧳 Becht Legacy & Logistics 2026")
    st.caption("Andy & Sue · Europe Trip")

    view = legacy_view  # aliased from sidebar

    # ══════════════════════════════════════════════════════════════════════════
    # VIEW 1 — HISTORIC LEGACY ARCHIVE
    # ══════════════════════════════════════════════════════════════════════════

    if "Archive" in view:
        st.title("🏛️ Historic Legacy Archive")
        st.caption("🇳🇱  Becht Family · Amsterdam Heritage Trail")

        tab_landmarks, tab_gallery, tab_history = st.tabs(
            [
                "📍 Family Landmarks",
                "🖼️ Artifact Gallery",
                "📜 Family History",
            ]
        )

        # ── FAMILY LANDMARKS ─────────────────────────────────────────────────
        with tab_landmarks:
            st.subheader("📍 Family Landmarks")
            st.write(
                "Key Amsterdam addresses connected to the Becht family. "
                "Visit these on your heritage walk."
            )
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
                            st.markdown(
                                f"> *{lm['inscription']}*  \n"
                                f"> \u201c{lm['inscription_translation']}\u201d"
                            )
                        st.caption("SIGNIFICANCE")
                        st.write(lm["description"])
                        st.link_button(
                            "📍 Open in Google Maps",
                            lm["maps"],
                            use_container_width=True,
                            type="primary",
                        )

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

        # ── ARTIFACT GALLERY ──────────────────────────────────────────────────
        with tab_gallery:
            st.subheader("🖼️ Artifact Gallery")
            st.write(
                "Family heirlooms from the Becht collection. "
                "Data and photo links are pulled live from the **Artifacts** tab in Google Sheets."
            )

            gal_col_r, gal_col_add = st.columns([1, 4])
            with gal_col_r:
                if st.button("🔄 Refresh", key="gallery_refresh"):
                    load_artifacts.clear()
                    st.rerun()

            art_df = load_artifacts()

            if art_df.empty:
                st.info("No artifacts in the sheet yet. Add one using the form below.")
            else:
                for idx, row in art_df.iterrows():
                    name = str(row.get("Name", "")).strip()
                    description = str(row.get("Description", "")).strip()
                    backstory = str(row.get("Backstory", "")).strip()
                    photo_link = str(row.get("Photo Link", "")).strip()

                    with st.container(border=True):
                        img_col, text_col = st.columns([1, 2])
                        with img_col:
                            if photo_link:
                                try:
                                    st.image(
                                        get_drive_url(photo_link),
                                        use_container_width=True,
                                    )
                                except Exception:
                                    st.warning(
                                        "Could not load image — check the Drive link."
                                    )
                            else:
                                st.info(
                                    "No photo linked.\nAdd a Google Drive link in the sheet's **Photo Link** column."
                                )
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

            # ── ADD ARTIFACT FORM ────────────────────────────────────────────
            with st.expander("➕ Add New Artifact"):
                art_name = st.text_input(
                    "Artifact Name", placeholder="e.g., Becht Family Bible"
                )
                art_desc = st.text_input(
                    "Brief Description",
                    placeholder="e.g., 19th century, currently with Uncle Jan",
                )
                art_link = st.text_input(
                    "Photo Link (Google Drive share URL)",
                    placeholder="https://drive.google.com/file/d/…",
                )
                art_story = st.text_area("Backstory", height=100)
                if st.button("💾 Save Artifact to Sheet", type="primary"):
                    if art_name.strip():
                        try:
                            save_artifact(
                                art_name.strip(),
                                art_desc.strip(),
                                art_story.strip(),
                                art_link.strip(),
                            )
                            st.success("Artifact saved to Google Sheets!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not save: {e}")
                    else:
                        st.warning("Please enter an artifact name.")

        # ── FAMILY HISTORY ────────────────────────────────────────────────────
        with tab_history:
            st.subheader("📜 Family History")
            st.write(
                "Record stories, memories, and research notes about the Becht family. "
                "Both Andy and Sue can add entries — all synced in real time via Google Sheets."
            )

            with st.expander("➕ Add a New Story", expanded=True):
                col_a, col_b = st.columns(2)
                with col_a:
                    author = st.selectbox("Author", ["Andy", "Sue"])
                with col_b:
                    subject = st.selectbox(
                        "About",
                        [
                            "Andre Becht (Grandfather)",
                            "Herman Becht (Great-Grandfather)",
                            "Becht Family — General",
                            "Amsterdam Observations",
                            "Other",
                        ],
                    )
                story = st.text_area(
                    "Story / Memory / Note",
                    placeholder=(
                        "Write a memory, research finding, family story, or observation here…\n\n"
                        "Examples:\n"
                        "- A story Andre told about growing up near Herengracht\n"
                        "- Research note from the City Archives\n"
                        "- A detail about the grandfather clock and Zandvoort"
                    ),
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
                filter_sub = st.selectbox(
                    "Filter",
                    [
                        "All Subjects",
                        "Andre Becht (Grandfather)",
                        "Herman Becht (Great-Grandfather)",
                        "Becht Family — General",
                        "Amsterdam Observations",
                        "Other",
                    ],
                    label_visibility="collapsed",
                )

            notes_df = load_notes()
            if filter_sub != "All Subjects":
                notes_df = notes_df[notes_df.get("Subject", pd.Series()) == filter_sub]

            if not notes_df.empty:
                for _, row in notes_df.iloc[::-1].iterrows():
                    with st.container(border=True):
                        c1, c2 = st.columns([1, 4])
                        with c1:
                            st.markdown(f"**{row.get('Author', '?')}**")
                            st.caption(row.get("Subject", ""))
                            ts = row.get("Timestamp", "")
                            if ts:
                                try:
                                    st.caption(
                                        datetime.fromisoformat(ts).strftime("%b %d, %Y")
                                    )
                                except Exception:
                                    st.caption(str(ts)[:10])
                        with c2:
                            st.write(row.get("Story", ""))
            else:
                st.info("No stories yet — add your first note above!")

            st.divider()
            st.subheader("🔍 City Archives & Research Links")
            st.write(
                "The Amsterdam City Archives hold digitized records going back to the 1600s — "
                "an essential resource for tracing the Becht family."
            )

            with st.container(border=True):
                st.markdown("#### Search for H.J.W. Becht")
                search_term = st.text_input("Search keyword", value="H.J.W. Becht")
                search_url = f"https://archief.amsterdam/indexen/search?search={search_term.replace(' ', '+')}"
                c1, c2 = st.columns(2)
                with c1:
                    st.link_button(
                        "🔍 Search Amsterdam Stadsarchief",
                        search_url,
                        use_container_width=True,
                        type="primary",
                    )
                with c2:
                    st.link_button(
                        "🏛️ Browse Full Archive",
                        "https://archief.amsterdam",
                        use_container_width=True,
                    )

            st.divider()
            with st.container(border=True):
                st.markdown("#### 🪙 Research: André Becht's 1973 Recognition")
                res_col1, res_col2 = st.columns(2)
                with res_col1:
                    with st.container(border=True):
                        st.markdown("**🥇 Lintjes.nl — Royal Honours**")
                        st.link_button(
                            "🔍 Search Lintjes.nl",
                            "https://lintjes.nl/decorandi?q=Becht",
                            use_container_width=True,
                            type="primary",
                        )
                    with st.container(border=True):
                        st.markdown("**📰 Delpher.nl — Dutch Newspapers**")
                        st.link_button(
                            "🔍 Search Delpher",
                            "https://www.delpher.nl/nl/kranten/results?query=Andr%C3%A9+Becht&facets%5Bperiode%5D%5B%5D=2%7C20e+eeuw%7C1970-1979%7C",
                            use_container_width=True,
                            type="primary",
                        )
                with res_col2:
                    with st.container(border=True):
                        st.markdown("**🏛️ Nationaal Archief — The Hague**")
                        st.link_button(
                            "🔍 Search Nationaal Archief",
                            "https://www.nationaalarchief.nl/onderzoeken/zoeken?q=Becht",
                            use_container_width=True,
                        )
                    with st.container(border=True):
                        st.markdown("**📚 KB Catalogue — Becht Publishing**")
                        st.link_button(
                            "🔍 Search KB Catalogue",
                            "https://opc.kb.nl/DB=1/LNG=NE/CMD?ACT=SRCHA&IKT=4&SRT=YOP&TRM=becht",
                            use_container_width=True,
                        )

            st.divider()
            cols = st.columns(3)
            links = [
                (
                    "🏠 Herengracht 172",
                    "Historical ownership of the publishing address",
                    "https://archief.amsterdam/indexen/search?search=Herengracht+172",
                ),
                (
                    "📚 Becht Publishing",
                    "Dutch publishers & booksellers records",
                    "https://archief.amsterdam/indexen/search?search=Becht+uitgeverij",
                ),
                (
                    "🗺️ Zandvoort Records",
                    "Zandvoort compensation & property records",
                    "https://archief.amsterdam/indexen/search?search=Zandvoort+Becht",
                ),
            ]
            for col, (label, desc, url) in zip(cols, links):
                with col:
                    with st.container(border=True):
                        st.markdown(f"**{label}**")
                        st.caption(desc)
                        st.link_button("Search →", url, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # VIEW 2 — EUROPE HERITAGE & HISTORY
    # ══════════════════════════════════════════════════════════════════════════

    elif "Heritage" in view:
        st.title("🌍 Europe Heritage & History")
        st.caption(
            "🇳🇱 Becht Family · 🇩🇪 Publishing Dynasty · 🇭🇺 Hungarian Roots — A Family Chronicle"
        )

        tab_becht, tab_publishing, tab_hungary = st.tabs(
            [
                "🏚️ The Becht Family",
                "📚 Publishing Legacy",
                "🇭🇺 Budapest & Hungarian Heritage",
            ]
        )

        with tab_becht:
            st.subheader("🏚️ The Becht Family — Dutch Roots & History")
            st.write(
                "The story of a family shaped by Amsterdam's Golden Age, Dutch ingenuity, "
                "World War II resilience, and a century of publishing excellence."
            )

            with st.container(border=True):
                st.markdown("### 🌳 Family Chronicle")
                st.markdown(
                    """
**H.J.W. Becht — Founder of the Publishing House**

The Becht name is inseparable from Dutch intellectual and cultural life. H.J.W. Becht
established the family publishing house in Amsterdam during the late 19th century,
choosing the prestigious Herengracht as their address — a deliberate statement of
the family's ambitions in the city's commercial and cultural world.

The motto carved into Huis Bartolotti at Herengracht 172 — *Ingenio et Assiduo Labore*,
"By Ingenuity and Diligent Labor" — reads as if written for a publishing family. It
captures exactly the spirit the Bechts brought to their work over generations.
                    """
                )

            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                with st.container(border=True):
                    st.markdown("#### 👴 Herman Becht — Great-Grandfather")
                    st.markdown(
                        """
Herman Becht built the publishing house into one of Amsterdam's distinguished
imprints. Under his stewardship, Becht Publishing grew from a local enterprise
into a respected name in Dutch cultural publishing.

He raised his family in Amsterdam during a period of tremendous change — the late
19th and early 20th centuries saw the Netherlands industrialize, the Dutch empire
shift, and Amsterdam transform into a modern European capital.

**Key Research:**
- Amsterdam Stadsarchief holds business registration records for Becht Publishing
- Property records along Herengracht may document the family's canal-side presence
- The KB (Royal Library) catalogue lists all titles published under the Becht imprint
                        """
                    )
                    st.link_button(
                        "🔍 Search Stadsarchief for Herman Becht",
                        "https://archief.amsterdam/indexen/search?search=Herman+Becht",
                        use_container_width=True,
                    )

            with col2:
                with st.container(border=True):
                    st.markdown("#### 👨 André Becht — Grandfather")
                    st.markdown(
                        """
André Becht is the grandfather at the heart of this trip. He guided the publishing
house through some of the most turbulent decades in Dutch history — including the
Nazi occupation of Amsterdam, the requisitioning of the family home on Koningslaan,
and the postwar rebuilding of Dutch cultural life.

He was formally recognized by the Dutch state around 1973 — either with a Royal
Honour (*Koninklijke Onderscheiding*) or a Royal Warrant (*Koninklijk Predikaat*)
for the publishing house — evidenced by the *'s Rijks Munt Utrecht* presentation
coin now in the family's possession in Oakville, Ontario.

**What we know:**
- He received a state presentation coin from the Royal Dutch Mint in 1973
- He retired around the same period — the honour likely marked his career's end
- The Zandvoort family property was demolished by the Nazis; post-war *Rechtsherstel*
  compensation included the 1700s grandfather clock now in Oakville
                        """
                    )
                    st.link_button(
                        "🏅 Search Lintjes.nl for André Becht",
                        "https://lintjes.nl/decorandi?q=Becht",
                        use_container_width=True,
                    )

            st.divider()
            with st.container(border=True):
                st.markdown("### 🏠 The Koningslaan Years — World War II")
                st.markdown(
                    """
**Koningslaan 70, Amsterdam — Mother's Childhood Home**

Koningslaan sits along the edge of Vondelpark in the Oud-Zuid district — one of
Amsterdam's most prestigious residential addresses. The Becht family lived here
before and during the war.

In 1940, Nazi Germany occupied the Netherlands. The German Sicherheitsdienst (SD)
and senior Wehrmacht officers systematically requisitioned the finest villas in
Amsterdam for their own use. The Becht family, like hundreds of other Amsterdam
families on these grand streets, were forced to leave with almost no notice.

This is not abstract history. Your mother lived this. Standing at Koningslaan 70
during this trip is a direct encounter with what the family endured.

**Post-War:** After liberation in 1945, the Dutch government's *Rechtsherstel*
(Restoration of Rights) program sought to compensate families whose properties
had been seized or destroyed. The grandfather clock — Zandvoort property compensation
— is the family's physical connection to this reckoning.
                    """
                )
                st.link_button(
                    "📍 Koningslaan 70 on Google Maps",
                    "https://maps.google.com/?q=Koningslaan+70,+Amsterdam",
                    use_container_width=True,
                )

            st.divider()
            with st.container(border=True):
                st.markdown("### 📸 Add Family Photos & Stories")
                st.write(
                    "Use the **🏛️ Historic Legacy Archive** to add family stories and artifact photos. "
                    "This page focuses on the broader historical narrative — the Archive is where "
                    "you record your personal memories and discoveries."
                )

        with tab_publishing:
            st.subheader("📚 Becht Publishing House — A Dutch Cultural Legacy")
            st.write(
                "For over a century, the name Becht on a book spine was a mark of quality "
                "in Dutch publishing. This tab documents the publishing house's history, "
                "output, and lasting cultural significance."
            )

            with st.container(border=True):
                st.markdown("### 🏢 Herengracht 172 — The Publishing Address")
                st.markdown(
                    """
**Huis Bartolotti — Built 1617 by Hendrick de Keyser**

The Becht family chose Herengracht 172 as the address of their publishing business.
This was not an ordinary choice. Huis Bartolotti is one of Amsterdam's landmark
canal houses — designed by Hendrick de Keyser, the most celebrated architect of
the Dutch Golden Age, and situated on the *Golden Bend* of the Herengracht, where
Amsterdam's wealthiest merchant families lived and worked.

The building's Latin inscription, *Ingenio et Assiduo Labore* ("By Ingenuity and
Diligent Labor"), resonates deeply for a publishing family whose entire enterprise
rested on intellectual craft and persistent effort.

**Visiting Herengracht 172** is one of the most powerful moments planned for this trip.
You can stand where André and Herman worked — at one of Amsterdam's finest addresses,
on the canal that defined Dutch Golden Age ambition.
                    """
                )
                col_m1, col_m2 = st.columns(2)
                with col_m1:
                    st.link_button(
                        "📍 Open in Google Maps",
                        "https://maps.google.com/?q=Herengracht+172,+Amsterdam",
                        use_container_width=True,
                        type="primary",
                    )
                with col_m2:
                    st.link_button(
                        "🏛️ Huis Bartolotti — Wikipedia",
                        "https://en.wikipedia.org/wiki/Huis_Bartolotti",
                        use_container_width=True,
                    )

            st.divider()
            with st.container(border=True):
                st.markdown("### 📖 What Becht Published")
                st.markdown(
                    """
Becht Publishing built its reputation across several categories that defined
Dutch cultural output in the 20th century:

- **Art & Architecture Books** — High-quality illustrated editions on Dutch and
  European art, reflecting Amsterdam's position as a European cultural capital
- **Natural History & Science** — A significant imprint in Dutch natural history
  publishing, with illustrated reference works
- **Literature & Cultural Writing** — Dutch-language literary works and essays
- **Travel & Geography** — Books reflecting the Netherlands' global trading legacy

The full Becht catalogue is preserved in the **Koninklijke Bibliotheek** (Royal Library)
in The Hague. Searching 'Becht' in the KB digital catalogue returns the complete
publishing record — a remarkable window into what André and Herman built.
                    """
                )
                st.link_button(
                    "📚 Browse Becht Titles in KB Catalogue",
                    "https://opc.kb.nl/DB=1/LNG=NE/CMD?ACT=SRCHA&IKT=4&SRT=YOP&TRM=becht",
                    use_container_width=True,
                    type="primary",
                )

            st.divider()
            with st.container(border=True):
                st.markdown("### 🏅 State Recognition — The 1973 Royal Honour")
                st.markdown(
                    """
The *'s Rijks Munt Utrecht* presentation coin in the family's possession is a
significant historical artifact. These were not sold or available for purchase —
they were distributed exclusively by the Royal Dutch Mint on behalf of the Dutch
state to individuals and institutions receiving formal recognition.

**Two likely explanations for André's 1973 recognition:**

1. **Koninklijke Onderscheiding (Royal Honour)** — André may have received the
   *Ridder in de Orde van Oranje-Nassau* or *Orde van de Nederlandse Leeuw*,
   awarded to distinguished publishers and businesspeople. The Queen Juliana
   Silver Jubilee (1973) was a common moment for such recognitions.

2. **Koninklijk Predikaat (Royal Warrant)** — Becht Publishing may have received
   the "By Appointment to the Royal House" designation, granted to companies of
   cultural significance. Publishing houses with long histories of quality output
   regularly received this during jubilee years.

**Research these archives to confirm:**
                    """
                )
                r1, r2, r3 = st.columns(3)
                with r1:
                    st.link_button(
                        "🥇 Lintjes.nl Registry",
                        "https://lintjes.nl/decorandi?q=Becht",
                        use_container_width=True,
                        type="primary",
                    )
                with r2:
                    st.link_button(
                        "📰 Delpher Newspapers",
                        "https://www.delpher.nl/nl/kranten/results?query=Andr%C3%A9+Becht&facets%5Bperiode%5D%5B%5D=2%7C20e+eeuw%7C1970-1979%7C",
                        use_container_width=True,
                    )
                with r3:
                    st.link_button(
                        "🏛️ Nationaal Archief",
                        "https://www.nationaalarchief.nl/onderzoeken/zoeken?q=Becht",
                        use_container_width=True,
                    )

            st.divider()
            with st.container(border=True):
                st.markdown("### 🌍 Becht in the Broader World of Dutch Publishing")
                st.markdown(
                    """
Dutch publishing in the 20th century punched well above its weight globally.
The Netherlands produced world-class imprints in art, science, and literature
that were distributed internationally. Becht was part of this tradition — a
family firm that prioritized quality and cultural purpose over volume.

**Key context:**
- **Amsterdam as publishing capital** — The Herengracht neighbourhood housed
  multiple distinguished publishers; Becht was among the most respected
- **Post-war Dutch cultural reconstruction** — Publishers played a vital role
  in rebuilding Dutch cultural identity after the occupation; Becht's continued
  operation in the 1940s–1970s reflects genuine resilience
- **The Royal Library connection** — Becht titles donated or deposited to the
  KB represent a permanent record of the family's contribution to Dutch culture

*This is your mother's family story — not just personal history but a thread
woven into the fabric of Dutch cultural life for over a century.*
                    """
                )

        with tab_hungary:
            st.subheader("🇭🇺 Budapest & Hungarian Heritage — Future Trip")
            st.info(
                "**Planning ahead:** This section is dedicated to your mother's Hungarian heritage "
                "and will grow as you prepare for a future Budapest trip. It connects the Hungarian "
                "side of the family story to the Solty history in Budapest."
            )

            with st.container(border=True):
                st.markdown("### 🌉 The Two Threads — Dutch and Hungarian")
                st.markdown(
                    """
Your mother carries two distinct European heritages:

**The Dutch Thread** — the Becht family, Amsterdam, the publishing house on the
Herengracht, the canal houses of Oud-Zuid, the Nazi occupation, and the postwar
*Rechtsherstel*. This trip — Amsterdam, Tuscany, Rome — is primarily a journey
into this Dutch story.

**The Hungarian Thread** — your mother's Hungarian heritage, rooted in Budapest,
with its own rich history of empire, revolution, occupation, and survival.
Budapest sits at the intersection of Central European history: Habsburg rule,
the Austro-Hungarian empire, World War I, the interwar period, the Nazi occupation
of Hungary, and Soviet rule after 1945.

**The connection to the Solty history in Budapest** makes this especially meaningful:
the Solty family story and the Hungarian heritage come together in one city —
Budapest — creating a future journey that mirrors the depth of this one.
                    """
                )

            st.divider()
            with st.container(border=True):
                st.markdown("### 🏙️ Budapest — Historical Context for the Visit")
                st.markdown(
                    """
**Why Budapest matters for this family story:**

Budapest is one of the great cities of Central Europe — a city that contains
within itself the entire arc of 20th century European history:

- **Habsburg Budapest (pre-1918)** — The Austro-Hungarian empire made Budapest
  a rival to Vienna in architecture, culture, and ambition. The grand boulevards,
  the Parliament building, and the chain of coffee houses date from this era.

- **Interwar Hungary (1918–1941)** — After World War I, Hungary lost two-thirds
  of its territory under the Treaty of Trianon. Budapest became the center of
  a much-reduced nation grappling with its new identity.

- **The Nazi Occupation (1944–1945)** — Hungary was one of the last countries
  occupied by Nazi Germany. The Jewish community of Budapest was devastated.
  The city itself sustained significant damage in the siege of Budapest.

- **Soviet Hungary (1945–1989)** — Forty years of Soviet-aligned rule, including
  the 1956 Hungarian Revolution — one of the most significant uprisings against
  Soviet power in the Cold War era.

- **Modern Budapest** — Since 1989, Budapest has reclaimed its place as a
  major European cultural capital, while grappling openly with its complex history.

*Your mother's family history is located within this arc. A future Budapest
trip is a journey into the other half of her European story.*
                    """
                )

            st.divider()
            with st.container(border=True):
                st.markdown("### 🔗 Connecting to the Solty History")
                st.markdown(
                    """
The Solty name connects the Hungarian thread to Budapest in a specific, researchable way.
As you prepare for a future Hungary trip, these are the key threads to pull:

**Research priorities for Budapest:**
- Family addresses in Budapest — neighbourhood, district, era of residence
- Hungarian civil records (anyakönyv) — births, marriages, and deaths are held
  in the Budapest Metropolitan Archives (*Budapest Főváros Levéltára*)
- Hungarian Jewish community records, if applicable — the Mazsihisz archives
  and the Jewish Museum in Budapest hold extensive genealogical records
- Emigration records — when did family members leave Hungary, and to where?

**Budapest archives and research resources:**
                    """
                )
                arch_col1, arch_col2 = st.columns(2)
                with arch_col1:
                    st.link_button(
                        "🏛️ Budapest Főváros Levéltára",
                        "https://www.bparchiv.hu",
                        use_container_width=True,
                        type="primary",
                    )
                    st.link_button(
                        "🔍 FamilySearch — Hungary Records",
                        "https://www.familysearch.org/en/wiki/Hungary_Genealogy",
                        use_container_width=True,
                    )
                with arch_col2:
                    st.link_button(
                        "📚 Arcanum — Hungarian Digital Archives",
                        "https://arcanum.com/en/",
                        use_container_width=True,
                    )
                    st.link_button(
                        "🕍 Jewish Museum Budapest",
                        "https://www.jewishmuseum.hu/en/",
                        use_container_width=True,
                    )

            st.divider()
            with st.container(border=True):
                st.markdown("### 📝 Notes for the Budapest Trip")
                st.write(
                    "Add any family knowledge, stories, or research notes about the Hungarian "
                    "heritage and the Solty connection in Budapest. These will sync to Google Sheets."
                )
                with st.expander("➕ Add a Budapest Heritage Note", expanded=False):
                    bud_author = st.selectbox(
                        "Author", ["Andy", "Sue"], key="bud_author"
                    )
                    bud_subject = st.selectbox(
                        "Subject",
                        [
                            "Hungarian Family History",
                            "Solty History in Budapest",
                            "Budapest Addresses / Locations",
                            "Hungarian Heritage — General",
                            "Future Trip Planning",
                            "Other",
                        ],
                        key="bud_subject",
                    )
                    bud_note = st.text_area(
                        "Note / Memory / Research Finding",
                        placeholder=(
                            "Add anything you know about the Hungarian heritage:\n"
                            "- Family stories about Hungary or Budapest\n"
                            "- Names and dates worth researching\n"
                            "- Connections between the Solty history and Budapest\n"
                            "- Ideas for the future Budapest trip…"
                        ),
                        height=150,
                        key="bud_note",
                    )
                    if st.button(
                        "💾 Save Budapest Note",
                        type="primary",
                        use_container_width=True,
                        key="save_bud",
                    ):
                        if bud_note.strip():
                            try:
                                save_note(
                                    bud_author, f"🇭🇺 {bud_subject}", bud_note.strip()
                                )
                                st.success("Note saved to Google Sheets!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Could not save: {e}")
                        else:
                            st.warning("Please write something before saving.")

            st.divider()
            with st.container(border=True):
                st.markdown("### 🗺️ Budapest Heritage Sites — Planning Ahead")
                st.markdown(
                    """
When the Budapest trip takes shape, these are the sites most likely to connect
the family story to the city's history:

| Site | Why It Matters |
|---|---|
| **Great Synagogue, Dohány St.** | Largest synagogue in Europe; adjacent memorial garden for Budapest victims |
| **Hungarian National Archives** | Civil records, property records, business registrations |
| **Budapest History Museum** | The full arc of Budapest's history in one place |
| **Terror Háza (House of Terror)** | Documents both Nazi and Soviet occupation |
| **Raoul Wallenberg Memorial** | Honoring Swedish diplomat who saved thousands of Hungarian Jews |
| **Fisherman's Bastion / Castle Hill** | Historic heart of Buda; Habsburg-era panorama |
| **Andrássy Avenue** | Budapest's grand boulevard — the city's equivalent of Herengracht |
                    """
                )
                st.link_button(
                    "🗺️ Plan Budapest Route",
                    "https://maps.google.com/?q=Budapest,+Hungary",
                    use_container_width=True,
                )

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
        for col in ["City", "Date", "Activity", "Vibe", "Weather", "Notes"]:
            if col not in itin_df.columns:
                itin_df[col] = ""

        tab_ams, tab_tus, tab_rome, tab_add, tab_edit = st.tabs(
            [
                "🇳🇱 Amsterdam",
                "🌿 Tuscany",
                "🏛️ Rome",
                "➕ Add Activity",
                "✏️ Edit All",
            ]
        )

        def render_city(city_key: str, flag: str):
            city_df = itin_df[
                itin_df["City"].astype(str).str.lower() == city_key.lower()
            ].copy()
            if city_df.empty:
                st.info(
                    f"No activities planned for {flag} {city_key} yet. Use the **➕ Add Activity** tab to get started."
                )
                return
            sunny = city_df[
                city_df["Weather"].astype(str).str.contains("Sunny", na=False)
            ]
            rainy = city_df[
                city_df["Weather"].astype(str).str.contains("Rain", na=False)
            ]
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Activities", len(city_df))
            m2.metric("☀️ Best in Sun", len(sunny))
            m3.metric("🌧️ Rain-Proof", len(rainy))
            st.divider()
            vibe_filter = st.selectbox(
                "Filter by Vibe", ["All Vibes"] + VIBES, key=f"vibe_{city_key}"
            )
            weather_filter = st.selectbox(
                "Filter by Weather",
                ["All Weather", "☀️ Sunny Day", "🌧️ Rain Day", "Either"],
                key=f"wx_{city_key}",
            )
            display_df = city_df.copy()
            if vibe_filter != "All Vibes":
                display_df = display_df[display_df["Vibe"] == vibe_filter]
            if weather_filter != "All Weather":
                display_df = display_df[display_df["Weather"] == weather_filter]
            for _, row in display_df.iterrows():
                wx_str = str(row.get("Weather", ""))
                wx_icon = "☀️" if "Sunny" in wx_str else "🌧️" if "Rain" in wx_str else "🌤️"
                vibe_icon = VIBE_ICONS.get(str(row.get("Vibe", "")), "📍")
                with st.container(border=True):
                    c_left, c_right = st.columns([4, 1])
                    with c_left:
                        st.markdown(
                            f"**{vibe_icon} {row.get('Activity', 'Activity')}**"
                        )
                        if str(row.get("Date", "")):
                            st.caption(f"📅 {row['Date']}")
                        if row.get("Notes"):
                            st.write(str(row["Notes"]))
                    with c_right:
                        st.markdown(f"### {wx_icon}")
                        st.caption(str(row.get("Vibe", "")))

        with tab_ams:
            st.subheader("🇳🇱 Amsterdam")
            render_city("Amsterdam", "🇳🇱")

        with tab_tus:
            st.subheader("🌿 Tuscany")
            render_city("Tuscany", "🌿")

        with tab_rome:
            st.subheader("🏛️ Rome")
            render_city("Rome", "🏛️")

        with tab_add:
            st.subheader("➕ Add New Activity")
            with st.form("add_activity_form", clear_on_submit=True):
                c1, c2 = st.columns(2)
                with c1:
                    city = st.selectbox("City", CITIES)
                    activity = st.text_input(
                        "Activity Name",
                        placeholder="e.g., Rijksmuseum · Pasta class in Siena · Colosseum",
                    )
                    vibe = st.selectbox(
                        "Vibe", VIBES, help="What kind of experience is this?"
                    )
                with c2:
                    act_date = st.date_input("Date (optional)", value=None)
                    weather = st.selectbox(
                        "Best Weather Condition",
                        WEATHER_OPTIONS,
                        help="☀️ Sunny = ideal outdoors  |  🌧️ Rain = great indoor backup",
                    )
                    who = st.multiselect(
                        "Who's going?", ["Andy", "Sue", "Both"], default=["Both"]
                    )
                    notes = st.text_area(
                        "Notes / Details",
                        placeholder="Address, booking info, tips, reservation time…",
                        height=90,
                    )
                submitted = st.form_submit_button(
                    "➕ Add to Itinerary", type="primary", use_container_width=True
                )

            if submitted:
                if activity.strip():
                    try:
                        date_str = str(act_date) if act_date else ""
                        note_str = notes.strip()
                        if who and "Both" not in who:
                            note_str = f"[{'/'.join(who)}] {note_str}".strip()
                        save_activity(
                            city, date_str, activity.strip(), vibe, weather, note_str
                        )
                        st.success(f"✅ Added **{activity}** to the {city} itinerary!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could not save: {e}")
                else:
                    st.warning("Please enter an activity name.")

        with tab_edit:
            st.subheader("✏️ Edit Full Itinerary")
            st.caption(
                "Make inline changes below, then click **Save All** to sync to Google Sheets."
            )
            if not itin_df.empty:
                edited = st.data_editor(
                    itin_df,
                    column_config={
                        "City": st.column_config.SelectboxColumn(
                            "City", options=CITIES
                        ),
                        "Vibe": st.column_config.SelectboxColumn("Vibe", options=VIBES),
                        "Weather": st.column_config.SelectboxColumn(
                            "Weather", options=WEATHER_OPTIONS
                        ),
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
