"""
angel_api.py  v3 — Angel One SmartAPI wrapper (Full Fix)
---------------------------------------------------------
FIXES v3 (on top of v2):
  ✅ #7  Upgraded SmartWebSocket → SmartWebSocketV2 (new Angel One API)
  ✅ #8  Corrected callback pattern for SmartWebSocketV2 (on_data, on_error, on_open, on_close)
  ✅ #9  Heartbeat / ping thread keeps connection alive (30s ping)
  ✅ #10 Dynamic NFO token subscription for open trades (subscribe_tokens / unsubscribe_tokens)
  ✅ #11 Polling interval signal exposed (ws_poll_interval_ms) — app.py uses 1-2s when WS live
  ✅ All v2 fixes retained:
       is_configured(), get_equity_ltp(), get_index_ltp(), get_option_ltp(), get_futures_ltp()

Install:
    pip install smartapi-python pyotp websocket-client
"""

import os
import logging
import threading
import time
import json
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_KEY     = os.getenv("ANGEL_API_KEY",     "WKZ1Ve6i")
CLIENT_ID   = os.getenv("ANGEL_CLIENT_ID",   "K258077")
PASSWORD    = os.getenv("ANGEL_PASSWORD",     "1811")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "HBIWFBUKBUJ4XXNY6MUTE65WIM")

try:
    import streamlit as st
    API_KEY     = st.secrets.get("ANGEL_API_KEY",     API_KEY)
    # Accept both ANGEL_CLIENT_ID and ANGEL_CLIENT_CODE (common typo/variant)
    CLIENT_ID   = (st.secrets.get("ANGEL_CLIENT_ID")
                   or st.secrets.get("ANGEL_CLIENT_CODE")
                   or CLIENT_ID)
    PASSWORD    = st.secrets.get("ANGEL_PASSWORD",     PASSWORD)
    TOTP_SECRET = st.secrets.get("ANGEL_TOTP_SECRET", TOTP_SECRET)
except Exception:
    pass

# ── Internal State ─────────────────────────────────────────────────────────────
_obj            = None      # SmartConnect instance
_data           = {}        # session data after login
_authenticated  = False

# WebSocket state
_ws_instance    = None      # SmartWebSocketV2 instance
_ws_thread      = None
_ws_connected   = False
_ws_last_ping   = 0.0
_ws_reconnect   = True      # flag to allow auto-reconnect
_heartbeat_thread = None

# Live tick cache: { token: {"ltp": float, "ts": float, "symbol": str} }
_tick_cache: dict = {}
_TICK_STALE_SEC   = 8

# Token → symbol reverse map (for WebSocket messages)
_token_map: dict  = {}      # "2885" → "RELIANCE.NS"

# Currently subscribed NFO tokens (for dynamic add/remove)
_subscribed_tokens: set = set()   # set of str token ids
_subscription_lock = threading.Lock()

# ── NSE Token Database ─────────────────────────────────────────────────────────
NSE_TOKEN_MAP = {
    # Index tokens
    "^NSEI":        ("NSE", "Nifty 50",     "26000"),
    "^NSEBANK":     ("NSE", "Nifty Bank",   "26009"),
    "^INDIAVIX":    ("NSE", "India VIX",    "26017"),
    "^CNXIT":       ("NSE", "Nifty IT",     "26004"),
    "^CNXPHARMA":   ("NSE", "Nifty Pharma", "26015"),
    "^CNXAUTO":     ("NSE", "Nifty Auto",   "26008"),
    "^CNXFMCG":     ("NSE", "Nifty FMCG",  "26010"),
    "^CNXMETAL":    ("NSE", "Nifty Metal",  "26018"),
    "^NSMIDCP":     ("NSE", "Nifty Midcap", "26013"),
    "^BSESN":       ("BSE", "SENSEX",       "1"),
    # Popular NSE equities
    "RELIANCE.NS":  ("NSE", "RELIANCE",     "2885"),
    "TCS.NS":       ("NSE", "TCS",          "11536"),
    "INFY.NS":      ("NSE", "INFY",         "1594"),
    "HDFCBANK.NS":  ("NSE", "HDFCBANK",     "1333"),
    "ICICIBANK.NS": ("NSE", "ICICIBANK",    "4963"),
    "SBIN.NS":      ("NSE", "SBIN",         "3045"),
    "BAJFINANCE.NS":("NSE", "BAJFINANCE",   "317"),
    "WIPRO.NS":     ("NSE", "WIPRO",        "3787"),
    "AXISBANK.NS":  ("NSE", "AXISBANK",     "5900"),
    "KOTAKBANK.NS": ("NSE", "KOTAKBANK",    "1922"),
    "LT.NS":        ("NSE", "LT",           "11483"),
    "HCLTECH.NS":   ("NSE", "HCLTECH",      "7229"),
    "ADANIENT.NS":  ("NSE", "ADANIENT",     "25"),
    "ADANIPORTS.NS":("NSE", "ADANIPORTS",   "15083"),
    "TATAMOTORS.NS":("NSE", "TATAMOTORS",   "3456"),
    "TATASTEEL.NS": ("NSE", "TATASTEEL",    "3499"),
    "BHARTIARTL.NS":("NSE", "BHARTIARTL",  "10604"),
    "ASIANPAINT.NS":("NSE", "ASIANPAINT",   "236"),
    "TITAN.NS":     ("NSE", "TITAN",        "3506"),
    "MARUTI.NS":    ("NSE", "MARUTI",       "10999"),
    "NTPC.NS":      ("NSE", "NTPC",         "11630"),
    "POWERGRID.NS": ("NSE", "POWERGRID",    "14977"),
    "ONGC.NS":      ("NSE", "ONGC",         "2475"),
    "BPCL.NS":      ("NSE", "BPCL",         "526"),
    "SUNPHARMA.NS": ("NSE", "SUNPHARMA",    "3351"),
    "CIPLA.NS":     ("NSE", "CIPLA",        "694"),
    "DRREDDY.NS":   ("NSE", "DRREDDY",      "881"),
    "DIVISLAB.NS":  ("NSE", "DIVISLAB",     "10940"),
    "APOLLOHOSP.NS":("NSE", "APOLLOHOSP",   "157"),
    "HINDALCO.NS":  ("NSE", "HINDALCO",     "1363"),
    "JSWSTEEL.NS":  ("NSE", "JSWSTEEL",     "11723"),
    "TECHM.NS":     ("NSE", "TECHM",        "13538"),
    "HDFCLIFE.NS":  ("NSE", "HDFCLIFE",     "467"),
    "SBILIFE.NS":   ("NSE", "SBILIFE",      "21808"),
    "BAJAJFINSV.NS":("NSE", "BAJAJFINSV",   "16675"),
    "EICHERMOT.NS": ("NSE", "EICHERMOT",    "910"),
    "HEROMOTOCO.NS":("NSE", "HEROMOTOCO",   "1348"),
    "BRITANNIA.NS": ("NSE", "BRITANNIA",    "547"),
    "INDUSINDBK.NS":("NSE", "INDUSINDBK",   "5258"),
    "ZOMATO.NS":    ("NSE", "ZOMATO",       "5097"),
    "IRCTC.NS":     ("NSE", "IRCTC",        "13611"),
    "BEL.NS":       ("NSE", "BEL",          "383"),
    "HAL.NS":       ("NSE", "HAL",          "2303"),
    "RVNL.NS":      ("NSE", "RVNL",         "19262"),
    "TATACONSUM.NS":("NSE", "TATACONSUM",   "3432"),
    "NESTLEIND.NS": ("NSE", "NESTLEIND",    "17963"),
    "ULTRACEMCO.NS":("NSE", "ULTRACEMCO",   "11532"),
    "GRASIM.NS":    ("NSE", "GRASIM",       "1232"),
    "COALINDIA.NS": ("NSE", "COALINDIA",    "20374"),
    "VEDL.NS":      ("NSE", "VEDL",         "3063"),
    "SAIL.NS":      ("NSE", "SAIL",         "2963"),
    "BANKBARODA.NS":("NSE", "BANKBARODA",   "4668"),
    "PNB.NS":       ("NSE", "PNB",          "10666"),
    "NHPC.NS":      ("NSE", "NHPC",         "13939"),
    "IRFC.NS":      ("NSE", "IRFC",         "18391"),
    "RECLTD.NS":    ("NSE", "RECLTD",       "11595"),
    "PFC.NS":       ("NSE", "PFC",          "14299"),
    "PERSISTENT.NS":("NSE", "PERSISTENT",   "18365"),
    "COFORGE.NS":   ("NSE", "COFORGE",      "17538"),
    "MPHASIS.NS":   ("NSE", "MPHASIS",      "4503"),
    "LTIM.NS":      ("NSE", "LTIM",         "17818"),
    "OFSS.NS":      ("NSE", "OFSS",         "10738"),
    "KPITTECH.NS":  ("NSE", "KPITTECH",     "983"),
    "TATAELXSI.NS": ("NSE", "TATAELXSI",    "3468"),
    "DIXON.NS":     ("NSE", "DIXON",        "20358"),
    "POLYCAB.NS":   ("NSE", "POLYCAB",      "16675"),
    "HAVELLS.NS":   ("NSE", "HAVELLS",      "1203"),
    "PIIND.NS":     ("NSE", "PIIND",        "18365"),
    "DEEPAKNTR.NS": ("NSE", "DEEPAKNTR",    "7287"),
    "DMART.NS":     ("NSE", "DMART",        "21060"),
    "TRENT.NS":     ("NSE", "TRENT",        "1964"),
    "PAGEIND.NS":   ("NSE", "PAGEIND",      "14413"),
    "SIEMENS.NS":   ("NSE", "SIEMENS",      "3150"),
    "ABB.NS":       ("NSE", "ABB",          "13"),
    "BHEL.NS":      ("NSE", "BHEL",         "438"),
    "AMBUJACEM.NS": ("NSE", "AMBUJACEM",    "1270"),
    "SHREECEM.NS":  ("NSE", "SHREECEM",     "3103"),
    "TORNTPHARM.NS":("NSE", "TORNTPHARM",   "3518"),
    "LUPIN.NS":     ("NSE", "LUPIN",        "10440"),
    "AUROPHARMA.NS":("NSE", "AUROPHARMA",   "275"),
    "BIOCON.NS":    ("NSE", "BIOCON",       "526"),
    "BALKRISIND.NS":("NSE", "BALKRISIND",   "335"),
    "MRF.NS":       ("NSE", "MRF",          "2277"),
    "BOSCHLTD.NS":  ("NSE", "BOSCHLTD",     "520"),
    "EXIDEIND.NS":  ("NSE", "EXIDEIND",     "939"),
    "MOTHERSON.NS": ("NSE", "MOTHERSON",    "4204"),
    "CHOLAFIN.NS":  ("NSE", "CHOLAFIN",     "685"),
    "MUTHOOTFIN.NS":("NSE", "MUTHOOTFIN",   "17916"),
    "SHRIRAMFIN.NS":("NSE", "SHRIRAMFIN",   "4306"),
    "HDFCAMC.NS":   ("NSE", "HDFCAMC",      "17378"),
    "GODREJCP.NS":  ("NSE", "GODREJCP",     "10099"),
    "DABUR.NS":     ("NSE", "DABUR",        "772"),
    "MARICO.NS":    ("NSE", "MARICO",       "4067"),
    "COLPAL.NS":    ("NSE", "COLPAL",       "1099"),
}

INDEX_TOKENS = {
    "NIFTY":     ("NSE", "Nifty 50",   "26000"),
    "BANKNIFTY": ("NSE", "Nifty Bank", "26009"),
    "FINNIFTY":  ("NSE", "Nifty Fin",  "26037"),
    "MIDCPNIFTY":("NSE", "Midcap",     "26074"),
}

# ── SmartWebSocketV2 exchange type map ─────────────────────────────────────────
# From Angel One SmartWebSocketV2 docs:
#   1 = NSE_CM (equity), 2 = NSE_FO (F&O), 3 = BSE_CM, 4 = BSE_FO, 5 = MCX_FO
_EXCHANGE_TYPE = {"NSE": 1, "NFO": 2, "BSE": 3, "BFO": 4, "MCX": 5}

# ── Helpers ────────────────────────────────────────────────────────────────────

def is_configured() -> bool:
    return (
        bool(API_KEY)     and API_KEY     not in ("YOUR_API_KEY", "")
        and bool(CLIENT_ID)   and CLIENT_ID   not in ("YOUR_CLIENT_ID", "")
        and bool(TOTP_SECRET) and TOTP_SECRET not in ("YOUR_TOTP_SECRET", "")
    )

def _is_stub_config() -> bool:
    return not is_configured()

# ── Auth ───────────────────────────────────────────────────────────────────────

def login() -> dict:
    global _obj, _data, _authenticated
    if _is_stub_config():
        _authenticated = False
        return {"status": False, "message": "Stub mode — fill in credentials"}
    try:
        import pyotp
        from SmartApi import SmartConnect
        totp  = pyotp.TOTP(TOTP_SECRET).now()
        _obj  = SmartConnect(api_key=API_KEY)
        _data = _obj.generateSession(CLIENT_ID, PASSWORD, totp)
        if _data and _data.get("status"):
            _authenticated = True
            logger.info("angel_api: login OK for %s", CLIENT_ID)
            return _data
        _authenticated = False
        return _data or {"status": False, "message": "Unknown login error"}
    except ImportError as e:
        raise ImportError("Run: pip install smartapi-python pyotp") from e
    except Exception:
        _authenticated = False
        logger.exception("angel_api: login exception")
        raise

def logout() -> dict:
    global _obj, _data, _authenticated, _ws_reconnect
    _ws_reconnect = False   # stop auto-reconnect on intentional logout
    stop_websocket()
    if _obj and _authenticated:
        try:
            result = _obj.terminateSession(CLIENT_ID)
            _authenticated = False; _obj = None; _data = {}
            return result
        except Exception:
            logger.exception("angel_api: logout failed"); raise
    _authenticated = False
    return {"status": True, "message": "No active session"}

def ensure_logged_in():
    if not _authenticated:
        login()

# ── Status ─────────────────────────────────────────────────────────────────────

def get_status() -> dict:
    if _is_stub_config():
        return {"authenticated": False, "client_id": CLIENT_ID, "stub_mode": True,
                "message": "Stub mode — set ANGEL_API_KEY / CLIENT_ID / PASSWORD / TOTP_SECRET",
                "session_data": None, "ws_connected": False,
                "ws_subscribed_tokens": 0, "ws_last_ping_ago": None,
                "poll_interval_ms": ws_poll_interval_ms()}
    return {
        "authenticated":         _authenticated,
        "client_id":             CLIENT_ID,
        "stub_mode":             False,
        "message":               "Connected" if _authenticated else "Not logged in",
        "session_data":          _data if _authenticated else None,
        "ws_connected":          _ws_connected,
        "ws_subscribed_tokens":  len(_subscribed_tokens),
        "ws_last_ping_ago":      round(time.time() - _ws_last_ping, 1) if _ws_last_ping else None,
        "poll_interval_ms":      ws_poll_interval_ms(),
    }

def ws_poll_interval_ms() -> int:
    """Return recommended Streamlit autorefresh interval in ms.
    1500 when WS is live (ticks arrive sub-second), 12000 when polling REST only."""
    return 1500 if _ws_connected else 12000

# ── LTP via REST ───────────────────────────────────────────────────────────────

def _ltp_rest(exchange: str, symbol: str, token: str) -> Optional[float]:
    ensure_logged_in()
    try:
        resp = _obj.ltpData(exchange, symbol, token)
        if resp and resp.get("status"):
            return float(resp["data"]["ltp"])
    except Exception:
        logger.debug("angel_api: ltpData failed for %s/%s", exchange, symbol)
    return None

def get_equity_ltp(yf_symbol: str) -> Optional[float]:
    if _is_stub_config(): return None
    ensure_logged_in()
    sym_clean = yf_symbol.upper().replace(".NS", "").replace(".BO", "")
    entry = NSE_TOKEN_MAP.get(yf_symbol) or NSE_TOKEN_MAP.get(yf_symbol.upper())
    if not entry:
        for k, v in NSE_TOKEN_MAP.items():
            if v[1] == sym_clean:
                entry = v; break
    if not entry: return None
    exchange, tradingsymbol, token = entry
    tick = _tick_cache.get(token)
    if tick and (time.time() - tick["ts"]) < _TICK_STALE_SEC:
        return tick["ltp"]
    return _ltp_rest(exchange, tradingsymbol, token)

def get_index_ltp(yf_symbol: str) -> Optional[float]:
    if _is_stub_config(): return None
    ensure_logged_in()
    entry = NSE_TOKEN_MAP.get(yf_symbol) or NSE_TOKEN_MAP.get(yf_symbol.upper())
    if not entry:
        u = yf_symbol.upper().replace("^", "")
        entry = INDEX_TOKENS.get(u)
    if not entry: return None
    exchange, tradingsymbol, token = entry
    tick = _tick_cache.get(token)
    if tick and (time.time() - tick["ts"]) < _TICK_STALE_SEC:
        return tick["ltp"]
    return _ltp_rest(exchange, tradingsymbol, token)

def get_option_ltp(index_name: str, strike: int, opt_type: str, expiry_str: str) -> Optional[float]:
    if _is_stub_config(): return None
    ensure_logged_in()
    try:
        from datetime import datetime as _dt
        for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d %b %Y", "%d-%b-%y"):
            try: exp_dt = _dt.strptime(expiry_str, fmt); break
            except ValueError: continue
        else: return None
        idx = ("BANKNIFTY" if "BANK" in index_name.upper()
               else "FINNIFTY" if "FIN" in index_name.upper() else "NIFTY")
        exp_code = exp_dt.strftime("%d%b%y").upper()
        sym = f"{idx}{exp_code}{int(strike)}{opt_type.upper()}"
        try:
            results = _obj.searchScrip("NFO", sym)
            hits = results.get("data") or []
            if hits:
                token = hits[0].get("symboltoken", "")
                if token:
                    tick = _tick_cache.get(token)
                    if tick and (time.time() - tick["ts"]) < _TICK_STALE_SEC:
                        return tick["ltp"]
                    ltp = _ltp_rest("NFO", sym, token)
                    if ltp:
                        # Auto-subscribe this NFO token to WS for live ticks
                        subscribe_tokens([("NFO", token)])
                        return ltp
        except Exception: pass
        try:
            results2 = _obj.searchScrip("NFO", f"{idx}{int(strike)}{opt_type}")
            hits2 = results2.get("data") or []
            for h in hits2:
                name = h.get("tradingsymbol", "")
                if str(int(strike)) in name and opt_type in name:
                    token2 = h.get("symboltoken", "")
                    if token2:
                        ltp2 = _ltp_rest("NFO", name, token2)
                        if ltp2:
                            subscribe_tokens([("NFO", token2)])
                            return ltp2
        except Exception: pass
    except Exception:
        logger.debug("angel_api: get_option_ltp failed for %s %s %s", index_name, strike, opt_type)
    return None

def get_futures_ltp(symbol: str, expiry_str: str = "") -> Optional[float]:
    if _is_stub_config(): return None
    ensure_logged_in()
    try:
        from datetime import datetime as _dt, date as _date
        base = symbol.upper().replace(".NS", "").replace(".BO", "").replace("_FUT", "")
        if base in ("NIFTY50", "NIFTY"): base = "NIFTY"
        if expiry_str:
            for fmt in ("%Y-%m-%d", "%d-%b-%Y"):
                try: exp_dt = _dt.strptime(expiry_str, fmt); break
                except ValueError: continue
            else: exp_dt = _dt.now()
        else:
            today = _date.today()
            exp_dt = _dt(today.year, today.month, 1)
        exp_code = exp_dt.strftime("%d%b%y").upper()
        sym_fut  = f"{base}{exp_code}FUT"
        try:
            results = _obj.searchScrip("NFO", sym_fut)
            hits = results.get("data") or []
            if not hits:
                sym_fut2 = f"{base}{exp_dt.strftime('%b%y').upper()}FUT"
                results2 = _obj.searchScrip("NFO", sym_fut2)
                hits = results2.get("data") or []
            if hits:
                token = hits[0].get("symboltoken", "")
                tname = hits[0].get("tradingsymbol", sym_fut)
                if token:
                    tick = _tick_cache.get(token)
                    if tick and (time.time() - tick["ts"]) < _TICK_STALE_SEC:
                        return tick["ltp"]
                    ltp = _ltp_rest("NFO", tname, token)
                    if ltp:
                        subscribe_tokens([("NFO", token)])
                        return ltp
        except Exception: pass
    except Exception:
        logger.debug("angel_api: get_futures_ltp failed for %s", symbol)
    return None

# ── Market Data (bulk) ─────────────────────────────────────────────────────────

def get_ltp(exchange: str, symbol: str, token: str) -> dict:
    ensure_logged_in()
    try:
        resp = _obj.ltpData(exchange, symbol, token)
        if resp and resp.get("status"):
            return {"ltp": resp["data"]["ltp"], "exchange": exchange, "symbol": symbol}
        raise ValueError(f"LTP fetch failed: {resp}")
    except Exception:
        logger.exception("angel_api: get_ltp failed"); raise

def get_market_data(mode: str, exchange_tokens: dict) -> dict:
    ensure_logged_in()
    try:
        return _obj.getMarketData(mode, exchange_tokens)
    except Exception:
        logger.exception("angel_api: get_market_data failed"); raise

def get_bulk_ltp(symbols: list) -> dict:
    if _is_stub_config(): return {}
    ensure_logged_in()
    nse_tokens = []; token_to_sym = {}
    for sym in symbols:
        entry = NSE_TOKEN_MAP.get(sym) or NSE_TOKEN_MAP.get(sym.upper())
        if entry and entry[0] == "NSE":
            nse_tokens.append(entry[2]); token_to_sym[entry[2]] = sym
    result = {}
    if nse_tokens:
        try:
            resp = _obj.getMarketData("LTP", {"NSE": nse_tokens})
            if resp and resp.get("status"):
                for item in (resp.get("data", {}).get("fetched") or []):
                    tok = str(item.get("symbolToken", ""))
                    ltp = item.get("ltp")
                    if tok in token_to_sym and ltp:
                        result[token_to_sym[tok]] = float(ltp)
        except Exception:
            logger.debug("angel_api: get_bulk_ltp failed")
    return result

# ── SmartWebSocketV2 — correct import path (v1.3.8+) ──────────────────────────
# Real class lives at SmartApi.smartWebSocketV2 (lowercase s in filename)
# NOT SmartApi.SmartWebSocketV2 — that path raises ImportError on Streamlit Cloud.
def _import_ws_v2():
    """Try all known import paths for SmartWebSocketV2. Returns class or raises."""
    try:
        from SmartApi.smartWebSocketV2 import SmartWebSocketV2   # correct (lowercase)
        return SmartWebSocketV2
    except ImportError:
        pass
    try:
        from SmartApi.SmartWebSocketV2 import SmartWebSocketV2   # legacy uppercase
        return SmartWebSocketV2
    except ImportError:
        pass
    raise ImportError(
        "SmartWebSocketV2 not found. Run: pip install smartapi-python>=1.3.8 logzero websocket-client"
    )


# ── SmartWebSocketV2 tick handler ──────────────────────────────────────────────

def _ws_on_data(wsapp, message):
    """Handle incoming SmartWebSocketV2 tick.
    SmartWebSocketV2._on_data already parses binary → calls on_data(wsapp, parsed_dict).
    parsed_dict keys: token, last_traded_price (in paise), exchange_type, etc.
    """
    global _tick_cache
    try:
        # message is already a parsed dict (SmartWebSocketV2 handles binary decode)
        if isinstance(message, dict):
            ticks = [message]
        elif isinstance(message, list):
            ticks = message
        else:
            return

        for tick in ticks:
            if not isinstance(tick, dict):
                continue
            token = str(tick.get("token") or "").strip()
            if not token:
                continue
            # last_traded_price is in PAISE (integer) — divide by 100
            raw_ltp = tick.get("last_traded_price")
            if raw_ltp is not None:
                ltp = float(raw_ltp) / 100.0
            else:
                continue   # skip ticks with no price
            if ltp > 0:
                _tick_cache[token] = {
                    "ltp":    ltp,
                    "ts":     time.time(),
                    "symbol": _token_map.get(token, token),
                }
    except Exception:
        pass


def _ws_on_error(wsapp, error):
    """Called by SmartWebSocketV2 on_error(message, error_message)."""
    logger.warning("angel_api: WS error — %s | %s", wsapp, error)


def _ws_on_open(wsapp):
    """Called by SmartWebSocketV2 on_open(wsapp) after successful handshake."""
    global _ws_connected
    _ws_connected = True
    logger.info("angel_api: SmartWebSocketV2 connected")


def _ws_on_close(wsapp):
    """Called by SmartWebSocketV2 on_close(wsapp) when connection drops."""
    global _ws_connected
    _ws_connected = False
    logger.info("angel_api: SmartWebSocketV2 closed")


# ── Heartbeat / Ping thread ────────────────────────────────────────────────────

def _heartbeat_loop():
    """Send periodic ping to keep the WS connection alive.
    SmartWebSocketV2 has HEART_BEAT_INTERVAL=10 internally, but we send
    an explicit ping every 25s as an extra safety net.
    """
    global _ws_last_ping
    while _ws_reconnect:
        time.sleep(25)
        if _ws_instance and _ws_connected:
            try:
                # SmartWebSocketV2 exposes wsapp (websocket.WebSocketApp)
                if hasattr(_ws_instance, "wsapp") and _ws_instance.wsapp:
                    _ws_instance.wsapp.send("ping")
                    _ws_last_ping = time.time()
            except Exception:
                pass


# ── Dynamic token subscription (Issue #10) ────────────────────────────────────

def subscribe_tokens(tokens: List[Tuple[str, str]]):
    """
    Dynamically subscribe additional tokens to the live WS stream.
    tokens: list of (exchange, token_str), e.g. [("NFO", "57352"), ("NSE", "2885")]
    Safe to call even before WS is started — tokens are queued and applied on connect.
    """
    global _subscribed_tokens
    if not tokens:
        return
    with _subscription_lock:
        new_tokens = [(e, t) for e, t in tokens if t not in _subscribed_tokens]
        if not new_tokens:
            return
        for e, t in new_tokens:
            _subscribed_tokens.add(t)
            # Register reverse map
            _token_map.setdefault(t, f"{e}:{t}")

        if _ws_instance and _ws_connected:
            try:
                SmartWebSocketV2 = _import_ws_v2()
                payload = _build_subscription_payload(new_tokens)
                _ws_instance.subscribe("dynamic_sub", SmartWebSocketV2.QUOTE, payload)
                logger.debug("angel_api: subscribed %d new tokens", len(new_tokens))
            except Exception:
                logger.debug("angel_api: dynamic subscribe failed (will retry on reconnect)")


def unsubscribe_tokens(tokens: List[Tuple[str, str]]):
    """Unsubscribe tokens from WS stream (e.g. when a trade is closed)."""
    global _subscribed_tokens
    with _subscription_lock:
        for e, t in tokens:
            _subscribed_tokens.discard(t)
        if _ws_instance and _ws_connected:
            try:
                SmartWebSocketV2 = _import_ws_v2()
                payload = _build_subscription_payload(tokens)
                _ws_instance.unsubscribe("dynamic_unsub", SmartWebSocketV2.QUOTE, payload)
            except Exception:
                pass


def _build_subscription_payload(tokens: List[Tuple[str, str]]) -> list:
    """Group tokens by exchange into SmartWebSocketV2 subscription format."""
    grouped: dict = {}
    for exchange, token in tokens:
        etype = _EXCHANGE_TYPE.get(exchange.upper(), 1)
        grouped.setdefault(etype, []).append(token)
    return [{"exchangeType": etype, "tokens": toks} for etype, toks in grouped.items()]


# ── WebSocket stream startup (SmartWebSocketV2) ────────────────────────────────

def start_websocket_stream(tokens: list = None):
    """
    Start SmartWebSocketV2 background thread for live tick streaming.
    tokens: list of (exchange, token) tuples, e.g. [("NSE", "2885"), ("NFO", "57352")]
    If None, subscribes to all tokens in NSE_TOKEN_MAP.
    """
    global _ws_thread, _ws_instance, _ws_connected, _ws_reconnect, _heartbeat_thread

    if not is_configured():
        return False

    ensure_logged_in()
    _ws_reconnect = True

    # Default: subscribe all equity/index tokens
    if not tokens:
        tokens = [(e, t) for e, s, t in NSE_TOKEN_MAP.values()]
        for sym, (e, s, t) in NSE_TOKEN_MAP.items():
            _token_map[t] = sym

    with _subscription_lock:
        for e, t in tokens:
            _subscribed_tokens.add(t)

    def _run_ws():
        global _ws_instance, _ws_connected
        retry_delay = 5
        while _ws_reconnect:
            try:
                SmartWebSocketV2 = _import_ws_v2()

                auth_token  = _data.get("data", {}).get("jwtToken", "")
                feed_token  = _data.get("data", {}).get("feedToken", "")

                # Correct SmartWebSocketV2 constructor (v1.3.8+):
                # SmartWebSocketV2(auth_token, api_key, client_code, feed_token,
                #                  max_retry_attempt, retry_strategy, retry_delay, ...)
                ws = SmartWebSocketV2(
                    auth_token=auth_token,
                    api_key=API_KEY,
                    client_code=CLIENT_ID,
                    feed_token=feed_token,
                    max_retry_attempt=5,
                    retry_strategy=0,
                    retry_delay=10,
                )
                _ws_instance = ws

                # Assign callbacks using correct attribute names
                def _on_open(wsapp):
                    global _ws_connected
                    _ws_connected = True
                    logger.info("angel_api: WS connected — subscribing tokens")
                    # Subscribe all queued tokens on open
                    with _subscription_lock:
                        snap = list(_subscribed_tokens)
                    if snap:
                        try:
                            # Group by exchange type using SmartWebSocketV2 constants
                            nse_toks = [t for t in snap if len(t) <= 5]   # short = NSE/index
                            nfo_toks = [t for t in snap if len(t) > 5]    # long  = NFO
                            payload = []
                            if nse_toks:
                                payload.append({"exchangeType": SmartWebSocketV2.NSE_CM,
                                                "tokens": nse_toks})
                            if nfo_toks:
                                payload.append({"exchangeType": SmartWebSocketV2.NSE_FO,
                                                "tokens": nfo_toks})
                            if not payload:
                                payload = [{"exchangeType": SmartWebSocketV2.NSE_CM,
                                            "tokens": snap}]
                            ws.subscribe("startup", SmartWebSocketV2.QUOTE, payload)
                            logger.info("angel_api: subscribed %d tokens", len(snap))
                        except Exception as e:
                            logger.warning("angel_api: initial subscribe failed: %s", e)

                def _on_data(wsapp, message):
                    _ws_on_data(wsapp, message)

                def _on_error(wsapp, error):
                    logger.warning("angel_api: WS error: %s | %s", wsapp, error)

                def _on_close(wsapp):
                    global _ws_connected
                    _ws_connected = False
                    logger.info("angel_api: WS closed")

                ws.on_open  = _on_open
                ws.on_data  = _on_data
                ws.on_error = _on_error
                ws.on_close = _on_close

                logger.info("angel_api: connecting SmartWebSocketV2 …")
                ws.connect()          # blocks until connection closes / max retries hit
                _ws_connected = False

                if not _ws_reconnect:
                    break
                logger.info("angel_api: WS exited, retrying in %ds …", retry_delay)
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

            except ImportError as e:
                logger.error("angel_api: %s", e)
                break   # can't recover without re-install
            except Exception:
                _ws_connected = False
                logger.exception("angel_api: WS thread error")
                if not _ws_reconnect:
                    break
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    if _ws_thread is None or not _ws_thread.is_alive():
        _ws_thread = threading.Thread(target=_run_ws, name="angel-ws", daemon=True)
        _ws_thread.start()

    # Start heartbeat thread
    if _heartbeat_thread is None or not _heartbeat_thread.is_alive():
        _heartbeat_thread = threading.Thread(target=_heartbeat_loop, name="angel-ping", daemon=True)
        _heartbeat_thread.start()

    time.sleep(1)   # brief wait for initial connect
    return True


def _nfo_token_set() -> set:
    """Helper: rough heuristic — NFO tokens are generally > 100000."""
    return {t for t in _subscribed_tokens if len(t) > 5}


def stop_websocket():
    global _ws_connected, _ws_reconnect, _ws_instance
    _ws_reconnect = False
    _ws_connected = False
    if _ws_instance:
        try:
            _ws_instance.close_connection()
        except Exception:
            pass
        _ws_instance = None

# ── Price staleness check ──────────────────────────────────────────────────────

def is_price_live(token: str) -> bool:
    tick = _tick_cache.get(str(token))
    if not tick: return False
    return (time.time() - tick["ts"]) < _TICK_STALE_SEC

def get_price_age(token: str) -> float:
    tick = _tick_cache.get(str(token))
    if not tick: return 9999.0
    return time.time() - tick["ts"]

# ── Candle Data ────────────────────────────────────────────────────────────────

def get_candle_data(token, exchange, symbol_token, interval, from_date, to_date) -> list:
    ensure_logged_in()
    try:
        param = {"exchange": exchange, "symboltoken": symbol_token,
                 "interval": interval, "fromdate": from_date, "todate": to_date}
        resp  = _obj.getCandleData(param)
        if resp and resp.get("status"): return resp["data"]
        raise ValueError(f"Candle fetch failed: {resp}")
    except Exception:
        logger.exception("angel_api: get_candle_data failed"); raise

# ── Portfolio / Orders ─────────────────────────────────────────────────────────

def get_order_book() -> list:
    ensure_logged_in()
    try: return _obj.orderBook().get("data") or []
    except Exception: logger.exception("angel_api: get_order_book failed"); raise

def get_trade_book() -> list:
    ensure_logged_in()
    try: return _obj.tradeBook().get("data") or []
    except Exception: logger.exception("angel_api: get_trade_book failed"); raise

def get_positions() -> list:
    ensure_logged_in()
    try: return _obj.position().get("data") or []
    except Exception: logger.exception("angel_api: get_positions failed"); raise

def get_holdings() -> list:
    ensure_logged_in()
    try: return _obj.holding().get("data") or []
    except Exception: logger.exception("angel_api: get_holdings failed"); raise

def get_funds() -> dict:
    ensure_logged_in()
    try: return _obj.rmsLimit().get("data") or {}
    except Exception: logger.exception("angel_api: get_funds failed"); raise

# ── Order Placement ────────────────────────────────────────────────────────────

def place_order(symbol, token, exchange, transaction_type, quantity,
                price=0, order_type="MARKET", product_type="INTRADAY",
                duration="DAY", trigger_price=0, variety="NORMAL") -> str:
    ensure_logged_in()
    try:
        order_params = {
            "variety": variety, "tradingsymbol": symbol, "symboltoken": token,
            "transactiontype": transaction_type, "exchange": exchange,
            "ordertype": order_type, "producttype": product_type,
            "duration": duration, "price": str(price), "squareoff": "0",
            "stoploss": "0", "triggerprice": str(trigger_price),
            "quantity": str(quantity),
        }
        resp = _obj.placeOrder(order_params)
        if resp and resp.get("status"): return resp["data"]["orderid"]
        raise ValueError(f"Order placement failed: {resp}")
    except Exception:
        logger.exception("angel_api: place_order failed"); raise

def cancel_order(order_id: str, variety: str = "NORMAL") -> bool:
    ensure_logged_in()
    try:
        resp = _obj.cancelOrder(order_id, variety)
        return bool(resp and resp.get("status"))
    except Exception:
        logger.exception("angel_api: cancel_order failed"); raise

def modify_order(order_id, quantity, price, order_type="LIMIT",
                 variety="NORMAL", duration="DAY", trigger_price=0) -> bool:
    ensure_logged_in()
    try:
        params = {"variety": variety, "orderid": order_id, "ordertype": order_type,
                  "producttype": "INTRADAY", "duration": duration,
                  "price": str(price), "quantity": str(quantity),
                  "triggerprice": str(trigger_price)}
        resp = _obj.modifyOrder(params)
        return bool(resp and resp.get("status"))
    except Exception:
        logger.exception("angel_api: modify_order failed"); raise

def search_scrip(exchange: str, query: str) -> list:
    ensure_logged_in()
    try:
        resp = _obj.searchScrip(exchange, query)
        return resp.get("data") or []
    except Exception:
        logger.exception("angel_api: search_scrip failed"); raise

def get_profile() -> dict:
    ensure_logged_in()
    try:
        resp = _obj.getProfile(_data.get("data", {}).get("refreshToken", ""))
        return resp.get("data") or {}
    except Exception:
        logger.exception("angel_api: get_profile failed"); raise
