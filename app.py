import os, json, time, threading, requests, pytz
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from flask import Flask, request, jsonify, send_from_directory
from datetime import datetime, timedelta
from groq import Groq
import re

app = Flask(__name__, static_folder='static')
VERSION = "8.0"

TEHRAN = pytz.timezone("Asia/Tehran")

# ==================== متغیرهای محیطی ====================
ALERTS_FILE = "alerts.json"
JOURNAL_FILE = "journal_data.json"
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
BOT_TOKEN_ENV = os.environ.get("BOT_TOKEN", "")
YOUR_CHAT_ID = "109419675"
BROADCAST_MODE = os.environ.get("BROADCAST_MODE", "false").lower() == "true"

_cache_alerts = None
_cache_journal = None

def now_teh():
    return datetime.now(TEHRAN).strftime("%Y-%m-%d %H:%M:%S")

def now_pretty():
    return now_teh()  # alias برای سازگاری

def get_pip_multiplier(symbol):
    sym_up = symbol.upper()
    crypto_list = ['BTC','ETH','SOL','BNB','XRP','ADA','DOGE','TRX','TON','AVAX','MATIC','DOT','LINK','UNI','ATOM','LTC','SHIB','OP','ARB','NEAR','FTM','SAND','MANA']
    if any(x in sym_up for x in crypto_list):
        return 1
    if "XAU" in sym_up or "XAG" in sym_up:
        return 10
    if "JPY" in sym_up:
        return 100
    return 10000

def is_crypto_symbol(sym):
    sym_up = sym.upper()
    crypto_list = ['BTC','ETH','SOL','BNB','XRP','ADA','DOGE','TRX','TON','AVAX','MATIC','DOT','LINK','UNI','ATOM','LTC','SHIB','OP','ARB','NEAR','FTM','SAND','MANA']
    return any(c in sym_up for c in crypto_list)

def _empty_alerts():
    return {
        "alerts": [], "archive": [], "telegram": {"bot_token": "", "chat_ids": []},
        "users": [], "errors": [], "last_update": None
    }

def fix_alerts(data):
    e = _empty_alerts()
    for k in e:
        if k not in data:
            data[k] = e[k]
    return data

# =====================================================================
# Supabase — جدول alerts (هر آلارم یه ردیف + یه ردیف config با id='__config__')
# =====================================================================

def _sb_upsert_alert(a):
    """یه آلارم رو upsert کن"""
    if not SUPABASE_KEY: return
    try:
        # فیلدهای پایه که حتماً توی جدول هستن
        payload = {
            "id":           a["id"],
            "symbol":       a.get("symbol",""),
            "type":         a.get("type","forex"),
            "condition":    a.get("condition","above"),
            "target_price": float(a.get("target_price",0)),
            "status":       "fired" if a.get("fired_at") else ("active" if a.get("active") else "cancelled"),
            "created_by":   a.get("created_by",""),
            "created_at":   a.get("created_at", now_teh()),
            "comment":      a.get("comment",""),
            "is_private":   bool(a.get("is_private", False)),
            "notify_only":  str(a.get("notify_only","")) if a.get("notify_only") else None,
            "active":       bool(a.get("active", True)),
            "last_price":   float(a["last_price"]) if a.get("last_price") is not None else None,
            "last_checked": a.get("last_checked"),
            "fired_at":     a.get("fired_at"),
            "fired_price":  float(a["fired_price"]) if a.get("fired_price") is not None else None,
        }
        # فیلدهای اختیاری — فقط اگه توی جدول وجود داشت اضافه میشن
        if a.get("tag"):
            payload["tag"] = a["tag"]
        if a.get("private_cid") is not None:
            payload["private_cid"] = str(a["private_cid"]) if a["private_cid"] else None

        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/alerts",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=10)
        if r.status_code not in (200,201,204):
            err_text = r.text
            # اگه ستون پیدا نشد، بدون اون فیلد دوباره تلاش کن
            if r.status_code == 400 and "PGRST204" in err_text:
                import re as _re
                missing = _re.search(r"Could not find the '([^']+)'", err_text)
                if missing:
                    col = missing.group(1)
                    print(f"[alerts] ستون '{col}' توی DB نیست — بدون اون retry میکنیم")
                    payload.pop(col, None)
                    r2 = requests.post(
                        f"{SUPABASE_URL}/rest/v1/alerts",
                        headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
                        json=payload, timeout=10)
                    if r2.status_code not in (200,201,204):
                        print(f"[alerts] upsert {a['id']} retry failed: {r2.status_code} {r2.text[:120]}")
                    return
            print(f"[alerts] upsert {a['id']}: {r.status_code} {r.text[:120]}")
    except Exception as e:
        print(f"[alerts] upsert error: {e}")

def _sb_upsert_config(tg, users, errors):
    """config (token + chat_ids + users) رو توی یه ردیف ثابت ذخیره کن"""
    if not SUPABASE_KEY: return
    try:
        payload = {
            "id": "__config__",
            "symbol": "__config__",
            "type": "config",
            "condition": "none",
            "target_price": 0,
            "telegram_token": tg.get("bot_token",""),
            "chat_ids": tg.get("chat_ids", []),
            "users": users,
            "active": False,
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/alerts",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=10)
        if r.status_code not in (200,201,204):
            print(f"[alerts] config save: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[alerts] config save error: {e}")

def _sb_load_all_alerts():
    """همه ردیف‌های جدول alerts رو بخون و به فرمت داخلی تبدیل کن"""
    if not SUPABASE_KEY: return None
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?select=*&limit=2000",
            headers=_sb_h(), timeout=10)
        if r.status_code != 200:
            print(f"[alerts] load failed: {r.status_code} {r.text[:80]}")
            return None
        rows = r.json()
        if not rows:
            print("[alerts] Supabase: جدول خالیه")
            return None

        config_row = next((x for x in rows if x["id"] == "__config__"), None)
        tg = {"bot_token": "", "chat_ids": []}
        users = []
        if config_row:
            tg["bot_token"] = config_row.get("telegram_token","") or ""
            raw_cids = config_row.get("chat_ids") or []
            tg["chat_ids"] = raw_cids if isinstance(raw_cids, list) else json.loads(raw_cids)
            raw_users = config_row.get("users") or []
            users = raw_users if isinstance(raw_users, list) else json.loads(raw_users)

        alerts = []
        archive = []
        for row in rows:
            if row["id"] == "__config__": continue
            a = {
                "id":           row["id"],
                "symbol":       row.get("symbol",""),
                "type":         row.get("type","forex"),
                "condition":    row.get("condition","above"),
                "target_price": row.get("target_price",0),
                "created_by":   row.get("created_by",""),
                "created_at":   row.get("created_at",""),
                "comment":      row.get("comment",""),
                "is_private":   row.get("is_private", False),
                "private_cid":  row.get("private_cid") or row.get("notify_only"),
                "notify_only":  row.get("notify_only"),
                "active":       row.get("active", True),
                "last_price":   row.get("last_price"),
                "last_checked": row.get("last_checked"),
                "fired_at":     row.get("fired_at"),
                "fired_price":  row.get("fired_price"),
            }
            status = row.get("status","active")
            if status == "fired" or row.get("fired_at"):
                archive.append(a)
            elif status == "active" and row.get("active"):
                alerts.append(a)

        data = {"alerts": alerts, "archive": archive, "telegram": tg,
                "users": users, "errors": [], "last_update": now_teh()}
        print(f"[alerts] Loaded from Supabase — {len(alerts)} active, {len(archive)} archived")
        return data
    except Exception as e:
        print(f"[alerts] load error: {e}")
        return None



def load_alerts():
    global _cache_alerts
    if _cache_alerts is not None:
        return _cache_alerts
    # 1. Supabase
    d = _sb_load_all_alerts()
    if d is not None:
        _cache_alerts = d
        return _cache_alerts
    # 2. local fallback
    if os.path.exists(ALERTS_FILE):
        try:
            with open(ALERTS_FILE, "r", encoding="utf-8") as f:
                _cache_alerts = fix_alerts(json.load(f))
                return _cache_alerts
        except: pass
    _cache_alerts = _empty_alerts()
    return _cache_alerts

def _sb_delete_alert(aid):
    """یه آلارم رو از Supabase حذف کن"""
    _deleted_ids.add(str(aid))  # فوری به blacklist اضافه کن
    if not SUPABASE_KEY: return
    try:
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/alerts?id=eq.{aid}",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            timeout=8)
        print(f"[alerts] delete {aid}: status={r.status_code} body={r.text[:80]}")
    except Exception as e:
        print(f"[alerts] delete error: {e}")

def save_alerts(data):
    """cache رو آپدیت کن + local backup — Supabase رو در background بزن"""
    global _cache_alerts
    _cache_alerts = data
    # local backup سریع
    try:
        with open(ALERTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[alerts] local backup error: {e}")
    # Supabase در background — بلاک نمیشه
    if SUPABASE_KEY:
        snapshot = (
            data.get("telegram",{}),
            data.get("users",[]),
            data.get("errors",[]),
            list(data.get("alerts",[]))
        )
        def _bg(snap=snapshot):
            tg, users, errors, alerts = snap
            _sb_upsert_config(tg, users, errors)
            for a in alerts:
                _sb_upsert_alert(a)
        threading.Thread(target=_bg, daemon=True).start()

def save_alert_fired(a):
    """وقتی آلارم fire میشه — ردیف رو آپدیت کن و cache رو پاک کن تا archive درست لود بشه"""
    global _cache_alerts
    _sb_upsert_alert(a)
    # cache رو پاک کن تا دفعه بعد از Supabase بخونه و archive آپدیت بشه
    _cache_alerts = None

# =====================================================================
# Supabase
# =====================================================================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://erwimqqskkzcsayvhxot.supabase.co")
APP_BASE_URL = os.environ.get("APP_BASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

def _sb_h():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }

def _sb_upsert(trade):
    """upsert یه ترید — اگه بود آپدیت کن اگه نبود insert"""
    tid = trade.get("id")
    if not tid: return
    trade_light = {k: v for k, v in trade.items() if k != "candle_snapshot"}
    candles = trade.get("candle_snapshot", [])
    payload = {
        "trade_id": tid,
        "sym": trade.get("sym", ""),
        "created_at": trade.get("createdAt", now_teh()),
        "data": json.dumps(trade_light, ensure_ascii=False),
        "candles": json.dumps(candles, ensure_ascii=False)
    }
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/trades",
        headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=payload, timeout=15)
    if r.status_code not in (200, 201, 204):
        print(f"[SB] upsert {tid}: {r.status_code} {r.text[:80]}")

def load_journal():
    global _cache_journal
    if _cache_journal is not None:
        print(f"[JOURNAL:LOAD] از cache — {len(_cache_journal)} ترید")
        return _cache_journal

    if SUPABASE_KEY:
        try:
            print("[JOURNAL:LOAD] از Supabase...")
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/trades?order=created_at.desc&limit=2000",
                headers=_sb_h(), timeout=15)
            if r.status_code == 200:
                _cache_journal = []
                for row in r.json():
                    d = row.get("data", {})
                    trade = json.loads(d) if isinstance(d, str) else (d or {})
                    candles_raw = row.get("candles", "[]")
                    trade["candle_snapshot"] = json.loads(candles_raw) if isinstance(candles_raw, str) else (candles_raw or [])
                    _cache_journal.append(trade)
                print(f"[JOURNAL:LOAD] ✅ {len(_cache_journal)} ترید از Supabase")
                return _cache_journal
            else:
                print(f"[JOURNAL:LOAD] Supabase: {r.status_code} {r.text[:100]}")
        except Exception as e:
            print(f"[JOURNAL:LOAD] Supabase error: {e}")


    _cache_journal = []
    return _cache_journal

def save_trade(trade):
    """فقط یه ترید رو ذخیره/آپدیت کن — سریع‌تر از save_journal"""
    global _cache_journal
    if _cache_journal is not None:
        exists = False
        for i, t in enumerate(_cache_journal):
            if t.get("id") == trade.get("id"):
                _cache_journal[i] = trade
                exists = True
                break
        if not exists:
            _cache_journal.insert(0, trade)
    _sb_upsert(trade)

def save_journal(journal_list):
    global _cache_journal
    _cache_journal = journal_list
    if not SUPABASE_KEY:
        print("[JOURNAL:SAVE] SUPABASE_KEY نیست")
        return
    try:
        for trade in journal_list:
            _sb_upsert(trade)
        print(f"[JOURNAL:SAVE] ✅ {len(journal_list)} ترید در Supabase")
    except Exception as e:
        print(f"[JOURNAL:SAVE] error: {e}")

def _sb_delete(trade_id):
    """یه ترید رو از Supabase حذف کن"""
    if not SUPABASE_KEY:
        return
    try:
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/trades?trade_id=eq.{trade_id}",
            headers=_sb_h(), timeout=10)
        if r.status_code in (200, 204):
            print(f"[SB] delete {trade_id}: ✅")
        else:
            print(f"[SB] delete {trade_id}: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[SB] delete error: {e}")

def _sb_delete_all():
    """همه تریدها رو از Supabase پاک کن"""
    if not SUPABASE_KEY:
        return
    try:
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/trades?trade_id=neq.null",
            headers=_sb_h(), timeout=15)
        if r.status_code in (200, 204):
            print(f"[SB] delete_all: ✅")
        else:
            print(f"[SB] delete_all: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[SB] delete_all error: {e}")

def get_trade_candles(trade_id):
    """کندل‌های یه ترید رو جداگانه بخون"""
    if not SUPABASE_KEY: return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/trades?trade_id=eq.{trade_id}&select=candles",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200 and r.json():
            c = r.json()[0].get("candles", "[]")
            return json.loads(c) if isinstance(c, str) else (c or [])
    except: pass
    return []

def log_error(msg):
    try:
        data = load_alerts()
        errs = data.get("errors", [])
        errs.append({"time": now_teh(), "msg": str(msg)})
        data["errors"] = errs[-20:]
        save_alerts(data)
    except:
        pass
    print(f"[ERR] {msg}")

def is_forex_market_open():
    now_utc = datetime.utcnow()
    wd = now_utc.weekday()
    if wd == 5: return False
    if wd == 6: return now_utc.hour >= 21
    return True

H = {"User-Agent": "Mozilla/5.0 (compatible; PriceBot/1.0)"}
_last_known = {}

def get_forex_prices_batch(symbols):
    if not symbols: return {}
    clean = [s.upper().replace("/", "").replace(" ", "") for s in symbols]
    qs = "&".join(f"symbols={s}" for s in clean)
    url = f"https://biquote.io/api/latest?{qs}"
    try:
        r = requests.get(url, timeout=12, headers=H)
        r.raise_for_status()
        raw = r.json()
        result = {}
        if isinstance(raw, list):
            for item in raw:
                sym = item.get("symbol","").upper().replace("/","")
                bid = item.get("bid") or item.get("price") or item.get("last")
                if sym and bid and float(bid) > 0:
                    result[sym] = float(bid)
                    _last_known[sym] = {"price": float(bid), "ts": now_teh(), "stale": False}
        elif isinstance(raw, dict):
            for sym, data in raw.items():
                if isinstance(data, dict):
                    bid = data.get("bid") or data.get("price") or data.get("last")
                elif isinstance(data, (int, float)):
                    bid = data
                else:
                    bid = None
                if bid and float(bid) > 0:
                    result[sym.upper()] = float(bid)
                    _last_known[sym.upper()] = {"price": float(bid), "ts": now_teh(), "stale": False}
        if result: return result
    except Exception: pass
    result = {}
    for sym in clean:
        if sym in _last_known:
            cached = _last_known[sym]
            _last_known[sym]["stale"] = True
            result[sym] = cached["price"]
        else:
            try:
                base, quote = sym[:3], sym[3:6]
                r3 = requests.get(f"https://api.frankfurter.app/latest?from={base}&to={quote}", timeout=7)
                if r3.ok:
                    rate = r3.json().get("rates", {}).get(quote)
                    if rate:
                        result[sym] = float(rate)
                        _last_known[sym] = {"price": float(rate), "ts": now_teh(), "stale": False}
            except Exception: pass
    return result

def get_forex_price(symbol):
    sym = symbol.upper().replace("/","").replace(" ","")
    batch = get_forex_prices_batch([sym])
    return batch.get(sym)

CG_MAP = {
    "BTC":"bitcoin","ETH":"ethereum","BNB":"binancecoin","SOL":"solana",
    "XRP":"ripple","ADA":"cardano","DOGE":"dogecoin","TRX":"tron",
    "TON":"toncoin","AVAX":"avalanche-2","LINK":"chainlink","DOT":"polkadot",
    "MATIC":"matic-network","UNI":"uniswap","ATOM":"cosmos","LTC":"litecoin",
    "SHIB":"shiba-inu","OP":"optimism","ARB":"arbitrum","NEAR":"near",
}

def _cg_price(base):
    gid = CG_MAP.get(base)
    if not gid: return None
    try:
        d = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={gid}&vs_currencies=usd", headers=H, timeout=8).json()
        return float(d[gid]["usd"])
    except:
        return None

def get_crypto_price(symbol):
    base = symbol.upper()
    for s in ["USDT","USDC","USD","BUSD"]:
        base = base.replace(s,"")
    base = base.replace("/","").strip()
    try:
        r = requests.get(f"https://biquote.io/api/latest?symbols={base}USD", timeout=8, headers=H)
        if r.ok:
            raw = r.json()
            bid = None
            if isinstance(raw, list) and raw:
                bid = raw[0].get("bid") or raw[0].get("price") or raw[0].get("last")
            elif isinstance(raw, dict):
                bid = raw.get("bid") or raw.get("price") or raw.get("last")
            if bid and float(bid) > 100:
                return float(bid)
    except Exception: pass
    sources = [
        ("OKX", lambda: float(requests.get(f"https://www.okx.com/api/v5/market/ticker?instId={base}-USDT", headers=H).json()["data"][0]["last"])),
        ("Binance-USDT", lambda: float(requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={base}USDT", headers=H).json()["price"])),
    ]
    for name, fn in sources:
        try:
            p = fn()
            if p and p > 0:
                return float(p)
        except: pass
    log_error(f"Crypto price failed for {symbol}")
    return None

def get_price(symbol, asset_type):
    if asset_type == "crypto":
        return get_crypto_price(symbol)
    return get_forex_price(symbol)

# ── ردیابی پیام‌های ربات برای پاک‌سازی چت ────────────────────
# { chat_id: [msg_id, msg_id, ...] }  — فقط پیام‌های غیر-fire
_bot_msg_ids: dict = {}
_BOT_MSG_MAX = 200  # حداکثر تعداد id هر چت

# ── ردیابی پیام‌های fired alert در همه چت‌ها ──────────────────
# { alert_id: { chat_id: message_id, ... } }
_fired_msg_ids: dict = {}

def _sb_save_fired_msgs(alert_id: str, cid_to_mid: dict):
    """map چت‌آیدی → مسیج‌آیدی یه آلارم fired رو توی Supabase ذخیره کن"""
    if not SUPABASE_KEY: return
    try:
        payload = {
            "id": alert_id,
            "msg_map": json.dumps(cid_to_mid),
            "created_at": now_teh()
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/fired_msgs",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=8)
        if r.status_code not in (200, 201, 204):
            print(f"[fired_msgs] save error: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[fired_msgs] save exc: {e}")

def _sb_load_fired_msgs():
    """همه fired_msgs رو از Supabase بخون و توی _fired_msg_ids لود کن"""
    if not SUPABASE_KEY: return
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/fired_msgs?select=*&limit=2000",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200:
            for row in r.json():
                aid = row.get("id")
                raw = row.get("msg_map", "{}")
                if aid:
                    _fired_msg_ids[aid] = json.loads(raw) if isinstance(raw, str) else (raw or {})
            print(f"[fired_msgs] لود شد — {len(_fired_msg_ids)} آلارم")
        else:
            print(f"[fired_msgs] load error: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[fired_msgs] load exc: {e}")

def _sb_delete_fired_msgs(alert_id: str):
    """fired_msgs یه آلارم رو از Supabase پاک کن"""
    if not SUPABASE_KEY: return
    try:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/fired_msgs?id=eq.{alert_id}",
            headers=_sb_h(), timeout=8)
    except Exception as e:
        print(f"[fired_msgs] delete exc: {e}")

# ── شمارنده هشتگ نماد ────────────────────────────────────────
# { "XAUUSD": 12, "BTCUSDT": 3, ... }  — در حافظه cache
_sym_counters: dict = {}

def _sb_next_sym_counter(sym: str) -> int:
    """
    شمارنده نماد رو یکی افزایش بده و مقدار جدید رو برگردون.
    اگه جدول symbol_counters نداشتیم از حافظه استفاده میکنه.
    """
    global _sym_counters
    if SUPABASE_KEY:
        try:
            # خوندن مقدار فعلی
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/symbol_counters?id=eq.{sym}&select=counter",
                headers=_sb_h(), timeout=6)
            if r.status_code == 200 and r.json():
                cur = int(r.json()[0]["counter"])
            else:
                cur = _sym_counters.get(sym, 0)
            new_val = cur + 1
            # upsert
            requests.post(
                f"{SUPABASE_URL}/rest/v1/symbol_counters",
                headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
                json={"id": sym, "counter": new_val}, timeout=6)
            _sym_counters[sym] = new_val
            return new_val
        except Exception as e:
            print(f"[counter] exc: {e}")
    # fallback حافظه
    _sym_counters[sym] = _sym_counters.get(sym, 0) + 1
    return _sym_counters[sym]

def _sb_load_sym_counters():
    """همه شمارنده‌ها رو از Supabase لود کن"""
    if not SUPABASE_KEY: return
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/symbol_counters?select=*",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            for row in r.json():
                _sym_counters[row["id"]] = int(row["counter"])
            print(f"[counter] لود شد — {len(_sym_counters)} نماد")
    except Exception as e:
        print(f"[counter] load exc: {e}")

def _make_alarm_tag(sym: str) -> str:
    """هشتگ منحصربه‌فرد برای آلارم — مثلاً #XAUUSD7"""
    n = _sb_next_sym_counter(sym)
    return f"#{sym}{n}"

# =====================================================================
# 📋 سیستم تقسیم مسئولیت آلارم‌ها بین اعضای تیم
# =====================================================================

SHIFT_MORNING = {
    "start": 8, "end": 16,
   "members": ["اتابک", "مهران"]
}
SHIFT_EVENING = {
    "start": 16, "end": 20,
    "members": ["مسعود", "فرهاد", "علی", "پیمان"]
}

def _is_weekend_tehran():
    """شنبه (5) و یکشنبه (6) بازار بسته — weekday: 0=Mon ... 5=Sat, 6=Sun"""
    return datetime.now(TEHRAN).weekday() in (5, 6)

def _get_current_shift():
    """
    شیفت فعلی بر اساس ساعت تهران.
    شنبه/یکشنبه همیشه night (بدون مسئول).
    """
    if _is_weekend_tehran():
        return "night"
    h = datetime.now(TEHRAN).hour
    if SHIFT_MORNING["start"] <= h < SHIFT_MORNING["end"]:
        return "morning"
    if SHIFT_EVENING["start"] <= h < SHIFT_EVENING["end"]:
        return "evening"
    return "night"

def _get_shift_members(shift_name):
    if shift_name == "morning": return SHIFT_MORNING["members"]
    if shift_name == "evening": return SHIFT_EVENING["members"]
    return []

def _next_monday_8am():
    """دوشنبه ۸ صبح بعدی — برای آلارم‌های آخر هفته"""
    now_dt = datetime.now(TEHRAN)
    days_ahead = (7 - now_dt.weekday()) % 7  # دوشنبه = 0
    if days_ahead == 0 and now_dt.hour >= 8:
        days_ahead = 7
    target = now_dt.replace(hour=8, minute=0, second=0, microsecond=0)
    target = target + timedelta(days=days_ahead)
    return target

# { member_name: count_of_active_assignments }  — in-memory cache
_active_assign_count: dict = {}

# جلوگیری از double-handover: startup و scheduler هر کدوم فقط یه بار اجرا کنن
_startup_handover_done: dict = {}   # {"8am": True, "16": True, "20": True}
_startup_handover_lock = threading.Lock()

# جلوگیری از race condition در /False — اگه یه آلارم داره false میشه، دیگران صبر کنن
_false_in_progress: set = set()
_false_in_progress_lock = threading.Lock()

# ─── Supabase helpers ────────────────────────────────────────────────

def _sb_save_assignment(alarm_id: str, alarm_tag: str, assignee: str, shift: str, fired_at: str,
                        symbol: str = "", target_price: float = 0, created_by: str = ""):
    """ذخیره/آپدیت assignment در Supabase"""
    if not SUPABASE_KEY: return
    try:
        payload = {
            "id": alarm_id,
            "alarm_tag": alarm_tag,
            "assigned_to": assignee,
            "shift": shift,
            "is_active": True,
            "fired_at": fired_at,
            "false_at": None,
            "false_by": None,
            "symbol": symbol,
            "target_price": target_price,
            "created_by": created_by
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=8)
        if r.status_code not in (200, 201, 204):
            print(f"[assign] save error: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[assign] save exc: {e}")

def _sb_handover_assignment(alarm_id: str, alarm_tag: str, assignee: str, new_shift: str):
    """
    آپدیت assignment هنگام handover بین شیفت‌ها.
    فقط assigned_to و shift رو عوض می‌کنه — false_at/false_by/false_history دست نمی‌زنه.
    """
    if not SUPABASE_KEY: return
    try:
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{alarm_id}",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json={"assigned_to": assignee, "shift": new_shift},
            timeout=8)
        if r.status_code not in (200, 204):
            print(f"[assign] handover error: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[assign] handover exc: {e}")
    """آپدیت shift آلارم در Supabase — برای انتقال بین شیفت‌ها"""
    if not SUPABASE_KEY: return
    try:
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{alarm_id}",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json={"shift": new_shift},
            timeout=8)
        if r.status_code not in (200, 204):
            print(f"[assign] update_shift error: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"[assign] update_shift exc: {e}")

def _sb_false_assignment(alarm_id: str, false_by: str, reason: str = ""):
    """
    وقتی /False زده میشه — assignment رو غیرفعال کن.
    اگه قبلاً false شده بود، یه ردیف history جدید append کن (به جای overwrite).
    """
    if not SUPABASE_KEY: return
    try:
        # خوندن وضعیت فعلی برای گرفتن history قبلی
        r_get = requests.get(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{alarm_id}&select=is_active,false_history",
            headers=_sb_h(), timeout=8)
        prev_history = []
        already_false = False
        if r_get.status_code == 200:
            rows = r_get.json()
            if rows:
                already_false = (rows[0].get("is_active") == False)
                prev_history = rows[0].get("false_history") or []
                if isinstance(prev_history, str):
                    try: prev_history = json.loads(prev_history)
                    except: prev_history = []

        new_entry = {"by": false_by, "at": now_teh(), "reason": reason}
        prev_history.append(new_entry)

        payload = {
            "is_active": False,
            "false_at": now_teh(),
            "false_by": false_by,
            "false_history": prev_history
        }
        if reason:
            payload["false_reason"] = reason
        r = requests.patch(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{alarm_id}",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json=payload, timeout=8)
        if r.status_code not in (200, 204):
            print(f"[assign] false error: {r.status_code} {r.text[:80]}")
        return already_false  # True اگه قبلاً false شده بود (یعنی این آپدیته)
    except Exception as e:
        print(f"[assign] false exc: {e}")
        return False

def _sb_load_active_assignments():
    """لود همه assignment‌های فعال از Supabase"""
    if not SUPABASE_KEY: return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?is_active=eq.true&select=*",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[assign] load exc: {e}")
    return []

def _sb_load_pending_shifts(shifts: list):
    """لود آلارم‌های active با shift های مشخص از Supabase"""
    if not SUPABASE_KEY: return []
    try:
        shift_filter = ",".join(f'"{s}"' for s in shifts)
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alarm_assignments?is_active=eq.true&shift=in.({shift_filter})&select=*",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[assign] load_pending exc: {e}")
    return []

def _rebuild_active_assign_count(rows):
    """بازسازی count از Supabase rows — فقط assigned آلارم‌ها"""
    global _active_assign_count
    _active_assign_count = {}
    for row in rows:
        name = row.get("assigned_to", "")
        if name:
            _active_assign_count[name] = _active_assign_count.get(name, 0) + 1
    print(f"[assign] active counts: {_active_assign_count}")

# ─── انتخاب مسئول ────────────────────────────────────────────────────

def _pick_assignee(members: list) -> str:
    """
    از بین اعضا کسی رو انتخاب کن که کمترین آلارم فعال داره.
    اگه چند نفر مساوی دارن، رندوم از بین اونها انتخاب کن.
    """
    import random
    if not members:
        return ""
    counts = {m: _active_assign_count.get(m, 0) for m in members}
    min_count = min(counts.values())
    candidates = [m for m, c in counts.items() if c == min_count]
    chosen = random.choice(candidates)
    _active_assign_count[chosen] = _active_assign_count.get(chosen, 0) + 1
    return chosen

def _get_assignee_for_alarm(alarm_id: str, alarm_tag: str, fired_at: str,
                            symbol: str = "", target_price: float = 0, created_by: str = "") -> tuple:
    """
    مسئول آلارم رو تعیین کن.
    شب / شنبه / یکشنبه → ("", "night")  بدون مسئول
    """
    shift = _get_current_shift()
    members = _get_shift_members(shift)
    if not members:
        threading.Thread(
            target=_sb_save_assignment,
            args=(alarm_id, alarm_tag, "", "night", fired_at),
            kwargs={"symbol": symbol, "target_price": target_price, "created_by": created_by},
            daemon=True
        ).start()
        return ("", "night")
    assignee = _pick_assignee(members)
    threading.Thread(
        target=_sb_save_assignment,
        args=(alarm_id, alarm_tag, assignee, shift, fired_at),
        kwargs={"symbol": symbol, "target_price": target_price, "created_by": created_by},
        daemon=True
    ).start()
    return (assignee, shift)

# ─── تابع مشترک ارسال reply تقسیم ──────────────────────────────────

def _send_handover_replies(rows: list, target_members: list, label: str):
    """
    برای هر آلارم در rows یه reply به همه چت‌ها بفرست و assignee تعیین کن.
    label: برچسب نمایشی مثل 'تقسیم آلارم صبح' یا 'تقسیم آلارم شب'
    """
    if not rows: return
    token, cids, _ = _get_token_and_cids()
    for row in rows:
        aid = row.get("id")
        tag = row.get("alarm_tag", "")
        old_assignee = row.get("assigned_to", "")
        assignee = _pick_assignee(target_members) if target_members else ""
        # کاهش شمارش مسئول قبلی (دیگه این آلارم رو نداره)
        if old_assignee and old_assignee in _active_assign_count:
            _active_assign_count[old_assignee] = max(0, _active_assign_count[old_assignee] - 1)
        if assignee:
            reply_text = f"🔄 <b>{label}</b>\n\n{tag}\n👤 مسئول: <b>{assignee}</b>"
            new_shift = "morning" if target_members == SHIFT_MORNING["members"] else "evening"
        else:
            reply_text = f"🔄 <b>{label}</b>\n\n{tag}\n⏳ تقسیم دوشنبه ۸ صبح"
            new_shift = "weekend_pending"
        # ارسال reply به همه چت‌ها
        msg_map = _fired_msg_ids.get(aid, {})
        for tc, tm in msg_map.items():
            if tc in ("__tag__", "__text__"): continue
            try:
                requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": tc, "text": reply_text,
                          "parse_mode": "HTML", "reply_to_message_id": tm},
                    timeout=8, headers=H)
            except: pass
        # آپدیت shift و assignee در Supabase — بدون دست زدن به false fields
        threading.Thread(
            target=lambda a=aid, s=new_shift, asn=assignee, tg=tag: (
                _sb_handover_assignment(a, tg, asn, s)
            ),
            daemon=True
        ).start()
    print(f"[assign] {label}: {len(rows)} آلارم تقسیم شد")

# ─── Scheduler اصلی ──────────────────────────────────────────────────

def _assignment_scheduler():
    """
    یه scheduler واحد که منتظر ساعت‌های کلیدی میمونه:
    ۸ صبح (روزهای کاری)  — تقسیم آلارم‌های شب/عصر/آخر هفته
    ۱۶                    — انتقال آلارم‌های صبح به شیفت عصر
    ۲۰                    — انتقال آلارم‌های عصر به شب
    دوشنبه ۸ صبح         — تقسیم آلارم‌های آخر هفته
    """
    while True:
        try:
            now_dt = datetime.now(TEHRAN)
            weekday = now_dt.weekday()  # 0=Mon ... 5=Sat, 6=Sun
            h = now_dt.hour

            # ── محاسبه نزدیک‌ترین event بعدی ──────────────────────
            candidates = []

            # ۸ صبح روزهای کاری (دوشنبه تا جمعه)
            for d in range(7):
                t = now_dt.replace(hour=8, minute=0, second=0, microsecond=0) + timedelta(days=d)
                if t > now_dt and t.weekday() not in (5, 6):
                    candidates.append(("8am", t))
                    break

            # ۱۶ روزهای کاری
            for d in range(7):
                t = now_dt.replace(hour=16, minute=0, second=0, microsecond=0) + timedelta(days=d)
                if t > now_dt and t.weekday() not in (5, 6):
                    candidates.append(("16", t))
                    break

            # ۲۰ روزهای کاری
            for d in range(7):
                t = now_dt.replace(hour=20, minute=0, second=0, microsecond=0) + timedelta(days=d)
                if t > now_dt and t.weekday() not in (5, 6):
                    candidates.append(("20", t))
                    break

            if not candidates:
                time.sleep(300)
                continue

            # نزدیک‌ترین event
            event_name, target_dt = min(candidates, key=lambda x: x[1])
            wait_sec = (target_dt - now_dt).total_seconds()
            print(f"[assign] scheduler: {event_name} — {wait_sec/3600:.1f} ساعت دیگه ({target_dt.strftime('%a %H:%M')})")
            time.sleep(max(wait_sec, 0))

            # ── بررسی missed event بعد از restart (grace window 10 دقیقه) ──
            # اگه سرور restart شده و event در ۱۰ دقیقه گذشته miss شده، همین الان اجرا کن
            _GRACE_SEC = 600
            now_after_sleep = datetime.now(TEHRAN)
            if now_after_sleep.weekday() not in (5, 6):
                for _mh, _me in [(8, "8am"), (16, "16"), (20, "20")]:
                    _mt = now_after_sleep.replace(hour=_mh, minute=0, second=0, microsecond=0)
                    _diff = (now_after_sleep - _mt).total_seconds()
                    if 0 < _diff <= _GRACE_SEC and event_name != _me:
                        print(f"[assign] ⚠️ event {_me} در {_diff:.0f} ثانیه پیش miss شده — اجرای فوری")
                        event_name = _me
                        break

            # ── اجرای event ────────────────────────────────────────
            # چک کن startup این event رو قبلاً اجرا کرده یا نه (جلوگیری از double-handover)
            with _startup_handover_lock:
                already_done = _startup_handover_done.pop(event_name, False)
            if already_done:
                print(f"[assign] scheduler: {event_name} قبلاً در startup اجرا شده — skip")
                continue

            if event_name == "8am":
                # تقسیم همه آلارم‌های باز: night, evening_handover, weekend_pending
                rows = _sb_load_pending_shifts(["night", "evening_handover", "weekend_pending", "evening"])
                if rows:
                    _send_handover_replies(rows, SHIFT_MORNING["members"], "تقسیم آلارم شب")
                    # rebuild count بعد از تقسیم
                    threading.Thread(target=lambda: _rebuild_active_assign_count(
                        _sb_load_active_assignments()), daemon=True).start()
                else:
                    print("[assign] ۸ صبح: آلارم باز برای تقسیم نبود")

            elif event_name == "16":
                # انتقال آلارم‌های صبح که False نخوردن به شیفت عصر
                rows = _sb_load_pending_shifts(["morning", "morning_handover"])
                if rows:
                    _send_handover_replies(rows, SHIFT_EVENING["members"], "انتقال از شیفت صبح")
                    threading.Thread(target=lambda: _rebuild_active_assign_count(
                        _sb_load_active_assignments()), daemon=True).start()
                else:
                    print("[assign] ۱۶: آلارم صبحی برای انتقال نبود")

            elif event_name == "20":
                # انتقال آلارم‌های عصر به night — بدون reply، فردا ۸ صبح تقسیم میشن
                rows = _sb_load_pending_shifts(["evening", "evening_handover"])
                if rows:
                    for row in rows:
                        threading.Thread(
                            target=_sb_update_shift,
                            args=(row["id"], "night"),
                            daemon=True
                        ).start()
                    print(f"[assign] ۲۰: {len(rows)} آلارم عصر → night (فردا ۸ صبح تقسیم)")
                else:
                    print("[assign] ۲۰: آلارم عصری برای انتقال نبود")

        except Exception as e:
            print(f"[assign] scheduler error: {e}")
            time.sleep(300)

# ─── startup: بازسازی state از Supabase ─────────────────────────────

def _check_missed_shifts_on_startup():
    """
    موقع startup چک کن کدوم شیفت‌های امروز miss شدن و اجرا کن.
    اگه الان بین ۱۶-۲۰ هستیم و آلارم morning داریم → انتقال بده
    اگه الان بعد از ۲۰ هستیم و آلارم morning/evening داریم → شب کن
    اگه الان بعد از ۸ هستیم و آلارم night داریم → تقسیم کن
    """
    now_dt = datetime.now(TEHRAN)
    weekday = now_dt.weekday()
    h = now_dt.hour

    if weekday in (5, 6):  # شنبه/یکشنبه
        return

    print(f"[assign] startup: چک missed shifts — ساعت {h}:00")

    # ساعت ۸ تا ۱۶ — چک کن آلارم‌های شب تقسیم شدن
    if 8 <= h < 16:
        rows = _sb_load_pending_shifts(["night", "evening_handover", "weekend_pending"])
        if rows:
            print(f"[assign] startup: {len(rows)} آلارم شب تقسیم نشده — انجام میشه")
            threading.Thread(
                target=_send_handover_replies,
                args=(rows, SHIFT_MORNING["members"], "تقسیم آلارم شب (جبران)"),
                daemon=True).start()
            with _startup_handover_lock:
                _startup_handover_done["8am"] = True

    # ساعت ۱۶ تا ۲۰ — چک کن صبح‌ها انتقال پیدا کردن
    elif 16 <= h < 20:
        rows = _sb_load_pending_shifts(["morning", "morning_handover"])
        if rows:
            print(f"[assign] startup: {len(rows)} آلارم صبح انتقال نشده — انجام میشه")
            threading.Thread(
                target=_send_handover_replies,
                args=(rows, SHIFT_EVENING["members"], "انتقال از شیفت صبح (جبران)"),
                daemon=True).start()
            with _startup_handover_lock:
                _startup_handover_done["16"] = True

    # بعد از ۲۰ — چک کن عصری‌ها به night رفتن
    elif h >= 20:
        rows_ev = _sb_load_pending_shifts(["morning", "evening", "evening_handover"])
        if rows_ev:
            print(f"[assign] startup: {len(rows_ev)} آلارم عصر→شب نرفته — انجام میشه")
            for row in rows_ev:
                threading.Thread(
                    target=_sb_update_shift,
                    args=(row["id"], "night"),
                    daemon=True).start()
            print(f"[assign] {len(rows_ev)} آلارم → night")
            with _startup_handover_lock:
                _startup_handover_done["20"] = True

def _sb_restore_on_startup():
    """
    بعد از هر restart همه چیز رو از Supabase بازسازی کن.
    هیچ چیزی از حافظه از دست نمیره.
    """
    rows = _sb_load_active_assignments()
    _rebuild_active_assign_count(rows)
    print(f"[assign] startup: {len(rows)} آلارم active از Supabase بازسازی شد")
    # جبران شیفت‌های miss شده
    _check_missed_shifts_on_startup()



def _track_msg(chat_id: str, msg_id: int):
    """id پیام ربات رو ذخیره کن (غیر از fired alerts)"""
    cid = str(chat_id)
    if cid not in _bot_msg_ids:
        _bot_msg_ids[cid] = []
    _bot_msg_ids[cid].append(msg_id)
    # سقف حافظه
    if len(_bot_msg_ids[cid]) > _BOT_MSG_MAX:
        _bot_msg_ids[cid] = _bot_msg_ids[cid][-_BOT_MSG_MAX:]

def delete_chat_history(token: str, chat_id: str):
    """همه پیام‌های track‌شده رو پاک کن — آلارم‌های fire دست‌نخورده می‌مونن"""
    cid = str(chat_id)
    ids = _bot_msg_ids.pop(cid, [])
    deleted = 0
    for mid in ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/deleteMessage",
                json={"chat_id": cid, "message_id": mid},
                timeout=5, headers=H)
            deleted += 1
        except: pass
    return deleted

def send_tg(token, chat_id, text):
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}, timeout=10, headers=H)
        mid = r.json().get("result", {}).get("message_id")
        if mid:
            _track_msg(str(chat_id), mid)
        return r.status_code == 200
    except: return False

def broadcast(token, chat_ids, text):
    return [send_tg(token, c, text) for c in chat_ids]

def send_reply_keyboard(token, chat_id, text, rows):
    """ارسال پیام با Reply Keyboard (جای کیبورد موبایل)"""
    try:
        markup = {
            "keyboard": rows,
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "input_field_placeholder": "یه گزینه انتخاب کن..."
        }
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": str(chat_id), "text": text,
                  "parse_mode": "HTML", "reply_markup": markup},
            timeout=10, headers=H)
        mid = r.json().get("result", {}).get("message_id")
        if mid:
            _track_msg(str(chat_id), mid)
        return mid
    except: return None

def edit_reply_keyboard(token, chat_id, message_id, text, rows=None):
    """ادیت پیام با Reply Keyboard یا بدون keyboard — برای flow ثبت آلارم"""
    try:
        if rows is not None:
            markup = {"keyboard": rows, "resize_keyboard": True, "one_time_keyboard": False}
        else:
            markup = {"remove_keyboard": True}
        # editMessageText نمیتونه reply_markup از نوع ReplyKeyboard داشته باشه
        # پس اول پیام رو ادیت میکنیم، بعد اگه keyboard جدید داریم یه پیام کمکی میفرستیم
        requests.post(
            f"https://api.telegram.org/bot{token}/editMessageText",
            json={"chat_id": str(chat_id), "message_id": message_id,
                  "text": text, "parse_mode": "HTML"},
            timeout=10, headers=H)
    except: pass

def _alarm_edit(token, cid, bot_msg_id, text):
    """ادیت پیام جاری flow آلارم (فقط متن، بدون keyboard)"""
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/editMessageText",
            json={"chat_id": str(cid), "message_id": bot_msg_id,
                  "text": text, "parse_mode": "HTML"},
            timeout=10, headers=H)
    except Exception as e:
        print(f"[alarm_edit] {e}")


    """حذف Reply Keyboard و ارسال پیام"""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": str(chat_id), "text": text,
                  "parse_mode": "HTML",
                  "reply_markup": {"remove_keyboard": True}},
            timeout=10, headers=H)
        mid = r.json().get("result", {}).get("message_id")
        if mid:
            _track_msg(str(chat_id), mid)
        return mid
    except: return None

def send_tg_keyboard(token, chat_id, text, keyboard, track=True):
    """ارسال پیام با inline keyboard"""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": str(chat_id), "text": text,
                  "parse_mode": "HTML", "reply_markup": {"inline_keyboard": keyboard}},
            timeout=10, headers=H)
        mid = r.json().get("result", {}).get("message_id")
        if mid and track:
            _track_msg(str(chat_id), mid)
        return mid
    except: return None

def edit_tg_keyboard(token, chat_id, message_id, text, keyboard):
    """ویرایش پیام با inline keyboard"""
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/editMessageText",
            json={"chat_id": str(chat_id), "message_id": message_id,
                  "text": text, "parse_mode": "HTML",
                  "reply_markup": {"inline_keyboard": keyboard}},
            timeout=10, headers=H)
    except: pass

def answer_callback(token, callback_id, text=""):
    """جواب به callback query"""
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text},
            timeout=10, headers=H)
    except: pass

# ── reminder state ─────────────────────────────────────────────
# memory: { cid: { sym: {"interval": int, "active": bool, "tf_sec": int} } }
# Supabase: جدول reminders — هر ردیف یه reminder فعال
_reminders = {}

# ── candle-close helpers ──────────────────────────────────────
_TF_OFFSET = {300: 60, 900: 300, 3600: 900, 14400: 900}  # ثانیه قبل از کلوز
_TF_LABEL  = {300:"M5", 900:"M15", 3600:"H1", 14400:"H4"}
REMINDER_QUICK_SYMBOLS = ["XAUUSD", "EURUSD", "GBPUSD", "BTCUSDT", "ETHUSDT", "USDJPY"]

def _candle_info(tf_sec):
    """
    برمی‌گردونه: (wait_sec, close_teh_str, progress_pct)
    - اگه هنوز وقت هشدار این کندل نرسیده → صبر کن
    - اگه توی پنجره هشدار هستیم (بین alert_time و close) → فوری (۵ ثانیه)
    - اگه کندل بسته → کندل بعدی
    """
    offset = _TF_OFFSET.get(tf_sec, 300)
    now_utc = time.time()
    cur_close = (int(now_utc) // tf_sec + 1) * tf_sec
    alert_t   = cur_close - offset
    wait      = alert_t - now_utc
    if wait <= 0:
        if now_utc < cur_close:
            wait, use_close = 5, cur_close          # توی پنجره → فوری
        else:
            use_close = cur_close + tf_sec          # کندل بسته → بعدی
            wait = (use_close - offset) - now_utc
    else:
        use_close = cur_close
    from datetime import datetime as _dt
    close_teh = _dt.fromtimestamp(use_close, tz=TEHRAN).strftime("%H:%M")
    elapsed   = now_utc - (use_close - tf_sec)
    progress  = min(99, int(elapsed / tf_sec * 100))
    return max(int(wait), 5), close_teh, progress

# ── Supabase reminder helpers ────────────────────────────────
def _sb_save_reminder(cid, sym, interval_sec, tf_sec=0):
    """ذخیره یه reminder در Supabase"""
    if not SUPABASE_KEY: return
    rid = f"{cid}_{sym}"
    try:
        payload = {
            "id": rid, "chat_id": str(cid), "symbol": sym,
            "interval_sec": interval_sec, "tf_sec": tf_sec,
            "created_at": now_teh(), "active": True
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/reminders",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=8)
        if r.status_code not in (200,201,204):
            print(f"[reminder] save error: {r.status_code} {r.text[:60]}")
    except Exception as e:
        print(f"[reminder] save exc: {e}")

def _sb_delete_reminder(cid, sym):
    """حذف یه reminder از Supabase"""
    if not SUPABASE_KEY: return
    rid = f"{cid}_{sym}"
    try:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/reminders?id=eq.{rid}",
            headers=_sb_h(), timeout=8)
    except Exception as e:
        print(f"[reminder] delete exc: {e}")

def _sb_delete_all_reminders(cid):
    """حذف همه reminder‌های یه کاربر از Supabase"""
    if not SUPABASE_KEY: return
    try:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/reminders?chat_id=eq.{cid}",
            headers=_sb_h(), timeout=8)
    except Exception as e:
        print(f"[reminder] delete_all exc: {e}")

def _sb_load_reminders():
    """لود همه reminder‌های فعال از Supabase — برای startup"""
    if not SUPABASE_KEY: return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/reminders?active=eq.true&select=*",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200:
            return r.json()
        print(f"[reminder] load error: {r.status_code}")
    except Exception as e:
        print(f"[reminder] load exc: {e}")
    return []

def _delete_msg_after(token, cid, msg_id, delay=120):
    """پیام رو بعد از delay ثانیه پاک کن"""
    def _do():
        time.sleep(delay)
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/deleteMessage",
                json={"chat_id": cid, "message_id": msg_id},
                timeout=10, headers=H)
        except: pass
    threading.Thread(target=_do, daemon=True).start()

# =====================================================================
# 📡 سیستم سیگنال
# =====================================================================

SIGNAL_CHANNEL = os.environ.get("SIGNAL_CHANNEL", "")  # مثلاً @mychannel یا chat_id

def _sb_next_signal_seq():
    """شماره سیگنال بعدی — از آخرین seq در جدول +1"""
    if not SUPABASE_KEY: return int(time.time()) % 100000
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/signals?select=seq&order=seq.desc&limit=1",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200:
            data = r.json()
            if data and data[0].get("seq") is not None:
                return int(data[0]["seq"]) + 1
        return 10001  # جدول خالیه
    except:
        return 10001

def _sb_save_signal(sig: dict):
    """ذخیره سیگنال در Supabase — فقط فیلدهای جدول"""
    if not SUPABASE_KEY: return
    # فقط فیلدهایی که در جدول signals وجود دارن
    allowed = {"id","seq","symbol","direction","entry","sl","tp1","tp2","tp3",
               "tf","risk_pips","rr","sent_by","sent_at","channel_msg_id","status","note"}
    clean = {k: v for k, v in sig.items() if k in allowed}
    # مقادیر None رو برای tp2/tp3 به null تبدیل کن
    for f in ("tp2","tp3","channel_msg_id","note"):
        if f not in clean:
            clean[f] = None
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/signals",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=clean, timeout=10)
        if r.status_code not in (200, 201, 204):
            print(f"[signal] save error: {r.status_code} {r.text[:200]}")
        else:
            print(f"[signal] saved OK: {clean.get('id')}")
    except Exception as e:
        print(f"[signal] save exc: {e}")

def _sb_update_signal(sig_id, patch: dict):
    """آپدیت یه فیلد سیگنال"""
    if not SUPABASE_KEY: return
    try:
        requests.patch(
            f"{SUPABASE_URL}/rest/v1/signals?id=eq.{sig_id}",
            headers={**_sb_h(), "Prefer": "return=minimal"},
            json=patch, timeout=8)
    except: pass

def _sb_load_signals(limit=10):
    """آخرین سیگنال‌ها رو بخون"""
    if not SUPABASE_KEY: return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/signals?select=*&order=sent_at.desc&limit={limit}",
            headers=_sb_h(), timeout=8)
        if r.status_code == 200: return r.json()
    except: pass
    return []

def _calc_signal(symbol: str, direction: str, entry: float, sl: float, rr: float = 1.5):
    """
    محاسبه TP بر اساس Entry، SL و ریوارد.
    direction: buy_limit / buy_stop / sell_limit / sell_stop
    برمیگردونه: (sl_calc, tp1, risk_pips)
    """
    is_buy = direction.startswith("buy")
    risk = abs(entry - sl)
    mul = get_pip_multiplier(symbol)
    risk_pips = round(risk * mul, 1)
    if is_buy:
        tp1 = round(entry + risk * rr, 5)
    else:
        tp1 = round(entry - risk * rr, 5)
    return sl, tp1, risk_pips

def _sl_from_pips(symbol: str, direction: str, entry: float, pips: float):
    """محاسبه SL از روی پیپ"""
    mul = get_pip_multiplier(symbol)
    dist = pips / mul
    is_buy = direction.startswith("buy")
    sl = round(entry - dist if is_buy else entry + dist, 5)
    return sl

def _fmt_signal_price(p, symbol=""):
    """فرمت عدد برای نمایش در سیگنال"""
    if p is None: return "—"
    v = float(p)
    su = symbol.upper()
    if any(x in su for x in ['BTC','ETH','SOL','BNB']):
        return f"{v:.1f}" if v > 1000 else f"{v:.4f}"
    if "XAU" in su or "XAG" in su: return f"{v:.2f}"
    if "JPY" in su: return f"{v:.3f}"
    return f"{v:.5f}"

def _build_signal_text(sig: dict) -> str:
    """ساخت متن سیگنال — عین فرمت درخواستی، اعداد قابل کپی"""
    sym     = sig.get("symbol","")
    d       = sig.get("direction","")
    entry   = sig.get("entry")
    sl      = sig.get("sl")
    tp1     = sig.get("tp1")
    tp2     = sig.get("tp2")
    tp3     = sig.get("tp3")
    tf      = sig.get("tf","H1")
    sig_id  = sig.get("id","")

    dir_map = {
        "buy_limit":  "✅ Buy limit",
        "buy_stop":   "✅ Buy stop",
        "sell_limit": "🔴 Sell limit",
        "sell_stop":  "🔴 Sell stop",
    }
    dir_txt = dir_map.get(d, d)

    def c(val):
        if val is None: return "<code>-</code>"
        return f"<code>{_fmt_signal_price(val, sym)}</code>"

    tp2_txt = c(tp2) if tp2 else "<code>-</code>"
    tp3_txt = c(tp3) if tp3 else "<code>-</code>"

    return (
        f"#{sig_id}\n"
        f"#{sym}\n"
        f"{dir_txt}\n"
        f"➡️ Entry: {c(entry)}\n"
        f"🛑 SL: {c(sl)}\n"
        f"🎯 TP:\n"
        f"TP1: {c(tp1)}\n"
        f"TP2: {tp2_txt}\n"
        f"TP3: {tp3_txt}\n"
        f"⏱ Timeframe: {tf}"
    )

def _build_signal_preview(sig: dict) -> str:
    """پیش‌نمایش سیگنال — عین چیزی که به کانال میره"""
    return _build_signal_text(sig)

# state ثبت سیگنال در حال ساخت
_pending_signal = {}  # cid → {"step": str, "data": dict, "bot_msg_id": int}

SIGNAL_QUICK_SYMBOLS = ["BTCUSDT", "XAUUSD", "EURUSD", "GBPUSD", "ETHUSDT"]
SIGNAL_DIRECTIONS = [
    ("✅ Buy Limit",  "buy_limit"),
    ("✅ Buy Stop",   "buy_stop"),
    ("🔴 Sell Limit", "sell_limit"),
    ("🔴 Sell Stop",  "sell_stop"),
]
SIGNAL_TF_OPTIONS = ["M5", "M15", "M30", "H1", "H4", "D1"]
SIGNAL_DEFAULT_TF = "H1"
SIGNAL_DEFAULT_RR = 1.5

def _show_signal_preview(token, cid, mid, data):
    """نمایش پیش‌نمایش سیگنال با دکمه‌های ویرایش"""
    tf  = data.get("tf", SIGNAL_DEFAULT_TF)
    note = data.get("note","")
    note_line = f"\n\n📝 <i>{note}</i>" if note else ""
    preview = f"<b>── پیش‌نمایش ──</b>\n\n{_build_signal_text(data)}{note_line}"
    kb = [
        [{"text": f"⏱ TF: {tf}", "callback_data": f"sig_tf:{cid}"},
         {"text": "🎯 TP2/TP3", "callback_data": f"sig_tp:{cid}"}],
        [{"text": "📝 یادداشت", "callback_data": f"sig_note:{cid}"},
         {"text": "🔄 ریوارد", "callback_data": f"sig_recalc:{cid}"}],
        [{"text": "📤 ارسال به گروه", "callback_data": f"sig_send:{cid}:channel"},
         {"text": "💾 ثبت در دیتا", "callback_data": f"sig_send:{cid}:dbonly"}],
        [{"text": "❌ لغو", "callback_data": f"sig_cancel:{cid}"}],
    ]
    edit_tg_keyboard(token, cid, mid, preview, kb)

def _send_reminder(token, cid, sym, tf_sec=0):
    """پیام هشدار کلوز کندل — حذف ۵ دقیقه بعد از ارسال"""
    tf_label = _TF_LABEL.get(tf_sec, "")
    if tf_sec and tf_label:
        _, close_teh, progress = _candle_info(tf_sec)
        offset_min = _TF_OFFSET.get(tf_sec, 300) // 60
        msg = (f"🕯 <b>کلوز کندل {tf_label} — {sym}</b>\n"
               f"━━━━━━━━━━━━━━━━━━\n"
               f"⏰ کلوز تهران: <b>{close_teh}</b>\n"
               f"⏳ مانده تا کلوز: <b>~{offset_min} دقیقه</b>\n"
               f"📊 پیشرفت کندل: <b>{progress}%</b>\n"
               f"━━━━━━━━━━━━━━━━━━\n"
               f"🗑 این پیام ۵ دقیقه دیگه حذف میشه.")
    else:
        msg = f"⚠️ <b>یادآوری:</b> <code>{sym}</code> بررسی بشه!\n\n🗑 این پیام ۵ دقیقه دیگه حذف میشه."
    kb = [[{"text": f"✕ کنسل {sym}", "callback_data": f"cancel_reminder_one:{cid}:{sym}"}]]
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": cid, "text": msg, "parse_mode": "HTML",
                  "reply_markup": {"inline_keyboard": kb}},
            timeout=10, headers=H)
        mid = r.json().get("result", {}).get("message_id")
        if mid:
            _delete_msg_after(token, cid, mid, delay=300)  # ۵ دقیقه
    except: pass

def _schedule_reminder(token, cid, sym, interval_sec, persist=True, tf_sec=0):
    """
    هشدار کلوز کندل — هر چرخه:
      1. محاسبه دقیق چند ثانیه تا هشدار کلوز بعدی
      2. sleep
      3. یه پیام بفرست
      4. برگرد به ۱ (برای کندل بعدی)
    tf_sec: طول تایم‌فریم (M5=300, M15=900, H1=3600, H4=14400)
    interval_sec: همون tf_sec هست (برای سازگاری Supabase)
    """
    tf = tf_sec or interval_sec
    if cid not in _reminders:
        _reminders[cid] = {}
    _reminders[cid][sym] = {"interval": tf, "active": True, "tf_sec": tf}
    entry = _reminders[cid][sym]
    if persist:
        threading.Thread(target=_sb_save_reminder, args=(cid, sym, tf, tf), daemon=True).start()
    def _loop():
        while entry.get("active") and _reminders.get(cid, {}).get(sym, {}).get("active"):
            wait, _, _ = _candle_info(entry["tf_sec"])
            time.sleep(wait)
            if not _reminders.get(cid, {}).get(sym, {}).get("active"):
                break
            _send_reminder(token, cid, sym, tf_sec=entry["tf_sec"])
            # بعد از ارسال پیام، صبر کن تا کلوز کندل رد بشه — جلوی تکرار رو میگیره
            tf = entry["tf_sec"]
            now_utc = time.time()
            cur_close = (int(now_utc) // tf + 1) * tf
            sleep_after = cur_close - now_utc + 5  # ۵ ثانیه بعد از کلوز
            if sleep_after > 0:
                time.sleep(sleep_after)
    threading.Thread(target=_loop, daemon=True).start()

def build_cancel_reminder_msg(cid):
    """لیست هشدارهای فعال با دکمه حذف جداگانه"""
    active = _reminders.get(cid, {})
    if not active:
        return "هیچ هشدار دوره‌ای فعالی نداری.", []
    labels = {300:"۵ دقیقه", 900:"۱۵ دقیقه", 3600:"۱ ساعت", 14400:"۴ ساعت"}
    lines = ["⏰ <b>هشدارهای دوره‌ای فعال:</b>\n"]
    keyboard = []
    for sym, info in active.items():
        tf = info.get("tf_sec", info.get("interval", 0))
        lbl = _TF_LABEL.get(tf, f"{tf//60}m") if tf else "؟"
        lines.append(f"• <b>{sym}</b> — کلوز {lbl}")
        keyboard.append([{"text": f"🗑 حذف {sym}", "callback_data": f"cancel_reminder_one:{cid}:{sym}"}])
    keyboard.append([{"text": "✕ کنسل همه", "callback_data": f"cancel_reminder_all:{cid}"}])
    keyboard.append([{"text": "✓ بستن", "callback_data": "close_myalerts"}])
    return "\n".join(lines), keyboard

def build_myalerts_msg(cid):
    """متن و keyboard لیست آلارم‌های شخصی"""
    alerts = load_alerts().get("alerts", [])
    my = [a for a in alerts if a.get("is_private") and str(a.get("private_cid","")) == cid and a.get("active")]
    if not my:
        return "📭 هیچ آلارم شخصی فعالی نداری.", []
    lines = ["🔒 <b>آلارم‌های شخصی تو:</b>"]
    keyboard = []
    for i, a in enumerate(my, 1):
        sym = a.get("symbol","")
        tgt = a.get("target_price",0)
        cond = "📈 BUY" if a.get("condition") == "below" else "📉 SELL"
        cur2 = a.get("last_price")
        cur_txt = f"<code>{fmt_price(cur2, sym)}</code>" if cur2 else "—"
        cmt = f"\n│  💬 {a['comment']}" if a.get("comment") else ""
        lines.append(
            f"┌─ {i}. <b>{sym}</b>  {cond}\n"
            f"│  🎯 هدف: <code>{tgt}</code>\n"
            f"│  💹 فعلی: {cur_txt}"
            f"{cmt}\n"
            f"└──────────────"
        )
        keyboard.append([{"text": f"🗑 حذف  {sym} @ {tgt}", "callback_data": f"del_confirm:{a['id']}"}])
    keyboard.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
    return "\n".join(lines), keyboard

def _get_token_and_cids():
    data = load_alerts()
    tg = data.get("telegram", {})
    token = BOT_TOKEN_ENV or tg.get("bot_token", "")
    cids = list(tg.get("chat_ids", []))
    leg = tg.get("chat_id", "")
    if leg and leg not in [str(x) for x in cids]:
        cids.append(leg)
    return token, cids, data


FF_NEWS_HOUR = int(os.environ.get("NEWS_HOUR", "7"))   # ساعت ارسال روزانه (تهران)
FF_NEWS_MINUTE = int(os.environ.get("NEWS_MINUTE", "0"))

def fetch_ff_news():
    """
    تقویم اقتصادی ForexFactory رو از RSS می‌گیره.
    فقط رویدادهای USD با impact بالا (⭐⭐⭐) برمی‌گردونه.
    """
    try:
        import xml.etree.ElementTree as ET
        from datetime import timezone
        r = requests.get(
            "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
            headers={**H, "User-Agent": "Mozilla/5.0"},
            timeout=10)
        if r.status_code == 200:
            events = r.json()
        else:
            # fallback: RSS
            r2 = requests.get("https://www.forexfactory.com/ffcal_week_this.xml",
                headers={**H, "User-Agent": "Mozilla/5.0"}, timeout=10)
            if r2.status_code != 200:
                return None, "❌ دریافت داده از ForexFactory ناموفق بود"
            root = ET.fromstring(r2.content)
            events = []
            for ev in root.findall("event"):
                events.append({
                    "title": ev.findtext("title",""),
                    "country": ev.findtext("country",""),
                    "date": ev.findtext("date",""),
                    "time": ev.findtext("time",""),
                    "impact": ev.findtext("impact",""),
                    "forecast": ev.findtext("forecast",""),
                    "previous": ev.findtext("previous",""),
                })

        # فیلتر: فقط رویدادهای high/medium impact — همه ارزها — امروز
        today_teh = datetime.now(TEHRAN).strftime("%Y-%m-%d")
        high_events = []
        for ev in events:
            impact = ev.get("impact","").lower()
            if impact not in ("high","medium","3","2"):
                continue
            ev_date = ev.get("date","")
            try:
                from datetime import datetime as dt
                parsed = dt.strptime(ev_date, "%Y-%m-%dT%H:%M:%S%z")
                ev_date_teh = parsed.astimezone(TEHRAN).strftime("%Y-%m-%d")
                if ev_date_teh != today_teh:
                    continue
                ev["time_teh"] = parsed.astimezone(TEHRAN).strftime("%H:%M")
            except:
                if today_teh not in ev_date:
                    continue
                ev["time_teh"] = ev.get("time","—")
            high_events.append(ev)

        # مرتب‌سازی بر اساس ساعت
        high_events.sort(key=lambda x: x.get("time_teh","99:99"))

        if not high_events:
            return [], "📭 امروز رویداد مهم فارکس نداریم."

        return high_events, None

    except Exception as e:
        return None, f"❌ خطا: {e}"


def format_ff_message(events):
    """پیام تلگرام رو فرمت می‌کنه"""
    today_str = datetime.now(TEHRAN).strftime("%A %d %B %Y")
    lines = [f"📅 <b>تقویم اقتصادی فارکس — امروز</b>\n{today_str}\n"]
    for ev in events:
        impact = ev.get("impact","").lower()
        star = "🔴" if impact in ("high","3") else "🟡"
        time_str = ev.get("time_teh") or ev.get("time","—")
        title = ev.get("title","—")
        forecast = ev.get("forecast","—") or "—"
        previous = ev.get("previous","—") or "—"
        country = ev.get("country","").upper()
        flag_map = {"USD":"🇺🇸","EUR":"🇪🇺","GBP":"🇬🇧","JPY":"🇯🇵","CAD":"🇨🇦",
                    "AUD":"🇦🇺","NZD":"🇳🇿","CHF":"🇨🇭","CNY":"🇨🇳","GER":"🇩🇪"}
        flag = flag_map.get(country, "🌐")
        lines.append(
            f"{star} {flag} <b>{title}</b>\n"
            f"   🕐 {time_str} (تهران)\n"
            f"   پیش‌بینی: <b>{forecast}</b>  |  قبلی: {previous}"
        )
    return "\n\n".join(lines)


def daily_news_scheduler():
    """هر روز سر ساعت NEWS_HOUR تهران اخبار می‌فرسته"""
    sent_today = None
    while True:
        try:
            now = datetime.now(TEHRAN)
            today = now.date()
            if (now.hour == FF_NEWS_HOUR and now.minute == FF_NEWS_MINUTE
                    and sent_today != today):
                token, cids, _ = _get_token_and_cids()
                if token and cids:
                    events, err = fetch_ff_news()
                    if err and not events:
                        msg = err
                    else:
                        msg = format_ff_message(events) if events else "📭 امروز رویداد مهم فارکس نداریم."
                    broadcast(token, cids, msg)
                    sent_today = today
                    print(f"[news] ارسال شد — {len(events or [])} رویداد")
        except Exception as e:
            print(f"[news_scheduler] {e}")
        time.sleep(50)

_pending_name  = {}  # cid → True
_pending_alarm = {}  # cid → {"step": str, "data": dict}
_pending_reminder = {}  # cid → {step, bot_msg_id}
_pending_weekly_search = {}  # cid → {"which": str, "msg_id": int}
# آلارم_آیدی → {chat_id: message_id آخرین پیام False/آپدیت}
_false_broadcast_ids: dict = {}

# ── Reply Keyboard rows ──────────────────────────────────────
MAIN_MENU = [
    ["📈 آلارم جدید"],
    ["⭐ آلارم‌های من", "📊 وضعیت"],
    ["⚡ آلارم فوری",   "⏰ هشدار دوره‌ای من"],
    ["📡 سیگنال جدید"],
]
MAIN_MENU_PRIVATE = [
    ["📈 آلارم جدید",  "🔒 آلارم شخصی"],
    ["⭐ آلارم‌های من", "📊 وضعیت"],
    ["⚡ آلارم فوری",   "⏰ هشدار دوره‌ای من"],
    ["📡 سیگنال جدید"],
]
MAIN_MENU_ADMIN = [
    ["📈 آلارم جدید",  "🔒 آلارم شخصی"],
    ["⭐ آلارم‌های من", "📊 وضعیت"],
    ["⚡ آلارم فوری",   "⏰ هشدار دوره‌ای من"],
    ["📡 سیگنال جدید", "⚙️ پنل ادمین"],
]
DIR_MENU = [["📈 BUY", "📉 SELL"], ["❌ انصراف"]]

def show_main_menu(token, cid, text, is_admin=False):
    if is_admin:
        rows = MAIN_MENU_ADMIN
    elif _has_private_access(cid):
        rows = MAIN_MENU_PRIVATE
    else:
        rows = MAIN_MENU
    send_reply_keyboard(token, cid, text, rows)

def _get_user_custom_name(cid):
    data = load_alerts()
    for u in data.get("users", []):
        if str(u.get("chat_id","")) == str(cid):
            return u.get("custom_name","") or u.get("username","")
    return ""

def _has_private_access(cid):
    """آیا این کاربر دسترسی آلارم شخصی داره؟ فقط ادمین و کسایی که تایید شدن"""
    if str(cid) == str(YOUR_CHAT_ID):
        return True
    data = load_alerts()
    for u in data.get("users", []):
        if str(u.get("chat_id","")) == str(cid):
            return bool(u.get("private_access", False))
    return False

def _get_sender_name(msg):
    """اسم فرستنده — اول custom_name، بعد اسم تلگرام"""
    u = msg.get("from", {})
    cid = str(msg.get("chat", {}).get("id", "") or u.get("id", ""))
    if cid:
        users = load_alerts().get("users", [])
        for usr in users:
            if str(usr.get("chat_id", "")) == cid and usr.get("custom_name"):
                return usr["custom_name"]
    fn = u.get("first_name", "")
    ln = u.get("last_name", "")
    un = u.get("username", "")
    return (fn + " " + ln).strip() or ("@" + un if un else "ناشناس")


def _build_myalerts_section(alerts_list, title, start_idx=1):
    """ساخت متن و keyboard برای نمایش لیست آلارم"""
    if not alerts_list:
        return None, []
    lines = [f"{title}"]
    kb = []
    for i, a in enumerate(alerts_list, start_idx):
        sym2  = a.get("symbol","")
        tgt2  = a.get("target_price", 0)
        cond2 = "📈 BUY" if a.get("condition") == "below" else "📉 SELL"
        cur2  = a.get("last_price")
        cur_txt = f"<code>{fmt_price(cur2, sym2)}</code>" if cur2 else "—"
        cmt2  = f"\n💬 {a['comment']}" if a.get("comment") else ""
        block = (
            f"┌─ {i}. <b>{sym2}</b>  {cond2}\n"
            f"│  🎯 هدف: <code>{fmt_price(tgt2, sym2)}</code>\n"
            f"│  💹 فعلی: {cur_txt}"
            f"{cmt2}\n"
            f"└──────────────"
        )
        lines.append(block)
        kb.append([{"text": f"🗑 {i}. {sym2} {cond2} @ {fmt_price(tgt2, sym2)}", "callback_data": f"del_confirm:{a['id']}"}])
    kb.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
    return "\n".join(lines), kb

def poll_telegram():
    last_id = 0
    while True:
        try:
            token, _, _ = _get_token_and_cids()
            if not token:
                time.sleep(30)
                continue
            r = requests.get(
                f"https://api.telegram.org/bot{token}/getUpdates",
                params={"offset": last_id+1, "timeout": 20, "limit": 100},
                timeout=30, headers=H)
            if r.status_code != 200:
                time.sleep(10)
                continue
            _all_updates = r.json().get("result", [])
            for upd in _all_updates:
                last_id = upd["update_id"]
            # هر update در thread جدا — poll بلاک نمیشه
            for upd in _all_updates:
                threading.Thread(target=_do_update, args=(upd, token), daemon=True).start()
            continue

        except Exception as e:
            print(f"[poll] {e}")
        time.sleep(5)

def _do_update(upd, token):
  try:
                # ── callback query (دکمه‌های inline) ─────────────────
                cbq = upd.get("callback_query", {})
                if cbq:
                    cbq_id = cbq.get("id","")
                    cbq_data = cbq.get("data","")
                    cbq_cid = str(cbq.get("from",{}).get("id","") or cbq.get("message",{}).get("chat",{}).get("id",""))
                    cbq_msg_id = cbq.get("message",{}).get("message_id")
                    token_cbq, _, _ = _get_token_and_cids()

                    if cbq_data.startswith("del_confirm:"):
                        # مرحله اول: نمایش تأیید حذف
                        aid = cbq_data.split(":",1)[1]
                        d_conf = load_alerts()
                        a_conf = next((a for a in d_conf["alerts"] if a["id"] == aid), None)
                        answer_callback(token_cbq, cbq_id)
                        if a_conf:
                            sym_c = a_conf.get("symbol","")
                            tgt_c = a_conf.get("target_price", 0)
                            cond_c = "📈 BUY" if a_conf.get("condition") == "below" else "📉 SELL"
                            confirm_text = (
                                f"⚠️ <b>آیا مطمئنی؟</b>\n\n"
                                f"میخوای این آلارم رو حذف کنی:\n"
                                f"<b>{sym_c}</b>  {cond_c}  @ <code>{fmt_price(tgt_c, sym_c)}</code>"
                            )
                            confirm_kb = [
                                [{"text": "✅ بله، حذف کن", "callback_data": f"del_alert:{aid}"}],
                                [{"text": "❌ نه، برگرد", "callback_data": f"del_cancel:{aid}"}],
                            ]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, confirm_text, confirm_kb)
                        else:
                            answer_callback(token_cbq, cbq_id, "⚠️ آلارم پیدا نشد")

                    elif cbq_data.startswith("del_cancel:"):
                        # برگشت به لیست آلارم‌ها بعد از انصراف از حذف
                        answer_callback(token_cbq, cbq_id, "❌ حذف لغو شد")
                        d_can = load_alerts()
                        all_a_can = d_can.get("alerts", [])
                        custom_name_can = _get_user_custom_name(cbq_cid)
                        pub_can  = [a for a in all_a_can if a.get("active") and not a.get("is_private") and a.get("created_by","") == custom_name_can]
                        priv_can = [a for a in all_a_can if a.get("active") and a.get("is_private") and (
                            str(a.get("private_cid","")) == cbq_cid or str(a.get("notify_only","")) == cbq_cid
                        )]
                        combined_can = pub_can + priv_can
                        if pub_can and not priv_can:
                            txt_can, kb_can = _build_myalerts_section(pub_can, f"🌐 <b>آلارم‌های تیمی</b>  ({len(pub_can)} مورد)")
                        elif priv_can and not pub_can:
                            txt_can, kb_can = _build_myalerts_section(priv_can, f"🔒 <b>آلارم‌های شخصی</b>  ({len(priv_can)} مورد)")
                        elif combined_can:
                            txt_can, kb_can = _build_myalerts_section(pub_can, f"📋 <b>همه آلارم‌های من</b>  ({len(combined_can)} مورد)", start_idx=1)
                            priv_txt_can, priv_kb_can = _build_myalerts_section(priv_can, "\n🔒 <b>شخصی</b>", start_idx=len(pub_can)+1)
                            if priv_txt_can:
                                txt_can = txt_can + "\n" + priv_txt_can
                                kb_can = kb_can[:-1] + priv_kb_can
                        else:
                            txt_can, kb_can = "📭 هیچ آلارم فعالی نداری.", [[{"text":"✕ بستن","callback_data":"close_myalerts"}]]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_can, kb_can)

                    elif cbq_data.startswith("del_alert:"):
                        # مرحله دوم: حذف واقعی بعد از تأیید
                        aid = cbq_data.split(":",1)[1]
                        d = load_alerts()
                        a_del = next((a for a in d["alerts"] if a["id"] == aid), None)
                        before = len(d["alerts"])
                        d["alerts"] = [a for a in d["alerts"] if a["id"] != aid]
                        if len(d["alerts"]) < before:
                            _cache_alerts = d
                            answer_callback(token_cbq, cbq_id, "✅ آلارم حذف شد")
                            threading.Thread(target=_sb_delete_alert, args=(aid,), daemon=True).start()
                        else:
                            answer_callback(token_cbq, cbq_id, "⚠️ آلارم پیدا نشد")
                        # بازسازی لیست در همان پیام — بدون sleep، بدون پیام جدید
                        d2 = load_alerts()
                        all_a2 = d2.get("alerts", [])
                        custom_name2 = _get_user_custom_name(cbq_cid)
                        pub2  = [a for a in all_a2 if a.get("active") and not a.get("is_private") and a.get("created_by","") == custom_name2]
                        priv2 = [a for a in all_a2 if a.get("active") and a.get("is_private") and (
                            str(a.get("private_cid","")) == cbq_cid or str(a.get("notify_only","")) == cbq_cid
                        )]
                        combined2 = pub2 + priv2
                        if not combined2:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ آلارم فعالی نداری.", [])
                        elif pub2 and not priv2:
                            txt2, kb2 = _build_myalerts_section(pub2, f"🌐 <b>آلارم‌های تیمی</b>  ({len(pub2)} مورد)")
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt2, kb2)
                        elif priv2 and not pub2:
                            txt2, kb2 = _build_myalerts_section(priv2, f"🔒 <b>آلارم‌های شخصی</b>  ({len(priv2)} مورد)")
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt2, kb2)
                        else:
                            txt2, kb2 = _build_myalerts_section(pub2, f"📋 <b>همه آلارم‌های من</b>  ({len(combined2)} مورد)", start_idx=1)
                            # اضافه کردن آلارم‌های شخصی به لیست ترکیبی
                            txt2_lines = txt2.split("\n")
                            priv_txt, priv_kb = _build_myalerts_section(priv2, f"\n🔒 <b>شخصی</b>", start_idx=len(pub2)+1)
                            if priv_txt:
                                txt2 = txt2 + "\n" + priv_txt
                                kb2 = kb2[:-1] + priv_kb  # بستن رو از pub حذف کن، priv_kb خودش داره
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt2, kb2)

                    elif cbq_data.startswith("reminder_new:"):
                        # reminder_new:CID — مستقیم از کاربر نماد بگیر
                        answer_callback(token_cbq, cbq_id)
                        _pending_reminder[cbq_cid] = {"step": "rem_symbol", "bot_msg_id": cbq_msg_id}
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            "➕ <b>هشدار جدید</b>\n\nنماد رو بنویس (مثلاً BTCUSDT یا XAUUSD):",
                            [[{"text": "❌ انصراف", "callback_data": "close_myalerts"}]])

                    elif cbq_data.startswith("reminder_sym:"):
                        # reminder_sym:CID:SYM — انتخاب تایم‌فریم
                        parts = cbq_data.split(":", 2)
                        r_cid = parts[1] if len(parts) > 1 else cbq_cid
                        r_sym = parts[2] if len(parts) > 2 else ""
                        answer_callback(token_cbq, cbq_id)
                        if r_sym == "__type__":
                            # کاربر باید خودش تایپ کنه
                            _pending_reminder[cbq_cid] = {"step": "rem_symbol", "bot_msg_id": cbq_msg_id}
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                "✏️ نماد رو بنویس (مثلاً EURUSD):",
                                [[{"text": "❌ انصراف", "callback_data": "close_myalerts"}]])
                        else:
                            kb_tf = [
                                [{"text": "🕯 M5  (۱ دق قبل کلوز)",  "callback_data": f"reminder_go:{r_cid}:{r_sym}:300"}],
                                [{"text": "🕯 M15 (۵ دق قبل کلوز)",  "callback_data": f"reminder_go:{r_cid}:{r_sym}:900"}],
                                [{"text": "🕯 H1  (۱۵ دق قبل کلوز)", "callback_data": f"reminder_go:{r_cid}:{r_sym}:3600"}],
                                [{"text": "🕯 H4  (۱۵ دق قبل کلوز)", "callback_data": f"reminder_go:{r_cid}:{r_sym}:14400"}],
                                [{"text": "✕ برگشت", "callback_data": f"reminder_new:{r_cid}"}],
                            ]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"🕯 تایم‌فریم هشدار برای <b>{r_sym}</b>:", kb_tf)

                    elif cbq_data.startswith("set_reminder:"):
                        # set_reminder:cid:SYM — از دکمه کنار الارم
                        parts = cbq_data.split(":", 2)
                        r_cid = parts[1] if len(parts) > 1 else cbq_cid
                        r_sym = parts[2] if len(parts) > 2 else "؟"
                        answer_callback(token_cbq, cbq_id)
                        kb_tf = [
                            [{"text": "🕯 M5  (۱ دق قبل کلوز)",  "callback_data": f"reminder_go:{r_cid}:{r_sym}:300"}],
                            [{"text": "🕯 M15 (۵ دق قبل کلوز)",  "callback_data": f"reminder_go:{r_cid}:{r_sym}:900"}],
                            [{"text": "🕯 H1  (۱۵ دق قبل کلوز)", "callback_data": f"reminder_go:{r_cid}:{r_sym}:3600"}],
                            [{"text": "🕯 H4  (۱۵ دق قبل کلوز)", "callback_data": f"reminder_go:{r_cid}:{r_sym}:14400"}],
                            [{"text": "✕ نه ممنون", "callback_data": "close_myalerts"}],
                        ]
                        send_tg_keyboard(token_cbq, cbq_cid,
                            f"🕯 تایم‌فریم هشدار کلوز برای <b>{r_sym}</b>:", kb_tf)

                    elif cbq_data.startswith("reminder_go:"):
                        parts = cbq_data.split(":")
                        r_cid = parts[1] if len(parts) > 1 else cbq_cid
                        r_sym = parts[2] if len(parts) > 2 else "؟"
                        r_tf  = int(parts[3]) if len(parts) > 3 else 3600
                        tf_label = _TF_LABEL.get(r_tf, f"{r_tf//60}m")
                        offset_min = _TF_OFFSET.get(r_tf, 300) // 60
                        wait_sec, close_teh, progress = _candle_info(r_tf)
                        wait_min = max(1, wait_sec // 60)
                        # کنسل reminder قبلی همین نماد
                        if _reminders.get(r_cid, {}).get(r_sym):
                            _reminders[r_cid][r_sym]["active"] = False
                            del _reminders[r_cid][r_sym]
                            threading.Thread(target=_sb_delete_reminder, args=(r_cid, r_sym), daemon=True).start()
                        _schedule_reminder(token_cbq, r_cid, r_sym, r_tf, tf_sec=r_tf)
                        def _bg_confirm(tok=token_cbq, cid_=cbq_cid, cbid=cbq_id,
                                        sym_=r_sym, tfl=tf_label, wm=wait_min, ct=close_teh,
                                        pr=progress, om=offset_min):
                            answer_callback(tok, cbid, f"✅ هشدار کلوز {tfl} فعال شد")
                            confirm_txt = (
                                f"✅ هشدار کلوز <b>{tfl}</b> برای <code>{sym_}</code> فعال شد.\n"
                                f"━━━━━━━━━━━━━━━━━━\n"
                                f"⏰ کلوز بعدی تهران: <b>{ct}</b>\n"
                                f"📊 پیشرفت کندل: <b>{pr}%</b>\n"
                                f"🔔 هشدار اول: <b>{wm} دقیقه</b> دیگه ({om} دق قبل کلوز)\n"
                                f"━━━━━━━━━━━━━━━━━━\n"
                                f"برای کنسل: /cancel_reminder"
                            )
                            send_tg(tok, cid_, confirm_txt)
                        threading.Thread(target=_bg_confirm, daemon=True).start()

                    elif cbq_data.startswith("cancel_reminder_one:"):
                        parts = cbq_data.split(":", 2)
                        r_cid = parts[1] if len(parts) > 1 else cbq_cid
                        r_sym = parts[2] if len(parts) > 2 else ""
                        if r_sym and _reminders.get(r_cid, {}).get(r_sym):
                            _reminders[r_cid][r_sym]["active"] = False
                            del _reminders[r_cid][r_sym]
                            if not _reminders.get(r_cid):
                                _reminders.pop(r_cid, None)
                            threading.Thread(target=_sb_delete_reminder, args=(r_cid, r_sym), daemon=True).start()
                            answer_callback(token_cbq, cbq_id, f"✅ هشدار {r_sym} کنسل شد")
                        else:
                            answer_callback(token_cbq, cbq_id, "هشداری پیدا نشد")
                        # آپدیت لیست
                        new_text, new_kb = build_cancel_reminder_msg(r_cid)
                        if new_kb:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, new_text, new_kb)
                        else:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "✅ همه هشدارها کنسل شدن.", [])

                    elif cbq_data.startswith("cancel_reminder_all:"):
                        r_cid = cbq_data.split(":", 1)[1] if ":" in cbq_data else cbq_cid
                        if r_cid in _reminders:
                            for info in _reminders[r_cid].values():
                                info["active"] = False
                            del _reminders[r_cid]
                            threading.Thread(target=_sb_delete_all_reminders, args=(r_cid,), daemon=True).start()
                            answer_callback(token_cbq, cbq_id, "✅ همه هشدارها کنسل شد")
                        else:
                            answer_callback(token_cbq, cbq_id, "هشداری فعال نبود")
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "✅ همه هشدارهای دوره‌ای کنسل شدن.", [])

                    elif cbq_data.startswith("myalerts:"):
                        # myalerts:pub:CID / myalerts:priv:CID / myalerts:all:CID
                        parts_mya = cbq_data.split(":", 2)
                        mya_type = parts_mya[1] if len(parts_mya) > 1 else "all"
                        mya_cid  = parts_mya[2] if len(parts_mya) > 2 else cbq_cid
                        answer_callback(token_cbq, cbq_id)
                        d_mya = load_alerts()
                        all_a_mya = d_mya.get("alerts", [])
                        custom_name_mya = _get_user_custom_name(mya_cid)
                        pub_mya  = [a for a in all_a_mya if a.get("active") and not a.get("is_private") and a.get("created_by","") == custom_name_mya]
                        # برای آلارم شخصی: هم private_cid هم notify_only چک کن
                        priv_mya = [a for a in all_a_mya if a.get("active") and a.get("is_private") and (
                            str(a.get("private_cid","")) == mya_cid or str(a.get("notify_only","")) == mya_cid
                        )]
                        if mya_type == "pub":
                            txt_mya, kb_mya = _build_myalerts_section(pub_mya, f"🌐 <b>آلارم‌های تیمی</b>  ({len(pub_mya)} مورد)")
                            if not txt_mya:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ آلارم تیمی فعالی نداری.", [[{"text":"✕ بستن","callback_data":"close_myalerts"}]])
                            else:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya, kb_mya)
                        elif mya_type == "priv":
                            txt_mya, kb_mya = _build_myalerts_section(priv_mya, f"🔒 <b>آلارم‌های شخصی</b>  ({len(priv_mya)} مورد)")
                            if not txt_mya:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ آلارم شخصی فعالی نداری.", [[{"text":"✕ بستن","callback_data":"close_myalerts"}]])
                            else:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya, kb_mya)
                        else:  # all
                            combined_mya = pub_mya + priv_mya
                            if not combined_mya:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ آلارم فعالی نداری.", [[{"text":"✕ بستن","callback_data":"close_myalerts"}]])
                            elif pub_mya and not priv_mya:
                                txt_mya_all, kb_mya_all = _build_myalerts_section(pub_mya, f"🌐 <b>آلارم‌های تیمی</b>  ({len(pub_mya)} مورد)")
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya_all, kb_mya_all)
                            elif priv_mya and not pub_mya:
                                txt_mya_all, kb_mya_all = _build_myalerts_section(priv_mya, f"🔒 <b>آلارم‌های شخصی</b>  ({len(priv_mya)} مورد)")
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya_all, kb_mya_all)
                            else:
                                txt_mya_all, kb_mya_all = _build_myalerts_section(pub_mya, f"📋 <b>همه آلارم‌های من</b>  ({len(combined_mya)} مورد)\n\n🌐 <b>تیمی</b>", start_idx=1)
                                priv_txt_all, priv_kb_all = _build_myalerts_section(priv_mya, "\n🔒 <b>شخصی</b>", start_idx=len(pub_mya)+1)
                                if priv_txt_all:
                                    txt_mya_all = txt_mya_all + "\n" + priv_txt_all
                                    kb_mya_all = kb_mya_all[:-1] + priv_kb_all
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, txt_mya_all, kb_mya_all)

                    elif cbq_data == "admin:news" and cbq_cid == YOUR_CHAT_ID:
                        answer_callback(token_cbq, cbq_id, "در حال دریافت اخبار...")
                        def _bg_news(tok=token_cbq, c=cbq_cid):
                            result = fetch_ff_news()
                            msg_text = result[1] if isinstance(result, tuple) else str(result)
                            send_tg(tok, c, msg_text)
                        threading.Thread(target=_bg_news, daemon=True).start()

                    elif cbq_data == "admin:broadcast" and cbq_cid == YOUR_CHAT_ID:
                        answer_callback(token_cbq, cbq_id)
                        _pending_alarm[cbq_cid] = {"step": "broadcast_text", "data": {}}
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=10, headers=H)
                        except: pass
                        send_tg(token_cbq, cbq_cid, "✉️ متن پیام رو بنویس:")

                    elif cbq_data.startswith("req_private:"):
                        # کاربر درخواست فعال‌سازی آلارم شخصی داده
                        req_cid = cbq_data.split(":")[1]
                        answer_callback(token_cbq, cbq_id, "✅ درخواست ارسال شد")
                        # پیام تایید به کاربر
                        send_tg(token_cbq, req_cid,
                            "📩 <b>درخواست شما ثبت شد</b>\n\n"
                            "درخواست فعال‌سازی آلارم شخصی شما ارسال شد و در دست بررسی است.\n"
                            "پس از تایید، یک پیام دریافت خواهید کرد. 🙏")
                        # پیام به ادمین
                        d_req = load_alerts()
                        req_user = next((u for u in d_req.get("users",[]) if str(u.get("chat_id","")) == req_cid), {})
                        req_name = req_user.get("custom_name","") or req_user.get("username","") or req_cid
                        admin_notif = (
                            f"📩 <b>درخواست آلارم شخصی</b>\n\n"
                            f"👤 نام: <b>{req_name}</b>\n"
                            f"🆔 Chat ID: <code>{req_cid}</code>\n"
                            f"⏰ {now_pretty()} (تهران)"
                        )
                        approve_kb = [
                            [{"text": "✅ تایید و فعال‌سازی", "callback_data": f"approve_private:{req_cid}"}],
                            [{"text": "❌ رد درخواست",       "callback_data": f"reject_private:{req_cid}"}],
                        ]
                        send_tg_keyboard(token_cbq, YOUR_CHAT_ID, admin_notif, approve_kb)

                    elif cbq_data.startswith("approve_private:"):
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            target_cid = cbq_data.split(":")[1]
                            answer_callback(token_cbq, cbq_id, "✅ فعال شد")
                            # آپدیت private_access توی users
                            d_apr = load_alerts()
                            found = False
                            for u in d_apr.get("users", []):
                                if str(u.get("chat_id","")) == target_cid:
                                    u["private_access"] = True
                                    found = True
                                    break
                            if not found:
                                d_apr.setdefault("users",[]).append({"chat_id": target_cid, "username": "", "joined_at": now_pretty(), "custom_name": "", "private_access": True})
                            save_alerts(d_apr)
                            # ویرایش پیام ادمین
                            try:
                                requests.post(f"https://api.telegram.org/bot{token_cbq}/editMessageReplyMarkup",
                                    json={"chat_id": cbq_cid, "message_id": cbq_msg_id, "reply_markup": {"inline_keyboard": []}},
                                    timeout=10, headers=H)
                            except: pass
                            send_tg(token_cbq, cbq_cid, f"✅ آلارم شخصی برای <code>{target_cid}</code> فعال شد.")
                            # پیام به کاربر
                            send_tg(token_cbq, target_cid,
                                "🎉 <b>آلارم شخصی شما فعال شد!</b>\n\n"
                                "یکبار /start بزنید تا منو به‌روز شود. 🔒")

                    elif cbq_data.startswith("reject_private:"):
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            target_cid = cbq_data.split(":")[1]
                            answer_callback(token_cbq, cbq_id, "❌ رد شد")
                            try:
                                requests.post(f"https://api.telegram.org/bot{token_cbq}/editMessageReplyMarkup",
                                    json={"chat_id": cbq_cid, "message_id": cbq_msg_id, "reply_markup": {"inline_keyboard": []}},
                                    timeout=10, headers=H)
                            except: pass
                            send_tg(token_cbq, cbq_cid, f"❌ درخواست <code>{target_cid}</code> رد شد.")
                            send_tg(token_cbq, target_cid,
                                "❌ <b>درخواست شما رد شد.</b>\n\n"
                                "برای اطلاعات بیشتر با ادمین تماس بگیرید.")

                    elif cbq_data.startswith("admin:users"):
                        # لیست کاربران برای ادمین
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            answer_callback(token_cbq, cbq_id)
                            d_usr = load_alerts()
                            all_users = d_usr.get("users", [])
                            if not all_users:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ کاربری ثبت نشده.", [[{"text": "✕ بستن", "callback_data": "close_myalerts"}]])
                            else:
                                lines_u = ["👥 <b>لیست کاربران</b>\n"]
                                kb_u = []
                                for u in all_users:
                                    ucid = str(u.get("chat_id",""))
                                    uname_u = u.get("custom_name","") or u.get("username","") or ucid
                                    priv_icon = "🔒" if u.get("private_access") else "👤"
                                    lines_u.append(f"{priv_icon} <b>{uname_u}</b>  <code>{ucid}</code>")
                                    kb_u.append([{"text": f"🗑 حذف {uname_u}", "callback_data": f"admin:deluser:{ucid}"}])
                                kb_u.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_u), kb_u)

                    elif cbq_data.startswith("admin:deluser:"):
                        # مرحله اول: تایید حذف
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            del_cid = cbq_data.split(":")[2]
                            answer_callback(token_cbq, cbq_id)
                            d_confirm = load_alerts()
                            u_confirm = next((u for u in d_confirm.get("users",[]) if str(u.get("chat_id","")) == del_cid), {})
                            uname_confirm = u_confirm.get("custom_name","") or u_confirm.get("username","") or del_cid
                            confirm_kb = [
                                [{"text": "✅ بله، حذف کن", "callback_data": f"admin:confirmdelete:{del_cid}"}],
                                [{"text": "❌ نه، برگشت",   "callback_data": "admin:users"}],
                            ]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"⚠️ <b>تایید حذف کاربر</b>\n\n"
                                f"👤 <b>{uname_confirm}</b>\n"
                                f"🆔 <code>{del_cid}</code>\n\n"
                                f"مطمئنی می‌خوای این کاربر رو حذف کنی؟",
                                confirm_kb)

                    elif cbq_data.startswith("admin:confirmdelete:"):
                        # مرحله دوم: انجام حذف
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            del_cid = cbq_data.split(":")[2]
                            answer_callback(token_cbq, cbq_id, "🗑 حذف شد")
                            d_del = load_alerts()
                            d_del["users"] = [u for u in d_del.get("users",[]) if str(u.get("chat_id","")) != del_cid]
                            d_del["telegram"]["chat_ids"] = [x for x in d_del["telegram"].get("chat_ids",[]) if str(x) != del_cid]
                            save_alerts(d_del)
                            all_users2 = d_del.get("users", [])
                            if not all_users2:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 لیست کاربران خالی شد.", [[{"text": "✕ بستن", "callback_data": "close_myalerts"}]])
                            else:
                                lines_u2 = ["👥 <b>لیست کاربران</b>\n"]
                                kb_u2 = []
                                for u in all_users2:
                                    ucid = str(u.get("chat_id",""))
                                    uname_u2 = u.get("custom_name","") or u.get("username","") or ucid
                                    priv_icon2 = "🔒" if u.get("private_access") else "👤"
                                    lines_u2.append(f"{priv_icon2} <b>{uname_u2}</b>  <code>{ucid}</code>")
                                    kb_u2.append([{"text": f"🗑 حذف {uname_u2}", "callback_data": f"admin:deluser:{ucid}"}])
                                kb_u2.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_u2), kb_u2)

                    elif cbq_data == "admin:users":
                        # برگشت به لیست کاربران (از صفحه تایید)
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            answer_callback(token_cbq, cbq_id)
                            d_back = load_alerts()
                            all_users_back = d_back.get("users", [])
                            if not all_users_back:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "📭 هیچ کاربری ثبت نشده.", [[{"text": "✕ بستن", "callback_data": "close_myalerts"}]])
                            else:
                                lines_back = ["👥 <b>لیست کاربران</b>\n"]
                                kb_back = []
                                for u in all_users_back:
                                    ucid = str(u.get("chat_id",""))
                                    uname_back = u.get("custom_name","") or u.get("username","") or ucid
                                    priv_icon_back = "🔒" if u.get("private_access") else "👤"
                                    lines_back.append(f"{priv_icon_back} <b>{uname_back}</b>  <code>{ucid}</code>")
                                    kb_back.append([{"text": f"🗑 حذف {uname_back}", "callback_data": f"admin:deluser:{ucid}"}])
                                kb_back.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_back), kb_back)

                    elif cbq_data.startswith("admin_sig:"):
                        # فقط ادمین
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                            return
                        parts_as2 = cbq_data.split(":", 2)
                        as2_action = parts_as2[1]
                        as2_arg    = parts_as2[2] if len(parts_as2) > 2 else ""

                        if as2_action == "list":
                            answer_callback(token_cbq, cbq_id, "⏳ بارگذاری...")
                            page = int(as2_arg) if as2_arg.isdigit() else 1
                            per_page = 5
                            all_sigs = _sb_load_signals(limit=50)
                            total = len(all_sigs)
                            start_i = (page - 1) * per_page
                            page_sigs = all_sigs[start_i: start_i + per_page]
                            if not page_sigs:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    "🗑 <b>مدیریت سیگنال‌ها</b>\n\n📭 سیگنالی وجود ندارد.",
                                    [[{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}]])
                                return
                            lines_asl = [f"🗑 <b>مدیریت سیگنال‌ها</b>  ({total} سیگنال)\n"]
                            kb_asl = []
                            for s in page_sigs:
                                sid2   = s.get("id","")
                                sym2   = s.get("symbol","")
                                dir2   = s.get("direction","")
                                entry2 = s.get("entry")
                                tf2    = s.get("tf","")
                                by2    = s.get("sent_by","")
                                at2    = (s.get("sent_at") or "")[:16]
                                ch2    = s.get("channel_msg_id")
                                origin2 = "📤" if ch2 else "💾"
                                dir_short2 = {"buy_limit":"BL↗","buy_stop":"BS↗",
                                              "sell_limit":"SL↘","sell_stop":"SS↘"}.get(dir2, dir2)
                                lines_asl.append(
                                    f"{origin2} <b>{sid2}</b>  #{sym2}  {dir_short2}\n"
                                    f"   ➡️ <code>{_fmt_signal_price(entry2, sym2)}</code>  "
                                    f"⏱{tf2}  👤{by2}  🕐{at2}"
                                )
                                kb_asl.append([{"text": f"🗑 حذف {sid2} — {sym2} {dir_short2}",
                                                "callback_data": f"admin_sig:confirm:{sid2}:{page}"}])
                            # pagination
                            nav_row = []
                            if page > 1:
                                nav_row.append({"text": "◀️ قبل", "callback_data": f"admin_sig:list:{page-1}"})
                            if start_i + per_page < total:
                                nav_row.append({"text": "بعد ▶️", "callback_data": f"admin_sig:list:{page+1}"})
                            if nav_row:
                                kb_asl.append(nav_row)
                            kb_asl.append([{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}])
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                "\n".join(lines_asl), kb_asl)

                        elif as2_action == "confirm":
                            # as2_arg = "S10001:page"
                            conf_parts = as2_arg.split(":", 1)
                            conf_sid   = conf_parts[0]
                            conf_page  = conf_parts[1] if len(conf_parts) > 1 else "1"
                            answer_callback(token_cbq, cbq_id)
                            # اطلاعات سیگنال برای نمایش در تأیید
                            all_sigs_c = _sb_load_signals(limit=50)
                            sig_c = next((s for s in all_sigs_c if s.get("id") == conf_sid), None)
                            if not sig_c:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    "⚠️ سیگنال پیدا نشد.",
                                    [[{"text": "↩️ برگشت به لیست", "callback_data": f"admin_sig:list:{conf_page}"}]])
                                return
                            sym_c   = sig_c.get("symbol","")
                            dir_c   = sig_c.get("direction","")
                            entry_c = sig_c.get("entry")
                            by_c    = sig_c.get("sent_by","")
                            at_c    = (sig_c.get("sent_at") or "")[:16]
                            ch_c    = sig_c.get("channel_msg_id")
                            ch_warn = "\n\n⚠️ این سیگنال <b>به گروه ارسال شده</b> — حذف از DB فقط رکورد رو پاک میکنه، پیام کانال دست‌نخورده میمونه." if ch_c else ""
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"⚠️ <b>تأیید حذف سیگنال</b>\n\n"
                                f"🆔 <b>{conf_sid}</b>  #{sym_c}  {dir_c}\n"
                                f"➡️ Entry: <code>{_fmt_signal_price(entry_c, sym_c)}</code>\n"
                                f"👤 {by_c}  🕐 {at_c}"
                                f"{ch_warn}\n\n"
                                f"مطمئنی؟",
                                [[{"text": "✅ بله، حذف کن",    "callback_data": f"admin_sig:delete:{conf_sid}:{conf_page}"}],
                                 [{"text": "❌ نه، برگشت",       "callback_data": f"admin_sig:list:{conf_page}"}]])

                        elif as2_action == "delete":
                            del_parts  = as2_arg.split(":", 1)
                            del_sid    = del_parts[0]
                            del_page   = del_parts[1] if len(del_parts) > 1 else "1"
                            answer_callback(token_cbq, cbq_id, "🗑 در حال حذف...")
                            # حذف از Supabase
                            try:
                                r_del = requests.delete(
                                    f"{SUPABASE_URL}/rest/v1/signals?id=eq.{del_sid}",
                                    headers={**_sb_h(), "Prefer": "return=minimal"},
                                    timeout=10)
                                ok_del = r_del.status_code in (200, 204)
                            except Exception as e:
                                print(f"[admin_sig] delete error: {e}")
                                ok_del = False
                            if not ok_del:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    f"❌ خطا در حذف سیگنال <b>{del_sid}</b>.",
                                    [[{"text": "↩️ برگشت", "callback_data": f"admin_sig:list:{del_page}"}]])
                                return
                            # بعد از حذف، لیست آپدیت‌شده رو نشون بده
                            page_back = int(del_page) if del_page.isdigit() else 1
                            per_page2 = 5
                            all_sigs2 = _sb_load_signals(limit=50)
                            total2    = len(all_sigs2)
                            # اگه صفحه الان خالی شد، یه صفحه برگرد
                            start_i2 = (page_back - 1) * per_page2
                            if start_i2 >= total2 and page_back > 1:
                                page_back -= 1
                                start_i2 = (page_back - 1) * per_page2
                            page_sigs2 = all_sigs2[start_i2: start_i2 + per_page2]
                            if not page_sigs2:
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    f"✅ سیگنال <b>{del_sid}</b> حذف شد.\n\n📭 سیگنال دیگری وجود ندارد.",
                                    [[{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}]])
                                return
                            lines_af = [f"✅ <b>{del_sid}</b> حذف شد.\n\n🗑 <b>مدیریت سیگنال‌ها</b>  ({total2} سیگنال)\n"]
                            kb_af = []
                            for s in page_sigs2:
                                sid_f   = s.get("id",""); sym_f2  = s.get("symbol","")
                                dir_f   = s.get("direction",""); entry_f = s.get("entry")
                                tf_f    = s.get("tf",""); by_f    = s.get("sent_by","")
                                at_f    = (s.get("sent_at") or "")[:16]
                                ch_f    = s.get("channel_msg_id")
                                orig_f  = "📤" if ch_f else "💾"
                                ds_f    = {"buy_limit":"BL↗","buy_stop":"BS↗",
                                           "sell_limit":"SL↘","sell_stop":"SS↘"}.get(dir_f, dir_f)
                                lines_af.append(
                                    f"{orig_f} <b>{sid_f}</b>  #{sym_f2}  {ds_f}\n"
                                    f"   ➡️ <code>{_fmt_signal_price(entry_f, sym_f2)}</code>  "
                                    f"⏱{tf_f}  👤{by_f}  🕐{at_f}"
                                )
                                kb_af.append([{"text": f"🗑 حذف {sid_f} — {sym_f2} {ds_f}",
                                               "callback_data": f"admin_sig:confirm:{sid_f}:{page_back}"}])
                            nav_f = []
                            if page_back > 1:
                                nav_f.append({"text": "◀️ قبل", "callback_data": f"admin_sig:list:{page_back-1}"})
                            if start_i2 + per_page2 < total2:
                                nav_f.append({"text": "بعد ▶️", "callback_data": f"admin_sig:list:{page_back+1}"})
                            if nav_f: kb_af.append(nav_f)
                            kb_af.append([{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}])
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_af), kb_af)

                        elif as2_action == "back":
                            answer_callback(token_cbq, cbq_id)
                            admin_kb_b = [
                                [{"text": "📰 اخبار فارکس",      "callback_data": "admin:news"}],
                                [{"text": "✉️ پیام به گروه",     "callback_data": "admin:broadcast"}],
                                [{"text": "👥 لیست کاربران",      "callback_data": "admin:users"}],
                                [{"text": "🗑 مدیریت سیگنال‌ها", "callback_data": "admin_sig:list:1"}],
                                [{"text": "📋 تعیین شیفت",        "callback_data": "admin:shift:1"}],
                                [{"text": "✕ بستن",               "callback_data": "close_myalerts"}],
                            ]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                "⚙️ <b>پنل ادمین</b>\n\nیه گزینه انتخاب کن:", admin_kb_b)

                    # ─── مدیریت شیفت (تعیین/جابجایی مسئول آلارم) ──────────────────
                    elif cbq_data.startswith("admin:shift"):
                        if cbq_cid != YOUR_CHAT_ID:
                            answer_callback(token_cbq, cbq_id, "⛔ فقط ادمین")
                        else:
                            parts_sh = cbq_data.split(":")
                            sh_action = parts_sh[2] if len(parts_sh) > 2 else "1"

                            def _admin_shift_back_kb():
                                return [
                                    [{"text": "📋 تعیین شیفت", "callback_data": "admin:shift:1"}],
                                    [{"text": "↩️ پنل ادمین",  "callback_data": "admin_sig:back"}],
                                ]

                            # ── لیست آلارم‌های فعال (صفحه‌بندی ۵تایی) ──
                            if sh_action.isdigit():
                                answer_callback(token_cbq, cbq_id, "⏳ بارگذاری...")
                                sh_page = int(sh_action)
                                sh_rows = _sb_load_active_assignments()
                                PER_PAGE_SH = 5
                                total_sh = len(sh_rows)
                                if total_sh == 0:
                                    edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                        "📭 <b>هیچ آلارم فعالی برای تعیین شیفت نیست.</b>",
                                        [[{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}]])
                                else:
                                    start_sh = (sh_page - 1) * PER_PAGE_SH
                                    page_rows = sh_rows[start_sh: start_sh + PER_PAGE_SH]
                                    lines_sh = [f"📋 <b>آلارم‌های فعال</b>  ({total_sh} عدد)\n"]
                                    kb_sh = []
                                    for row_sh in page_rows:
                                        tag_sh   = row_sh.get("alarm_tag", "")
                                        asn_sh   = row_sh.get("assigned_to") or "—"
                                        sym_sh   = row_sh.get("symbol", "")
                                        shift_sh = row_sh.get("shift", "")
                                        lines_sh.append(
                                            f"• {tag_sh}  <code>{sym_sh}</code>\n"
                                            f"  👤 {asn_sh}  |  🕐 {shift_sh}"
                                        )
                                        aid_sh = row_sh.get("id","")
                                        kb_sh.append([{"text": f"🔀 {tag_sh} ({asn_sh})",
                                                        "callback_data": f"admin:shift:assign:{aid_sh}:{sh_page}"}])
                                    # ناوبری صفحه
                                    nav_sh = []
                                    if sh_page > 1:
                                        nav_sh.append({"text": "◀️ قبل", "callback_data": f"admin:shift:{sh_page-1}"})
                                    if start_sh + PER_PAGE_SH < total_sh:
                                        nav_sh.append({"text": "بعد ▶️", "callback_data": f"admin:shift:{sh_page+1}"})
                                    if nav_sh:
                                        kb_sh.append(nav_sh)
                                    kb_sh.append([{"text": "🔄 اجرای دستی تقسیم الان",
                                                    "callback_data": "admin:shift:run_now"}])
                                    kb_sh.append([{"text": "↩️ پنل ادمین", "callback_data": "admin_sig:back"}])
                                    edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                        "\n".join(lines_sh), kb_sh)

                            # ── اجرای دستی تقسیم الان ──
                            elif sh_action == "run_now":
                                answer_callback(token_cbq, cbq_id, "⏳ در حال تقسیم...")
                                def _do_run_now(tok=token_cbq, c=cbq_cid, mid=cbq_msg_id):
                                    sh_now = _get_current_shift()
                                    if sh_now == "morning":
                                        rows_n = _sb_load_pending_shifts(["night","evening_handover","weekend_pending","evening"])
                                        members_n = SHIFT_MORNING["members"]
                                        label_n = "تقسیم دستی — شیفت صبح"
                                    elif sh_now == "evening":
                                        rows_n = _sb_load_pending_shifts(["morning","morning_handover"])
                                        members_n = SHIFT_EVENING["members"]
                                        label_n = "تقسیم دستی — شیفت عصر"
                                    else:
                                        rows_n = []
                                        members_n = []
                                        label_n = "شب/آخر هفته"
                                    if rows_n and members_n:
                                        _send_handover_replies(rows_n, members_n, label_n)
                                        result_txt = f"✅ <b>{len(rows_n)} آلارم تقسیم شد</b>\nشیفت: {sh_now}"
                                    else:
                                        result_txt = f"⚠️ آلارم باز برای تقسیم نبود\nشیفت فعلی: {sh_now}"
                                    edit_tg_keyboard(tok, c, mid, result_txt,
                                        [[{"text": "📋 برگشت به لیست", "callback_data": "admin:shift:1"}],
                                         [{"text": "↩️ پنل ادمین",    "callback_data": "admin_sig:back"}]])
                                threading.Thread(target=_do_run_now, daemon=True).start()

                            # ── انتخاب آلارم برای reassign — نشون دادن لیست اعضا ──
                            elif sh_action == "assign" and len(parts_sh) >= 5:
                                answer_callback(token_cbq, cbq_id)
                                aid_asgn  = parts_sh[3]
                                page_asgn = parts_sh[4]
                                # همه اعضا برای انتخاب
                                all_members = SHIFT_MORNING["members"] + SHIFT_EVENING["members"]
                                kb_asgn = []
                                for m in all_members:
                                    kb_asgn.append([{"text": f"👤 {m}",
                                                      "callback_data": f"admin:shift:do:{aid_asgn}:{m}:{page_asgn}"}])
                                kb_asgn.append([{"text": "↩️ برگشت به لیست",
                                                  "callback_data": f"admin:shift:{page_asgn}"}])
                                edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                    f"👤 <b>انتخاب مسئول جدید</b>\n\n<code>{aid_asgn}</code>\n\nکدوم نفر؟",
                                    kb_asgn)

                            # ── تایید reassign — ذخیره در Supabase ──
                            elif sh_action == "do" and len(parts_sh) >= 6:
                                answer_callback(token_cbq, cbq_id, "✅ در حال جابجایی...")
                                aid_do    = parts_sh[3]
                                new_asn   = parts_sh[4]
                                page_do   = parts_sh[5]
                                def _do_reassign(aid=aid_do, asn=new_asn, tok=token_cbq,
                                                  c=cbq_cid, mid=cbq_msg_id, pg=page_do):
                                    # خوندن اطلاعات آلارم
                                    try:
                                        r_get = requests.get(
                                            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{aid}&select=*",
                                            headers=_sb_h(), timeout=8)
                                        row_d = r_get.json()[0] if r_get.status_code == 200 and r_get.json() else {}
                                    except:
                                        row_d = {}
                                    old_asn = row_d.get("assigned_to","")
                                    tag_d   = row_d.get("alarm_tag","")
                                    # آپدیت شمارش حافظه
                                    if old_asn and old_asn in _active_assign_count:
                                        _active_assign_count[old_asn] = max(0, _active_assign_count[old_asn] - 1)
                                    _active_assign_count[asn] = _active_assign_count.get(asn, 0) + 1
                                    # ذخیره در Supabase
                                    sh_cur = _get_current_shift() or row_d.get("shift","morning")
                                    requests.patch(
                                        f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{aid}",
                                        headers={**_sb_h(), "Prefer": "return=minimal"},
                                        json={"assigned_to": asn, "shift": sh_cur},
                                        timeout=8)
                                    # ارسال reply به گروه
                                    tg_tok, cids_d, _ = _get_token_and_cids()
                                    msg_map_d = _fired_msg_ids.get(aid, {})
                                    reply_d = (f"🔀 <b>جابجایی دستی</b>\n\n"
                                               f"{tag_d}\n"
                                               f"👤 مسئول جدید: <b>{asn}</b>"
                                               + (f"\n↩️ قبلی: {old_asn}" if old_asn else ""))
                                    for tc_d, tm_d in msg_map_d.items():
                                        if tc_d in ("__tag__","__text__"): continue
                                        try:
                                            requests.post(
                                                f"https://api.telegram.org/bot{tg_tok}/sendMessage",
                                                json={"chat_id": tc_d, "text": reply_d,
                                                      "parse_mode": "HTML",
                                                      "reply_to_message_id": tm_d},
                                                timeout=8, headers=H)
                                        except: pass
                                    edit_tg_keyboard(tok, c, mid,
                                        f"✅ <b>جابجایی انجام شد</b>\n\n{tag_d}\n👤 {asn}",
                                        [[{"text": "📋 برگشت به لیست", "callback_data": f"admin:shift:{pg}"}],
                                         [{"text": "↩️ پنل ادمین",    "callback_data": "admin_sig:back"}]])
                                threading.Thread(target=_do_reassign, daemon=True).start()

                    elif cbq_data.startswith("clean_chat:"):
                        answer_callback(token_cbq, cbq_id, "🧹 در حال پاک‌سازی...")
                        target_cid = cbq_data.split(":", 1)[1]
                        if cbq_msg_id:
                            _track_msg(cbq_cid, cbq_msg_id)
                        def _do_clean(tok=token_cbq, c=cbq_cid, tc=target_cid):
                            cnt = delete_chat_history(tok, tc)
                            send_tg_keyboard(tok, c,
                                f"✅ <b>{cnt} پیام پاک شد.</b>",
                                [[{"text": "✕ بستن", "callback_data": "close_myalerts"}]])
                        threading.Thread(target=_do_clean, daemon=True).start()

                    elif cbq_data.startswith("edit_name:"):
                        # شروع flow ویرایش اسم از طریق استاتوس
                        en_cid = cbq_data.split(":", 1)[1]
                        answer_callback(token_cbq, cbq_id)
                        d_en = load_alerts()
                        cur_name_en = next(
                            (u.get("custom_name","") for u in d_en.get("users",[]) if str(u.get("chat_id","")) == en_cid),
                            "")
                        cur_info_en = f"\nاسم فعلی: <b>{cur_name_en}</b>" if cur_name_en else ""
                        edit_tg_keyboard(token_cbq, en_cid, cbq_msg_id,
                            f"✏️ <b>ویرایش اسم</b>{cur_info_en}\n\nاسم جدیدت رو بنویس:",
                            [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{en_cid}"}]])
                        _pending_alarm[en_cid] = {"step": "edit_name_input", "data": {}, "bot_msg_id": cbq_msg_id}

                    elif cbq_data == "close_myalerts":
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=10, headers=H)
                        except: pass

                    elif cbq_data.startswith("flow_cancel:"):
                        # لغو هر flow در حال اجرا (آلارم عادی یا SOS)
                        f_cid = cbq_data.split(":", 1)[1]
                        _pending_alarm.pop(f_cid, None)
                        _pending_signal.pop(f_cid, None)
                        answer_callback(token_cbq, cbq_id, "لغو شد")
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            "❌ <b>عملیات لغو شد.</b>", [])

                    # ── signal callbacks ──────────────────────────────
                    elif cbq_data.startswith("sig_cancel:"):
                        sc_cid = cbq_data.split(":",1)[1]
                        _pending_signal.pop(sc_cid, None)
                        answer_callback(token_cbq, cbq_id, "لغو شد")
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            "❌ <b>ساخت سیگنال لغو شد.</b>", [])

                    elif cbq_data.startswith("sig_sym:"):
                        # انتخاب نماد از shortcut
                        parts_ss = cbq_data.split(":", 2)
                        ss_cid = parts_ss[1]; ss_sym = parts_ss[2]
                        ps = _pending_signal.get(ss_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["data"]["symbol"] = ss_sym
                        ps["step"] = "sig_direction"
                        dir_kb = [[{"text": lbl, "callback_data": f"sig_dir:{ss_cid}:{val}"} for lbl,val in SIGNAL_DIRECTIONS[:2]],
                                  [{"text": lbl, "callback_data": f"sig_dir:{ss_cid}:{val}"} for lbl,val in SIGNAL_DIRECTIONS[2:]],
                                  [{"text": "❌ انصراف", "callback_data": f"sig_cancel:{ss_cid}"}]]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"📡 <b>{ss_sym}</b>\n\nنوع سفارش:", dir_kb)

                    elif cbq_data.startswith("sig_dir:"):
                        # انتخاب جهت
                        parts_sd2 = cbq_data.split(":", 2)
                        sd2_cid = parts_sd2[1]; sd2_dir = parts_sd2[2]
                        ps = _pending_signal.get(sd2_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        dir_lbl_map = {"buy_limit":"✅ Buy Limit","buy_stop":"✅ Buy Stop",
                                       "sell_limit":"🔴 Sell Limit","sell_stop":"🔴 Sell Stop"}
                        ps["data"]["direction"] = sd2_dir
                        ps["data"]["dir_lbl"] = dir_lbl_map.get(sd2_dir, sd2_dir)
                        ps["step"] = "sig_sl_mode"
                        sym_sd2 = ps["data"].get("symbol","")
                        # انتخاب نوع SL
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"📡 <b>{sym_sd2}</b>  {ps['data']['dir_lbl']}\n\nاستاپ رو چطور میدی؟",
                            [[{"text": "🔢 عدد مستقیم", "callback_data": f"sig_slmode:{sd2_cid}:price"},
                              {"text": "📏 پیپ", "callback_data": f"sig_slmode:{sd2_cid}:pip"}],
                             [{"text": "❌ انصراف", "callback_data": f"sig_cancel:{sd2_cid}"}]])

                    elif cbq_data.startswith("sig_slmode:"):
                        parts_sm = cbq_data.split(":", 2)
                        sm_cid = parts_sm[1]; sm_mode = parts_sm[2]
                        ps = _pending_signal.get(sm_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["data"]["sl_mode"] = sm_mode
                        ps["step"] = "sig_entry_sl"
                        sym_sm = ps["data"].get("symbol","")
                        dir_lbl_sm = ps["data"].get("dir_lbl","")
                        mode_hint = "قیمت SL" if sm_mode == "price" else "پیپ SL"
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"📡 <b>{sym_sm}</b>  {dir_lbl_sm}\n\n"
                            f"بنویس:  <code>Entry  {mode_hint}</code>\n"
                            f"مثال:   <code>73370  {'72550' if sm_mode == 'price' else '82'}</code>",
                            [[{"text": "❌ انصراف", "callback_data": f"sig_cancel:{sm_cid}"}]])

                    elif cbq_data.startswith("sig_tf:"):
                        # انتخاب تایم‌فریم
                        stf_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(stf_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        cur_tf = ps["data"].get("tf", SIGNAL_DEFAULT_TF)
                        tf_kb = [[{"text": f"{'✅ ' if tf==cur_tf else ''}{tf}",
                                   "callback_data": f"sig_settf:{stf_cid}:{tf}"}
                                  for tf in SIGNAL_TF_OPTIONS[:3]],
                                 [{"text": f"{'✅ ' if tf==cur_tf else ''}{tf}",
                                   "callback_data": f"sig_settf:{stf_cid}:{tf}"}
                                  for tf in SIGNAL_TF_OPTIONS[3:]],
                                 [{"text": "↩️ بازگشت", "callback_data": f"sig_back:{stf_cid}"}]]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"⏱ تایم‌فریم رو انتخاب کن (فعلی: <b>{cur_tf}</b>):", tf_kb)

                    elif cbq_data.startswith("sig_settf:"):
                        parts_stf = cbq_data.split(":", 2)
                        stf_cid2 = parts_stf[1]; stf_val = parts_stf[2]
                        ps = _pending_signal.get(stf_cid2)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id, f"✅ {stf_val}")
                        ps["data"]["tf"] = stf_val
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token_cbq, cbq_cid, cbq_msg_id, ps["data"])

                    elif cbq_data.startswith("sig_tp:"):
                        stp_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(stp_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["data"]["_editing_tp"] = "tp2"
                        ps["step"] = "sig_tp_edit"
                        cur_tp2 = ps["data"].get("tp2")
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"🎯 <b>TP2</b> رو بنویس (اختیاری):\n"
                            + (f"فعلی: <code>{_fmt_signal_price(cur_tp2, ps['data'].get('symbol',''))}</code>\n" if cur_tp2 else "")
                            + "برای حذف بنویس: <code>0</code>",
                            [[{"text": "⏭ رد کن", "callback_data": f"sig_skip_tp:{stp_cid}:tp2"},
                              {"text": "↩️ بازگشت", "callback_data": f"sig_back:{stp_cid}"}]])

                    elif cbq_data.startswith("sig_skip_tp:"):
                        parts_skp = cbq_data.split(":", 2)
                        skp_cid = parts_skp[1]; skp_which = parts_skp[2]
                        ps = _pending_signal.get(skp_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        # اگه tp2 رد شد، tp3 رو هم رد کن
                        ps["data"]["tp2"] = None
                        ps["data"]["tp3"] = None
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token_cbq, cbq_cid, cbq_msg_id, ps["data"])

                    elif cbq_data.startswith("sig_note:"):
                        sn_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(sn_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["step"] = "sig_note"
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            "📝 یادداشت بنویس (اختیاری):",
                            [[{"text": "⏭ رد کن", "callback_data": f"sig_back:{sn_cid}"},
                              {"text": "❌ انصراف", "callback_data": f"sig_cancel:{sn_cid}"}]])

                    elif cbq_data.startswith("sig_recalc:"):
                        # محاسبه مجدد با RR متفاوت
                        src_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(src_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        rr_kb = [[{"text": f"{'✅ ' if ps['data'].get('rr')==v else ''}{v}R",
                                   "callback_data": f"sig_setrr:{src_cid}:{v}"}
                                  for v in [1.0, 1.5, 2.0]],
                                 [{"text": f"{'✅ ' if ps['data'].get('rr')==v else ''}{v}R",
                                   "callback_data": f"sig_setrr:{src_cid}:{v}"}
                                  for v in [2.5, 3.0]],
                                 [{"text": "↩️ بازگشت", "callback_data": f"sig_back:{src_cid}"}]]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"🔄 ریوارد رو انتخاب کن (فعلی: <b>{ps['data'].get('rr', SIGNAL_DEFAULT_RR)}R</b>):", rr_kb)

                    elif cbq_data.startswith("sig_setrr:"):
                        parts_rr = cbq_data.split(":", 2)
                        rr_cid = parts_rr[1]; rr_val = float(parts_rr[2])
                        ps = _pending_signal.get(rr_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id, f"✅ ریوارد {rr_val}R")
                        d_rr = ps["data"]
                        # محاسبه مجدد
                        _, tp1_new, _ = _calc_signal(d_rr["symbol"], d_rr["direction"],
                                                      d_rr["entry"], d_rr["sl"], rr_val)
                        d_rr["rr"] = rr_val
                        d_rr["tp1"] = tp1_new
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token_cbq, cbq_cid, cbq_msg_id, d_rr)

                    elif cbq_data.startswith("sig_back:"):
                        sb_cid = cbq_data.split(":",1)[1]
                        ps = _pending_signal.get(sb_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id)
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token_cbq, cbq_cid, cbq_msg_id, ps["data"])

                    elif cbq_data.startswith("signals_view:"):
                        # نمایش لیست سیگنال‌ها
                        parts_sv = cbq_data.split(":", 2)
                        sv_cid  = parts_sv[1]
                        sv_mode = parts_sv[2]  # mine | all
                        answer_callback(token_cbq, cbq_id, "⏳ در حال بارگذاری...")
                        sigs = _sb_load_signals(limit=20)
                        if sv_mode == "mine":
                            my_name = _get_user_custom_name(sv_cid) or sv_cid
                            sigs = [s for s in sigs if s.get("sent_by","") == my_name]
                            title = f"📡 <b>سیگنال‌های من</b>"
                        else:
                            title = f"📊 <b>همه سیگنال‌ها</b>"
                        if not sigs:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"{title}\n\n📭 سیگنالی یافت نشد.",
                                [[{"text": "↩️ بازگشت", "callback_data": f"signals_close:{sv_cid}"}]])
                            return
                        lines = [title, ""]
                        for s in sigs:
                            sym_sv   = s.get("symbol","")
                            sid_sv   = s.get("id","")
                            dir_sv   = s.get("direction","")
                            entry_sv = s.get("entry")
                            sl_sv    = s.get("sl")
                            tp1_sv   = s.get("tp1")
                            tf_sv    = s.get("tf","")
                            sent_by_sv = s.get("sent_by","")
                            sent_at_sv = s.get("sent_at","")[:16] if s.get("sent_at") else ""
                            ch_mid_sv  = s.get("channel_msg_id")
                            # آیکون ارسال به کانال یا فقط DB
                            origin = "📤" if ch_mid_sv else "💾"
                            dir_short = {"buy_limit":"BL↗","buy_stop":"BS↗",
                                         "sell_limit":"SL↘","sell_stop":"SS↘"}.get(dir_sv, dir_sv)
                            lines.append(
                                f"{origin} <b>{sid_sv}</b>  #{sym_sv}  <i>{dir_short}</i>\n"
                                f"   ➡️ <code>{_fmt_signal_price(entry_sv,sym_sv)}</code>  "
                                f"🛑 <code>{_fmt_signal_price(sl_sv,sym_sv)}</code>  "
                                f"🎯 <code>{_fmt_signal_price(tp1_sv,sym_sv)}</code>\n"
                                f"   ⏱ {tf_sv}  👤 {sent_by_sv}  🕐 {sent_at_sv}"
                            )
                            lines.append("──────────────────")
                        legend = "\n📤 = ارسال به گروه   💾 = فقط ثبت دیتا"
                        kb_sv = [
                            [{"text": "📡 سیگنال‌های من", "callback_data": f"signals_view:{sv_cid}:mine"},
                             {"text": "📊 همه سیگنال‌ها", "callback_data": f"signals_view:{sv_cid}:all"}],
                            [{"text": "✕ بستن", "callback_data": f"signals_close:{sv_cid}"}]
                        ]
                        full_text = "\n".join(lines) + legend
                        # تلگرام max 4096 کاراکتر
                        if len(full_text) > 4000:
                            full_text = full_text[:3980] + "\n\n<i>... (برای دیدن بیشتر فیلتر کن)</i>"
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, full_text, kb_sv)

                    elif cbq_data.startswith("signals_close:"):
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=8, headers=H)
                        except: pass

                    elif cbq_data.startswith("trigger_list:"):
                        answer_callback(token_cbq, cbq_id, "⏳ در حال بارگذاری...")
                        tl_cid = cbq_data.split(":", 1)[1]
                        rows_tl_all = _sb_load_active_assignments()
                        # فقط آلارم‌های تیمی — active + archive بدون کش
                        _raw_tl = _sb_load_all_alerts()
                        if _raw_tl and isinstance(_raw_tl, dict):
                            all_alerts_tl = _raw_tl.get("alarms", []) + _raw_tl.get("archive", [])
                        else:
                            _fb_tl = load_alerts()
                            all_alerts_tl = _fb_tl.get("alarms", []) + _fb_tl.get("archive", [])
                        private_ids = {str(a["id"]) for a in all_alerts_tl if a.get("is_private")}
                        rows_tl = [r for r in rows_tl_all if str(r.get("id","")) not in private_ids]
                        my_name_tl = _get_user_custom_name(tl_cid) or ""
                        if not rows_tl:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                "🎯 <b>لیست تریگر</b>\n\n📭 هیچ آلارم فعالی در لیست تریگر نیست.",
                                [[{"text": "↩️ بازگشت", "callback_data": f"trigger_list_close:{tl_cid}"}]])
                        else:
                            # گروه‌بندی بر اساس مسئول
                            by_member = {}
                            unassigned = []
                            for row_tl in rows_tl:
                                m = row_tl.get("assigned_to", "")
                                tag = row_tl.get("alarm_tag", "—")
                                shift_tl = row_tl.get("shift", "")
                                if m:
                                    by_member.setdefault(m, []).append((tag, shift_tl))
                                else:
                                    unassigned.append((tag, shift_tl))
                            lines_tl = ["🎯 <b>لیست تریگر فعال</b>\n"]
                            for member, items in sorted(by_member.items()):
                                marker = " 👈" if member == my_name_tl else ""
                                lines_tl.append(f"👤 <b>{member}</b>{marker}")
                                for tag_tl, sh_tl in items:
                                    lines_tl.append(f"   • {tag_tl}")
                                lines_tl.append("")
                            if unassigned:
                                lines_tl.append("⏳ <b>منتظر تقسیم (شب/آخر هفته)</b>")
                                for tag_tl, sh_tl in unassigned:
                                    lines_tl.append(f"   • {tag_tl}")
                            full_tl = "\n".join(lines_tl)
                            if len(full_tl) > 4000:
                                full_tl = full_tl[:3980] + "\n\n<i>...</i>"
                            kb_tl = [[{"text": "🔄 بروزرسانی", "callback_data": f"trigger_list:{tl_cid}"},
                                      {"text": "✕ بستن",       "callback_data": f"trigger_list_close:{tl_cid}"}]]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, full_tl, kb_tl)

                    elif cbq_data.startswith("trigger_list_close:"):
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=8, headers=H)
                        except: pass

                    elif cbq_data.startswith("weekly_menu:"):
                        answer_callback(token_cbq, cbq_id, "")
                        wm_cid = cbq_data.split(":", 1)[1]
                        kb_menu = [
                            [{"text": "📅 این هفته", "callback_data": f"weekly_report:{wm_cid}:this:0"},
                             {"text": "📅 هفته قبل", "callback_data": f"weekly_report:{wm_cid}:last:0"}],
                        ]
                        if APP_BASE_URL:
                            kb_menu.append([{"text": "🌐 نسخه وب", "url": f"{APP_BASE_URL}/report/weekly"}])
                        kb_menu.append([{"text": "✕ بستن", "callback_data": f"weekly_report_close:{wm_cid}"}])
                        send_tg_keyboard(token_cbq, cbq_cid, "📊 <b>آنالیز هفتگی تیم</b>\n\nیک گزینه رو انتخاب کن:", kb_menu)

                    elif cbq_data.startswith("weekly_report:"):
                        answer_callback(token_cbq, cbq_id, "⏳ در حال بارگذاری...")
                        wr_parts = cbq_data.split(":")
                        cbq_cid_wr = wr_parts[1]
                        wr_which   = wr_parts[2] if len(wr_parts) > 2 else "this"
                        wr_page    = int(wr_parts[3]) if len(wr_parts) > 3 else 0
                        PER_PAGE   = 5
                        now_dt_wr = datetime.now(TEHRAN)
                        days_since_sat = (now_dt_wr.weekday() - 5) % 7
                        this_week_start = (now_dt_wr - timedelta(days=days_since_sat)).replace(
                            hour=0, minute=0, second=0, microsecond=0)
                        if wr_which == "last":
                            week_start = this_week_start - timedelta(days=7)
                            week_end   = this_week_start
                        else:
                            week_start = this_week_start
                            week_end   = None
                        week_start_str = week_start.strftime("%Y-%m-%dT%H:%M:%S")
                        week_label = f"{week_start.strftime('%d/%m')} — {(week_end - timedelta(days=1)).strftime('%d/%m') if week_end else 'الان'}"
                        rows_wr = []
                        if SUPABASE_KEY:
                            try:
                                url_wr = (f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                                          f"?fired_at=gte.{week_start_str}&select=*&order=fired_at.asc")
                                if week_end:
                                    url_wr += f"&fired_at=lt.{week_end.strftime('%Y-%m-%dT%H:%M:%S')}"
                                r_wr = requests.get(url_wr, headers=_sb_h(), timeout=10)
                                if r_wr.status_code == 200:
                                    rows_wr = r_wr.json()
                            except Exception as e:
                                print(f"[weekly] load exc: {e}")
                        # لود همه آلارم‌ها — active + archive
                        _raw_wr = _sb_load_all_alerts()
                        if _raw_wr and isinstance(_raw_wr, dict):
                            all_alerts_wr = _raw_wr.get("alarms", []) + _raw_wr.get("archive", [])
                        else:
                            _fb = load_alerts()
                            all_alerts_wr = _fb.get("alarms", []) + _fb.get("archive", [])
                        private_ids_wr = {str(a["id"]) for a in all_alerts_wr if a.get("is_private")}
                        alerts_by_id_wr = {str(a["id"]): a for a in all_alerts_wr}
                        rows_wr = [r for r in rows_wr if str(r.get("id","")) not in private_ids_wr]
                        # صفحه‌بندی
                        total = len(rows_wr)
                        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
                        wr_page = max(0, min(wr_page, total_pages - 1))
                        page_rows = rows_wr[wr_page * PER_PAGE:(wr_page + 1) * PER_PAGE]

                        # فقط صفحه‌بندی و بستن
                        kb_wr_nav = []
                        if total_pages > 1:
                            page_row = []
                            if wr_page > 0:
                                page_row.append({"text": "‹ قبلی", "callback_data": f"weekly_report:{cbq_cid_wr}:{wr_which}:{wr_page-1}"})
                            page_row.append({"text": f"{wr_page+1}/{total_pages}", "callback_data": "noop"})
                            if wr_page < total_pages - 1:
                                page_row.append({"text": "بعدی ›", "callback_data": f"weekly_report:{cbq_cid_wr}:{wr_which}:{wr_page+1}"})
                            kb_wr_nav.append(page_row)
                        kb_wr_nav.append([{"text": "🔍 جستجو", "callback_data": f"weekly_search:{cbq_cid_wr}:{wr_which}"},
                                          {"text": "✕ بستن", "callback_data": f"weekly_report_close:{cbq_cid_wr}"}])

                        if not rows_wr:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"📋 <b>گزارش هفتگی تیم</b>\n<i>{week_label}</i>\n\n📭 هیچ آلارم تیمی ثبت نشده.",
                                kb_wr_nav)
                        else:
                            lines_wr = [
                                f"📋 <b>گزارش هفتگی تیم</b>",
                                f"<i>{week_label} — {total} آلارم | صفحه {wr_page+1}/{total_pages}</i>",
                                ""
                            ]
                            for row_wr in page_rows:
                                aid_wr       = str(row_wr.get("id", ""))
                                tag_wr       = row_wr.get("alarm_tag", "—")
                                assignee_wr  = row_wr.get("assigned_to", "") or "—"
                                fired_wr     = row_wr.get("fired_at", "")[:16]
                                false_by_wr  = row_wr.get("false_by", "") or ""
                                false_at_wr  = row_wr.get("false_at", "")[:16] if row_wr.get("false_at") else ""
                                false_rsn_wr = row_wr.get("false_reason", "") or ""
                                is_active_wr = row_wr.get("is_active", True)
                                alert_wr   = alerts_by_id_wr.get(aid_wr, {})
                                sym_wr     = alert_wr.get("symbol", "") or row_wr.get("symbol", "") or ""
                                tgt_raw    = alert_wr.get("target_price", 0) or row_wr.get("target_price", 0) or 0
                                target_wr  = fmt_price(float(tgt_raw), sym_wr) if tgt_raw else "—"
                                creator_wr = alert_wr.get("created_by", "") or row_wr.get("created_by", "") or "—"
                                created_wr = str(alert_wr.get("created_at", ""))[:16]
                                cond_wr    = alert_wr.get("condition", "") or row_wr.get("condition", "")
                                dir_wr     = "📈 ناحیه سل" if cond_wr == "above" else ("📉 ناحیه بای" if cond_wr == "below" else "")
                                lines_wr.append(f"🔖 <b>{tag_wr}</b>  |  #{sym_wr}" + (f"  |  {dir_wr}" if dir_wr else ""))
                                if created_wr:
                                    lines_wr.append(f"📅 ثبت: {created_wr}")
                                lines_wr.append(f"🎯 هدف: <code>{target_wr}</code>")
                                lines_wr.append(f"👤 سازنده: {creator_wr}")
                                lines_wr.append(f"⏰ فایر شد: {fired_wr}")
                                lines_wr.append(f"🙋 مسئول: {assignee_wr}")
                                if is_active_wr:
                                    lines_wr.append(f"✅ وضعیت: فعال")
                                else:
                                    hist_wr = row_wr.get("false_history") or []
                                    if isinstance(hist_wr, str):
                                        try: hist_wr = json.loads(hist_wr)
                                        except: hist_wr = []
                                    if hist_wr:
                                        lines_wr.append("❌ <b>تاریخچه False:</b>")
                                        for i_h, h_wr in enumerate(hist_wr, 1):
                                            h_at  = str(h_wr.get("at",""))[:16]
                                            h_by  = h_wr.get("by","")
                                            h_rsn = h_wr.get("reason","")
                                            h_line = f"  {i_h}. {h_by}  |  {h_at}"
                                            if h_rsn:
                                                h_line += f"\n     📝 {h_rsn}"
                                            lines_wr.append(h_line)
                                    else:
                                        lines_wr.append(f"❌ وضعیت: False — {false_by_wr}  |  {false_at_wr}")
                                        if false_rsn_wr:
                                            lines_wr.append(f"📝 علت: {false_rsn_wr}")
                                lines_wr.append("──────────────")
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "\n".join(lines_wr), kb_wr_nav)

                    elif cbq_data.startswith("weekly_report_close:"):
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=8, headers=H)
                        except: pass

                    elif cbq_data.startswith("weekly_search:"):
                        answer_callback(token_cbq, cbq_id, "")
                        ws_parts = cbq_data.split(":")
                        ws_cid   = ws_parts[1]
                        ws_which = ws_parts[2] if len(ws_parts) > 2 else "this"
                        _pending_weekly_search[ws_cid] = {"which": ws_which, "msg_id": cbq_msg_id}
                        send_tg(token_cbq, ws_cid, "🔍 اسم مسئول، نماد یا تگ رو بفرست (مثلاً: مسعود):")

                    elif cbq_data.startswith("resend_active:"):
                        answer_callback(token_cbq, cbq_id, "⏳ در حال ارسال...")
                        ra_cid = cbq_data.split(":", 1)[1]
                        rows_ra = _sb_load_pending_shifts(
                            ["morning", "evening", "night", "morning_handover",
                             "evening_handover", "weekend_pending"])
                        # فقط تیمی
                        all_alerts_ra = load_alerts().get("alarms", [])
                        private_ids_ra = {str(a["id"]) for a in all_alerts_ra if a.get("is_private")}
                        rows_ra = [r for r in rows_ra if str(r.get("id","")) not in private_ids_ra]
                        if not rows_ra:
                            send_tg(token_cbq, ra_cid, "📭 هیچ آلارم فعالی در لیست تریگر نیست.")
                        else:
                            send_tg(token_cbq, ra_cid,
                                f"🔔 <b>{len(rows_ra)} آلارم فعال</b> — ریپلای زیر:")
                            for row_ra in rows_ra:
                                aid_ra = str(row_ra.get("id", ""))
                                tag_ra = row_ra.get("alarm_tag", "—")
                                assignee_ra = row_ra.get("assigned_to", "") or "⏳ منتظر تقسیم"
                                # پیدا کردن message_id اصلی برای این کاربر
                                cid_map_ra = _fired_msg_ids.get(aid_ra, {})
                                orig_mid = cid_map_ra.get(ra_cid)
                                orig_text = cid_map_ra.get("__text__", "")
                                if orig_mid:
                                    # ریپلای روی همون پیام اصلی
                                    try:
                                        requests.post(
                                            f"https://api.telegram.org/bot{token_cbq}/sendMessage",
                                            json={"chat_id": ra_cid,
                                                  "text": f"🔔 <b>{tag_ra}</b>\n👤 مسئول: {assignee_ra}",
                                                  "parse_mode": "HTML",
                                                  "reply_to_message_id": orig_mid},
                                            timeout=8, headers=H)
                                    except: pass
                                else:
                                    # پیام اصلی پیدا نشد — متن خلاصه بفرست
                                    send_tg(token_cbq, ra_cid,
                                        f"🔔 <b>{tag_ra}</b>\n👤 مسئول: {assignee_ra}")

                    elif cbq_data.startswith("today_alarms:"):
                        answer_callback(token_cbq, cbq_id, "⏳ در حال بارگذاری...")
                        ta_parts = cbq_data.split(":")
                        ta_cid   = ta_parts[1]
                        ta_mode  = ta_parts[2] if len(ta_parts) > 2 else "active"
                        # ابتدای هفته — شنبه تهران
                        now_dt_ta = datetime.now(TEHRAN)
                        days_since_sat = (now_dt_ta.weekday() - 5) % 7
                        week_start_ta = (now_dt_ta - timedelta(days=days_since_sat)).replace(
                            hour=0, minute=0, second=0, microsecond=0)
                        week_start_str_ta = week_start_ta.strftime("%Y-%m-%dT%H:%M:%S")
                        rows_ta = []
                        if SUPABASE_KEY:
                            try:
                                if ta_mode == "active":
                                    url_ta = (
                                        f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                                        f"?fired_at=gte.{week_start_str_ta}&is_active=eq.true"
                                        f"&select=*&order=fired_at.asc"
                                    )
                                else:
                                    url_ta = (
                                        f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                                        f"?fired_at=gte.{week_start_str_ta}"
                                        f"&select=*&order=fired_at.asc"
                                    )
                                r_ta = requests.get(url_ta, headers=_sb_h(), timeout=10)
                                if r_ta.status_code == 200:
                                    rows_ta = r_ta.json()
                            except Exception as e:
                                print(f"[weekly_list] load exc: {e}")
                        # فقط تیمی
                        all_alerts_ta = load_alerts().get("alarms", [])
                        private_ids_ta = {str(a["id"]) for a in all_alerts_ta if a.get("is_private")}
                        rows_ta = [r for r in rows_ta if str(r.get("id","")) not in private_ids_ta]
                        mode_label = "فعال" if ta_mode == "active" else "همه"
                        week_label = week_start_ta.strftime("%d/%m")
                        if not rows_ta:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                                f"📋 <b>هفتگی ({mode_label}) از {week_label}</b>\n\n📭 آلارمی پیدا نشد.",
                                [[{"text": "✅ فعال", "callback_data": f"today_alarms:{ta_cid}:active"},
                                  {"text": "📊 همه",  "callback_data": f"today_alarms:{ta_cid}:all"}],
                                 [{"text": "✕ بستن", "callback_data": f"today_alarms_close:{ta_cid}"}]])
                        else:
                            lines_ta = [f"📋 <b>هفتگی ({mode_label}) از {week_label} — {len(rows_ta)} آلارم</b>", ""]
                            alerts_by_id_ta = {str(a["id"]): a for a in all_alerts_ta}
                            for row_ta in rows_ta:
                                aid_ta      = str(row_ta.get("id",""))
                                tag_ta      = row_ta.get("alarm_tag", "—")
                                assignee_ta = row_ta.get("assigned_to", "") or "⏳ منتظر تقسیم"
                                fired_ta    = row_ta.get("fired_at", "")[:16]
                                is_act_ta   = row_ta.get("is_active", True)
                                alert_ta    = alerts_by_id_ta.get(aid_ta, {})
                                sym_ta      = alert_ta.get("symbol", "")
                                target_ta   = alert_ta.get("target_price", "") or alert_ta.get("price", "")
                                status_icon = "✅" if is_act_ta else "❌"
                                lines_ta.append(f"{status_icon} <b>{tag_ta}</b>  {sym_ta}")
                                if target_ta:
                                    lines_ta.append(f"   🎯 هدف: <code>{target_ta}</code>")
                                lines_ta.append(f"   ⏰ {fired_ta}  |  👤 {assignee_ta}")
                                lines_ta.append("")
                            full_ta = "\n".join(lines_ta)
                            if len(full_ta) > 4000:
                                full_ta = full_ta[:3980] + "\n<i>...</i>"
                            kb_ta = [
                                [{"text": "✅ فعال", "callback_data": f"today_alarms:{ta_cid}:active"},
                                 {"text": "📊 همه",  "callback_data": f"today_alarms:{ta_cid}:all"}],
                                [{"text": "🔄 بروزرسانی", "callback_data": f"today_alarms:{ta_cid}:{ta_mode}"},
                                 {"text": "✕ بستن",        "callback_data": f"today_alarms_close:{ta_cid}"}]
                            ]
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, full_ta, kb_ta)

                    elif cbq_data.startswith("today_alarms_close:"):
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=8, headers=H)
                        except: pass

                    elif cbq_data.startswith("sig_send:"):
                        parts_snd = cbq_data.split(":", 2)
                        send_cid  = parts_snd[1]
                        send_mode = parts_snd[2] if len(parts_snd) > 2 else "channel"
                        ps = _pending_signal.get(send_cid)
                        if not ps:
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد")
                            return
                        answer_callback(token_cbq, cbq_id, "⏳ در حال ثبت...")
                        d_send = ps["data"]
                        seq = _sb_next_signal_seq()
                        sig_id = f"S{seq:05d}"
                        d_send["id"] = sig_id
                        d_send["seq"] = seq
                        d_send["sent_by"] = _get_user_custom_name(send_cid) or send_cid
                        d_send["sent_at"] = now_teh()
                        d_send["status"]  = "active"
                        d_send["channel_msg_id"] = None
                        # ارسال به کانال فقط در حالت channel
                        channel_mid = None
                        if send_mode == "channel" and SIGNAL_CHANNEL:
                            try:
                                r_ch = requests.post(
                                    f"https://api.telegram.org/bot{token_cbq}/sendMessage",
                                    json={"chat_id": SIGNAL_CHANNEL, "text": _build_signal_text(d_send),
                                          "parse_mode": "HTML"},
                                    timeout=10, headers=H)
                                if r_ch.status_code == 200:
                                    channel_mid = r_ch.json().get("result",{}).get("message_id")
                                    d_send["channel_msg_id"] = channel_mid
                            except Exception as e:
                                print(f"[signal] channel send error: {e}")
                        # همیشه در Supabase ذخیره میشه
                        threading.Thread(target=_sb_save_signal, args=(d_send,), daemon=True).start()
                        del _pending_signal[send_cid]
                        sig_text_conf = _build_signal_text(d_send)
                        if send_mode == "channel":
                            status_line = f"📤 ارسال شد به گروه" if channel_mid else "⚠️ کانال تنظیم نشده — فقط ذخیره شد"
                        else:
                            status_line = "💾 فقط در دیتابیس ثبت شد"
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"✅ <b>سیگنال {sig_id} ثبت شد</b>  {status_line}\n\n{sig_text_conf}", [])

                    elif cbq_data.startswith("alarm_dir:"):
                        # alarm_dir:cid:buy|sell
                        parts_ad = cbq_data.split(":", 2)
                        ad_cid = parts_ad[1] if len(parts_ad) > 1 else cbq_cid
                        ad_raw = parts_ad[2] if len(parts_ad) > 2 else "sell"
                        pend_ad = _pending_alarm.get(ad_cid)
                        if not pend_ad or pend_ad.get("step") != "alarm_dir":
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد — دوباره شروع کن")
                            return
                        answer_callback(token_cbq, cbq_id)
                        dw_ad = pend_ad["data"]
                        if ad_raw == "buy":
                            dw_ad["condition"] = "below"
                            dir_lbl_ad = "📈 BUY"
                        else:
                            dw_ad["condition"] = "above"
                            dir_lbl_ad = "📉 SELL"
                        pend_ad["step"] = "alarm_price"
                        ptype_lbl_ad = "🔒 شخصی" if dw_ad.get("ptype") == "private" else "🌐 تیمی"
                        edit_tg_keyboard(token_cbq, ad_cid, cbq_msg_id,
                            f"🔔 <b>{dw_ad['symbol']}</b>  {dir_lbl_ad}  ({ptype_lbl_ad})\n\nقیمت هدف رو بنویس:\nمثال: <code>1.08500</code>  یا  <code>2350</code>",
                            [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{ad_cid}"}]])

                    elif cbq_data.startswith("alarm_submit:"):
                        # ثبت آلارم بدون یادداشت از طریق دکمه inline
                        as_cid = cbq_data.split(":", 1)[1]
                        pend_as = _pending_alarm.get(as_cid)
                        if not pend_as or pend_as.get("step") != "alarm_comment":
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد — دوباره شروع کن")
                            return
                        answer_callback(token_cbq, cbq_id, "⏳ در حال ثبت...")
                        dw_as = pend_as["data"]
                        as_uname = cbq.get("from",{}).get("username","") or cbq.get("from",{}).get("first_name","")
                        is_private_as = dw_as.get("ptype","public") == "private"
                        sender_as = _get_user_custom_name(as_cid) or as_uname
                        sym_as = dw_as["symbol"]
                        atype_as = dw_as["atype"]
                        new_alert_as = {
                            "id": str(int(time.time()*1000)),
                            "symbol": sym_as, "type": atype_as,
                            "target_price": dw_as["target_price"], "condition": dw_as["condition"],
                            "comment": "", "created_by": sender_as,
                            "active": True, "last_price": None, "last_checked": None,
                            "created_at": now_teh(),
                            "is_private": is_private_as,
                            "private_cid": as_cid if is_private_as else None,
                            "notify_only": as_cid if is_private_as else (YOUR_CHAT_ID if not BROADCAST_MODE else None)
                        }
                        d_as = load_alerts()
                        d_as["alerts"].append(new_alert_as)
                        _sb_upsert_alert(new_alert_as)
                        _cache_alerts = d_as
                        del _pending_alarm[as_cid]
                        dir_lbl_as = "📈 BUY" if new_alert_as["condition"] == "below" else "📉 SELL"
                        priv_lbl_as = "  🔒 شخصی" if is_private_as else "  🌐 تیمی"
                        confirm_as = (
                            f"✅ <b>آلارم ثبت شد!</b>\n\n"
                            f"💰 <b>{sym_as}</b>  {dir_lbl_as}{priv_lbl_as}\n"
                            f"🎯 هدف: <code>{fmt_price(new_alert_as['target_price'], sym_as)}</code>\n"
                            f"\n⏰ {now_teh()} (تهران)"
                        )
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, confirm_as, [])
                        def _bg_as(alert=new_alert_as, s=sym_as, t=atype_as):
                            try:
                                cur = get_price(s, t)
                                if cur:
                                    alert["last_price"] = cur
                                    alert["last_checked"] = now_teh()
                                    _sb_upsert_alert(alert)
                            except: pass
                        threading.Thread(target=_bg_as, daemon=True).start()

                    elif cbq_data.startswith("sos_dir:"):
                        # sos_dir:cid:buy|sell
                        parts_sd = cbq_data.split(":", 2)
                        sd_cid = parts_sd[1] if len(parts_sd) > 1 else cbq_cid
                        sd_raw = parts_sd[2] if len(parts_sd) > 2 else "sell"
                        pend = _pending_alarm.get(sd_cid)
                        if not pend or pend.get("step") != "sos_dir":
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد — دوباره شروع کن")
                            return
                        answer_callback(token_cbq, cbq_id)
                        dw_sd = pend["data"]
                        if sd_raw == "buy":
                            dw_sd["condition"] = "below"
                            dw_sd["dir_lbl"] = "📈 BUY"
                        else:
                            dw_sd["condition"] = "above"
                            dw_sd["dir_lbl"] = "📉 SELL"
                        pend["step"] = "sos_comment"
                        sym_sd = dw_sd.get("symbol", "")
                        dir_lbl_sd = dw_sd["dir_lbl"]
                        edit_tg_keyboard(token_cbq, sd_cid, cbq_msg_id,
                            f"⚡ <b>{sym_sd}</b>  {dir_lbl_sd}\n\nیادداشت اختیاری بنویس\nیا دکمه «بدون یادداشت» رو بزن:",
                            [
                                [{"text": "✅ ارسال بدون یادداشت", "callback_data": f"sos_nocomment:{sd_cid}"}],
                                [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{sd_cid}"}]
                            ])

                    elif cbq_data.startswith("sos_nocomment:"):
                        # کاربر بدون یادداشت ارسال کرد
                        sc_cid = cbq_data.split(":", 1)[1]
                        pend_sc = _pending_alarm.get(sc_cid)
                        if not pend_sc or pend_sc.get("step") != "sos_comment":
                            answer_callback(token_cbq, cbq_id, "⚠️ جلسه منقضی شد — دوباره شروع کن")
                            return
                        answer_callback(token_cbq, cbq_id, "⏳ در حال ارسال...")
                        dw_sc = pend_sc["data"]
                        sym_sc = dw_sc["symbol"]
                        condition_sc = dw_sc["condition"]
                        dir_lbl_sc = dw_sc.get("dir_lbl", "📈 BUY" if condition_sc == "below" else "📉 SELL")
                        atype_sc = "forex" if any(x in sym_sc for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                        sender_sc = _get_user_custom_name(sc_cid) or cbq_cid
                        alarm_num_tag_sc = _make_alarm_tag(sym_sc)
                        sender_tag_sc = "#" + re.sub(r"[^\w]","_", sender_sc).strip("_")
                        arrow_sc = "📈 ناحیه سل" if condition_sc == "above" else "📉 ناحیه بای"
                        try: cur_sc = get_price(sym_sc, atype_sc)
                        except: cur_sc = None
                        out_sc = (
                            f"🚨 <b>آلارم فوری!</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"💰 <b>#{sym_sc}</b>  {arrow_sc}\n"
                            f"🔖 {alarm_num_tag_sc}\n"
                            f"👤 {sender_tag_sc}\n"
                            f"📊 قیمت: <b>{fmt_price(cur_sc, sym_sc) if cur_sc else '—'}</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"⏰ {now_pretty()} (تهران)"
                        )
                        _, all_cids_sc, _ = _get_token_and_cids()
                        targets_sc = all_cids_sc if BROADCAST_MODE else [YOUR_CHAT_ID]
                        del _pending_alarm[sc_cid]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"✅ <b>آلارم فوری ارسال شد!</b>\n\n"
                            f"💰 <b>{sym_sc}</b>  {dir_lbl_sc}\n"
                            f"⏰ {now_pretty()} (تهران)", [])
                        sos_aid = f"sos_{sym_sc}_{int(time.time())}"
                        def _bg_sos(tok=token_cbq, tgts=targets_sc, msg=out_sc, s=sym_sc, aid=sos_aid,
                                    atag=alarm_num_tag_sc, sndr=sender_sc, cond=condition_sc, atp=atype_sc, cur=cur_sc):
                            sos_cid_to_mid = {}
                            for tc_sc in tgts:
                                kb_sc = [[{"text": "⏰ هشدار دوره‌ای", "callback_data": f"set_reminder:{tc_sc}:{s}"}]]
                                mid_sc = send_tg_keyboard(tok, str(tc_sc), msg, kb_sc, track=False)
                                if mid_sc:
                                    sos_cid_to_mid[str(tc_sc)] = mid_sc
                            if sos_cid_to_mid:
                                sos_cid_to_mid["__tag__"] = atag
                                sos_cid_to_mid["__text__"] = msg
                                _fired_msg_ids[aid] = sos_cid_to_mid
                                threading.Thread(target=_sb_save_fired_msgs, args=(aid, sos_cid_to_mid), daemon=True).start()
                            # ذخیره توی archive
                            sos_arch_entry = {"id": aid, "symbol": s, "type": atp,
                                "condition": cond, "comment": "", "created_by": sndr,
                                "active": False, "fired_at": now_teh(), "fired_price": cur,
                                "instant": True, "created_at": now_teh(), "tag": atag}
                            d_arc = load_alerts()
                            d_arc.setdefault("archive", []).append(sos_arch_entry)
                            save_alerts(d_arc)
                            threading.Thread(target=_sb_upsert_alert, args=(sos_arch_entry,), daemon=True).start()
                        threading.Thread(target=_bg_sos, daemon=True).start()

                msg = upd.get("message", {})
                raw_txt = msg.get("text", "") or ""
                # normalize: /cmd@botname → /cmd
                txt = raw_txt.split("@")[0] if raw_txt.startswith("/") else raw_txt
                ch = msg.get("chat", {})
                cid = str(ch.get("id", ""))
                uname = ch.get("username", "") or ch.get("first_name", "")

                # track پیام کاربر برای پاک‌سازی
                user_msg_id = msg.get("message_id")
                if cid and user_msg_id:
                    _track_msg(cid, user_msg_id)

                # ── /start ──────────────────────────────────────────
                if txt.startswith("/start") and cid:
                    data = load_alerts()
                    users = data.get("users", [])
                    existing_user = next((u for u in users if str(u.get("chat_id","")) == cid), None)
                    existing_name = existing_user.get("custom_name","").strip() if existing_user else ""
                    # ثبت کاربر اگه جدیده
                    if not existing_user:
                        users.append({"chat_id": cid, "username": uname, "joined_at": now_teh(), "custom_name": "", "private_access": False})
                        data["users"] = users
                        ids = data.get("telegram", {}).get("chat_ids", [])
                        if cid not in [str(x) for x in ids]:
                            ids.append(cid)
                        data["telegram"]["chat_ids"] = ids
                        save_alerts(data)
                    is_adm = (cid == YOUR_CHAT_ID)
                    if existing_name:
                        # کاربر قبلاً اسم داده — مستقیم به منو برو
                        show_main_menu(token, cid,
                            f"👋 خوش برگشتی <b>{existing_name}</b>!\n\nاز منوی زیر انتخاب کن 👇",
                            is_adm)
                    else:
                        # اولین بار — اسم بخواه
                        _pending_name[cid] = True
                        send_tg(token, cid,
                            f"👋 سلام <b>{uname}</b>!\n\n"
                            f"لطفاً <b>اسمی که در سایت استفاده می‌کنی</b> رو بنویس:\n"
                            f"(آلارم‌های شخصیت با همین اسم شناسایی میشن)")

                # ── دریافت کوئری جستجوی گزارش هفتگی ──────────────────
                elif cid in _pending_weekly_search and not txt.startswith("/"):
                    ws_info = _pending_weekly_search.pop(cid)
                    ws_which = ws_info["which"]
                    query = txt.strip().lower()

                    now_dt_ws = datetime.now(TEHRAN)
                    days_since_sat_ws = (now_dt_ws.weekday() - 5) % 7
                    this_week_start_ws = (now_dt_ws - timedelta(days=days_since_sat_ws)).replace(
                        hour=0, minute=0, second=0, microsecond=0)
                    if ws_which == "last":
                        week_start_ws = this_week_start_ws - timedelta(days=7)
                        week_end_ws   = this_week_start_ws
                    else:
                        week_start_ws = this_week_start_ws
                        week_end_ws   = None
                    week_start_str_ws = week_start_ws.strftime("%Y-%m-%dT%H:%M:%S")
                    week_label_ws = f"{week_start_ws.strftime('%d/%m')} — {(week_end_ws - timedelta(days=1)).strftime('%d/%m') if week_end_ws else 'الان'}"

                    rows_ws = []
                    if SUPABASE_KEY:
                        try:
                            url_ws = (f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                                      f"?fired_at=gte.{week_start_str_ws}&select=*&order=fired_at.asc")
                            if week_end_ws:
                                url_ws += f"&fired_at=lt.{week_end_ws.strftime('%Y-%m-%dT%H:%M:%S')}"
                            r_ws = requests.get(url_ws, headers=_sb_h(), timeout=10)
                            if r_ws.status_code == 200:
                                rows_ws = r_ws.json()
                        except Exception as e:
                            print(f"[weekly_search] load exc: {e}")

                    _raw_ws = _sb_load_all_alerts()
                    if _raw_ws and isinstance(_raw_ws, dict):
                        all_alerts_ws = _raw_ws.get("alarms", []) + _raw_ws.get("archive", [])
                    else:
                        _fb_ws = load_alerts()
                        all_alerts_ws = _fb_ws.get("alarms", []) + _fb_ws.get("archive", [])
                    alerts_by_id_ws = {str(a["id"]): a for a in all_alerts_ws}
                    private_ids_ws = {str(a["id"]) for a in all_alerts_ws if a.get("is_private")}
                    rows_ws = [r for r in rows_ws if str(r.get("id","")) not in private_ids_ws]

                    # فیلتر بر اساس کوئری — مسئول، نماد، تگ، سازنده
                    filtered = []
                    for row_ws in rows_ws:
                        aid_ws  = str(row_ws.get("id",""))
                        alert_ws = alerts_by_id_ws.get(aid_ws, {})
                        sym_ws  = (alert_ws.get("symbol","") or row_ws.get("symbol","") or "").lower()
                        tag_ws  = (row_ws.get("alarm_tag","") or "").lower()
                        assignee_ws = (row_ws.get("assigned_to","") or "").lower()
                        creator_ws  = (alert_ws.get("created_by","") or row_ws.get("created_by","") or "").lower()
                        if query in sym_ws or query in tag_ws or query in assignee_ws or query in creator_ws:
                            filtered.append(row_ws)

                    if not filtered:
                        send_tg(token, cid, f"🔍 نتیجه‌ای برای «<b>{txt.strip()}</b>» پیدا نشد.")
                    else:
                        lines_ws = [f"🔍 <b>نتایج جستجو: {txt.strip()}</b>",
                                     f"<i>{week_label_ws} — {len(filtered)} نتیجه</i>", ""]
                        for row_ws in filtered[:10]:
                            aid_ws       = str(row_ws.get("id",""))
                            tag_ws       = row_ws.get("alarm_tag","—")
                            assignee_ws  = row_ws.get("assigned_to","") or "—"
                            fired_ws     = row_ws.get("fired_at","")[:16]
                            is_active_ws = row_ws.get("is_active", True)
                            false_by_ws  = row_ws.get("false_by","") or ""
                            false_at_ws  = row_ws.get("false_at","")[:16] if row_ws.get("false_at") else ""
                            false_rsn_ws = row_ws.get("false_reason","") or ""
                            alert_ws     = alerts_by_id_ws.get(aid_ws, {})
                            sym_ws       = alert_ws.get("symbol","") or row_ws.get("symbol","") or ""
                            tgt_raw_ws   = alert_ws.get("target_price",0) or row_ws.get("target_price",0) or 0
                            target_ws    = fmt_price(float(tgt_raw_ws), sym_ws) if tgt_raw_ws else "—"
                            creator_ws   = alert_ws.get("created_by","") or row_ws.get("created_by","") or "—"
                            created_ws   = str(alert_ws.get("created_at",""))[:16]
                            cond_ws      = alert_ws.get("condition","") or row_ws.get("condition","")
                            dir_ws       = "📈 ناحیه سل" if cond_ws == "above" else ("📉 ناحیه بای" if cond_ws == "below" else "")
                            lines_ws.append(f"🔖 <b>{tag_ws}</b>  |  #{sym_ws}" + (f"  |  {dir_ws}" if dir_ws else ""))
                            if created_ws:
                                lines_ws.append(f"📅 ثبت: {created_ws}")
                            lines_ws.append(f"🎯 هدف: <code>{target_ws}</code>")
                            lines_ws.append(f"👤 سازنده: {creator_ws}")
                            lines_ws.append(f"⏰ فایر شد: {fired_ws}")
                            lines_ws.append(f"🙋 مسئول: {assignee_ws}")
                            if is_active_ws:
                                lines_ws.append(f"✅ وضعیت: فعال")
                            else:
                                hist_ws = row_ws.get("false_history") or []
                                if isinstance(hist_ws, str):
                                    try: hist_ws = json.loads(hist_ws)
                                    except: hist_ws = []
                                if hist_ws:
                                    lines_ws.append("❌ <b>تاریخچه False:</b>")
                                    for i_hw, h_ws in enumerate(hist_ws, 1):
                                        h_at_w  = str(h_ws.get("at",""))[:16]
                                        h_by_w  = h_ws.get("by","")
                                        h_rsn_w = h_ws.get("reason","")
                                        h_line_w = f"  {i_hw}. {h_by_w}  |  {h_at_w}"
                                        if h_rsn_w:
                                            h_line_w += f"\n     📝 {h_rsn_w}"
                                        lines_ws.append(h_line_w)
                                else:
                                    lines_ws.append(f"❌ وضعیت: False — {false_by_ws}  |  {false_at_ws}")
                                    if false_rsn_ws:
                                        lines_ws.append(f"📝 علت: {false_rsn_ws}")
                            lines_ws.append("──────────────")
                        if len(filtered) > 10:
                            lines_ws.append(f"<i>... و {len(filtered)-10} مورد دیگر</i>")
                        full_ws = "\n".join(lines_ws)
                        if len(full_ws) > 4000:
                            full_ws = full_ws[:3980] + "\n\n<i>...</i>"
                        send_tg(token, cid, full_ws)

                # ── دریافت اسم custom بعد از /start یا /setname ──────
                elif cid in _pending_name and not txt.startswith("/"):
                    custom_name = txt.strip()
                    if len(custom_name) < 2:
                        send_tg(token, cid, "⚠️ اسم باید حداقل ۲ حرف باشه. دوباره بنویس:")
                    else:
                        data = load_alerts()
                        users = data.get("users", [])
                        found = False
                        for usr in users:
                            if str(usr.get("chat_id", "")) == cid:
                                usr["custom_name"] = custom_name
                                found = True
                                break
                        if not found:
                            users.append({"chat_id": cid, "username": uname, "joined_at": now_teh(), "custom_name": custom_name})
                            data["users"] = users
                            ids = data.get("telegram", {}).get("chat_ids", [])
                            if cid not in [str(x) for x in ids]:
                                ids.append(cid)
                            data["telegram"]["chat_ids"] = ids
                        data["users"] = users
                        save_alerts(data)
                        del _pending_name[cid]
                        is_adm = (cid == YOUR_CHAT_ID)
                        show_main_menu(token, cid,
                            f"✅ خوش اومدی <b>{custom_name}</b>!\n\n"
                            f"آلارم‌هات با اسم <b>{custom_name}</b> شناسایی میشن.\n"
                            f"از منوی زیر انتخاب کن 👇",
                            is_adm)

                # ── /setname — تغییر اسم ────────────────────────────
                elif txt.startswith("/setname"):
                    _pending_name[cid] = True
                    data = load_alerts()
                    users = data.get("users", [])
                    cur = next((u.get("custom_name","") for u in users if str(u.get("chat_id",""))==cid), "")
                    cur_info = f"\nاسم فعلی: <b>{cur}</b>" if cur else ""
                    send_tg(token, cid, f"✏️ اسم جدیدت رو بنویس:{cur_info}")

                # ── /del — حذف پیام fired از همه چت‌ها ────────────
                # فرمت‌ها: ریپلای روی پیام + /del
                #          یا /del XAUUSD7
                elif txt.strip().startswith("/del"):
                    del_parts = txt.strip().split(maxsplit=1)
                    del_tag = del_parts[1].upper().lstrip("#") if len(del_parts) > 1 else None
                    replied = msg.get("reply_to_message", {})
                    replied_mid = replied.get("message_id")

                    target_aid = None

                    if del_tag:
                        # جستجو با هشتگ — مثلاً /del XAUUSD7
                        tag_search = f"#{del_tag}"
                        for aid, cid_map in _fired_msg_ids.items():
                            if cid_map.get("__tag__") == tag_search:
                                target_aid = aid
                                break
                        if not target_aid:
                            send_tg(token, cid, f"⚠️ آلارم <b>{tag_search}</b> پیدا نشد یا قبلاً پاک شده.")
                    elif replied_mid:
                        # جستجو با ریپلای
                        for aid, cid_map in _fired_msg_ids.items():
                            if str(cid_map.get(cid)) == str(replied_mid):
                                target_aid = aid
                                break
                        if not target_aid:
                            send_tg(token, cid, "⚠️ این پیام توی لیست آلارم‌های ذخیره‌شده نیست یا قبلاً پاک شده.")
                    else:
                        send_tg(token, cid, "⚠️ روی پیام آلارم ریپلای بزن یا بنویس /del XAUUSD7")

                    if target_aid:
                        cid_map = _fired_msg_ids.pop(target_aid, {})
                        deleted_count = 0
                        for tc, tm in cid_map.items():
                            if tc in ("__tag__", "__text__"): continue
                            try:
                                r_del = requests.post(
                                    f"https://api.telegram.org/bot{token}/deleteMessage",
                                    json={"chat_id": tc, "message_id": tm},
                                    timeout=5, headers=H)
                                if r_del.status_code == 200:
                                    deleted_count += 1
                            except: pass
                        # حذف پیام fired از Supabase
                        threading.Thread(target=_sb_delete_fired_msgs, args=(target_aid,), daemon=True).start()
                        # حذف خود آلارم از Supabase — تا دیگه fire نشه
                        threading.Thread(target=_sb_delete_alert, args=(target_aid,), daemon=True).start()
                        _cache_alerts = None
                        send_tg(token, cid, f"🗑 پیام آلارم از <b>{deleted_count}</b> چت پاک شد.")

                # ── /false — آلارم منقضی/ترید شده، از لیست تریگر خارج بشه ──
                elif txt.strip().lower() in ("/false",) or txt.strip().lower().startswith("/false"):
                    # کامنت بعد از /False — مثلاً: /False شکسته شد ناحیه
                    # یا /False XAUUSD5 برای جستجو با هشتگ
                    false_parts = txt.strip().split(maxsplit=1)
                    false_arg = false_parts[1].strip() if len(false_parts) > 1 else ""
                    false_reason = ""
                    false_tag_search = None
                    # چک کن اول arg یه هشتگ/تگ نماد هست؟ (مثلاً XAUUSD5 یا #XAUUSD5)
                    if false_arg:
                        candidate = false_arg.upper().lstrip("#").split()[0]
                        # اگه شبیه تگ نماد باشه (حروف + عدد، بدون فاصله)
                        import re as _re2
                        if _re2.match(r'^[A-Z0-9]+$', candidate) and any(c.isdigit() for c in candidate):
                            false_tag_search = f"#{candidate}"
                            # بقیه arg رو reason بدون
                            rest = false_arg[len(candidate):].strip().lstrip("#").strip()
                            false_reason = rest
                        else:
                            false_reason = false_arg
                    false_tag = None
                    replied = msg.get("reply_to_message", {})
                    replied_mid = replied.get("message_id")
                    target_aid_false = None
                    # ۱. جستجو با هشتگ (اگه داده شده)
                    if false_tag_search:
                        for aid_f, cid_map_f in _fired_msg_ids.items():
                            if cid_map_f.get("__tag__") == false_tag_search:
                                target_aid_false = aid_f
                                false_tag = cid_map_f.get("__tag__", "")
                                break
                        if not target_aid_false:
                            send_tg(token, cid, f"⚠️ آلارم <b>{false_tag_search}</b> پیدا نشد یا قبلاً پاک شده.\n\nشاید سرور restart شده — مستقیم روی پیام آلارم ریپلای بزن.")
                    # ۲. جستجو با ریپلای
                    elif replied_mid:
                        for aid_f, cid_map_f in _fired_msg_ids.items():
                            if str(cid_map_f.get(cid)) == str(replied_mid):
                                target_aid_false = aid_f
                                false_tag = cid_map_f.get("__tag__", "")
                                break
                    if target_aid_false:
                        sender_name_false = _get_user_custom_name(cid) or uname

                        # جلوگیری از race condition — اگه الان داره false میشه، ignore کن
                        with _false_in_progress_lock:
                            _fp_busy = target_aid_false in _false_in_progress
                            if not _fp_busy:
                                _false_in_progress.add(target_aid_false)

                        if _fp_busy:
                            send_tg(token, cid, "⏳ این آلارم الان داره پردازش میشه، چند ثانیه صبر کن.")
                        else:
                            try:
                                # چک کن این آلارم قبلاً false شده یا نه (sync — قبل از ارسال پیام)
                                already_false = False
                                if SUPABASE_KEY:
                                    try:
                                        r_chk = requests.get(
                                            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{target_aid_false}&select=is_active",
                                            headers=_sb_h(), timeout=8)
                                        if r_chk.status_code == 200:
                                            chk_rows = r_chk.json()
                                            if chk_rows:
                                                already_false = (chk_rows[0].get("is_active") == False)
                                    except Exception as e:
                                        print(f"[false] check exc: {e}")

                                threading.Thread(
                                    target=_sb_false_assignment,
                                    args=(target_aid_false, sender_name_false, false_reason),
                                    daemon=True).start()
                                threading.Thread(target=lambda: _rebuild_active_assign_count(
                                    _sb_load_active_assignments()), daemon=True).start()
                                tag_txt = f" <b>{false_tag}</b>" if false_tag else ""
                                reason_line = f"\n📝 علت: {false_reason}" if false_reason else ""
                                now_label = now_pretty()

                                false_cid_map = _fired_msg_ids.get(target_aid_false, {})
                                prev_broadcast_map = _false_broadcast_ids.get(target_aid_false, {})
                                if not prev_broadcast_map and already_false and SUPABASE_KEY:
                                    try:
                                        r_bm = requests.get(
                                            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{target_aid_false}&select=false_broadcast_map",
                                            headers=_sb_h(), timeout=8)
                                        if r_bm.status_code == 200:
                                            bm_rows = r_bm.json()
                                            if bm_rows and bm_rows[0].get("false_broadcast_map"):
                                                prev_broadcast_map = bm_rows[0]["false_broadcast_map"]
                                                if isinstance(prev_broadcast_map, str):
                                                    prev_broadcast_map = json.loads(prev_broadcast_map)
                                    except Exception as e:
                                        print(f"[false] load broadcast_map exc: {e}")
                                new_broadcast_map = {}

                                if already_false and prev_broadcast_map:
                                    # ── آپدیت — ریپلای روی پیام False قبلی، متن قبلی حذف نمیشه
                                    update_msg = (
                                        f"🔄 <b>آپدیت</b> — {now_label}\n"
                                        f"👤 توسط: <b>{sender_name_false}</b>"
                                        f"{reason_line}"
                                    )
                                    for tc, tm in false_cid_map.items():
                                        if tc in ("__tag__", "__text__"): continue
                                        reply_target = prev_broadcast_map.get(tc, tm)
                                        try:
                                            r_send = requests.post(
                                                f"https://api.telegram.org/bot{token}/sendMessage",
                                                json={"chat_id": tc, "text": update_msg,
                                                      "parse_mode": "HTML", "reply_to_message_id": reply_target},
                                                timeout=8, headers=H)
                                            if r_send.status_code == 200:
                                                new_mid = r_send.json().get("result", {}).get("message_id")
                                                if new_mid:
                                                    new_broadcast_map[tc] = new_mid
                                        except: pass
                                else:
                                    # ── اولین بار false میشه
                                    false_broadcast = (
                                        f"❌ آلارم{tag_txt} از لیست تریگر خارج شد\n"
                                        f"👤 توسط: <b>{sender_name_false}</b>"
                                        f"{reason_line}"
                                    )
                                    for tc, tm in false_cid_map.items():
                                        if tc in ("__tag__", "__text__"): continue
                                        try:
                                            r_send = requests.post(
                                                f"https://api.telegram.org/bot{token}/sendMessage",
                                                json={"chat_id": tc, "text": false_broadcast,
                                                      "parse_mode": "HTML", "reply_to_message_id": tm},
                                                timeout=8, headers=H)
                                            if r_send.status_code == 200:
                                                new_mid = r_send.json().get("result", {}).get("message_id")
                                                if new_mid:
                                                    new_broadcast_map[tc] = new_mid
                                        except: pass

                                if new_broadcast_map:
                                    _false_broadcast_ids[target_aid_false] = new_broadcast_map
                                    threading.Thread(
                                        target=lambda aid=target_aid_false, m=new_broadcast_map: requests.patch(
                                            f"{SUPABASE_URL}/rest/v1/alarm_assignments?id=eq.{aid}",
                                            headers={**_sb_h(), "Prefer": "return=minimal"},
                                            json={"false_broadcast_map": m}, timeout=8) if SUPABASE_KEY else None,
                                        daemon=True).start()
                            finally:
                                # همیشه lock رو آزاد کن
                                with _false_in_progress_lock:
                                    _false_in_progress.discard(target_aid_false)
                    else:
                        send_tg(token, cid, "⚠️ روی پیام آلارم ریپلای بزن و /False بنویس.\n\n💡 اگه آلارم قبل از restart سرور آمده، با /False XAUUSD5 (هشتگ آلارم) امتحان کن.")

                # ── /check — ثبت در ژورنال روی پیام ─────────────
                elif txt.strip().startswith("/check"):
                    check_parts = txt.strip().split(maxsplit=1)
                    check_note = check_parts[1].strip() if len(check_parts) > 1 else ""
                    replied = msg.get("reply_to_message", {})
                    replied_mid = replied.get("message_id")
                    replied_text = replied.get("text") or replied.get("caption") or ""
                    if not replied_mid:
                        send_tg(token, cid, "⚠️ باید روی پیام آلارم ریپلای بزنی و /check بنویسی.")
                    else:
                        target_aid = None
                        for aid, cid_map in _fired_msg_ids.items():
                            if str(cid_map.get(cid)) == str(replied_mid):
                                target_aid = aid
                                break
                        if not target_aid:
                            send_tg(token, cid, "⚠️ این پیام توی لیست آلارم‌های ذخیره‌شده نیست یا قبلاً ثبت شده.")
                        else:
                            cid_map = _fired_msg_ids.get(target_aid, {})
                            orig_text = cid_map.get("__text__") or replied_text
                            note_line = f"\n🗒 {check_note}" if check_note else ""
                            journal_line = f"\n──────────────\n📋 ثبت شد در ژورنال{note_line}\n🕐 {now_pretty()}"
                            new_text = orig_text + journal_line
                            edited_count = 0
                            for tc, tm in cid_map.items():
                                if tc in ("__tag__", "__text__"): continue
                                try:
                                    r_edit = requests.post(
                                        f"https://api.telegram.org/bot{token}/editMessageText",
                                        json={"chat_id": tc, "message_id": tm,
                                              "text": new_text, "parse_mode": "HTML"},
                                        timeout=8, headers=H)
                                    if r_edit.status_code == 200:
                                        edited_count += 1
                                except: pass
                            if edited_count:
                                send_tg(token, cid, f"✅ در <b>{edited_count}</b> چت ثبت شد.")
                            else:
                                send_tg(token, cid, "⚠️ نشد ویرایش کرد.")
                elif txt in ("📊 وضعیت",) or (txt.startswith("/status") and txt not in ("/statuspage",)):
                    d2 = load_alerts()
                    all_active2 = [a for a in d2.get("alerts",[]) if a.get("active")]
                    team_active2 = [a for a in all_active2 if not a.get("is_private")]
                    private_active2 = [a for a in all_active2 if a.get("is_private")]
                    my_rem = _reminders.get(cid, {})
                    is_open = is_forex_market_open()
                    is_adm = (cid == YOUR_CHAT_ID)
                    has_priv = _has_private_access(cid)
                    status_text = (
                        f"📊 <b>وضعیت سیستم</b>\n\n"
                        f"{'🟢' if is_open else '🔴'} فارکس: {'باز' if is_open else 'بسته'}\n"
                        f"📈 آلارم فعال (کل): <b>{len(all_active2)}</b>\n"
                        f"🌐 تیمی: <b>{len(team_active2)}</b> | 🔒 شخصی: <b>{len(private_active2)}</b>\n"
                        f"⏰ هشدار دوره‌ای من: <b>{len(my_rem)}</b>\n"
                        f"🔒 آلارم شخصی: {'✅ فعال' if has_priv else '❌ غیرفعال'}\n"
                        f"⏱ {now_pretty()} (تهران)"
                    )
                    status_kb = []
                    if not has_priv:
                        status_kb.append([{"text": "📩 درخواست فعال‌سازی آلارم شخصی", "callback_data": f"req_private:{cid}"}])
                    status_kb.append([{"text": "✏️ ویرایش اسم", "callback_data": f"edit_name:{cid}"}])
                    status_kb.append([{"text": "📡 سیگنال‌های من", "callback_data": f"signals_view:{cid}:mine"},
                                      {"text": "📊 همه سیگنال‌ها", "callback_data": f"signals_view:{cid}:all"}])
                    status_kb.append([{"text": "🎯 لیست تریگر", "callback_data": f"trigger_list:{cid}"}])
                    status_kb.append([{"text": "📊 آنالیز هفتگی", "callback_data": f"weekly_menu:{cid}"}])
                    status_kb.append([{"text": "🔔 نمایش آلارم‌های فعال", "callback_data": f"resend_active:{cid}"}])
                    send_tg_keyboard(token, cid, status_text, status_kb)

                elif txt == "⭐ آلارم‌های من":
                    is_adm = (cid == YOUR_CHAT_ID)
                    has_priv = _has_private_access(cid)
                    btns = [{"text": "🌐 آلارم‌های تیمی", "callback_data": f"myalerts:pub:{cid}"}]
                    if has_priv:
                        btns.append({"text": "🔒 آلارم‌های شخصی", "callback_data": f"myalerts:priv:{cid}"})
                    btns = [[b] if isinstance(b, dict) else b for b in btns]
                    btns.append([{"text": "📋 همه آلارم‌های من", "callback_data": f"myalerts:all:{cid}"}])
                    btns.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                    send_tg_keyboard(token, cid, "⭐ <b>آلارم‌های من</b>\n\nکدوم رو می‌خوای ببینی؟", btns)

                elif txt == "⏰ هشدار دوره‌ای من":
                    text_msg2, kb2 = build_cancel_reminder_msg(cid)
                    is_adm = (cid == YOUR_CHAT_ID)
                    # دکمه ثبت هشدار جدید همیشه نشون داده میشه
                    kb2.append([{"text": "➕ هشدار جدید", "callback_data": f"reminder_new:{cid}"}])
                    send_tg_keyboard(token, cid, text_msg2 if kb2 else "هیچ هشدار فعالی نداری.\n\nمیخوای هشدار جدید بذاری؟", kb2)

                elif txt == "📡 سیگنال جدید":
                    # مرحله ۱ — نماد
                    quick_btns = [[{"text": s, "callback_data": f"sig_sym:{cid}:{s}"} for s in SIGNAL_QUICK_SYMBOLS[:3]],
                                  [{"text": s, "callback_data": f"sig_sym:{cid}:{s}"} for s in SIGNAL_QUICK_SYMBOLS[3:]],
                                  [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]]
                    mid_sig = send_tg_keyboard(token, cid,
                        "📡 <b>سیگنال جدید</b>\n\nنماد رو انتخاب کن یا بنویس:",
                        quick_btns)
                    _pending_signal[cid] = {"step": "sig_symbol", "data": {}, "bot_msg_id": mid_sig}

                elif txt == "⚙️ پنل ادمین" and cid == YOUR_CHAT_ID:
                    admin_kb = [
                        [{"text": "📰 اخبار فارکس",       "callback_data": "admin:news"}],
                        [{"text": "✉️ پیام به گروه",      "callback_data": "admin:broadcast"}],
                        [{"text": "👥 لیست کاربران",       "callback_data": "admin:users"}],
                        [{"text": "🗑 مدیریت سیگنال‌ها",  "callback_data": "admin_sig:list:1"}],
                        [{"text": "📋 تعیین شیفت",         "callback_data": "admin:shift:1"}],
                        [{"text": "✕ بستن",                "callback_data": "close_myalerts"}],
                    ]
                    send_tg_keyboard(token, cid,
                        "⚙️ <b>پنل ادمین</b>\n\nیه گزینه انتخاب کن:", admin_kb)


                elif txt == "❌ انصراف":
                    pend_cancel = _pending_alarm.pop(cid, None)
                    if pend_cancel and pend_cancel.get("bot_msg_id"):
                        edit_tg_keyboard(token, cid, pend_cancel["bot_msg_id"],
                            "❌ <b>عملیات لغو شد.</b>", [])

                elif txt == "📈 آلارم جدید" and (cid == YOUR_CHAT_ID or BROADCAST_MODE):
                    kb_new = [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]]
                    mid_new = send_tg_keyboard(token, cid,
                        "🔔 <b>آلارم جدید</b>\n\nاسم نماد رو بنویس:\n<code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                        kb_new)
                    _pending_alarm[cid] = {"step": "alarm_symbol", "data": {"ptype": "public"}, "bot_msg_id": mid_new}

                elif txt == "🔒 آلارم شخصی":
                    if not _has_private_access(cid):
                        is_adm = (cid == YOUR_CHAT_ID)
                        show_main_menu(token, cid, "⚠️ این قابلیت برای شما فعال نیست.\nاز بخش 📊 وضعیت می‌توانید درخواست دسترسی بدهید.", is_adm)
                    else:
                        kb_priv = [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]]
                        mid_priv = send_tg_keyboard(token, cid,
                            "🔒 <b>آلارم شخصی</b>\n\nاسم نماد رو بنویس:\n<code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                            kb_priv)
                        _pending_alarm[cid] = {"step": "alarm_symbol", "data": {"ptype": "private"}, "bot_msg_id": mid_priv}


                elif txt == "⚡ آلارم فوری" and (cid == YOUR_CHAT_ID or BROADCAST_MODE):
                    kb_sos = [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]]
                    mid_sos = send_tg_keyboard(token, cid,
                        "⚡ <b>آلارم فوری</b>\n\nاسم نماد رو بنویس:\n<code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                        kb_sos)
                    _pending_alarm[cid] = {"step": "sos_symbol", "data": {}, "bot_msg_id": mid_sos}

                elif cid in _pending_reminder and not txt.startswith("/"):
                    pr_step = _pending_reminder[cid].get("step")
                    pr_mid  = _pending_reminder[cid].get("bot_msg_id")
                    if pr_step == "rem_symbol":
                        r_sym = txt.upper().replace("/","").strip()
                        if len(r_sym) < 2:
                            edit_tg_keyboard(token, cid, pr_mid,
                                "❌ نماد نامعتبر. دوباره بنویس:", [[{"text":"❌ انصراف","callback_data":"close_myalerts"}]])
                        else:
                            del _pending_reminder[cid]
                            kb_tf = [
                                [{"text": "🕯 M5  (۱ دق قبل کلوز)",  "callback_data": f"reminder_go:{cid}:{r_sym}:300"}],
                                [{"text": "🕯 M15 (۵ دق قبل کلوز)",  "callback_data": f"reminder_go:{cid}:{r_sym}:900"}],
                                [{"text": "🕯 H1  (۱۵ دق قبل کلوز)", "callback_data": f"reminder_go:{cid}:{r_sym}:3600"}],
                                [{"text": "🕯 H4  (۱۵ دق قبل کلوز)", "callback_data": f"reminder_go:{cid}:{r_sym}:14400"}],
                                [{"text": "✕ انصراف", "callback_data": "close_myalerts"}],
                            ]
                            edit_tg_keyboard(token, cid, pr_mid,
                                f"🕯 تایم‌فریم هشدار برای <b>{r_sym}</b>:", kb_tf)

                elif cid in _pending_alarm and not txt.startswith("/"):
                    step = _pending_alarm[cid]["step"]
                    dw   = _pending_alarm[cid]["data"]
                    bot_msg_id = _pending_alarm[cid].get("bot_msg_id")
                    is_adm = (cid == YOUR_CHAT_ID)

                    # ── لغو در هر مرحله ─────────────────────────────────
                    if txt in ("↩️ برگشت", "❌ انصراف"):
                        del _pending_alarm[cid]
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id, "❌ <b>عملیات لغو شد.</b>", [])
                        # پیام کاربر رو ادیت نکن، فقط state رو پاک کردیم

                    elif step == "alarm_symbol":
                        sym_w = txt.upper().replace("/","")
                        if len(sym_w) < 2:
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    "🔔 <b>آلارم جدید</b>\n\n❌ نماد نامعتبر.\nاسم نماد رو بنویس:\n<code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                                    [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]])
                        else:
                            dw["symbol"] = sym_w
                            dw["atype"]  = "forex" if any(x in sym_w for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                            _pending_alarm[cid]["step"] = "alarm_dir"
                            ptype_lbl = "🔒 شخصی" if dw.get("ptype") == "private" else "🌐 تیمی"
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"🔔 <b>{sym_w}</b>  ({ptype_lbl})\n\nجهت معامله رو انتخاب کن:",
                                    [
                                        [{"text": "📈 BUY", "callback_data": f"alarm_dir:{cid}:buy"},
                                         {"text": "📉 SELL", "callback_data": f"alarm_dir:{cid}:sell"}],
                                        [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                    ])

                    elif step == "alarm_dir":
                        # این step از inline callback میاد (alarm_dir:cid:buy|sell)
                        # اگه کاربر text فرستاد، remind کن
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id,
                                f"🔔 <b>{dw.get('symbol','')}</b>\n\nلطفاً از دکمه‌های زیر انتخاب کن:",
                                [
                                    [{"text": "📈 BUY", "callback_data": f"alarm_dir:{cid}:buy"},
                                     {"text": "📉 SELL", "callback_data": f"alarm_dir:{cid}:sell"}],
                                    [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                ])

                    elif step == "alarm_price":
                        try:
                            dw["target_price"] = float(txt.replace(",",""))
                            _pending_alarm[cid]["step"] = "alarm_comment"
                            dir_lbl2 = "📈 BUY" if dw["condition"] == "below" else "📉 SELL"
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"🔔 <b>{dw['symbol']}</b>  {dir_lbl2}  @  <code>{fmt_price(dw['target_price'], dw['symbol'])}</code>\n\n"
                                    f"یادداشت بنویس یا بدون یادداشت ثبت کن:",
                                    [
                                        [{"text": "✅ ثبت بدون یادداشت", "callback_data": f"alarm_submit:{cid}"}],
                                        [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                    ])
                        except ValueError:
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"🔔 <b>{dw.get('symbol','')}</b>\n\n❌ عدد نامعتبر. مثال: <code>1.08500</code> یا <code>2350</code>\n\nدوباره قیمت هدف رو بنویس:",
                                    [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]])

                    elif step == "alarm_comment":
                        comment_w = "" if txt in ("✅ ثبت بدون یادداشت", "✅ ثبت") else txt
                        is_private_w = dw.get("ptype","public") == "private"
                        sender_name_w = _get_user_custom_name(cid) or uname
                        sym_f = dw["symbol"]
                        atype_f = dw["atype"]
                        new_alert_w = {
                            "id": str(int(time.time()*1000)),
                            "symbol": sym_f, "type": atype_f,
                            "target_price": dw["target_price"], "condition": dw["condition"],
                            "comment": comment_w, "created_by": sender_name_w,
                            "active": True, "last_price": None, "last_checked": None,
                            "created_at": now_teh(),
                            "is_private": is_private_w,
                            "private_cid": cid if is_private_w else None,
                            "notify_only": cid if is_private_w else (YOUR_CHAT_ID if not BROADCAST_MODE else None)
                        }
                        d2 = load_alerts()
                        d2["alerts"].append(new_alert_w)
                        _sb_upsert_alert(new_alert_w)
                        _cache_alerts = d2
                        del _pending_alarm[cid]
                        dir_lbl_f = "📈 BUY" if new_alert_w["condition"] == "below" else "📉 SELL"
                        priv_lbl_f = "  🔒 شخصی" if is_private_w else "  🌐 تیمی"
                        confirm_txt_f = (
                            f"✅ <b>آلارم ثبت شد!</b>\n\n"
                            f"💰 <b>{sym_f}</b>  {dir_lbl_f}{priv_lbl_f}\n"
                            f"🎯 هدف: <code>{fmt_price(new_alert_w['target_price'], sym_f)}</code>\n"
                            + (f"💬 {comment_w}\n" if comment_w else "")
                            + f"\n⏰ {now_teh()} (تهران)"
                        )
                        # ادیت پیام اصلی به تأیید نهایی — هیچ پیام جدیدی فرستاده نمیشه
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id, confirm_txt_f, [])
                        def _bgw(alert=new_alert_w, s=sym_f, t=atype_f):
                            try:
                                cur = get_price(s, t)
                                if cur:
                                    alert["last_price"] = cur
                                    alert["last_checked"] = now_teh()
                                    _sb_upsert_alert(alert)
                            except: pass
                        threading.Thread(target=_bgw, daemon=True).start()

                    elif step == "edit_name_input":
                        new_name = txt.strip()
                        if len(new_name) < 2:
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    "✏️ <b>ویرایش اسم</b>\n\n❌ اسم باید حداقل ۲ حرف باشه.\nدوباره بنویس:",
                                    [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]])
                        else:
                            d_en2 = load_alerts()
                            found_en = False
                            for u in d_en2.get("users", []):
                                if str(u.get("chat_id","")) == cid:
                                    u["custom_name"] = new_name
                                    found_en = True
                                    break
                            if not found_en:
                                d_en2.setdefault("users",[]).append({
                                    "chat_id": cid, "username": uname,
                                    "joined_at": now_teh(), "custom_name": new_name
                                })
                            save_alerts(d_en2)
                            del _pending_alarm[cid]
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"✅ <b>اسم با موفقیت ذخیره شد!</b>\n\nاسم جدید: <b>{new_name}</b>", [])
                        sym_w2 = txt.upper().replace("/","")
                        if len(sym_w2) < 2:
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    "⚡ <b>آلارم فوری</b>\n\n❌ نماد نامعتبر.\nمثال: <code>EURUSD</code>  <code>XAUUSD</code>  <code>BTC</code>",
                                    [[{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]])
                        else:
                            dw["symbol"] = sym_w2
                            _pending_alarm[cid]["step"] = "sos_dir"
                            if bot_msg_id:
                                edit_tg_keyboard(token, cid, bot_msg_id,
                                    f"⚡ <b>آلارم فوری</b>  ─  <b>{sym_w2}</b>\n\nجهت معامله رو انتخاب کن:",
                                    [
                                        [{"text": "📈 BUY", "callback_data": f"sos_dir:{cid}:buy"},
                                         {"text": "📉 SELL", "callback_data": f"sos_dir:{cid}:sell"}],
                                        [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                    ])

                    elif step == "sos_dir":
                        # این step دیگه از text نمیاد — از callback میاد (sos_dir:cid:buy/sell)
                        # اگه کاربر text فرستاد، remind کن که از دکمه استفاده کنه
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id,
                                f"⚡ <b>آلارم فوری</b>  ─  <b>{dw.get('symbol','')}</b>\n\nلطفاً از دکمه‌های زیر انتخاب کن:",
                                [
                                    [{"text": "📈 BUY", "callback_data": f"sos_dir:{cid}:buy"},
                                     {"text": "📉 SELL", "callback_data": f"sos_dir:{cid}:sell"}],
                                    [{"text": "❌ انصراف", "callback_data": f"flow_cancel:{cid}"}]
                                ])

                    elif step == "sos_comment":
                        comment_s = "" if txt in ("✅ ارسال بدون یادداشت", "ارسال") else txt
                        sym_s = dw["symbol"]
                        condition_s = dw["condition"]
                        atype_s = "forex" if any(x in sym_s for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                        sender_s = _get_user_custom_name(cid) or uname
                        alarm_num_tag_s = _make_alarm_tag(sym_s)
                        sender_tag_s = "#" + re.sub(r"[^\w]","_", sender_s).strip("_")
                        arrow_s = "📈 ناحیه سل" if condition_s == "above" else "📉 ناحیه بای"
                        dir_lbl_s = dw.get("dir_lbl", "📈 BUY" if condition_s == "below" else "📉 SELL")
                        try: cur_s = get_price(sym_s, atype_s)
                        except: cur_s = None
                        out_s = (
                            f"🚨 <b>آلارم فوری!</b>\n"
                            f"━━━━━━━━━━━━━━━━━━\n"
                            f"💰 <b>#{sym_s}</b>  {arrow_s}\n"
                            f"🔖 {alarm_num_tag_s}\n"
                            f"👤 {sender_tag_s}\n"
                            f"📊 قیمت: <b>{fmt_price(cur_s, sym_s) if cur_s else '—'}</b>\n"
                            + (f"💬 {comment_s}\n" if comment_s else "")
                            + f"━━━━━━━━━━━━━━━━━━\n"
                            f"⏰ {now_pretty()} (تهران)"
                        )
                        _, all_cids2, _ = _get_token_and_cids()
                        targets2 = all_cids2 if BROADCAST_MODE else [YOUR_CHAT_ID]
                        del _pending_alarm[cid]
                        # ادیت پیام اصلی به تأییدیه
                        if bot_msg_id:
                            edit_tg_keyboard(token, cid, bot_msg_id,
                                f"✅ <b>آلارم فوری ارسال شد!</b>\n\n"
                                f"💰 <b>{sym_s}</b>  {dir_lbl_s}\n"
                                + (f"💬 {comment_s}\n" if comment_s else "")
                                + f"⏰ {now_pretty()} (تهران)", [])
                        # broadcast به بقیه
                        for tc2 in targets2:
                            kb2 = [[{"text": "⏰ هشدار دوره‌ای", "callback_data": f"set_reminder:{tc2}:{sym_s}"}]]
                            send_tg_keyboard(token, str(tc2), out_s, kb2, track=False)

                    elif step == "broadcast_text":
                        _, all_cids3, _ = _get_token_and_cids()
                        ok3 = sum(1 for tc3 in all_cids3 if send_tg(token, tc3, txt))
                        del _pending_alarm[cid]
                        show_main_menu(token, cid, f"✅ پیام به {ok3} نفر ارسال شد.", is_adm)

                # ── signal pending text steps ─────────────────────────
                elif cid in _pending_signal and not txt.startswith("/"):
                    ps = _pending_signal[cid]
                    ps_step = ps["step"]
                    ps_data = ps["data"]
                    ps_mid  = ps.get("bot_msg_id")

                    if txt in ("❌ انصراف",):
                        del _pending_signal[cid]
                        if ps_mid:
                            edit_tg_keyboard(token, cid, ps_mid, "❌ <b>ساخت سیگنال لغو شد.</b>", [])

                    elif ps_step == "sig_symbol":
                        # کاربر نماد تایپ کرد
                        sym_s = txt.upper().replace("/","").strip()
                        if len(sym_s) < 2:
                            edit_tg_keyboard(token, cid, ps_mid,
                                "📡 <b>سیگنال جدید</b>\n\n❌ نماد نامعتبر. دوباره بنویس:",
                                [[{"text": s, "callback_data": f"sig_sym:{cid}:{s}"} for s in SIGNAL_QUICK_SYMBOLS[:3]],
                                 [{"text": s, "callback_data": f"sig_sym:{cid}:{s}"} for s in SIGNAL_QUICK_SYMBOLS[3:]],
                                 [{"text": "❌ انصراف", "callback_data": f"sig_cancel:{cid}"}]])
                        else:
                            ps_data["symbol"] = sym_s
                            ps["step"] = "sig_direction"
                            dir_kb = [[{"text": lbl, "callback_data": f"sig_dir:{cid}:{val}"} for lbl,val in SIGNAL_DIRECTIONS[:2]],
                                      [{"text": lbl, "callback_data": f"sig_dir:{cid}:{val}"} for lbl,val in SIGNAL_DIRECTIONS[2:]],
                                      [{"text": "❌ انصراف", "callback_data": f"sig_cancel:{cid}"}]]
                            edit_tg_keyboard(token, cid, ps_mid,
                                f"📡 <b>{sym_s}</b>\n\nنوع سفارش:", dir_kb)

                    elif ps_step == "sig_entry_sl":
                        # کاربر Entry + SL رو نوشته: "73370 72550"
                        parts_es = txt.strip().split()
                        if len(parts_es) < 2:
                            edit_tg_keyboard(token, cid, ps_mid,
                                f"📡 <b>{ps_data.get('symbol')}</b>  {ps_data.get('dir_lbl','')}\n\n"
                                "❌ دو عدد بنویس: <code>Entry  SL</code>\nمثال: <code>73370 72550</code>",
                                [[{"text": "❌ انصراف", "callback_data": f"sig_cancel:{cid}"}]])
                            return
                        try:
                            entry_v = float(parts_es[0].replace(",",""))
                            sl_raw  = float(parts_es[1].replace(",",""))
                        except:
                            edit_tg_keyboard(token, cid, ps_mid,
                                f"📡 <b>{ps_data.get('symbol')}</b>\n\n❌ عدد نامعتبر. دوباره بنویس:",
                                [[{"text": "❌ انصراف", "callback_data": f"sig_cancel:{cid}"}]])
                            return
                        # تشخیص پیپ vs قیمت — از inline button قبلاً تعیین شده
                        sl_mode = ps_data.get("sl_mode", "price")
                        sym_v   = ps_data["symbol"]
                        direction_v = ps_data["direction"]
                        if sl_mode == "pip":
                            sl_v = _sl_from_pips(sym_v, direction_v, entry_v, sl_raw)
                        else:
                            sl_v = sl_raw
                        sl_final, tp1, risk_pips = _calc_signal(sym_v, direction_v, entry_v, sl_v, SIGNAL_DEFAULT_RR)
                        ps_data.update({"entry": entry_v, "sl": sl_final, "tp1": tp1,
                                        "tp2": None, "tp3": None, "risk_pips": risk_pips,
                                        "tf": SIGNAL_DEFAULT_TF, "rr": SIGNAL_DEFAULT_RR})
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token, cid, ps_mid, ps_data)

                    elif ps_step == "sig_tp_edit":
                        # کاربر TP2 یا TP3 تایپ کرد
                        which = ps_data.get("_editing_tp", "tp2")
                        try:
                            val = float(txt.replace(",",""))
                            ps_data[which] = val
                        except:
                            pass
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token, cid, ps_mid, ps_data)

                    elif ps_step == "sig_note":
                        ps_data["note"] = txt.strip()
                        ps["step"] = "sig_preview"
                        _show_signal_preview(token, cid, ps_mid, ps_data)

                # ── /sos ─────────────────────────────────────────────
                elif txt.startswith("/sos") and (cid == YOUR_CHAT_ID or BROADCAST_MODE):
                    parts = txt.split(maxsplit=3)
                    if len(parts) < 2:
                        send_tg(token, cid,
                            "⚠️ فرمت:\n<code>/sos SYMBOL [buy|sell] [کامنت]</code>\n"
                            "مثال:\n<code>/sos GBPUSD sell</code>")
                    else:
                        sym = parts[1].upper().replace("/", "")
                        raw_dir = parts[2].lower() if len(parts) > 2 else "sell"
                        comment = parts[3] if len(parts) > 3 else ""
                        condition = "above" if raw_dir in ("sell","s","سل","above") else "below"
                        atype = "forex" if any(x in sym for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                        sender_name = _get_sender_name(msg)
                        cur = None
                        try: cur = get_price(sym, atype)
                        except: pass
                        arrow = "📈 ناحیه سل" if condition == "above" else "📉 ناحیه بای"
                        cmt = f"\n💬 <i>{comment}</i>" if comment else ""
                        price_text = fmt_price(cur, sym) if cur else "—"
                        alarm_num_tag = _make_alarm_tag(sym)
                        sender_hashtag = "#" + re.sub(r'[^\w]', '_', sender_name).strip('_')
                        out_msg = (
                            f"🚨 <b>آلارم فوری!</b>\n\n"
                            f"💰 <b>#{sym}</b> — {arrow}\n"
                            f"🔖 {alarm_num_tag}\n"
                            f"👤 {sender_hashtag}\n\n"
                            f"📊 قیمت لحظه‌ای: <b>{price_text}</b>"
                            f"{cmt}\n\n⏰ {now_pretty()} (تهران)"
                        )
                        _, all_cids, _ = _get_token_and_cids()
                        targets = all_cids if BROADCAST_MODE else [YOUR_CHAT_ID]
                        sos_aid_txt = f"sos_{sym}_{int(time.time())}"
                        sos_cid_to_mid_txt = {}
                        for tc in targets:
                            mid_sos_txt = send_tg_keyboard(token, tc, out_msg,
                                [[{"text": "⏰ هشدار دوره‌ای", "callback_data": f"set_reminder:{tc}:{sym}"}]],
                                track=False)
                            if mid_sos_txt:
                                sos_cid_to_mid_txt[str(tc)] = mid_sos_txt
                        if sos_cid_to_mid_txt:
                            sos_cid_to_mid_txt["__tag__"] = alarm_num_tag
                            sos_cid_to_mid_txt["__text__"] = out_msg
                            _fired_msg_ids[sos_aid_txt] = sos_cid_to_mid_txt
                            threading.Thread(target=_sb_save_fired_msgs, args=(sos_aid_txt, sos_cid_to_mid_txt), daemon=True).start()
                        d = load_alerts()
                        arch = d.get("archive", [])
                        new_sos_entry = {"id": str(int(time.time()*1000)), "symbol": sym, "type": atype,
                            "condition": condition, "comment": comment, "created_by": sender_name,
                            "active": False, "fired_at": now_teh(), "fired_price": cur,
                            "instant": True, "created_at": now_teh(), "tag": alarm_num_tag}
                        arch.append(new_sos_entry)
                        d["archive"] = arch
                        save_alerts(d)
                        threading.Thread(target=_sb_upsert_alert, args=(new_sos_entry,), daemon=True).start()

                # ── /alarm ───────────────────────────────────────────
                elif txt.startswith("/alarm") and (cid == YOUR_CHAT_ID or BROADCAST_MODE):
                    parts = txt.split(maxsplit=4)
                    if len(parts) < 4:
                        send_tg(token, cid,
                            "⚠️ فرمت:\n<code>/alarm SYMBOL buy|sell PRICE [کامنت]</code>\n\n"
                            "مثال‌ها:\n"
                            "<code>/alarm eurusd sell 1.12345 ناحیه سل</code>\n"
                            "<code>/alarm xauusd sell 2350 مقاومت مهم</code>")
                    else:
                        sym = parts[1].upper().replace("/", "")
                        raw_dir = parts[2].lower()
                        raw_price = parts[3]
                        comment = parts[4] if len(parts) > 4 else ""
                        condition = "above" if raw_dir in ("sell","s","سل","above") else "below"
                        atype = "forex" if any(x in sym for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                        sender_name = _get_sender_name(msg)
                        tgt_f = None
                        try:
                            tgt_f = float(raw_price)
                        except ValueError:
                            send_tg(token, cid, f"❌ قیمت نامعتبر: <code>{raw_price}</code>")
                        if tgt_f is not None:
                            dir_arrow = "📈 BUY" if condition == "below" else "📉 SELL"
                            new_alert = {
                                "id": str(int(time.time()*1000)),
                                "symbol": sym, "type": atype,
                                "target_price": tgt_f, "condition": condition,
                                "comment": comment, "created_by": sender_name,
                                "active": True, "last_price": None,
                                "last_checked": None,
                                "created_at": now_teh(),
                                "notify_only": YOUR_CHAT_ID if not BROADCAST_MODE else None
                            }
                            d = load_alerts()
                            d["alerts"].append(new_alert)
                            _sb_upsert_alert(new_alert)
                            _cache_alerts = d
                            _is_adm_alarm = (cid == YOUR_CHAT_ID)
                            confirm_alarm_txt = (
                                f"✅ آلارم ثبت شد\n\n"
                                f"<b>{sym}</b>  {dir_arrow}  @  <code>{fmt_price(tgt_f, sym)}</code>"
                                + (f"\n💬 {comment}" if comment else "")
                            )
                            show_main_menu(token, cid, confirm_alarm_txt, _is_adm_alarm)
                            def _bg_price(alert=new_alert, s=sym, t=atype, tok=token, c=cid):
                                try:
                                    cur = get_price(s, t)
                                    if cur:
                                        alert["last_price"] = cur
                                        alert["last_checked"] = now_teh()
                                        _sb_upsert_alert(alert)
                                except: pass
                            threading.Thread(target=_bg_price, daemon=True).start()

                # ── /mealarm — آلارم شخصی (فعلاً فقط ادمین) ───
                elif txt.startswith("/mealarm"):
                    if cid != YOUR_CHAT_ID:
                        send_tg(token, cid, "⚠️ این قابلیت فعلاً در دسترس نیست.")
                    else:
                        parts = txt.split(maxsplit=4)
                        if len(parts) < 4:
                            send_tg(token, cid,
                                "⚠️ فرمت:\n<code>/mealarm SYMBOL buy|sell PRICE [کامنت]</code>\n\n"
                                "مثال:\n"
                                "<code>/mealarm xauusd sell 2350 ناحیه شخصی</code>\n\n"
                                "این آلارم فقط برای شما ثبت میشه و بقیه نمیبینن.")
                        else:
                            sym = parts[1].upper().replace("/", "")
                            raw_dir = parts[2].lower()
                            raw_price = parts[3]
                            comment = parts[4] if len(parts) > 4 else ""
                            condition = "above" if raw_dir in ("sell","s","سل","above") else "below"
                            atype = "forex" if any(x in sym for x in ["EUR","GBP","JPY","XAU","XAG","CHF","CAD","AUD","NZD"]) else "crypto"
                            sender_name = _get_sender_name(msg)
                            tgt_f = None
                            try:
                                tgt_f = float(raw_price)
                            except ValueError:
                                send_tg(token, cid, f"❌ قیمت نامعتبر: <code>{raw_price}</code>")
                            if tgt_f is not None:
                                arrow = "سل 📈" if condition == "above" else "بای 📉"
                                new_alert = {
                                    "id": str(int(time.time()*1000)),
                                    "symbol": sym, "type": atype,
                                    "target_price": tgt_f, "condition": condition,
                                    "comment": comment, "created_by": sender_name,
                                    "active": True, "last_price": None,
                                    "last_checked": None,
                                    "created_at": now_teh(),
                                    "notify_only": cid,
                                    "private_cid": cid,
                                    "is_private": True
                                }
                                d = load_alerts()
                                d["alerts"].append(new_alert)
                                _sb_upsert_alert(new_alert)
                                _cache_alerts = d
                                _is_adm_me = (cid == YOUR_CHAT_ID)
                                _me_dir = "📈 BUY" if condition == "below" else "📉 SELL"
                                _me_confirm = (
                                    f"✅ آلارم شخصی ثبت شد 🔒\n\n"
                                    f"<b>{sym}</b>  {_me_dir}  @  <code>{fmt_price(tgt_f, sym)}</code>"
                                    + (f"\n💬 {comment}" if comment else "")
                                )
                                show_main_menu(token, cid, _me_confirm, _is_adm_me)
                                def _bg_price_me(alert=new_alert, s=sym, t=atype):
                                    try:
                                        cur = get_price(s, t)
                                        if cur:
                                            alert["last_price"] = cur
                                            alert["last_checked"] = now_teh()
                                            _sb_upsert_alert(alert)
                                    except: pass
                                threading.Thread(target=_bg_price_me, daemon=True).start()
                # ── /news ────────────────────────────────────────────
                elif txt.startswith("/cancel_reminder"):
                    text_msg, keyboard = build_cancel_reminder_msg(cid)
                    if keyboard:
                        send_tg_keyboard(token, cid, text_msg, keyboard)
                    else:
                        send_tg(token, cid, text_msg)

                elif txt.startswith("/myalerts"):
                    btns2 = [[{"text": "🌐 آلارم\u200cهای تیمی", "callback_data": f"myalerts:pub:{cid}"}]]
                    if _has_private_access(cid):
                        btns2.append([{"text": "🔒 آلارم\u200cهای شخصی", "callback_data": f"myalerts:priv:{cid}"}])
                    btns2.append([{"text": "📋 همه آلارم\u200cهای من", "callback_data": f"myalerts:all:{cid}"}])
                    btns2.append([{"text": "✕ بستن", "callback_data": "close_myalerts"}])
                    send_tg_keyboard(token, cid, "⭐ <b>آلارم\u200cهای من</b>\n\nکدوم رو می\u200cخوای ببینی؟", btns2)

                elif txt.startswith("/news") and cid == YOUR_CHAT_ID:
                    send_tg(token, cid, "⏳ در حال دریافت تقویم اقتصادی...")
                    events, err = fetch_ff_news()
                    if err and not events:
                        send_tg(token, cid, err)
                    else:
                        msg = format_ff_message(events) if events else "📭 امروز رویداد مهم فارکس نداریم."
                        send_tg(token, cid, msg)

                # ── /text ────────────────────────────────────────────
                elif txt.startswith("/text") and cid == YOUR_CHAT_ID:
                    body_text = txt[5:].strip()
                    if not body_text:
                        send_tg(token, cid, "\u26a0\ufe0f \u0641\u0631\u0645\u062a:\n<code>/text \u0645\u062a\u0646 \u067e\u06cc\u0627\u0645\u062a \u0627\u06cc\u0646\u062c\u0627</code>")
                    else:
                        _, all_cids, _ = _get_token_and_cids()
                        if not all_cids:
                            send_tg(token, cid, "\u274c \u0647\u06cc\u0686 \u06a9\u0627\u0631\u0628\u0631\u06cc \u062b\u0628\u062a \u0646\u0634\u062f\u0647")
                        else:
                            ok_count = 0
                            for tc in all_cids:
                                r = send_tg(token, tc, body_text)
                                if r: ok_count += 1
                            send_tg(token, cid, f"\u2705 \u067e\u06cc\u0627\u0645 \u0628\u0647 {ok_count} \u0646\u0641\u0631 \u0627\u0631\u0633\u0627\u0644 \u0634\u062f")

  except Exception as e:
    print(f"[do_update] {e}")

notified = set()
_deleted_ids: set = set()  # آلارم‌هایی که پاک شدن — دیگه fire نشن
_loop_count = 0

_price_fetch_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="price_fetch")
PRICE_FETCH_TIMEOUT = 15  # ثانیه — اگه یه API بیشتر از این طول کشید، skip میشه

def _fetch_with_timeout(fn, *args, timeout=PRICE_FETCH_TIMEOUT):
    """یه تابع price رو با timeout مستقل اجرا کن"""
    future = _price_fetch_executor.submit(fn, *args)
    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError:
        print(f"[check] ⚠️ price fetch timeout ({timeout}s): {args}")
        future.cancel()
        return None
    except Exception as e:
        print(f"[check] price fetch error: {e}")
        return None

def check_alerts():
    global _loop_count
    while True:
        try:
            _loop_count += 1
            global _cache_alerts
            _cache_alerts = None
            token, cids, data = _get_token_and_cids()
            active = [a for a in data.get("alerts", []) if a.get("active") and str(a["id"]) not in _deleted_ids and not a.get("fired_at")]
            if not active:
                save_alerts(data)
                time.sleep(60)
                continue
            forex_open = is_forex_market_open()
            due_forex, due_crypto = [], []
            for a in active:
                sym = a["symbol"]
                atype = a.get("type", "crypto")
                if atype == "forex" and not forex_open:
                    continue
                if atype == "forex":
                    due_forex.append(sym)
                else:
                    due_crypto.append(sym)

            price_map = {}

            # forex: یه batch call با timeout
            if due_forex:
                batch = _fetch_with_timeout(get_forex_prices_batch, due_forex)
                if batch:
                    for sym, p in batch.items():
                        price_map[(sym, "forex")] = p

            # crypto: همزمان، هر کدوم timeout مستقل
            if due_crypto:
                unique_crypto = list(dict.fromkeys(s.upper() for s in due_crypto))
                futures_map = {sym: _price_fetch_executor.submit(get_crypto_price, sym) for sym in unique_crypto}
                for sym, fut in futures_map.items():
                    try:
                        p = fut.result(timeout=PRICE_FETCH_TIMEOUT)
                        price_map[(sym, "crypto")] = p
                    except FuturesTimeoutError:
                        print(f"[check] ⚠️ crypto timeout: {sym}")
                        fut.cancel()
                    except Exception as e:
                        print(f"[check] crypto error {sym}: {e}")

            print(f"[check] loop={_loop_count} forex_open={forex_open} due_f={len(due_forex)} due_c={len(due_crypto)} prices={len(price_map)}")
            fired = []
            now = now_teh()
            for a in active:
                sym = a["symbol"]
                atype = a.get("type", "crypto")
                key = (sym.upper(), atype)
                if key not in price_map: continue
                cur = price_map[key]
                if cur is None: continue
                tgt = float(a["target_price"])
                cond = a.get("condition", "above")
                # ✅ آپدیت قیمت لحظه‌ای برای همه آلارم‌ها
                a["last_price"] = cur
                a["last_checked"] = now
                stale_ts = price_map.get((sym.upper(), atype, "stale"))
                a["price_stale"] = stale_ts if stale_ts else None
                data["last_update"] = now
                triggered = (cond == "above" and cur >= tgt) or (cond == "below" and cur <= tgt)
                print(f"[check] {sym} cur={fmt_price(cur,sym)} tgt={fmt_price(tgt,sym)} cond={cond} → {'🔥 FIRE' if triggered else 'ok'}")
                if triggered and a["id"] not in notified and str(a["id"]) not in _deleted_ids:
                    notified.add(a["id"])
                    _deleted_ids.add(str(a["id"]))  # ← فوری blacklist — جلوگیری از double-fire
                    a["active"] = False
                    a["fired_at"] = now
                    a["fired_price"] = cur
                    fired.append(a["id"])
                    if token and cids:
                        comment = a.get("comment", "")
                        if str(YOUR_CHAT_ID) in str(comment):
                            notify_cids = [str(YOUR_CHAT_ID)]
                            print(f"[FILTER] comment contains {YOUR_CHAT_ID} → only to you")
                        elif a.get("notify_only"):
                            notify_cids = [str(a["notify_only"])]
                            print(f"[FILTER] notify_only → {a['notify_only']}")
                        else:
                            notify_cids = cids
                            print(f"[FILTER] broadcast → {len(cids)} users")
                        arrow = "📈 ناحیه سل" if cond == "above" else "📉 ناحیه بای"
                        creator = a.get("created_by") or "سیستم"
                        cmt = f"\n💬 <i>{comment}</i>" if comment else ""
                        dist = calc_dist_str(sym, atype, cur, tgt)
                        private_label = "\n\n🔒 <i>آلارم شخصی — فقط برای شما ارسال شده</i>" if a.get("is_private") else ""
                        alarm_num_tag = _make_alarm_tag(sym)
                        creator_tag = "#" + re.sub(r'[^\w]', '_', creator).strip('_')
                        # ── تعیین مسئول تریگر ──────────────────────────
                        _assignee, _shift = _get_assignee_for_alarm(
                            a["id"], alarm_num_tag, now,
                            symbol=sym, target_price=float(tgt), created_by=creator
                        )
                        if _assignee:
                            assignee_line = f"\n\n🎯 مسئول تریگر: <b>{_assignee}</b>"
                        else:
                            assignee_line = ""
                        created_at_raw = a.get("created_at", "")
                        created_label = f" | 📅 ثبت: <i>{created_at_raw[:16]}</i>" if created_at_raw else ""
                        fired_msg = (
                            f"🚨 <b>آلارم قیمت!</b>\n\n"
                            f"💰 <b>#{sym}</b> — {arrow}\n"
                            f"🔖 {alarm_num_tag}\n"
                            f"👤 {creator_tag}\n\n"
                            f"🎯 هدف: <code>{fmt_price(tgt,sym)}</code>\n"
                            f"📊 قیمت لحظه‌ای: <b>{fmt_price(cur,sym)}</b>\n"
                            f"📏 فاصله: <b>{dist}</b>"
                            f"{cmt}"
                            f"{private_label}"
                            f"{assignee_line}\n\n⏰ {now_pretty()} (تهران){created_label}"
                        )
                        # دکمه هشدار دوره‌ای برای همه — چه شخصی چه عمومی
                        reminder_kb = lambda cid: [[{"text": "⏰ هشدار دوره‌ای", "callback_data": f"set_reminder:{cid}:{sym}"}]]
                        fired_cid_to_mid = {}
                        if a.get("is_private") and a.get("notify_only"):
                            priv_cid = str(a["notify_only"])
                            mid_f = send_tg_keyboard(token, priv_cid, fired_msg, reminder_kb(priv_cid), track=False)
                            if mid_f:
                                fired_cid_to_mid[priv_cid] = mid_f
                        else:
                            for cid in notify_cids:
                                mid_f = send_tg_keyboard(token, str(cid), fired_msg, reminder_kb(str(cid)), track=False)
                                if mid_f:
                                    fired_cid_to_mid[str(cid)] = mid_f
                        # ذخیره map چت→پیام برای /del
                        if fired_cid_to_mid:
                            fired_cid_to_mid["__tag__"] = alarm_num_tag
                            fired_cid_to_mid["__text__"] = fired_msg
                            _fired_msg_ids[a["id"]] = fired_cid_to_mid
                            threading.Thread(target=_sb_save_fired_msgs, args=(a["id"], fired_cid_to_mid), daemon=True).start()
                        # ذخیره tag روی آلارم
                        a["tag"] = alarm_num_tag
                        # فوری توی Supabase آپدیت کن
                        save_alert_fired(a)
            if fired:
                arch = data.get("archive", [])
                for fid in fired:
                    obj = next((x for x in data["alerts"] if x["id"] == fid), None)
                    if obj: arch.append(obj)
                data["archive"] = arch
                data["alerts"] = [x for x in data["alerts"] if x["id"] not in fired]
            save_alerts(data)
        except Exception as e:
            log_error(f"check_alerts: {e}")
        time.sleep(60)

def fmt_price(p, sym=""):
    if p is None: return "—"
    v = float(p)
    su = sym.upper()
    if "XAU" in su or "XAG" in su:
        return f"${v:.2f}"
    if "JPY" in su:
        return f"{v:.3f}"
    return f"{v:.5f}"

def calc_dist_str(symbol, atype, cur, tgt):
    if not cur or not tgt: return ""
    diff = abs(float(cur) - float(tgt))
    sym_up = symbol.upper()
    if atype == "crypto": return f"{diff/float(tgt)*100:.2f}%"
    if "XAU" in sym_up or "XAG" in sym_up: return f"{diff:.2f} $"
    if "JPY" in sym_up: return f"{round(diff*100):,} pip"
    return f"{round(diff*10000):,} pip"

def tehran_to_utc(tehran_str):
    try:
        parts = tehran_str.strip().split(" ")
        dparts = parts[0].split("-")
        tparts = (parts[1] if len(parts) > 1 else "00:00").split(":")
        y, m, d = int(dparts[0]), int(dparts[1]), int(dparts[2])
        h, mi = int(tparts[0]), int(tparts[1])
        dt_teh = datetime(y, m, d, h, mi)
        return dt_teh - timedelta(hours=3, minutes=30)
    except: return None

# ====================================================================
# تابع اصلی بررسی کندل‌ها – اصلاح شده برای snapshot تا SL یا 3R
# ====================================================================
def check_sltp_hit_with_details(symbol, tf, entry_time_str, direction, entry_price, sl_price, tp_price, size=1.0, max_post_sl_pips=300, r3_override=None, from_time_str=None):
    """
    بازگشت: (hit, hit_price, last_close, pnl, mfe_pip, mae_pip, candle_lines, found_3r,
             free_risk_was_possible, free_risk_saved, reached_1r_at, pullback_after_1r,
             mfe_before_sl, passed_1r, snapshot_bars)
    * hit: 'sl' / 'tp' / 'tp3' (اولین رویداد)
    * snapshot_bars: کندل‌ها تا آخرین برخورد با SL یا 3R (حتی اگر TP زودتر خورده باشد)
    """
    try:
        # محاسبه limit هوشمند بر اساس فاصله زمانی واقعی
        tf_minutes = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
        tf_min = tf_minutes.get(tf, 60)
        if from_time_str:
            try:
                from_utc = tehran_to_utc(from_time_str)
                elapsed_minutes = (datetime.utcnow() - from_utc).total_seconds() / 60
                # تعداد کندل‌های تشکیل‌شده + ۳ بافر اضافه
                bar_limit = max(5, int(elapsed_minutes / tf_min) + 3)
            except:
                bar_limit = 20  # fallback امن
        else:
            tf_limits = {"1m": 5000, "5m": 3000, "15m": 800, "1h": 200, "4h": 50, "1d": 30}
            bar_limit = tf_limits.get(tf, 200)
        print(f"[CANDLE] {symbol} tf={tf} limit={bar_limit} from={'incremental' if from_time_str else 'full'}")
        url = f"https://biquote.io/api/{symbol}/ohlc?interval={tf}&limit={bar_limit}"
        r = requests.get(url, timeout=12, headers=H)
        if r.status_code != 200:
            return (None, None, None, None, None, None, None, False, False, False, None, False, None, None, None, None, None, 0.0, False, [])
        data = r.json()
        bars = data.get("bars") or data.get("data") or (data if isinstance(data, list) else [])
        if not bars:
            return (None, None, None, None, None, None, None, False, False, False, None, False, None, None, None, None, None, 0.0, False, [])

        entry_utc = tehran_to_utc(entry_time_str)
        if not entry_utc:
            return (None, None, None, None, None, None, None, False, False, False, None, False, None, None, None, None, None, 0.0, False, [])

        all_bars_sorted = []
        for b in bars:
            ts = b.get("openTime") or b.get("time") or b.get("timestamp")
            if not ts: continue
            if isinstance(ts, (int, float)):
                bdt = datetime.utcfromtimestamp(ts)
            else:
                try:
                    bdt = datetime.strptime(ts.replace("Z",""), "%Y-%m-%dT%H:%M:%S")
                except: continue
            all_bars_sorted.append((bdt, b))
        all_bars_sorted.sort(key=lambda x: x[0])

        entry_bar_idx = 0
        for i, (bdt, b) in enumerate(all_bars_sorted):
            if bdt >= entry_utc:
                entry_bar_idx = i
                break

        # اگه from_time داریم، فقط کندل‌های جدید (بعد از آخرین چک) رو بررسی کن
        if from_time_str:
            from_utc = tehran_to_utc(from_time_str)
            if from_utc:
                from_idx = entry_bar_idx
                for i, (bdt, b) in enumerate(all_bars_sorted):
                    if bdt >= from_utc:
                        from_idx = i
                        break
                after = all_bars_sorted[from_idx:]
            else:
                after = all_bars_sorted[entry_bar_idx:]
        else:
            after = all_bars_sorted[entry_bar_idx:]
        if not after:
            return (None, None, None, None, None, None, None, False, False, False, None, False, None, None, None, None, None, 0.0, False, [])

        is_buy = (direction == "BUY")
        hit = None
        hit_price = None
        hit_idx = None
        hit_time = None
        mul = get_pip_multiplier(symbol)
        mfe_pip = 0.0
        mae_pip = 0.0
        candle_lines = []
        found_3r = False
        risk_pips = None
        if sl_price and entry_price:
            # abs تضمین می‌کنه risk_pips همیشه مثبته حتی اگه کاربر SL اشتباه وارد کرده
            if is_buy:
                risk_pips = abs(entry_price - sl_price) * mul
            else:
                risk_pips = abs(sl_price - entry_price) * mul

        passed_1r = False
        reached_1r_at = None
        free_risk_was_possible = False
        free_risk_saved = False
        pullback_after_1r = False
        mae_stopped = False

        sl_hit_occurred = False
        mfe_before_sl = 0.0

        stop_price = None
        if sl_price and max_post_sl_pips > 0:
            if is_buy:
                stop_price = sl_price + (max_post_sl_pips / mul)
            else:
                stop_price = sl_price - (max_post_sl_pips / mul)

        # محاسبه قیمت 3R برای تعیین پایان snapshot
        _r3_price = r3_override if r3_override else (
            ((entry_price + 3*risk_pips/mul) if is_buy else (entry_price - 3*risk_pips/mul)) if risk_pips else None
        )

        # متغیرهای مربوط به snapshot: snap_end_idx تا جایی که SL یا 3R برخورد کند
        snap_end_idx = len(after) - 1  # پیش‌فرض آخرین کندل
        snap_resolved = False

        for i, (bar_dt_i, b) in enumerate(after):
            high = float(b.get("high", 0))
            low  = float(b.get("low",  0))
            close= float(b.get("close",0))
            open_= float(b.get("open", 0))

            if is_buy:
                profit_now = (high - entry_price) * mul
                if not mae_stopped and low < entry_price:
                    d = (entry_price - low) * mul
                    if d > mae_pip: mae_pip = d
            else:
                profit_now = (entry_price - low) * mul
                if not mae_stopped and high > entry_price:
                    d = (high - entry_price) * mul
                    if d > mae_pip: mae_pip = d

            mfe_pip = max(mfe_pip, profit_now)
            if risk_pips and mfe_pip > risk_pips * 3.0:
                mfe_pip = risk_pips * 3.0  # cap 3R

            if hit is None:
                mfe_before_sl = max(mfe_before_sl, profit_now)

            if risk_pips and not passed_1r and profit_now >= risk_pips:
                passed_1r = True
                reached_1r_at = i
                mae_stopped = True

            if passed_1r and reached_1r_at is not None and i > reached_1r_at:
                if not free_risk_was_possible:
                    free_risk_was_possible = True
                if not pullback_after_1r:
                    if is_buy and low <= entry_price:
                        pullback_after_1r = True
                    elif not is_buy and high >= entry_price:
                        pullback_after_1r = True

            dt_teh = bar_dt_i + timedelta(hours=3, minutes=30)
            thr = dt_teh.strftime("%m/%d %H:%M")
            dir_c = "▲" if close >= open_ else "▼"
            body_p = abs(close - open_) * mul
            candle_lines.append(f"{thr}: {dir_c} {body_p:.1f}pip | H:{high:.5f} L:{low:.5f} C:{close:.5f}")

            # ===== تعیین اولین رویداد (hit) برای بستن ترید =====
            if hit is None:
                if is_buy:
                    if sl_price is not None and low <= sl_price:
                        hit, hit_price, hit_idx = "sl", sl_price, i
                        hit_time = (bar_dt_i + timedelta(hours=3, minutes=30)).strftime("%Y-%m-%d %H:%M")
                        sl_hit_occurred = True
                    elif tp_price is not None and high >= tp_price:
                        hit, hit_price, hit_idx = "tp", tp_price, i
                        hit_time = (bar_dt_i + timedelta(hours=3, minutes=30)).strftime("%Y-%m-%d %H:%M")
                    elif _r3_price and high >= _r3_price:
                        hit, hit_price, hit_idx = "tp3", _r3_price, i
                        hit_time = (bar_dt_i + timedelta(hours=3, minutes=30)).strftime("%Y-%m-%d %H:%M")
                        found_3r = True
                else:
                    if sl_price is not None and high >= sl_price:
                        hit, hit_price, hit_idx = "sl", sl_price, i
                        hit_time = (bar_dt_i + timedelta(hours=3, minutes=30)).strftime("%Y-%m-%d %H:%M")
                        sl_hit_occurred = True
                    elif tp_price is not None and low <= tp_price:
                        hit, hit_price, hit_idx = "tp", tp_price, i
                        hit_time = (bar_dt_i + timedelta(hours=3, minutes=30)).strftime("%Y-%m-%d %H:%M")
                    elif _r3_price and low <= _r3_price:
                        hit, hit_price, hit_idx = "tp3", _r3_price, i
                        hit_time = (bar_dt_i + timedelta(hours=3, minutes=30)).strftime("%Y-%m-%d %H:%M")
                        found_3r = True

            # ===== تعیین پایان snapshot (فقط SL یا 3R) =====
            if not snap_resolved:
                if is_buy:
                    if sl_price is not None and low <= sl_price:
                        snap_end_idx = i
                        snap_resolved = True
                    elif _r3_price and high >= _r3_price:
                        snap_end_idx = i
                        snap_resolved = True
                else:
                    if sl_price is not None and high >= sl_price:
                        snap_end_idx = i
                        snap_resolved = True
                    elif _r3_price and low <= _r3_price:
                        snap_end_idx = i
                        snap_resolved = True
                if not snap_resolved:
                    snap_end_idx = i  # هنوز نرسیده، آپدیت کن

            # برگشت بعد SL — دستی توسط کاربر وارد میشه، محاسبه خودکار نداریم

            # stop بعد از SL (max_post_sl_pips)
            if sl_hit_occurred and stop_price is not None:
                if is_buy and high >= stop_price:
                    snap_end_idx = i
                    snap_resolved = True
                if not is_buy and low <= stop_price:
                    snap_end_idx = i
                    snap_resolved = True

            # اگه TP/3R یا SL خورد تموم
            if snap_resolved and hit is not None:
                break

        last_close = float(after[-1][1]["close"]) if not found_3r else (after[-1][1]["close"] if after else 0)
        pnl = None
        if hit:
            diff = (hit_price - entry_price) if is_buy else (entry_price - hit_price)
            pnl = diff * size

        # free_risk_saved = تصمیم کاربر است، خودکار set نمی‌شه
        # (کاربر در review_trade این را تأیید می‌کند)
        free_risk_saved = False

        # ساخت snapshot بر اساس snap_end_idx
        snap_start = max(0, entry_bar_idx - 20)
        snap_end = entry_bar_idx + snap_end_idx + 1
        snapshot_bars = []
        for bar_dt_snap, b_snap in all_bars_sorted[snap_start:snap_end]:
            dt_teh_snap = bar_dt_snap + timedelta(hours=3, minutes=30)
            snapshot_bars.append({
                "t": dt_teh_snap.strftime("%Y-%m-%d %H:%M"),
                "o": float(b_snap.get("open", 0)),
                "h": float(b_snap.get("high", 0)),
                "l": float(b_snap.get("low", 0)),
                "c": float(b_snap.get("close", 0)),
            })

        return (hit, hit_price, hit_time, last_close, pnl, mfe_pip, mae_pip, candle_lines, found_3r,
                free_risk_was_possible, free_risk_saved, reached_1r_at, pullback_after_1r,
                None, None, None, None, None,
                mfe_before_sl, passed_1r, snapshot_bars)
    except Exception as e:
        log_error(f"check_sltp_hit_with_details: {e}")
        return (None, None, None, None, None, None, None, False, False, False, None, False, None, None, None, None, None, 0.0, False, [])

def groq_analyze(prompt):
    if not GROQ_API_KEY:
        return "⚠️ کلید API Groq تنظیم نشده است."
    try:
        client = Groq(api_key=GROQ_API_KEY)
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3, max_tokens=800
        )
        return completion.choices[0].message.content
    except Exception as e:
        log_error(f"Groq error: {e}")
        return f"❌ خطا: {str(e)}"

# ==================== Routes ====================
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/journal")
def journal():
    return send_from_directory("static", "journal.html")

@app.route("/api/config", methods=["GET","POST"])
def config():
    data = load_alerts()
    if request.method == "POST":
        body = request.json or {}
        tg = data.get("telegram", {})
        if body.get("bot_token"):
            tg["bot_token"] = body["bot_token"]
        if body.get("chat_id"):
            cid = str(body["chat_id"])
            ids = [str(x) for x in tg.get("chat_ids", [])]
            if cid not in ids: ids.append(cid)
            tg["chat_ids"] = ids
            tg["chat_id"] = cid
        data["telegram"] = tg
        save_alerts(data)
        return jsonify({"ok": True})
    tg = data.get("telegram", {})
    return jsonify({
        "bot_token": tg.get("bot_token",""), "chat_id": tg.get("chat_id",""),
        "chat_ids": tg.get("chat_ids",[]), "user_count": len(data.get("users",[]))
    })

@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    all_alerts = load_alerts().get("alerts", [])
    # آلارم‌های شخصی (is_private=True) رو از لیست عمومی حذف کن
    public = [a for a in all_alerts if not a.get("is_private")]
    return jsonify(public)

@app.route("/api/alerts/my", methods=["GET"])
def get_my_alerts():
    """آلارم‌های شخصی یه کاربر — با name یا cid فیلتر میشه"""
    name = request.args.get("name", "").strip()
    cid  = request.args.get("cid",  "").strip()
    if not name and not cid:
        return jsonify([])
    data = load_alerts()
    all_alerts = data.get("alerts", [])
    # اگه name داریم، chat_id متناظر رو از users پیدا کن
    resolved_cid = cid
    if name and not resolved_cid:
        for usr in data.get("users", []):
            if usr.get("custom_name", "").strip() == name:
                resolved_cid = str(usr.get("chat_id", ""))
                break
    my = [
        a for a in all_alerts
        if a.get("is_private") and (
            (resolved_cid and str(a.get("private_cid", "")) == resolved_cid) or
            (name and a.get("created_by", "").strip() == name)
        )
    ]
    return jsonify(my)

@app.route("/api/alerts", methods=["POST"])
def add_alert():
    data = load_alerts()
    body = request.json or {}
    sym = body.get("symbol","").upper().strip()
    atype = body.get("type","forex")
    tgt = float(body.get("target_price", 0))
    cur = get_price(sym, atype) if (atype!="forex" or is_forex_market_open()) else None
    creator = body.get("creator", "").strip()
    a = {
        "id": str(int(time.time() * 1000)), "symbol": sym, "type": atype,
        "target_price": tgt, "condition": body.get("condition","above"),
        "comment": body.get("comment","").strip(), "active": True,
        "created_by": creator or "ناشناس",
        "notify_only": None,
        "last_price": cur, "last_checked": now_teh() if cur else None,
        "created_at": now_teh()
    }
    data["alerts"].append(a)
    save_alerts(data)
    return jsonify({"ok": True, "alert": a})

@app.route("/api/alerts/<aid>", methods=["DELETE"])
def del_alert(aid):
    data = load_alerts()
    data["alerts"] = [a for a in data.get("alerts", []) if a["id"] != aid]
    global _cache_alerts
    _cache_alerts = data
    threading.Thread(target=_sb_delete_alert, args=(aid,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/archive", methods=["GET"])
def get_archive():
    """مستقیم از Supabase بخون — بدون cache"""
    global _cache_alerts
    if not SUPABASE_KEY:
        _cache_alerts = None
        return jsonify(load_alerts().get("archive", []))
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts",
            headers=_sb_h(),
            params={"status": "eq.fired", "order": "fired_at.desc", "limit": "500"},
            timeout=10
        )
        rows = r.json() if r.status_code == 200 else []
        archive = []
        for row in rows:
            archive.append({
                "id":          row.get("id"),
                "symbol":      row.get("symbol"),
                "condition":   row.get("condition"),
                "target_price":row.get("target_price"),
                "fired_price": row.get("fired_price"),
                "fired_at":    row.get("fired_at"),
                "created_at":  row.get("created_at"),
                "comment":     row.get("comment",""),
                "created_by":  row.get("created_by",""),
                "active":      False,
            })
        return jsonify(archive)
    except Exception as e:
        print(f"[archive] direct fetch error: {e}")
        return jsonify(load_alerts().get("archive", []))

@app.route("/api/archive", methods=["DELETE"])
def clear_archive():
    data = load_alerts()
    data["archive"] = []
    save_alerts(data)
    return jsonify({"ok": True})

@app.route("/api/archive/<aid>", methods=["DELETE"])
def del_archive(aid):
    data = load_alerts()
    data["archive"] = [a for a in data.get("archive",[]) if a["id"] != aid]
    save_alerts(data)
    return jsonify({"ok": True})

@app.route("/api/users", methods=["GET"])
def get_users():
    return jsonify(load_alerts().get("users", []))

@app.route("/api/users/<cid>", methods=["DELETE"])
def del_user(cid):
    data = load_alerts()
    data["users"] = [u for u in data.get("users",[]) if str(u["chat_id"]) != str(cid)]
    data["telegram"]["chat_ids"] = [x for x in data["telegram"].get("chat_ids",[]) if str(x) != str(cid)]
    save_alerts(data)
    return jsonify({"ok": True})

@app.route("/api/price/<atype>/<symbol>")
def live_price(atype, symbol):
    sym = symbol.upper().replace("-","/")
    p = get_price(sym, atype)
    if p is None:
        return jsonify({"error": "قیمت پیدا نشد"}), 404
    return jsonify({"symbol": sym, "price": p})

@app.route("/api/instant-alert", methods=["POST"])
def instant_alert():
    body = request.json or {}
    sym = body.get("symbol", "").upper().strip()
    if not sym:
        return jsonify({"ok": False, "error": "نماد وارد نشده"}), 400
    atype = body.get("type", "forex")
    condition = body.get("condition", "above")
    comment = body.get("comment", "").strip()
    creator = body.get("creator", "").strip()
    target_price = body.get("target_price")
    only_me = body.get("only_me", False)

    token, all_cids, data = _get_token_and_cids()
    if not token:
        return jsonify({"ok": False, "error": "توکن تلگرام تنظیم نشده"}), 400

    targets = [YOUR_CHAT_ID] if only_me else (all_cids if BROADCAST_MODE else [YOUR_CHAT_ID])
    if not targets:
        return jsonify({"ok": False, "error": "هیچ chat_id‌ای ثبت نشده"}), 400

    # قیمت لحظه‌ای
    cur = None
    try:
        cur = get_price(sym, atype)
    except:
        pass

    arrow = "📈 ناحیه سل" if condition == "above" else "📉 ناحیه بای"
    cmt = f"\n💬 <i>{comment}</i>" if comment else ""
    price_text = fmt_price(cur, sym) if cur else "—"
    _creator = creator or 'سیستم'
    alarm_num_tag = _make_alarm_tag(sym)
    creator_tag = "#" + re.sub(r'[^\w]', '_', _creator).strip('_')
    out_msg = (
        f"🚨 <b>{'آلارم قیمت' if target_price else 'آلارم فوری'}!</b>\n\n"
        f"💰 <b>#{sym}</b> — {arrow}\n"
        f"🔖 {alarm_num_tag}\n"
        f"👤 {creator_tag}\n\n"
        + (f"🎯 هدف: <code>{fmt_price(target_price, sym)}</code>\n" if target_price else "")
        + f"📊 قیمت لحظه‌ای: <b>{price_text}</b>"
        f"{cmt}\n\n⏰ {now_pretty()} (تهران)"
    )

    # هر کاربر جداگانه با دکمه هشدار دوره‌ای
    sent_count = 0
    for cid in targets:
        kb = [[{"text": "⏰ هشدار دوره‌ای", "callback_data": f"set_reminder:{cid}:{sym}"}]]
        mid = send_tg_keyboard(token, str(cid), out_msg, kb)
        if mid: sent_count += 1

    # ذخیره در آرشیو
    try:
        d = load_alerts()
        d.setdefault("archive", []).append({
            "id": str(int(time.time() * 1000)),
            "symbol": sym, "type": atype,
            "condition": condition, "comment": comment,
            "created_by": creator, "active": False,
            "fired_at": now_teh(), "fired_price": cur,
            "target_price": target_price,
            "instant": True, "created_at": now_teh()
        })
        save_alerts(d)
    except Exception as e:
        log_error(f"instant_alert archive: {e}")

    print(f"[INSTANT] {sym} ارسال شد به {sent_count}/{len(targets)} نفر")
    return jsonify({"ok": True, "sent": sent_count, "total": len(targets)})


def test_tg():
    token, cids, _ = _get_token_and_cids()
    if not token or not cids:
        return jsonify({"ok": False, "error": "توکن یا chat_id ست نشده"})
    res = broadcast(token, cids, f"✅ تست موفق\n⏰ {now_pretty()}")
    return jsonify({"ok": any(res), "sent": sum(res), "total": len(cids)})

# ===================== SIGNALS API (وب‌سایت) =====================
SIGNAL_VALID_DIRECTIONS = {"buy_limit", "buy_stop", "sell_limit", "sell_stop"}

def _sb_delete_signal(sig_id):
    """حذف سیگنال از Supabase"""
    if not SUPABASE_KEY: return
    try:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/signals?id=eq.{sig_id}",
            headers=_sb_h(), timeout=8)
    except: pass

def _build_signal_record(body: dict):
    """از روی دیتای فرم سایت، رکوردی عیناً هم‌ساختار با چیزی که ربات می‌سازه برمی‌گردونه"""
    sym = (body.get("symbol") or "").upper().strip()
    direction = (body.get("direction") or "").lower().strip()
    if not sym:
        return None, "نماد وارد نشده"
    if direction not in SIGNAL_VALID_DIRECTIONS:
        return None, "جهت سیگنال نامعتبره (buy_limit/buy_stop/sell_limit/sell_stop)"
    try:
        entry = float(body.get("entry"))
    except (TypeError, ValueError):
        return None, "قیمت ورود الزامی است"

    def _f(key):
        v = body.get(key)
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    sl  = _f("sl")
    tp1 = _f("tp1")
    tp2 = _f("tp2")
    tp3 = _f("tp3")
    tf  = (body.get("tf") or SIGNAL_DEFAULT_TF).strip()
    note = (body.get("note") or "").strip()
    creator = (body.get("creator") or "وب‌سایت").strip()

    risk_pips = round(abs(entry - sl) * get_pip_multiplier(sym), 1) if sl is not None else None
    rr = None
    if sl is not None and tp1 is not None and abs(entry - sl) > 0:
        rr = round(abs(tp1 - entry) / abs(entry - sl), 2)

    seq = _sb_next_signal_seq()
    sig = {
        "id": f"S{seq:05d}", "seq": seq, "symbol": sym, "direction": direction,
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "tf": tf, "risk_pips": risk_pips, "rr": rr,
        "sent_by": creator, "sent_at": now_teh(), "channel_msg_id": None,
        "status": "active", "note": note or None,
    }
    return sig, None

@app.route("/api/signals", methods=["GET"])
def get_signals():
    limit = request.args.get("limit", 50)
    try: limit = int(limit)
    except: limit = 50
    return jsonify(_sb_load_signals(limit=limit))

@app.route("/api/signals", methods=["POST"])
def add_signal():
    """ثبت سیگنال در دیتابیس — بدون ارسال به کانال تلگرام"""
    body = request.json or {}
    sig, err = _build_signal_record(body)
    if err:
        return jsonify({"ok": False, "error": err}), 400
    _sb_save_signal(sig)
    sig["text"] = _build_signal_text(sig)
    return jsonify({"ok": True, "signal": sig})

@app.route("/api/signals/<sig_id>", methods=["DELETE"])
def del_signal(sig_id):
    threading.Thread(target=_sb_delete_signal, args=(sig_id,), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/send-signal", methods=["POST"])
def send_signal():
    """ثبت سیگنال + ارسال عیناً به کانال تلگرام، با همون فرمتی که ربات می‌سازه"""
    body = request.json or {}
    sig, err = _build_signal_record(body)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    token, _, _ = _get_token_and_cids()
    channel_mid = None
    if token and SIGNAL_CHANNEL:
        try:
            r_ch = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": SIGNAL_CHANNEL, "text": _build_signal_text(sig), "parse_mode": "HTML"},
                timeout=10, headers=H)
            if r_ch.status_code == 200:
                channel_mid = r_ch.json().get("result", {}).get("message_id")
                sig["channel_msg_id"] = channel_mid
        except Exception as e:
            print(f"[signal] web channel send error: {e}")

    _sb_save_signal(sig)
    sig["text"] = _build_signal_text(sig)
    if not SIGNAL_CHANNEL:
        return jsonify({"ok": True, "signal": sig, "sent": 0,
                         "warning": "کانال سیگنال (SIGNAL_CHANNEL) تنظیم نشده — فقط در دیتابیس ذخیره شد"})
    return jsonify({"ok": True, "signal": sig, "sent": 1 if channel_mid else 0})
# ===================================================================

@app.route("/api/status")
def status():
    alerts = load_alerts()
    journal = load_journal()
    all_active = [a for a in alerts.get("alerts", []) if a.get("active")]
    team_active = [a for a in all_active if not a.get("is_private")]
    private_active = [a for a in all_active if a.get("is_private")]
    return jsonify({
        "status": "ok", "last_update": alerts.get("last_update"),
        "errors": alerts.get("errors", [])[-5:], "time_tehran": now_teh(),
        "alert_count": len(all_active),          # کل (تیمی + شخصی)
        "team_alert_count": len(team_active),     # فقط تیمی
        "private_alert_count": len(private_active), # فقط شخصی
        "forex_open": is_forex_market_open(),
        "loop_count": _loop_count, "journal_count": len(journal)
    })

@app.route("/api/version")
def version():
    return jsonify({"version": VERSION})

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

# ==================== ژورنال ====================
@app.route("/api/journal", methods=["GET"])
def get_journal():
    trades = load_journal()
    print(f"[GET /api/journal] {len(trades)} ترید برگشت")
    return jsonify(trades)

@app.route("/api/journal", methods=["POST"])
def add_journal():
    return jsonify({"ok": False, "error": "ثبت دستی غیرفعال — از MT5 EA استفاده کن"}), 403
    entry = float(body.get("entry", 0))
    direction = body.get("direction", "BUY")
    size = 1.0
    is_missed_zone = bool(body.get("is_missed_zone", False))
    sl_pips = body.get("sl_pips")
    tp_pips = body.get("tp_pips")
    sl_price = body.get("sl_price")
    tp_price = body.get("tp_price")
    print(f"[AUTO] دریافت ترید — sym={sym} direction={direction} entry={entry} missed={is_missed_zone}")

    mul = get_pip_multiplier(sym)
    if sl_price is None and sl_pips is not None:
        sl_diff = sl_pips / mul
        sl_price = entry - sl_diff if direction == "BUY" else entry + sl_diff
    if tp_price is None and tp_pips is not None:
        tp_diff = tp_pips / mul
        tp_price = entry + tp_diff if direction == "BUY" else entry - tp_diff
    if sl_pips is None and sl_price is not None:
        sl_pips = abs(entry - sl_price) * mul
    if tp_pips is None and tp_price is not None:
        tp_pips = abs(tp_price - entry) * mul

    trade = {
        "id": generate_id(), "sym": sym, "tf": body.get("tf", "1h"),
        "direction": direction, "entry": entry, "size": size,
        "sl_pips": round(sl_pips, 1) if sl_pips else None,
        "tp_pips": round(tp_pips, 1) if tp_pips else None,
        "sl_price": sl_price, "tp_price": tp_price,
        "note": body.get("note", "").strip(), "entryTime": body.get("entryTime", now_teh()),
        "createdAt": now_teh(), "status": "open", "exit": None, "exitTime": None,
        "candle_snapshot": [], "pending_check": True,
        "exitNote": None, "pnl": None, "outcome": None,
        "ai_analysis": None, "ai_summary": None,
        "review_mfe": None, "review_mae": None, "review_pullback": None, "review_note": None,
        "review_reversal_occurred": None, "review_reversal_from_sl": None, "review_reversal_target_pips": None,
        "found_3r": False, "mae_pip": 0, "mfe_pip": 0,
        "free_risk_was_possible": False, "free_risk_saved": False, "pullback_after_1r": False,

        "mfe_before_sl_pip": 0, "passed_1r": False,
        "is_missed_zone": is_missed_zone
    }
    print(f"[AUTO] ترید ساخته شد — id={trade['id']} missed={is_missed_zone}")
    try:
        # محاسبه 3R override
        _mul = get_pip_multiplier(sym)
        _r3 = None
        if sl_price and entry:
            _risk = abs(entry - float(sl_price)) * _mul
            _r3 = (entry + 3*_risk/_mul) if direction == "BUY" else (entry - 3*_risk/_mul)
        # یک بار فراخوانی که snapshot تا SL/3R ادامه پیدا می‌کند و hit اولین رویداد است
        res = check_sltp_hit_with_details(sym, trade["tf"], trade["entryTime"], direction, entry, sl_price, tp_price, size, r3_override=_r3)
        (hit, hit_price, hit_time, last_close, pnl, mfe_pip, mae_pip, candle_lines, found_3r,
         fr_possible, fr_saved, fr_at, pullback, post_max, post_1r, post_1_5r, post_2r, post_3r,
         mfe_before_sl, passed_1r, snapshot_bars) = res

        if hit == "sl":
            # استاپ خورد → بسته شو
            trade["exit"] = hit_price
            trade["exitTime"] = hit_time or now_teh()
            trade["exitNote"] = "خودکار: استاپ لاس در زمان ثبت"
            trade["pnl"] = round(pnl, 2) if pnl is not None else 0
            trade["outcome"] = "loss"
            trade["exit_type"] = "sl"
            trade["status"] = "closed"
            trade["pending_check"] = False
            trade["mfe_pip"] = round(mfe_pip, 1)
            trade["mae_pip"] = round(mae_pip, 1)
            trade["found_3r"] = False
            trade["free_risk_was_possible"] = fr_possible
            trade["free_risk_saved"] = fr_saved
            trade["pullback_after_1r"] = pullback
            trade["mfe_before_sl_pip"] = round(mfe_before_sl, 1) if mfe_before_sl else 0
            trade["passed_1r"] = passed_1r
            trade["candle_snapshot"] = snapshot_bars
            print(f"[AUTO] ✅ استاپ خورد — outcome=loss missed={is_missed_zone}")
        elif hit in ("tp", "tp3"):
            # TP یا 3R خورد → ثبت برد ولی watching برای چارت کامل تا SL/3R
            trade["exit"] = hit_price
            trade["exitTime"] = hit_time or now_teh()
            trade["pnl"] = round(pnl, 2) if pnl is not None else 0
            trade["outcome"] = "win"
            trade["exit_type"] = hit
            trade["mfe_pip"] = round(mfe_pip, 1)
            trade["mae_pip"] = round(mae_pip, 1)
            trade["found_3r"] = (hit == "tp3")
            trade["free_risk_was_possible"] = fr_possible
            trade["free_risk_saved"] = fr_saved
            trade["pullback_after_1r"] = pullback
            trade["mfe_before_sl_pip"] = round(mfe_before_sl, 1) if mfe_before_sl else 0
            trade["passed_1r"] = passed_1r
            trade["candle_snapshot"] = snapshot_bars
            if hit == "tp3":
                # 3R کامل → مستقیم بسته شو
                trade["exitNote"] = "خودکار: 3R کامل در زمان ثبت"
                trade["status"] = "closed"
                trade["pending_check"] = False
                print(f"[AUTO] ✅ 3R کامل — outcome=tp3 missed={is_missed_zone}")
            else:
                # TP خورد → برد ثبت، watching برای snapshot تا SL/3R
                trade["exitNote"] = "خودکار: تارگت در زمان ثبت"
                trade["status"] = "watching"
                trade["pending_check"] = True
                trade["last_poll"] = now_teh()
                print(f"[AUTO] ✅ TP خورد — outcome=win, watching برای SL/3R missed={is_missed_zone}")
        else:
            # هیچ چیز نخورده → در جریان
            trade["status"] = "open"
            trade["pending_check"] = True
            trade["candle_snapshot"] = snapshot_bars
            trade["last_poll"] = now_teh()
    except Exception as e:
        log_error(f"auto check error: {e}")
        return jsonify({"ok": False, "error": f"خطا: {str(e)}"}), 500
    journal.insert(0, trade)
    print(f"[AUTO] ✅ ترید {trade['id']} ذخیره شد — sym={sym} missed={is_missed_zone}")
    save_journal(journal)
    return jsonify({"ok": True, "trade": trade})

import uuid as _uuid

def generate_id():
    """ID یکتا حتی برای تریدهای همزمان"""
    return str(int(time.time() * 1000)) + str(_uuid.uuid4().hex[:4])

def calc_exit_type(outcome, risk_pips, mfe_pip, found_3r, exit_type_stored=None):
    """نوع خروج رو بر اساس outcome و MFE محاسبه کن"""
    if exit_type_stored in ("sl", "tp", "tp3"):
        return exit_type_stored
    if outcome == "loss":
        return "sl"
    if found_3r:
        return "tp3"
    if risk_pips and risk_pips > 0 and mfe_pip:
        mfe_r = mfe_pip / risk_pips
        if mfe_r >= 3.0:
            return "tp3"
    return "tp"

@app.route("/api/journal/manual", methods=["POST"])
def add_journal_manual():
    journal = load_journal()
    body = request.json or {}
    sym = body.get("sym", "").upper().strip()
    if not sym:
        return jsonify({"ok": False, "error": "sym الزامی است"}), 400
    entry = float(body.get("entry", 0))
    direction = body.get("direction", "BUY")
    size = 1.0
    tf = body.get("tf", "1h")
    entryTime = body.get("entryTime", now_teh())
    note = body.get("note", "").strip()
    sl_price = body.get("sl_price")
    tp_price = body.get("tp_price")
    exit_price = body.get("exit")
    outcome = body.get("outcome")
    exitTime = body.get("exitTime", now_teh())
    exitNote = body.get("exitNote", "ثبت دستی")
    mul = get_pip_multiplier(sym)
    sl_pips = abs(entry - sl_price) * mul if sl_price else None
    tp_pips = abs(tp_price - entry) * mul if tp_price else None
    pnl = None
    # اگه exit وارد نشده، از tp_price (win) یا sl_price (loss) fallback بگیر
    exit_for_pnl = exit_price or (tp_price if outcome == "win" else (sl_price if outcome == "loss" else None))
    if exit_for_pnl and entry:
        diff = (float(exit_for_pnl) - entry) if direction == "BUY" else (entry - float(exit_for_pnl))
        pnl = diff * size
    # exit_price هم اگه خالیه ولی outcome داریم، پر کن
    if not exit_price and exit_for_pnl:
        exit_price = exit_for_pnl
    review_mfe = body.get("review_mfe")
    review_mae = body.get("review_mae")
    review_pullback = body.get("review_pullback", False)
    review_note = body.get("review_note", "")
    review_reversal_occurred = body.get("review_reversal_occurred", False)
    review_reversal_from_sl = body.get("review_reversal_from_sl")
    review_reversal_target_pips = body.get("review_reversal_target_pips")
    review_free_risk_saved = body.get("review_free_risk_saved", False)
    is_missed_zone = bool(body.get("is_missed_zone", False))
    print(f"[MANUAL] sym={body.get('sym')} outcome={body.get('outcome')} missed={is_missed_zone}")
    is_crypto = is_crypto_symbol(sym)
    risk_pips = None
    if sl_price and entry:
        risk_pips = abs(entry - float(sl_price)) * mul
    if is_crypto:
        mfe_r = body.get("review_mfe_r")
        mae_r = body.get("review_mae_r")
        reversal_target_r = body.get("review_reversal_target_pips_r")
        if mfe_r is not None and risk_pips and risk_pips > 0:
            review_mfe = mfe_r * risk_pips
        if mae_r is not None and risk_pips and risk_pips > 0:
            review_mae = mae_r * risk_pips
        if reversal_target_r is not None and risk_pips and risk_pips > 0:
            review_reversal_target_pips = reversal_target_r * risk_pips
    trade = {
        "id": generate_id(), "sym": sym, "tf": tf,
        "direction": direction, "entry": entry, "size": size,
        "sl_pips": round(sl_pips, 1) if sl_pips else None,
        "tp_pips": round(tp_pips, 1) if tp_pips else None,
        "sl_price": sl_price, "tp_price": tp_price,
        "note": note, "entryTime": entryTime, "createdAt": now_teh(),
        "status": "closed", "exit": exit_price, "exitTime": exitTime, "exitNote": exitNote,
        "pnl": round(pnl, 2) if pnl is not None else 0, "outcome": outcome,
        "ai_analysis": None, "ai_summary": None,
        "review_mfe": review_mfe, "review_mae": review_mae,
        "review_pullback": review_pullback, "review_note": review_note,
        "review_reversal_occurred": review_reversal_occurred,
        "review_reversal_from_sl": review_reversal_from_sl,
        "review_reversal_target_pips": review_reversal_target_pips,
        "review_free_risk_saved": review_free_risk_saved,
        "found_3r": False, "mae_pip": review_mae if review_mae else 0,
        "mfe_pip": review_mfe if review_mfe else 0,
        "free_risk_was_possible": False, "free_risk_saved": review_free_risk_saved,
        "pullback_after_1r": review_pullback,

        "mfe_before_sl_pip": 0, "passed_1r": False, "candle_snapshot": [],
        "is_missed_zone": is_missed_zone
    }
    mfe_for_calc = float(review_mfe) if review_mfe else 0
    mae_for_calc = float(review_mae) if review_mae else 0
    # محاسبه passed_1r از review_mfe
    _passed_1r = False
    _fr_possible = False
    if risk_pips and risk_pips > 0 and mfe_for_calc > 0:
        _passed_1r = mfe_for_calc >= risk_pips * 0.98
        _fr_possible = _passed_1r  # conservative — review میتونه override کنه
    trade["passed_1r"] = _passed_1r
    trade["free_risk_was_possible"] = _fr_possible
    trade["free_risk_saved"] = review_free_risk_saved
    trade["pullback_after_1r"] = review_pullback
    # found_3r از review_mfe
    _found_3r = bool(risk_pips and risk_pips > 0 and mfe_for_calc >= risk_pips * 3.0)
    trade["found_3r"] = _found_3r
    trade["exit_type"] = calc_exit_type(outcome, risk_pips, mfe_for_calc, _found_3r)
    if trade.get("outcome") and not trade.get("candle_snapshot"):
        try:
            r3_guess = None
            if risk_pips and risk_pips > 0:
                if direction == "BUY":
                    r3_guess = entry + 3 * risk_pips / mul
                else:
                    r3_guess = entry - 3 * risk_pips / mul
            res_snap = check_sltp_hit_with_details(
                sym, tf, entryTime, direction, entry, sl_price,
                tp_price if tp_price else r3_guess,
                size, r3_override=r3_guess
            )
            trade["candle_snapshot"] = res_snap[-1] if res_snap else []
            if trade["candle_snapshot"]:
                trade["snapshot_locked"] = True
        except Exception as e:
            log_error(f"manual snapshot: {e}")
            trade["candle_snapshot"] = []
    journal.insert(0, trade)
    save_journal(journal)
    return jsonify({"ok": True, "trade": trade})

@app.route("/api/weekly_review", methods=["GET"])
def weekly_review():
    from datetime import datetime as _dt, timedelta as _td
    now = _dt.now()
    days_since_monday = now.weekday()
    week_start = (now - _td(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end = week_start + _td(days=6, hours=23, minutes=59)

@app.route("/api/assignments", methods=["GET"])
def api_assignments():
    """
    لیست کامل assignments با join از alerts جدول.
    پارامترها:
      ?active=true    — فقط فعال‌ها
      ?week=true      — از ابتدای هفته جاری (شنبه تهران)
      ?all=true       — همه (پیش‌فرض)
    """
    try:
        active_only = request.args.get("active") == "true"
        week_only   = request.args.get("week") == "true"

        # لود assignments از Supabase
        url = f"{SUPABASE_URL}/rest/v1/alarm_assignments?select=*&order=fired_at.desc"
        if active_only:
            url += "&is_active=eq.true"
        if week_only:
            now_dt = datetime.now(TEHRAN)
            days_since_sat = (now_dt.weekday() - 5) % 7
            week_start = (now_dt - timedelta(days=days_since_sat)).replace(
                hour=0, minute=0, second=0, microsecond=0)
            url += f"&fired_at=gte.{week_start.strftime('%Y-%m-%dT%H:%M:%S')}"

        r = requests.get(url, headers=_sb_h(), timeout=10)
        if r.status_code != 200:
            return jsonify({"ok": False, "error": r.text[:100]}), 500
        rows = r.json()

        # لود همه alerts (active + archive) برای join
        raw = _sb_load_all_alerts()
        if raw and isinstance(raw, dict):
            all_alerts = raw.get("alarms", []) + raw.get("archive", [])
        else:
            fb = load_alerts()
            all_alerts = fb.get("alarms", []) + fb.get("archive", [])
        alerts_map = {str(a["id"]): a for a in all_alerts}

        result = []
        for row in rows:
            aid    = str(row.get("id", ""))
            alert  = alerts_map.get(aid, {})
            sym    = alert.get("symbol", "")
            tgt    = alert.get("target_price", 0) or 0
            result.append({
                # از alarm_assignments
                "id":           aid,
                "alarm_tag":    row.get("alarm_tag", ""),
                "assigned_to":  row.get("assigned_to", "") or "",
                "shift":        row.get("shift", ""),
                "is_active":    row.get("is_active", True),
                "fired_at":     row.get("fired_at", ""),
                "false_at":     row.get("false_at", "") or "",
                "false_by":     row.get("false_by", "") or "",
                "false_reason": row.get("false_reason", "") or "",
                # از alerts جدول
                "symbol":       sym,
                "condition":    alert.get("condition", ""),
                "target_price": tgt,
                "target_fmt":   fmt_price(float(tgt), sym) if tgt else "",
                "created_by":   alert.get("created_by", "") or "",
                "created_at":   alert.get("created_at", "") or "",
                "comment":      alert.get("comment", "") or "",
                "fired_price":  alert.get("fired_price", "") or "",
                "is_private":   alert.get("is_private", False),
            })

        # فیلتر آلارم‌های شخصی
        result = [r for r in result if not r["is_private"]]
        return jsonify({"ok": True, "count": len(result), "items": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    journal = load_journal()
    week_trades = []
    for t in journal:
        et = str(t.get("entryTime",""))[:16]
        try:
            dt = _dt.strptime(et, "%Y-%m-%d %H:%M")
            if week_start <= dt <= week_end:
                week_trades.append(t)
        except: pass

    if not week_trades:
        return jsonify({"ok": True, "analysis": "این هفته هیچ تریدی ثبت نشده.", "count": 0,
                        "week_start": week_start.strftime("%Y-%m-%d"), "week_end": week_end.strftime("%Y-%m-%d")})

    wins = [t for t in week_trades if t.get("outcome")=="win"]
    losses = [t for t in week_trades if t.get("outcome")=="loss"]
    total = len(week_trades)
    wr = round(len(wins)/total*100) if total else 0

    def get_mul(s): return get_pip_multiplier(s)
    def get_risk_pips(t):
        if t.get("sl_price") and t.get("entry"):
            return abs(float(t["entry"]) - float(t["sl_price"])) * get_mul(t.get("sym","EURUSD"))
        return None

    def taken_r(t):
        rp = get_risk_pips(t)
        if not rp or rp <= 0: return None
        if t.get("outcome") == "win":
            ep = float(t.get("exit") or t.get("tp_price") or 0)
            diff = abs(float(t.get("entry",0)) - ep) * get_mul(t.get("sym","EURUSD")) if ep else 0
            return round(diff/rp, 2)
        if t.get("outcome") == "loss":
            return -1.0
        return None

    def get_mfe_r(t):
        rp = get_risk_pips(t)
        if not rp or rp <= 0: return None
        mfe = float(t.get("review_mfe") or t.get("mfe_pip") or 0)
        return round(mfe/rp, 2) if mfe else None

    r_list = [r for t in week_trades if (r:=taken_r(t)) is not None]
    mfe_r_list = [m for t in week_trades if (m:=get_mfe_r(t)) is not None]
    total_r = round(sum(r_list), 2) if r_list else 0
    avg_r = round(sum(r_list)/len(r_list), 2) if r_list else 0
    avg_mfe_r = round(sum(mfe_r_list)/len(mfe_r_list), 2) if mfe_r_list else 0

    # چند ترید از تارگت گذشتن (mfe > tp)
    beyond_tp = 0
    early_exit_r_left = []
    for t in week_trades:
        if t.get("outcome") != "win": continue
        rp = get_risk_pips(t)
        if not rp: continue
        mfe = float(t.get("review_mfe") or t.get("mfe_pip") or 0)
        ep = float(t.get("exit") or t.get("tp_price") or 0)
        if not ep: continue
        taken = abs(float(t.get("entry",0)) - ep) * get_mul(t.get("sym","EURUSD"))
        if mfe > taken * 1.1:  # MFE بیشتر از TP
            beyond_tp += 1
            early_exit_r_left.append(round((mfe - taken)/rp, 2))
    avg_left = round(sum(early_exit_r_left)/len(early_exit_r_left), 2) if early_exit_r_left else 0

    # passed_1r در باخت‌ها
    loss_passed_1r = sum(1 for t in losses if t.get("passed_1r"))
    fr_missed = sum(1 for t in week_trades if t.get("free_risk_was_possible") and not t.get("free_risk_saved") and t.get("outcome")=="loss")

    # نمادها
    sym_r = {}
    for t in week_trades:
        s = t.get("sym","?"); r = taken_r(t)
        if r is not None: sym_r[s] = round(sym_r.get(s,0) + r, 2)
    sym_lines = " | ".join(f"{s}:{v:+.1f}R" for s,v in sorted(sym_r.items(), key=lambda x:-x[1]))

    # بهترین/بدترین
    best = max(week_trades, key=lambda t: taken_r(t) or -99)
    worst = min(week_trades, key=lambda t: taken_r(t) or 99)
    best_str = f"{best.get('sym')} {taken_r(best):+.1f}R" if taken_r(best) else "—"
    worst_str = f"{worst.get('sym')} {taken_r(worst):+.1f}R" if taken_r(worst) else "—"

    # روزانه
    day_lines = []
    for i in range(7):
        d = week_start + _td(days=i)
        d_trades = [t for t in week_trades if str(t.get("entryTime",""))[:10] == d.strftime("%Y-%m-%d")]
        if d_trades:
            dw = sum(1 for t in d_trades if t.get("outcome")=="win")
            dl = sum(1 for t in d_trades if t.get("outcome")=="loss")
            dr = round(sum(r for t in d_trades if (r:=taken_r(t)) is not None), 2)
            day_lines.append(f"{d.strftime('%A')} {d.strftime('%m/%d')}: {dw}W {dl}L {dr:+.1f}R")

    # لیست تریدها با جزئیات
    trade_lines = []
    for t in week_trades:
        r = taken_r(t); mfe = get_mfe_r(t)
        r_str = f"{r:+.1f}R" if r is not None else "?"
        mfe_str = f"MFE:{mfe:.1f}R" if mfe else ""
        passed = "✓1R" if t.get("passed_1r") else ""
        trade_lines.append(f"{t.get('sym')} {t.get('tf')} {t.get('direction')} → {r_str} {mfe_str} {passed} ({t.get('outcome','?')})")

    prompt = (
        f"هفته {week_start.strftime('%Y/%m/%d')} تا {week_end.strftime('%Y/%m/%d')}\n\n"
        f"=== آمار کلی ===\n"
        f"تریدها: {total} | برد: {len(wins)} | باخت: {len(losses)} | نرخ برد: {wr}%\n"
        f"مجموع R: {total_r:+.2f}R | میانگین: {avg_r:+.2f}R | میانگین MFE: {avg_mfe_r:.2f}R\n\n"
        f"=== تارگت و MFE ===\n"
        f"تریدهایی که بعد از TP هنوز MFE داشتن (زود بستی): {beyond_tp} از {len(wins)} برد\n"
        f"{'→ میانگین R که روی میز موند: '+str(avg_left)+'R' if beyond_tp else ''}\n"
        f"باخت‌هایی که به 1R رسیده بودن: {loss_passed_1r} از {len(losses)}\n"
        f"فری‌ریسک ممکن بود ولی SL خورد: {fr_missed}\n\n"
        f"=== روزانه ===\n" + "\n".join(day_lines) + "\n\n"
        f"=== نمادها ===\n{sym_lines}\n\n"
        f"=== بهترین: {best_str} | بدترین: {worst_str} ===\n\n"
        f"=== تریدها ===\n" + "\n".join(trade_lines) + "\n\n"
        f"گزارش هفتگی فارسی بنویس (بدون مقدمه):\n"
        f"1. خلاصه یه جمله\n"
        f"2. تارگت‌گذاری: {beyond_tp} ترید MFE از TP گذشته — زود بستی یا درست؟\n"
        f"3. باخت‌ها: {loss_passed_1r} باخت بعد از 1R — فری‌ریسک بزن\n"
        f"4. بهترین/بدترین نماد این هفته\n"
        f"5. یه توصیه مشخص برای هفته بعد\n"
        f"اعداد دقیق، کوتاه و مفید."
    )

    analysis = groq_analyze(prompt)
    return jsonify({
        "ok": True, "analysis": analysis,
        "count": total, "wins": len(wins), "losses": len(losses),
        "winrate": wr, "total_r": total_r, "avg_mfe_r": avg_mfe_r,
        "beyond_tp": beyond_tp, "avg_left": avg_left,
        "loss_passed_1r": loss_passed_1r, "fr_missed": fr_missed,
        "week_start": week_start.strftime("%Y-%m-%d"),
        "week_end": week_end.strftime("%Y-%m-%d"),
        "day_lines": day_lines, "sym_lines": sym_lines,
        "best": best_str, "worst": worst_str,
    })


def recalculate_all():
    """همه تریدهای بسته رو با منطق جدید recalc کن — فیلدهای review دست نخورد"""
    journal = load_journal()
    fixed = 0
    errors = 0
    report = []

    for trade in journal:
        if trade.get("status") not in ("closed", "watching"):
            continue
        tid = trade.get("id", "?")
        sym = trade.get("sym", "")
        direction = trade.get("direction", "BUY")
        entry = trade.get("entry")
        sl_price = trade.get("sl_price")
        outcome = trade.get("outcome")
        if not entry or not sl_price or not outcome:
            report.append(f"{tid} {sym}: رد شد (entry/sl/outcome ناقص)")
            continue

        try:
            mul = get_pip_multiplier(sym)
            risk_pips = abs(float(entry) - float(sl_price)) * mul
            if risk_pips <= 0:
                report.append(f"{tid} {sym}: رد شد (risk_pips=0)")
                continue

            # مقادیر review دست نخورد — فقط از اونا استفاده کن
            mfe_for_calc = float(trade.get("review_mfe") or trade.get("mfe_pip") or 0)
            mae_for_calc = float(trade.get("review_mae") or trade.get("mae_pip") or 0)

            # fallback MFE از TP برای win بدون review
            if mfe_for_calc == 0 and outcome == "win" and trade.get("tp_price"):
                mfe_for_calc = abs(float(trade["tp_price"]) - float(entry)) * mul

            # recalc passed_1r
            passed_1r = mfe_for_calc >= risk_pips * 0.98 if mfe_for_calc > 0 else False

            # recalc found_3r
            found_3r = mfe_for_calc >= risk_pips * 3.0 if mfe_for_calc > 0 else False

            # recalc free_risk_was_possible
            fr_possible = passed_1r  # conservative baseline

            # recalc exit_type
            exit_type = calc_exit_type(outcome, risk_pips, mfe_for_calc, found_3r)

            # recalc pnl
            exit_price = trade.get("exit")
            if exit_price and entry:
                diff = (float(exit_price) - float(entry)) if direction == "BUY" else (float(entry) - float(exit_price))
                trade["pnl"] = round(diff, 5)

            # recalc exitTime — اگه exitTime مشکوک باشه (بعد از poll)، از کندل‌ها پیدا کن
            exit_time_fixed = False
            tp_price = trade.get("tp_price")
            entry_time = trade.get("entryTime")
            exit_time = trade.get("exitTime")
            if outcome == "win" and tp_price and entry_time and exit_time:
                try:
                    # اگه exitTime خیلی بعد از entryTime باشه (بیشتر از ۴ ساعت)، شک داریم
                    from datetime import datetime as _dt
                    et = _dt.strptime(str(entry_time)[:16], "%Y-%m-%d %H:%M")
                    xt = _dt.strptime(str(exit_time)[:16], "%Y-%m-%d %H:%M")
                    hours_diff = (xt - et).total_seconds() / 3600
                    tf = trade.get("tf", "1h")
                    # برای 15m اگه بیشتر از ۸ ساعت گذشته، احتمالاً exitTime از poll اومده
                    threshold = {"15m": 8, "1h": 24, "4h": 48}.get(tf, 12)
                    if hours_diff > threshold:
                        res_fix = check_sltp_hit_with_details(
                            sym, tf, entry_time, direction, float(entry),
                            float(sl_price), float(tp_price), 1.0
                        )
                        real_hit, _, real_hit_time, *_ = res_fix
                        if real_hit in ("tp", "tp3") and real_hit_time:
                            old_exit = trade["exitTime"]
                            trade["exitTime"] = real_hit_time
                            report.append(f"{sym} #{tid[-4:]}: exitTime {old_exit} → {real_hit_time} ✓")
                            exit_time_fixed = True
                except Exception as ex:
                    log_error(f"exitTime fix {tid}: {ex}")
            # repair snapshot — همه تریدها (نه فقط locked)، با fallback به tf بزرگتر
            existing_snap = trade.get("candle_snapshot", [])
            if len(existing_snap) < 10 and sl_price and trade.get("entryTime"):
                try:
                    tf_orig = trade.get("tf", "1h")
                    tf_fallbacks = {
                        "1m": ["1m","5m","15m"], "5m": ["5m","15m","1h"],
                        "15m": ["15m","1h"], "1h": ["1h","4h"],
                        "4h": ["4h","1d"], "1d": ["1d"]
                    }
                    tfs_to_try = tf_fallbacks.get(tf_orig, [tf_orig])
                    r3_guess = None
                    if risk_pips > 0:
                        r3_guess = (float(entry) + 3*risk_pips/mul) if direction=="BUY" else (float(entry) - 3*risk_pips/mul)
                    best_snap = existing_snap
                    used_tf = tf_orig
                    for try_tf in tfs_to_try:
                        res_snap = check_sltp_hit_with_details(
                            sym, try_tf, trade.get("entryTime"), direction,
                            float(entry), float(sl_price),
                            float(tp_price) if tp_price else r3_guess,
                            1.0, r3_override=r3_guess
                        )
                        new_snap = res_snap[-1] if res_snap else []
                        if len(new_snap) > len(best_snap):
                            best_snap = new_snap
                            used_tf = try_tf
                        if len(best_snap) >= 10:
                            break
                    if len(best_snap) > len(existing_snap):
                        tf_note = f" (tf:{used_tf})" if used_tf != tf_orig else ""
                        trade["candle_snapshot"] = best_snap
                        trade["snapshot_locked"] = True
                        report.append(f"{sym} #{tid[-4:]}: snapshot {len(existing_snap)}→{len(best_snap)} کندل{tf_note} ✓")
                        exit_time_fixed = True
                except Exception as ex:
                    log_error(f"snapshot repair {tid}: {ex}")
            old = {k: trade.get(k) for k in ["passed_1r","found_3r","free_risk_was_possible","exit_type","mfe_pip","mae_pip"]}
            trade["passed_1r"] = passed_1r
            trade["found_3r"] = found_3r
            trade["free_risk_was_possible"] = trade.get("review_free_risk_saved") or fr_possible
            trade["exit_type"] = exit_type
            # mae_pip/mfe_pip فقط اگه review نداشت و عدد قدیمی صفر بود
            if trade.get("review_mfe") is None and float(trade.get("mfe_pip") or 0) == 0 and mfe_for_calc > 0:
                trade["mfe_pip"] = round(mfe_for_calc, 1)
            if trade.get("review_mae") is None and float(trade.get("mae_pip") or 0) == 0 and mae_for_calc > 0:
                trade["mae_pip"] = round(mae_for_calc, 1)

            new = {k: trade.get(k) for k in ["passed_1r","found_3r","free_risk_was_possible","exit_type","mfe_pip","mae_pip"]}
            changed_fields = [k for k in old if old[k] != new[k]]
            if changed_fields:
                report.append(f"{sym} #{tid[-4:]}: {', '.join(changed_fields)} آپدیت شد")
            if changed_fields or exit_time_fixed:
                fixed += 1

        except Exception as e:
            log_error(f"recalc {tid}: {e}")
            report.append(f"{tid} {sym}: خطا — {e}")
            errors += 1

    save_journal(journal)
    print(f"[RECALC] تمام — {fixed} ترید آپدیت، {errors} خطا")
    return jsonify({"ok": True, "fixed": fixed, "errors": errors, "report": report})


@app.route("/api/journal/<tid>/edit", methods=["PUT"])
def edit_trade(tid):
    journal = load_journal()
    trade = next((t for t in journal if t["id"] == tid), None)
    if not trade:
        return jsonify({"ok": False, "error": "ترید یافت نشد"}), 404
    body = request.json or {}

    # ---- ادیت فیلدهای اصلی ----
    for key in ["sym","direction","entry","exit","sl_price","tp_price","entryTime","exitTime","pnl","outcome","note","exitNote"]:
        if key in body:
            trade[key] = body[key] if key in ["note","exitNote","outcome","direction","sym"] else float(body[key]) if body[key] is not None else None
    for key in ["review_mfe","review_mae","review_pullback","review_note","review_reversal_occurred","review_reversal_from_sl","review_reversal_target_pips","review_free_risk_saved"]:
        if key in body:
            trade[key] = body[key]

    # ---- recalc کامل بعد از ادیت ----
    sym = trade.get("sym", "")
    mul = get_pip_multiplier(sym)
    entry = trade.get("entry")
    sl_price = trade.get("sl_price")
    tp_price = trade.get("tp_price")
    direction = trade.get("direction", "BUY")
    outcome = trade.get("outcome", "")

    # sl_pips / tp_pips
    if sl_price and entry:
        trade["sl_pips"] = round(abs(float(entry) - float(sl_price)) * mul, 1)
    if tp_price and entry:
        trade["tp_pips"] = round(abs(float(tp_price) - float(entry)) * mul, 1)

    # pnl از exit یا fallback به tp/sl
    exit_px = trade.get("exit")
    if not exit_px and outcome == "win" and tp_price:
        exit_px = tp_price
    elif not exit_px and outcome == "loss" and sl_price:
        exit_px = sl_price
    if exit_px and entry:
        diff = (float(exit_px) - float(entry)) if direction == "BUY" else (float(entry) - float(exit_px))
        trade["pnl"] = round(diff, 5)

    # ---- recalc risk-based fields ----
    if sl_price and entry:
        risk_pips = abs(float(entry) - float(sl_price)) * mul
        if risk_pips > 0:
            mfe_raw = float(trade.get("review_mfe") or trade.get("mfe_pip") or 0)
            mae_raw = float(trade.get("review_mae") or trade.get("mae_pip") or 0)

            # اگه MFE خالیه و win داریم، از tp تخمین بزن
            if mfe_raw == 0 and outcome == "win" and tp_price:
                mfe_raw = abs(float(tp_price) - float(entry)) * mul

            # passed_1r
            passed_1r = mfe_raw >= risk_pips * 0.98 if mfe_raw > 0 else False
            trade["passed_1r"] = passed_1r

            # found_3r
            found_3r = mfe_raw >= risk_pips * 3.0 if mfe_raw > 0 else False
            trade["found_3r"] = found_3r

            # free_risk_was_possible
            fr_saved = trade.get("review_free_risk_saved", False)
            trade["free_risk_was_possible"] = fr_saved or passed_1r

            # mfe_pip / mae_pip sync (فقط اگه review نداره)
            if trade.get("review_mfe") is None and mfe_raw > 0:
                trade["mfe_pip"] = round(mfe_raw, 1)
            if trade.get("review_mae") is None and mae_raw > 0:
                trade["mae_pip"] = round(mae_raw, 1)

            # exit_type recalc
            trade["exit_type"] = calc_exit_type(outcome, risk_pips, mfe_raw, found_3r)

            print(f"[EDIT] {sym} sl_price={sl_price} risk={risk_pips:.1f}p passed_1r={passed_1r} found_3r={found_3r} exit_type={trade['exit_type']}")

    save_journal(journal)
    return jsonify({"ok": True, "trade": trade})

@app.route("/api/journal/all", methods=["DELETE"])
def delete_all_trades():
    global _cache_journal
    print("[DELETE_ALL] درخواست پاک کردن همه تریدها")
    _cache_journal = []
    _sb_delete_all()
    save_journal([])
    print("[DELETE_ALL] ✅ همه تریدها پاک شدند")
    return jsonify({"ok": True, "deleted": True})

@app.route("/api/journal/<tid>/delete", methods=["DELETE"])
def delete_trade(tid):
    print(f"[DELETE] درخواست حذف ترید: {tid}")
    journal = load_journal()
    before = len(journal)
    journal = [t for t in journal if str(t.get("id","")) != str(tid)]
    after = len(journal)
    if after == before:
        print(f"[DELETE] ❌ ترید {tid} یافت نشد — IDs موجود: {[str(t.get('id')) for t in journal[:5]]}")
        return jsonify({"ok": False, "error": f"ترید {tid} یافت نشد"}), 404
    global _cache_journal
    _cache_journal = journal
    _sb_delete(tid)
    save_journal(journal)
    print(f"[DELETE] ✅ ترید {tid} حذف شد — باقیمانده: {after}")
    return jsonify({"ok": True})

@app.route("/api/journal/<tid>/review", methods=["POST"])
def review_trade(tid):
    journal = load_journal()
    trade = next((t for t in journal if t["id"] == tid), None)
    if not trade:
        return jsonify({"ok": False, "error": "ترید یافت نشد"}), 404
    body = request.json or {}
    sym = trade.get("sym", "")
    mul = get_pip_multiplier(sym)
    entry = float(trade.get("entry", 0))
    sl_px = trade.get("sl_price")
    risk_pips = abs(entry - float(sl_px)) * mul if sl_px and entry else None
    if "review_mfe" in body:
        mfe_val = body["review_mfe"]
        if mfe_val is not None and is_crypto_symbol(sym) and risk_pips and risk_pips > 0:
            mfe_raw = round(float(mfe_val) * risk_pips, 4)
        else:
            mfe_raw = mfe_val
        # cap روی 3R — بیشتر از 3R در محاسبات تفاوتی نمیکنه
        if mfe_raw is not None and risk_pips and risk_pips > 0:
            cap_3r = risk_pips * 3.0
            if float(mfe_raw) > cap_3r:
                print(f"[REVIEW] MFE cap: {mfe_raw} → {cap_3r} (3R)")
                mfe_raw = round(cap_3r, 4)
        trade["review_mfe"] = mfe_raw
    # ---- بازمحاسبه passed_1r از روی review_mfe دستی ----
    if "review_mfe" in body and risk_pips and risk_pips > 0:
        mfe_corrected = trade.get("review_mfe")
        if mfe_corrected is not None:
            mfe_f = float(mfe_corrected)
            if mfe_f < risk_pips * 0.98:
                old_p1r = trade.get("passed_1r")
                old_frp = trade.get("free_risk_was_possible")
                trade["passed_1r"] = False
                trade["free_risk_was_possible"] = False
                trade["mfe_before_sl_pip"] = round(mfe_f, 1)
                print(f"[REVIEW] MFE دستی={mfe_f:.1f} < 1R={risk_pips:.1f} → passed_1r: {old_p1r}→False, fr_possible: {old_frp}→False")
            else:
                trade["passed_1r"] = True
                print(f"[REVIEW] MFE دستی={mfe_f:.1f} >= 1R={risk_pips:.1f} → passed_1r=True")

    if "review_mae" in body:
        mae_val = body["review_mae"]
        if mae_val is not None and is_crypto_symbol(sym) and risk_pips and risk_pips > 0:
            trade["review_mae"] = round(float(mae_val) * risk_pips, 4)
        else:
            trade["review_mae"] = mae_val
    if "review_reversal_target_pips" in body:
        rt_val = body["review_reversal_target_pips"]
        if rt_val is not None and is_crypto_symbol(sym) and risk_pips and risk_pips > 0:
            trade["review_reversal_target_pips"] = round(float(rt_val) * risk_pips, 4)
        else:
            trade["review_reversal_target_pips"] = rt_val
    for f in ["review_pullback","review_note","review_reversal_occurred","review_reversal_from_sl","review_free_risk_saved","is_missed_zone"]:
        if f in body:
            old_val = trade.get(f)
            trade[f] = body[f]
            if old_val != body[f]:
                print(f"[REVIEW] فیلد {f}: {old_val} → {body[f]}")

    # ---- recalc کامل بعد از ثبت ارزیابی ----
    if risk_pips and risk_pips > 0:
        outcome = trade.get("outcome", "")
        mfe_final = float(trade.get("review_mfe") or trade.get("mfe_pip") or 0)
        mae_final = float(trade.get("review_mae") or trade.get("mae_pip") or 0)
        tp_price = trade.get("tp_price")
        entry_val = float(trade.get("entry", 0))

        # اگه MFE خالیه و win داریم، از tp تخمین بزن
        if mfe_final == 0 and outcome == "win" and tp_price and entry_val:
            mfe_final = abs(float(tp_price) - entry_val) * mul

        # passed_1r — منبع اصلی حقیقت
        passed_1r = mfe_final >= risk_pips * 0.98 if mfe_final > 0 else False
        trade["passed_1r"] = passed_1r

        # found_3r
        found_3r = mfe_final >= risk_pips * 3.0 if mfe_final > 0 else False
        trade["found_3r"] = found_3r

        # free_risk_was_possible — از passed_1r یا review_free_risk_saved
        fr_saved = trade.get("review_free_risk_saved", False)
        trade["free_risk_was_possible"] = passed_1r or fr_saved

        # mfe_pip / mae_pip sync با review
        if mfe_final > 0:
            trade["mfe_pip"] = round(mfe_final, 1)
        if mae_final > 0:
            trade["mae_pip"] = round(mae_final, 1)

        # exit_type recalc
        trade["exit_type"] = calc_exit_type(outcome, risk_pips, mfe_final, found_3r)

        print(f"[REVIEW] recalc — passed_1r={passed_1r} found_3r={found_3r} fr_possible={trade['free_risk_was_possible']} exit_type={trade['exit_type']} mfe_pip={trade['mfe_pip']}")

    print(f"[REVIEW] وضعیت نهایی — passed_1r={trade.get('passed_1r')} fr_possible={trade.get('free_risk_was_possible')} review_mfe={trade.get('review_mfe')} missed={trade.get('is_missed_zone')}")
    save_trade(trade)
    print(f"[REVIEW] ✅ ذخیره ترید {tid} انجام شد")
    return jsonify({"ok": True, "trade": trade})

@app.route("/api/analyze/<trade_id>", methods=["GET"])
def analyze_trade(trade_id):
    journal = load_journal()
    trade = next((t for t in journal if t["id"] == trade_id), None)
    if not trade:
        return jsonify({"ok": False, "error": "ترید یافت نشد"}), 404
    symbol = trade["sym"]
    entry = float(trade["entry"])
    sl = trade.get("sl_price")
    direction = trade.get("direction", "BUY")
    outcome = trade.get("outcome", "")
    exit_px = trade.get("exit")
    mul = get_pip_multiplier(symbol)
    risk_pips = None
    if sl and entry:
        risk_pips = abs(entry - float(sl)) * mul
    risk_pips_safe = risk_pips if (risk_pips and risk_pips > 0) else 1.0
    # exit fallback: اگه exit نداریم از tp_price (win) یا sl_price (loss) استفاده کن
    exit_for_analyze = exit_px or (trade.get("tp_price") if outcome == "win" else (sl if outcome == "loss" else None))
    if exit_for_analyze and entry:
        raw_diff = (float(exit_for_analyze) - entry) if direction == "BUY" else (entry - float(exit_for_analyze))
        taken_pips = raw_diff * mul  # مثبت=برد، منفی=باخت
    else:
        taken_pips = 0.0
    taken_r = round(taken_pips / risk_pips_safe, 2)
    mfe_raw = float(trade.get("review_mfe") or trade.get("mfe_pip") or 0)
    # اگه win ولی MFE نداریم، از TP برآورد کن
    if mfe_raw == 0 and outcome == "win" and trade.get("tp_price") and trade.get("entry"):
        tp_dist = abs(float(trade["tp_price"]) - float(trade["entry"])) * get_pip_multiplier(trade.get("sym",""))
        mfe_raw = tp_dist
        print(f"[ANALYZE] MFE خالی — از TP fallback: {mfe_raw:.1f}")
    mfe_pip = mfe_raw
    if risk_pips_safe > 0 and mfe_pip > risk_pips_safe * 3.0:
        mfe_pip = risk_pips_safe * 3.0  # cap 3R
    mfe_r = round(mfe_pip / risk_pips_safe, 2)
    taken_pips_abs = abs(taken_pips)
    left_pip = round(mfe_pip - taken_pips_abs, 1) if mfe_pip > taken_pips_abs else 0
    left_r = round(left_pip / risk_pips_safe, 2)
    mfe_bsl = float(trade.get("mfe_before_sl_pip") or 0)
    mfe_bsl_r = round(mfe_bsl / risk_pips_safe, 2)
    rev_occurred = trade.get("review_reversal_occurred", False)
    rev_target = trade.get("review_reversal_target_pips") or 0
    rev_target_r = round(float(rev_target) / risk_pips_safe, 2)
    review_mfe_raw = trade.get("review_mfe")
    if review_mfe_raw is not None and risk_pips_safe > 0:
        passed_1r = float(review_mfe_raw) >= risk_pips_safe * 0.98
        print(f"[ANALYZE] {trade.get('sym')} review_mfe={review_mfe_raw} risk_pips={risk_pips_safe:.1f} → passed_1r={passed_1r}")
    else:
        passed_1r = trade.get("review_passed_1r", trade.get("passed_1r", False))
    fr_possible = trade.get("free_risk_was_possible", False)
    if not passed_1r:
        fr_possible = False
    fr_done = trade.get("review_free_risk_saved", False)
    pullback = trade.get("review_pullback", trade.get("pullback_after_1r", False))
    print(f"[ANALYZE] {trade.get('sym')} passed_1r={passed_1r} fr_possible={fr_possible} fr_done={fr_done} outcome={outcome}")
    lines = []
    if outcome == "loss":
        lines.append(f"استاپ خورد — ضرر {abs(taken_r):.1f}R" if taken_r else "استاپ خورد — ضرر نامشخص")
    else:
        lines.append(f"تارگت زده شد — سود {taken_r:.1f}R" if taken_r else "تارگت زده شد — سود نامشخص")
    if outcome == "loss" and mfe_bsl_r > 0.05:
        lines.append(f"قبل از استاپ تا {mfe_bsl_r:.1f}R سود رفت")
    if outcome == "loss" and rev_occurred and rev_target_r > 0:
        lines.append(f"بعد از استاپ تا {rev_target_r:.1f}R برگشت")
    if outcome == "win" and left_r > 0.2:
        lines.append(f"حداکثر سود {mfe_r:.1f}R بود — {left_r:.1f}R روی میز ماند")
    if fr_done:
        lines.append("فری‌ریسک انجام شد")
    elif passed_1r and fr_possible and outcome == "loss":
        lines.append("فری‌ریسک ممکن بود ولی انجام نشد")
    elif not passed_1r and outcome == "loss":
        lines.append("به 1R نرسید")
    if mfe_r >= 1 and outcome != "win":
        lines.append(f"قیمت تا {mfe_r:.1f}R رفت")
    summary_text = " | ".join(lines)

    # ─── پرامپت کامل برای AI ───
    mae_r_val = round(float(trade.get("review_mae") or trade.get("mae_pip") or 0) / risk_pips_safe, 2)
    entry_quality = "عالی (MAE<0.3R)" if mae_r_val < 0.3 else "متوسط (MAE 0.3-0.7R)" if mae_r_val < 0.7 else "ضعیف (MAE>0.7R)"
    note_text = (trade.get("review_note") or trade.get("note") or "ندارد")[:200]
    tf_val = trade.get("tf", "؟")
    sym_val = trade.get("sym", "؟")
    dir_val = trade.get("direction", "؟")

    ai_prompt = (
        f"یک ترید خاص رو تحلیل کن. فقط بر اساس این داده‌ها.\n\n"
        f"=== مشخصات ترید ===\n"
        f"نماد: {sym_val} | تایم‌فریم: {tf_val} | جهت: {dir_val} | نتیجه: {outcome}\n"
        f"ریسک (1R): {round(risk_pips_safe,1)} pip\n"
        f"R/R برنامه: {round(abs(float(trade.get('tp_price') or trade.get('entry') or 0) - float(trade.get('entry',0))) * get_pip_multiplier(symbol) / risk_pips_safe, 2) if trade.get('tp_price') else '؟'}\n\n"
        f"=== نتایج ===\n"
        f"R گرفته: {taken_r:+.2f}R\n"
        f"MFE (بیشترین سود بالقوه): {mfe_r:.2f}R\n"
        f"MAE (بیشترین برگشت منفی): {mae_r_val:.2f}R → کیفیت ورود: {entry_quality}\n"
        + (f"MFE قبل از SL: {mfe_bsl_r:.2f}R\n" if outcome == "loss" and mfe_bsl_r > 0.05 else "")
        + (f"برگشت بعد از SL: {rev_target_r:.2f}R\n" if outcome == "loss" and rev_occurred and rev_target_r > 0 else "")
        + (f"R روی میز موند: {left_r:.2f}R\n" if outcome == "win" and left_r > 0.1 else "")
        + f"\n=== مدیریت ===\n"
        f"به 1R رسید: {'بله' if passed_1r else 'خیر'}\n"
        f"فری‌ریسک ممکن بود: {'بله' if fr_possible else 'خیر'} | انجام شد: {'بله' if fr_done else 'خیر'}\n"
        f"Pullback به ورود: {'بله' if pullback else 'خیر'}\n\n"
        f"=== یادداشت تریدر ===\n{note_text}\n\n"
        f"تحلیل فارسی بنویس (۳-۵ جمله، بدون مقدمه):\n"
        f"1. این ترید چطور مدیریت شد؟ (زود بستی / دیر بستی / درست بستی)\n"
        f"2. کیفیت ورود چطور بود؟ MAE چه می‌گوید؟\n"
        f"3. اگه بهتر مدیریت می‌شد چقدر R بیشتر/کمتر ضرر داشتی؟\n"
        f"4. یک چیز مشخص که دفعه بعد باید فرق کنه.\n"
        f"اعداد دقیق بزن، کلی‌گویی نکن."
    )
    ai_result = groq_analyze(ai_prompt)
    trade["ai_analysis"] = ai_result or summary_text
    trade["ai_summary"] = summary_text
    save_trade(trade)
    return jsonify({"ok": True, "analysis": ai_result or summary_text, "summary": summary_text})

@app.route("/api/overall-analysis", methods=["GET"])
def overall_analysis():
    custom_prompt = request.args.get("custom_prompt","").strip()
    sym_filter = request.args.get("sym_filter","").strip().upper()
    mode = request.args.get("mode","all").strip()  # all | trades_only | zones_only
    print(f"[AI] overall_analysis — sym={sym_filter or 'همه'} mode={mode} custom={'بله' if custom_prompt else 'خیر'}")
    trades = load_journal()
    # watching = TP خورده، برد ثبت شده، منتظر SL/3R برای چارت — در آنالیز کلی به عنوان win حساب می‌شه
    closed = [t for t in trades if t.get("status") in ("closed", "watching") and t.get("outcome") in ("win", "loss")]
    if sym_filter:
        closed = [t for t in closed if t.get("sym","").upper() == sym_filter]
        print(f"[AI] بعد از فیلتر نماد: {len(closed)} ترید")
    if mode == "trades_only":
        closed = [t for t in closed if not t.get("is_missed_zone")]
        print(f"[AI] فقط تریدها: {len(closed)}")
    elif mode == "zones_only":
        closed = [t for t in closed if t.get("is_missed_zone")]
        print(f"[AI] فقط نواحی جا مونده: {len(closed)}")
    if not closed:
        mode_label = {"trades_only":"فقط تریدها","zones_only":"فقط نواحی جا مونده"}.get(mode,"")
        err = f"هیچ رکوردی برای {mode_label}{(' نماد '+sym_filter) if sym_filter else ''} وجود ندارد"
        print(f"[AI] ❌ {err}")
        return jsonify({"ok": False, "error": err}), 404
    total = len(closed)
    wins = sum(1 for t in closed if t.get("outcome") == "win")
    losses = total - wins
    wr = round(wins/total*100, 1) if total else 0
    def _et(t):
        mul = get_pip_multiplier(t["sym"])
        rp = None
        if t.get("sl_price") and t.get("entry"):
            rp = abs(float(t["entry"]) - float(t["sl_price"])) * mul
        mfe = float(t.get("review_mfe") or t.get("mfe_pip") or 0)
        return t.get("exit_type") or calc_exit_type(t.get("outcome",""), rp, mfe, t.get("found_3r", False))
    win_tp_count  = sum(1 for t in closed if _et(t) == "tp")
    win_3r_count  = sum(1 for t in closed if _et(t) == "tp3")
    win_manual_count = sum(1 for t in closed if _et(t) == "manual")

    early_exit_count = 0
    early_exit_left_r_list = []

    fr_missed_count = 0
    free_risk_done_count = 0
    reached_1r_count = 0
    reached_1_5r_count = 0
    reached_2r_count = 0
    reached_3r_count = 0
    taken_r_list = []
    mfe_r_list = []
    sym_stats = {}
    hour_stats = {}
    mae_ratio_list = []
    trade_details = []
    planned_rr_list, rr_gap_list = [], []
    rr_bucket = {"<1.5":{"w":0,"l":0},"1.5-2":{"w":0,"l":0},"2-3":{"w":0,"l":0},"3+":{"w":0,"l":0}}
    setup_stats = {}
    detail_1r, detail_2r, detail_3r, detail_sl_rev, detail_early = [], [], [], [], []
    detail_fr_missed, detail_fr_done = [], []
    reversal_r_list = []

    # ─── محاسبات جدید ───
    hypothetical_3r_list = []   # اگه همه تریدها تا 3R نگه میداشتیم
    entry_quality_list = []     # MAE/risk — هرچی کمتر entry بهتر
    streak_results = []         # ترتیب نتایج برای streak analysis
    day_stats = {}              # آمار روز هفته

    for idx_t, t in enumerate(closed):
        sym = t["sym"]
        mul = get_pip_multiplier(sym)
        entry = float(t["entry"])
        sl_px = t.get("sl_price")
        exit_px = t.get("exit")
        outcome = t["outcome"]
        direction = t["direction"]
        mfe_pip = float(t.get("review_mfe") or t.get("mfe_pip") or 0)
        # fallback: اگه WIN و MFE=0، از TP تخمین بزن (همانند analyze_trade)
        if mfe_pip == 0 and t.get("outcome") == "win" and t.get("tp_price") and t.get("entry"):
            mfe_pip = abs(float(t["tp_price"]) - float(t["entry"])) * get_pip_multiplier(t.get("sym","EURUSD"))
        mae_pip = float(t.get("review_mae") or t.get("mae_pip") or 0)
        rev_occurred = t.get("review_reversal_occurred", False)
        rev_target_pips = t.get("review_reversal_target_pips")
        _rmfe = t.get("review_mfe")
        _rp_raw = (abs(float(t.get("entry",0)) - float(t.get("sl_price",0))) * get_pip_multiplier(t.get("sym","EURUSD"))) if t.get("sl_price") and t.get("entry") else None
        if _rmfe is not None and _rp_raw and _rp_raw > 0:
            passed_1r = float(_rmfe) >= _rp_raw * 0.98
            fr_possible = t.get("free_risk_was_possible", False) if passed_1r else False
        else:
            fr_possible = t.get("free_risk_was_possible", False)
            passed_1r = t.get("passed_1r", False)
        found_3r = t.get("found_3r", False)
        entry_time = t.get("entryTime", "")
        free_risk_done = t.get("review_free_risk_saved", False)

        # ─── risk_pips: اگه SL نداره، این ترید از همه میانگین‌ها حذف می‌شه ───
        risk_pips = None
        if sl_px and entry:
            risk_pips = abs(float(entry) - float(sl_px)) * mul
        has_valid_risk = risk_pips and risk_pips > 0

        # مقداردهی اولیه — برای جلوگیری از NameError در else block
        mbe_r = 0.0

        # ─── R محاسبه‌ها (فقط وقتی risk معتبر داره) ───
        if has_valid_risk:
            # taken_r: برد مثبت، باخت منفی — با fallback به tp/sl اگه exit خالیه
            exit_for_r = exit_px or (t.get("tp_price") if outcome == "win" else (sl_px if outcome == "loss" else None))
            if exit_for_r:
                raw_pips = (float(exit_for_r) - entry) if direction == "BUY" else (entry - float(exit_for_r))
                taken_r = round(raw_pips * mul / risk_pips, 2)
            else:
                taken_r = 0.0
            mfe_r  = round(mfe_pip  / risk_pips, 2)
            mae_r  = round(mae_pip  / risk_pips, 2)
            # left_r: فقط برای win — چقدر از MFE نگرفتیم
            taken_abs_r = abs(taken_r)
            left_r = round(mfe_r - taken_abs_r, 2) if (outcome == "win" and mfe_r > taken_abs_r) else 0.0
        else:
            # ترید بدون SL — R قابل محاسبه نیست، از همه lists حذف می‌شه
            taken_r = 0.0
            mfe_r = 0.0
            mae_r = 0.0
            left_r = 0.0

        exit_type = t.get("exit_type") or calc_exit_type(
            outcome, risk_pips,
            float(t.get("review_mfe") or t.get("mfe_pip") or 0),
            t.get("found_3r", False)
        )
        exit_type_label = {"sl": "SL", "tp": "TP(زودخروج)", "tp3": "3R(کامل)"}.get(exit_type, exit_type)

        # ─── محاسبات جدید ───
        if has_valid_risk:
            # ۱. hypothetical 3R: اگه همه تریدها تا 3R نگه می‌داشتیم چی می‌شد؟
            # باگ‌فیکس: برای win، hypothetical باید mfe_r باشه نه taken_r
            # taken_r ممکنه کمتر از mfe_r باشه (زود بستیم)
            if outcome == "win":
                h3r = min(3.0, mfe_r) if mfe_r > 0 else taken_r
            else:
                h3r = -1.0  # باخت همیشه -1R
            hypothetical_3r_list.append(h3r)

            # ۲. entry quality: MAE/risk (0=عالی، 1=یه ریسک کامل برگشت)
            if mae_pip > 0:
                eq = round(mae_r, 2)  # کمتر = ورود بهتر
                entry_quality_list.append(eq)

        # ۳. streak tracking (ترتیب نتایج)
        streak_results.append("W" if outcome == "win" else "L")

        # ۴. روز هفته
        try:
            et_str = str(entry_time).replace("T", " ").strip()
            if len(et_str) >= 10:
                from datetime import datetime as _dt
                day_num = _dt.strptime(et_str[:10], "%Y-%m-%d").weekday()  # 0=Mon
                day_name = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][day_num]
                day_stats.setdefault(day_name, {"wins": 0, "losses": 0, "r": 0.0})
                if outcome == "win": day_stats[day_name]["wins"] += 1
                else: day_stats[day_name]["losses"] += 1
                if has_valid_risk: day_stats[day_name]["r"] += taken_r
        except: pass

        # ─── فقط تریدهای با SL معتبر وارد میانگین‌ها می‌شن ───
        if has_valid_risk:
            taken_r_list.append(taken_r)   # برد: مثبت | باخت: منفی
            if mfe_pip > 0: mfe_r_list.append(mfe_r)
            if mae_pip > 0: mae_ratio_list.append(mae_r)

        # ─── R/R برنامه (planned) ───
        tp_px = t.get("tp_price")
        planned_rr = None
        if has_valid_risk and tp_px and entry:
            tp_dist = abs(float(tp_px) - entry) * mul
            planned_rr = round(tp_dist / risk_pips, 2)
            # فقط planned_rr های معقول (بین 0.5 و 10)
            if 0.5 <= planned_rr <= 10:
                planned_rr_list.append(planned_rr)
                rr_gap_list.append(round(taken_r - planned_rr, 2))  # منفی=کمتر از plan گرفتیم
        if planned_rr and 0.5 <= planned_rr <= 10:
            bkt = "<1.5" if planned_rr < 1.5 else "1.5-2" if planned_rr < 2 else "2-3" if planned_rr < 3 else "3+"
            if outcome == "win": rr_bucket[bkt]["w"] += 1
            else: rr_bucket[bkt]["l"] += 1

        # ─── setup stats (فقط اگه setup_type وارد شده) ───
        stype = (t.get("setup_type") or "").strip()
        if stype:
            setup_stats.setdefault(stype, {"w": 0, "l": 0, "r": 0.0})
            if outcome == "win": setup_stats[stype]["w"] += 1
            else: setup_stats[stype]["l"] += 1
            if has_valid_risk: setup_stats[stype]["r"] += taken_r

        # ─── sym stats براساس R (نه pnl خام) ───
        sym_stats.setdefault(sym, {"wins": 0, "losses": 0, "total_r": 0.0})
        if outcome == "win": sym_stats[sym]["wins"] += 1
        else: sym_stats[sym]["losses"] += 1
        if has_valid_risk: sym_stats[sym]["total_r"] += taken_r

        # ─── ساعت ورود ───
        try:
            et = str(entry_time).replace("T", " ").strip()
            if len(et) >= 13:
                hour = int(et[11:13])
                if 0 <= hour <= 23:
                    hour_stats.setdefault(hour, {"wins": 0, "losses": 0})
                    if outcome == "win": hour_stats[hour]["wins"] += 1
                    else: hour_stats[hour]["losses"] += 1
        except:
            pass

        display_num = len(closed) - idx_t
        sl_r_show  = f"{risk_pips:.1f}p({1:.0f}R)" if has_valid_risk else "—"
        tp_r_show  = f"{planned_rr:.2f}R" if planned_rr else "—"
        entry_time_short = str(t.get("entryTime", ""))[:16]
        is_manual_mfe = t.get("review_mfe") is not None

        tshort = {
            "idx": display_num, "tid": t["id"], "sym": sym, "dir": direction,
            "tf": t.get("tf", ""), "outcome": outcome, "entry": entry,
            "exit": float(t.get("exit") or t.get("tp_price") or t.get("sl_price") or 0), "sl_price": t.get("sl_price"),
            "tp_price": t.get("tp_price"),
            "sl_pips": round(risk_pips, 1) if has_valid_risk else None,
            "tp_pips": round(abs(float(tp_px or 0) - entry) * mul, 1) if tp_px else None,
            "taken_r": taken_r, "mfe_r": mfe_r, "left_r": left_r,
            "entry_time": entry_time_short, "is_manual": is_manual_mfe,
            "note": (t.get("note") or "")[:80],
        }

        # ─── reached counts — WIN و LOSS کاملاً جدا ───
        if outcome == "win":
            # بردها: براساس MFE یا passed_1r
            if has_valid_risk:
                if mfe_r >= 1.0 or passed_1r:
                    reached_1r_count += 1
                    detail_1r.append({**tshort, "detail": f"MFE={mfe_r:.2f}R taken={taken_r:.2f}R",
                        "extra": {"mfe_r": mfe_r, "taken_r": taken_r, "left_r": left_r, "is_manual": is_manual_mfe}})
                if mfe_r >= 1.5: reached_1_5r_count += 1
                if mfe_r >= 2.0:
                    reached_2r_count += 1
                    detail_2r.append({**tshort, "detail": f"MFE={mfe_r:.2f}R taken={taken_r:.2f}R",
                        "extra": {"mfe_r": mfe_r, "taken_r": taken_r}})
                if found_3r or mfe_r >= 3.0:
                    reached_3r_count += 1
                    detail_3r.append({**tshort, "detail": f"MFE={mfe_r:.2f}R taken={taken_r:.2f}R",
                        "extra": {"mfe_r": mfe_r}})
                if left_r > 0.3:
                    early_exit_count += 1
                    early_exit_left_r_list.append(left_r)
                    detail_early.append({**tshort,
                        "detail": f"taken={taken_r:.2f}R | MFE={mfe_r:.2f}R | {left_r:.2f}R جا موند",
                        "extra": {"taken_r": taken_r, "mfe_r": mfe_r, "left_r": left_r}})
        else:
            # باخت‌ها
            # باگ‌فیکس: برای باخت، review_mfe = MFE قبل از SL (کاربر وارد کرده)
            # اگه review_mfe نداشت، از mfe_before_sl_pip بخون نه mfe_pip
            # mfe_pip برای باخت = MFE کل ترید (که ممکنه بعد از SL هم ادامه داشته باشه)
            mbe_pip = float(
                t.get("review_mfe")             # اول review دستی (پیپ)
                or t.get("mfe_before_sl_pip")   # بعد مقدار سیستمی قبل از SL
                or 0
            )
            mbe_r = round(mbe_pip / risk_pips, 2) if has_valid_risk else 0.0

            psl_manual = t.get("review_reversal_target_pips")
            # فقط دستی — سیستمی حذف شد
            psl_pips = float(psl_manual) if psl_manual is not None else 0
            psl_r = round(psl_pips / risk_pips, 2) if (has_valid_risk and psl_pips > 0) else 0.0

            rev_manual = t.get("review_reversal_occurred")
            # فقط تأیید دستی (rev_manual is True) — None و False هیچ‌کدام حساب نمیشن
            actual_rev = (rev_manual is True) and psl_r > 0

            # ─ MFE قبل از SL — فقط اگه برگشت دستی تأیید نشده
            if has_valid_risk and mbe_r >= 1.0 and not actual_rev:
                reached_1r_count += 1
                detail_1r.append({**tshort,
                    "detail": f"MFE قبل SL={mbe_r:.2f}R (استاپ خورد)",
                    "extra": {"mbe_r": mbe_r}})
                if mbe_r >= 2.0:
                    reached_2r_count += 1
                    detail_2r.append({**tshort, "detail": f"MFE قبل SL={mbe_r:.2f}R", "extra": {"mbe_r": mbe_r}})
                if mbe_r >= 3.0:
                    reached_3r_count += 1
                    detail_3r.append({**tshort, "detail": f"MFE قبل SL={mbe_r:.2f}R", "extra": {"mbe_r": mbe_r}})

            # ─ برگشت بعد SL — فقط دستی تأیید شده
            if actual_rev:
                lvls = []
                from_sl = t.get("review_reversal_from_sl")
                from_sl_r = round(float(from_sl) / risk_pips, 2) if (from_sl and has_valid_risk) else None
                from_sl_str = f" از {from_sl_r:.2f}R بعد SL" if from_sl_r else ""
                detail_entry = {**tshort,
                    "detail": f"SL خورد ← برگشت {psl_r:.2f}R{from_sl_str}",
                    "extra": {"psl_r": psl_r, "is_manual": rev_manual is True, "from_sl_r": from_sl_r}}
                if psl_r >= 1.0:
                    reached_1r_count += 1; lvls.append("→1R")
                    detail_1r.append({**detail_entry, "detail": f"SL خورد ← برگشت {psl_r:.2f}R (بعد SL){from_sl_str}"})
                if psl_r >= 1.5:
                    reached_1_5r_count += 1; lvls.append("→1.5R")
                if psl_r >= 2.0:
                    reached_2r_count += 1; lvls.append("→2R")
                    detail_2r.append({**detail_entry, "detail": f"SL خورد ← برگشت {psl_r:.2f}R{from_sl_str}"})
                if psl_r >= 3.0:
                    reached_3r_count += 1; lvls.append("→3R")
                    detail_3r.append({**detail_entry, "detail": f"SL خورد ← برگشت {psl_r:.2f}R{from_sl_str}"})
                detail_sl_rev.append({**tshort,
                    "detail": f"برگشت={psl_r:.2f}R {' '.join(lvls)}{from_sl_str}",
                    "extra": {"psl_r": psl_r, "is_manual": rev_manual is True, "from_sl_r": from_sl_r}})
                reversal_r_list.append(psl_r)


        # ─── فری‌ریسک ───
        # باگ‌فیکس: تریدهای manual (زود بسته) که passed_1r بودن
        # هم باید در fr_missed حساب بشن — چون به 1R رسیدی ولی نگه نداشتی
        _et_this = t.get("exit_type") or calc_exit_type(outcome, risk_pips,
            float(t.get("review_mfe") or t.get("mfe_pip") or 0), t.get("found_3r", False))
        if outcome == "loss":
            if free_risk_done:
                free_risk_done_count += 1
                detail_fr_done.append({**tshort, "detail": f"SL={round(risk_pips,1) if has_valid_risk else '—'}p | {entry_time_short}"})
            elif fr_possible:
                fr_missed_count += 1
                detail_fr_missed.append({**tshort, "detail": f"SL={round(risk_pips,1) if has_valid_risk else '—'}p | {entry_time_short}"})
        elif outcome == "win" and _et_this == "manual" and passed_1r and has_valid_risk:
            # زود بستن بعد از 1R — missed opportunity برای نگه‌داشتن بیشتر
            detail_fr_missed.append({**tshort, "detail": f"manual بسته شد بعد از {taken_r:.1f}R (1R رسیده بود) | {entry_time_short}"})

        # ─── خلاصه متنی برای AI ───
        mfe_r_str  = f"{mfe_r:.2f}R"  if (has_valid_risk and mfe_r)  else "—"
        mae_r_str  = f"{mae_r:.2f}R"  if (has_valid_risk and mae_r)  else "—"
        mbe_r_str  = f"{mbe_r:.2f}R"  if (outcome == "loss" and has_valid_risk and mbe_r > 0) else "—"
        fr_str = "فری‌ریسک✓" if free_risk_done else ("فری‌ریسک✗(ممکن)" if fr_possible else "")
        pb_str = "pullback✓" if t.get("review_pullback") else ""
        note_short = (t.get("review_note") or t.get("note") or "")[:60]
        risk_r_show = f"SL={round(risk_pips,1) if has_valid_risk else '?'}p(1R) TP={tp_r_show}"
        detail_parts = [
            f"{sym}/{t.get('tf','?')} {direction}:{outcome}({exit_type_label})",
            risk_r_show,
            f"taken={taken_r:+.2f}R MFE={mfe_r_str} MAE={mae_r_str}",
        ]
        if outcome == "loss" and mbe_r > 0:
            detail_parts.append(f"MFEbeforeSL={mbe_r_str}")
        if fr_str: detail_parts.append(fr_str)
        if pb_str: detail_parts.append(pb_str)
        if note_short: detail_parts.append(f"note:{note_short}")
        trade_details.append(f"• {' | '.join(detail_parts)}")

    avg_taken_r = round(sum(taken_r_list)/len(taken_r_list), 2) if taken_r_list else 0
    avg_mfe_r = round(sum(mfe_r_list)/len(mfe_r_list), 2) if mfe_r_list else 0
    avg_planned_rr = round(sum(planned_rr_list)/len(planned_rr_list),2) if planned_rr_list else 0
    avg_rr_gap = round(sum(rr_gap_list)/len(rr_gap_list),2) if rr_gap_list else 0
    avg_reversal_r = round(sum(reversal_r_list)/len(reversal_r_list),2) if reversal_r_list else 0

    # ─── محاسبات جدید ───
    # ۱. hypothetical 3R
    total_actual_r = round(sum(taken_r_list), 2) if taken_r_list else 0
    total_hypothetical_3r = round(sum(hypothetical_3r_list), 2) if hypothetical_3r_list else 0
    gain_if_held = round(total_hypothetical_3r - total_actual_r, 2)

    # ۲. entry quality
    avg_entry_quality = round(sum(entry_quality_list)/len(entry_quality_list), 2) if entry_quality_list else 0
    # درصد تریدها با MAE < 0.3R (ورود عالی)
    clean_entries = sum(1 for q in entry_quality_list if q < 0.3)
    clean_entry_pct = round(clean_entries / len(entry_quality_list) * 100) if entry_quality_list else 0

    # ۳. streak analysis
    max_win_streak = max_loss_streak = cur_win = cur_loss = 0
    after_loss_results = []  # نتیجه ترید بعد از باخت
    after_2loss_results = [] # نتیجه ترید بعد از ۲ باخت پشت سر هم
    for i, r in enumerate(streak_results):
        if r == "W":
            cur_win += 1; cur_loss = 0
            max_win_streak = max(max_win_streak, cur_win)
        else:
            cur_loss += 1; cur_win = 0
            max_loss_streak = max(max_loss_streak, cur_loss)
        if i > 0 and streak_results[i-1] == "L":
            after_loss_results.append(r)
        if i > 1 and streak_results[i-2] == "L" and streak_results[i-1] == "L":
            after_2loss_results.append(r)
    wr_after_loss = round(after_loss_results.count("W")/len(after_loss_results)*100) if after_loss_results else None
    wr_after_2loss = round(after_2loss_results.count("W")/len(after_2loss_results)*100) if after_2loss_results else None

    # ۴. بهترین/بدترین روز هفته
    best_day = worst_day = "—"
    day_lines = []
    if day_stats:
        def day_wr(d): s=day_stats[d]; t=s["wins"]+s["losses"]; return s["wins"]/t if t else 0
        best_day = max(day_stats, key=day_wr)
        worst_day = min(day_stats, key=day_wr)
        for d, v in day_stats.items():
            tot = v["wins"]+v["losses"]
            if tot: day_lines.append(f"{d}: {round(v['wins']/tot*100)}% ({v['wins']}/{tot}) R:{v['r']:+.2f}R")
    rr_bucket_lines = [
        f"R/R {b}: {round(v['w']/(v['w']+v['l'])*100)}% برد ({v['w']}/{v['w']+v['l']})"
        for b,v in rr_bucket.items() if v['w']+v['l']>0
    ]
    # setup_lines فقط اگه setup_type واقعی وجود داشته باشه
    setup_lines = [
        f"{st}: {round(v['w']/(v['w']+v['l'])*100)}% برد ({v['w']}/{v['w']+v['l']}) R:{v['r']:+.2f}R"
        for st,v in sorted(setup_stats.items(), key=lambda x:-(x[1]['w']+x[1]['l']))
        if v['w']+v['l']>0 and st
    ]
    trade_detail_data = {
        "r1": detail_1r, "r2": detail_2r, "r3": detail_3r,
        "sl_rev": detail_sl_rev, "early": detail_early,
        "fr_missed": detail_fr_missed, "fr_done": detail_fr_done,
        "avg_reversal_r": avg_reversal_r  # ← R نه پیپ
    }
    avg_early_r = round(sum(early_exit_left_r_list)/len(early_exit_left_r_list), 2) if early_exit_left_r_list else 0
    avg_mae_r = round(sum(mae_ratio_list)/len(mae_ratio_list), 2) if mae_ratio_list else 0

    # best/worst sym براساس total_r نه pnl خام
    best_sym = max(sym_stats, key=lambda s: sym_stats[s]["total_r"]) if sym_stats else "—"
    worst_sym = min(sym_stats, key=lambda s: sym_stats[s]["total_r"]) if sym_stats else "—"
    best_hour = worst_hour = "—"
    if hour_stats:
        def hour_wr(h):
            s = hour_stats[h]
            tot = s["wins"] + s["losses"]
            return s["wins"]/tot if tot else 0
        best_hour = "%02d:00" % max(hour_stats, key=hour_wr)
        worst_hour = "%02d:00" % min(hour_stats, key=hour_wr)
    sym_lines = []
    for s, v in sym_stats.items():
        tot = v["wins"] + v["losses"]
        wr_s = round(v["wins"]/tot*100) if tot else 0
        sym_lines.append(f"{s}: {wr_s}% برد ({v['wins']}/{tot}) | R:{v['total_r']:+.2f}R")
    numeric = {
        "total": total, "wins": wins, "losses": losses, "winrate": wr,
        "win_tp_count": win_tp_count, "win_3r_count": win_3r_count, "win_manual_count": win_manual_count,
        "avg_taken_r": avg_taken_r, "avg_mfe_r": avg_mfe_r,
        "early_exit_count": early_exit_count, "early_exit_avg_pip": avg_early_r,
        "fr_missed_count": fr_missed_count,
        "free_risk_done_count": free_risk_done_count,
        "reached_1r_count": reached_1r_count,
        "reached_1_5r_count": reached_1_5r_count,
        "reached_2r_count": reached_2r_count,
        "reached_3r_count": reached_3r_count,
        "avg_mae_ratio": avg_mae_r,
        "best_sym": best_sym, "worst_sym": worst_sym,
        "best_hour": best_hour, "worst_hour": worst_hour,
        "avg_planned_rr": avg_planned_rr, "avg_rr_gap": avg_rr_gap,
        "rr_bucket_lines": rr_bucket_lines, "setup_lines": setup_lines,
        "trade_detail_data": trade_detail_data,
        # جدید
        "total_actual_r": total_actual_r,
        "total_hypothetical_3r": total_hypothetical_3r,
        "gain_if_held": gain_if_held,
        "avg_entry_quality": avg_entry_quality,
        "clean_entry_pct": clean_entry_pct,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
        "wr_after_loss": wr_after_loss,
        "wr_after_2loss": wr_after_2loss,
        "best_day": best_day, "worst_day": worst_day,
        "day_lines": day_lines,
    }
    no_sl_count = total - len(taken_r_list)  # تریدهای بدون SL
    streak_str = "".join(streak_results[-20:])  # ۲۰ ترید آخر
    prompt = (
        f"تو یک تحلیلگر حرفه‌ای داده ترید هستی. فقط بر اساس اعداد زیر تحلیل کن.\n"
        f"⚠️ همه اعداد R-based هستند (1R = ریسک هر ترید). پیپ استفاده نکن.\n\n"
        f"=== آمار کلی ===\n"
        f"تعداد: {total} | برد: {wins} | باخت: {losses} | نرخ برد: {wr}%\n"
        f"{'⚠️ '+str(no_sl_count)+' ترید بدون SL (از محاسبات R حذف شدند)\n' if no_sl_count else ''}"
        f"بردها: {win_3r_count} تا 3R کامل | {win_tp_count} تا TP زودخروج" + (f" | {win_manual_count} تا بسته‌شدن دستی قبل از TP" if win_manual_count else "") + "\n"
        f"R واقعی گرفته شده: {total_actual_r:+.2f}R | اگه همه تا 3R نگه می‌داشتیم: {total_hypothetical_3r:+.2f}R\n"
        f"→ با نگه داشتن تا 3R، {gain_if_held:+.2f}R بیشتر/کمتر می‌گرفتیم\n"
        f"میانگین R گرفته: {avg_taken_r:+.2f}R | میانگین MFE: {avg_mfe_r:.2f}R | میانگین MAE: {avg_mae_r:.2f}R\n\n"
        f"=== کیفیت ورود (Entry Quality) ===\n"
        f"میانگین MAE/R: {avg_entry_quality:.2f}R (هرچی کمتر ورود بهتر)\n"
        f"ورودهای تمیز (MAE<0.3R): {clean_entry_pct}% از تریدها\n\n"
        f"=== رسیدن به سطوح ریوارد ===\n"
        f"1R: {reached_1r_count}/{total} | 1.5R: {reached_1_5r_count}/{total} | 2R: {reached_2r_count}/{total} | 3R: {reached_3r_count}/{total}\n\n"
        f"=== مدیریت معامله ===\n"
        f"زود بستیم: {early_exit_count} تا (میانگین {avg_early_r:.2f}R روی میز موند)\n"
        f"میانگین برگشت بعد SL: {avg_reversal_r:.2f}R\n"
        f"فری‌ریسک ممکن بود ولی SL خورد: {fr_missed_count} | فری‌ریسک انجام شد: {free_risk_done_count}\n\n"
        f"=== Streak Analysis ===\n"
        f"بیشترین برد پشت سر هم: {max_win_streak} | بیشترین باخت: {max_loss_streak}\n"
        f"نرخ برد بعد از ۱ باخت: {wr_after_loss}%" + (f" ({len(after_loss_results)} نمونه)" if after_loss_results else " (کافی نیست)") + "\n"
        f"نرخ برد بعد از ۲ باخت پشت سر هم: {wr_after_2loss}%" + (f" ({len(after_2loss_results)} نمونه)" if after_2loss_results else " (کافی نیست)") + "\n"
        f"ترتیب ۲۰ ترید آخر: {streak_str}\n\n"
        f"=== بهترین/بدترین روز هفته ===\n"
        + ("\n".join(day_lines) if day_lines else "داده کافی نیست") + "\n\n"
        f"=== نمادها ===\n" + "\n".join(sym_lines) + "\n\n"
        f"=== ساعت: بهترین {best_hour} | بدترین {worst_hour} ===\n\n"
        f"=== خلاصه تریدها ===\n" + "\n".join(trade_details[:20]) + "\n\n"
        f"گزارش فارسی بنویس (بدون مقدمه):\n"
        f"1. آمار کلی یک جمله\n"
        f"2. تحلیل «اگه تا 3R نگه می‌داشتی»: آیا ارزش داشت؟ چرا؟\n"
        f"3. کیفیت ورود: MAE میانگین {avg_entry_quality:.2f}R چه می‌گوید؟\n"
        f"4. سطوح ریوارد: چند درصد به 1R/2R/3R رسیدند — تارگت بهینه کجاست؟\n"
        f"5. مدیریت ریسک: فری‌ریسک و زودخروج\n"
        f"6. Streak: بعد از باخت چه اتفاقی می‌افتد؟ آیا overtrading یا revenge trading وجود دارد؟\n"
        f"7. بهترین روز/ساعت/نماد — بدترین کدام است؟\n"
        f"8. نمره کلی از 10 با دلیل.\n\n"
        f"مهم: اعداد دقیق، فرضی ننویس. همه R-based."
    )
    if custom_prompt:
        data_block = (
            f"\n\n=== داده‌های آماری (همه R-based) ===\n"
            f"تعداد:{total}|برد:{wins}|باخت:{losses}|نرخ برد:{wr}%\n"
            f"R گرفته:{avg_taken_r:+.2f}R|MFE:{avg_mfe_r:.2f}R|MAE:{avg_mae_r:.2f}R\n"
            f"R/R برنامه:{avg_planned_rr}|اختلاف:{avg_rr_gap:+.2f}R\n"
            f"میانگین برگشت بعد SL:{avg_reversal_r:.2f}R\n"
            f"رسیدن 1R:{reached_1r_count}|2R:{reached_2r_count}|3R:{reached_3r_count}\n"
            f"فری‌ریسک ممکن:{fr_missed_count}|انجام شد:{free_risk_done_count}\n"
            f"نرخ برد R/R:{' | '.join(rr_bucket_lines)}\n"
            f"{'ستاپ‌ها:'+' | '.join(setup_lines[:4]) if setup_lines else 'ستاپ‌ها: وارد نشده'}\n"
            f"بهترین:{best_sym}|بدترین:{worst_sym}|بهترین ساعت:{best_hour}|بدترین:{worst_hour}\n"
            f"خلاصه:\n" + "\n".join(trade_details[:15])
        )
        final_prompt = custom_prompt + data_block
    else:
        final_prompt = prompt
    analysis = groq_analyze(final_prompt)
    return jsonify({"ok": True, "analysis": analysis, **numeric})

@app.route("/api/ohlc/<symbol>/<tf>", methods=["GET"])
def get_ohlc_proxy(symbol, tf):
    limit = request.args.get("limit", 200)
    url = f"https://biquote.io/api/{symbol.upper()}/ohlc?interval={tf}&limit={limit}"
    try:
        r = requests.get(url, timeout=12, headers=H)
        r.raise_for_status()
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/chart/<trade_id>", methods=["GET"])
def get_chart(trade_id):
    journal = load_journal()
    trade = next((t for t in journal if t["id"] == trade_id), None)
    if not trade:
        return jsonify({"ok": False, "error": "ترید پیدا نشد"}), 404
    sym = trade["sym"]
    tf = trade.get("tf", "1h")
    entry = float(trade["entry"])
    sl = trade.get("sl_price")
    tp = trade.get("tp_price")
    entry_time_str = trade.get("entryTime", "")
    direction = trade.get("direction", "BUY")
    outcome = trade.get("outcome", "")
    exit_px = trade.get("exit")
    mul = get_pip_multiplier(sym)
    status = trade.get("status", "closed")
    risk_pips = None
    reward_levels = {}
    if sl and entry:
        if direction == "BUY":
            risk_pips = (entry - float(sl)) * mul
            reward_levels = {"r1": entry + risk_pips / mul, "r2": entry + 2 * risk_pips / mul, "r3": entry + 3 * risk_pips / mul}
        else:
            risk_pips = (float(sl) - entry) * mul
            reward_levels = {"r1": entry - risk_pips / mul, "r2": entry - 2 * risk_pips / mul, "r3": entry - 3 * risk_pips / mul}
    snapshot = trade.get("candle_snapshot", [])
    candles = []
    if snapshot:
        def candle_dt(b):
            t = b.get("t")
            if isinstance(t, (int, float)):
                return datetime.utcfromtimestamp(t)
            try: return datetime.strptime(str(t), "%Y-%m-%d %H:%M")
            except:
                try: return datetime.strptime(str(t), "%Y-%m-%d %H:%M:%S")
                except: return datetime.utcfromtimestamp(0)

        def candle_t_str(b):
            t = b.get("t")
            if isinstance(t, (int, float)):
                dt_teh = datetime.utcfromtimestamp(t) + timedelta(hours=3, minutes=30)
                return dt_teh.strftime("%Y-%m-%d %H:%M")
            return str(t)

        entry_utc = tehran_to_utc(entry_time_str)
        entry_idx_snap = 0
        for i, b in enumerate(snapshot):
            try:
                bt_utc = candle_dt(b)
                if bt_utc >= (entry_utc or datetime.utcfromtimestamp(0)):
                    entry_idx_snap = i
                    break
            except: pass

        for i, b in enumerate(snapshot):
            phase = "before" if i < entry_idx_snap else ("hit" if i == len(snapshot)-1 else "after")
            candles.append({"t": candle_t_str(b), "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"], "phase": phase})
    else:
        # fallback: از API بگیریم (به ندرت)
        try:
            tf_chart_limits = {"1m":5000,"5m":3000,"15m":800,"1h":200,"4h":50,"1d":30}
            chart_limit = tf_chart_limits.get(tf, 200)
            url = f"https://biquote.io/api/{sym}/ohlc?interval={tf}&limit={chart_limit}"
            print(f"[CHART] fallback fetch {sym} tf={tf} limit={chart_limit}")
            r = requests.get(url, timeout=12, headers=H)
            raw = r.json()
            bars = raw.get("bars") or raw.get("data") or (raw if isinstance(raw, list) else [])
        except Exception as e:
            return jsonify({"ok": False, "error": f"خطا در دریافت کندل: {e}"}), 502
        if not bars:
            return jsonify({"ok": False, "error": "کندلی دریافت نشد"}), 502
        def bar_dt(b):
            ts = b.get("openTime") or b.get("time") or b.get("timestamp") or ""
            try:
                if isinstance(ts, (int, float)):
                    return datetime.utcfromtimestamp(ts)
                return datetime.strptime(ts.replace("Z",""), "%Y-%m-%dT%H:%M:%S")
            except:
                return datetime.utcfromtimestamp(0)
        bars.sort(key=bar_dt)
        entry_utc = tehran_to_utc(entry_time_str)
        entry_idx = 0
        if entry_utc:
            for i, b in enumerate(bars):
                if bar_dt(b) >= entry_utc:
                    entry_idx = i
                    break
        end_idx = min(entry_idx + 100, len(bars) - 1)
        r3_price = reward_levels.get("r3") if reward_levels else None
        is_buy = direction == "BUY"
        for i in range(entry_idx, len(bars)):
            b = bars[i]
            high = float(b.get("high", 0))
            low  = float(b.get("low", 0))
            if is_buy:
                if sl and low <= float(sl):  end_idx = i; break
                if r3_price and high >= r3_price: end_idx = i; break
            else:
                if sl and high >= float(sl): end_idx = i; break
                if r3_price and low <= r3_price: end_idx = i; break
            end_idx = i
        start_idx = max(0, entry_idx - 20)
        visible = bars[start_idx:end_idx+1]
        rel_entry = entry_idx - start_idx
        for i, b in enumerate(visible):
            dt_teh = bar_dt(b) + timedelta(hours=3, minutes=30)
            phase = "before" if i < rel_entry else ("hit" if i == len(visible)-1 else "after")
            candles.append({"t": dt_teh.strftime("%Y-%m-%d %H:%M"), "o": float(b.get("open",0)), "h": float(b.get("high",0)), "l": float(b.get("low",0)), "c": float(b.get("close",0)), "phase": phase})
    return jsonify({
        "ok": True, "sym": sym, "tf": tf, "direction": direction, "outcome": outcome, "status": status,
        "entry": entry, "sl": float(sl) if sl else None, "tp": float(tp) if tp else None,
        "exit_px": float(exit_px) if exit_px else None, "risk_pips": round(risk_pips, 1) if risk_pips else None,
        "reward_levels": reward_levels, "candles": candles,
        "entry_candle_idx": next((i for i,c in enumerate(candles) if c["phase"] != "before"), 0),
    })

def merge_snapshot(existing, new_bars):
    """کندل‌های جدید رو به snapshot موجود اضافه کن — بدون تکرار، مرتب‌شده"""
    if not new_bars:
        return existing or []
    if not existing:
        return new_bars
    existing_times = {b.get("t") for b in existing}
    merged = list(existing) + [b for b in new_bars if b.get("t") not in existing_times]
    merged.sort(key=lambda b: b.get("t", ""))
    return merged

def poll_open_trades():
    time.sleep(30)
    while True:
        try:
            journal = load_journal()
            # هم open (هنوز TP/SL نخورده) هم watching (TP خورده، منتظر SL/3R برای چارت)
            pending = [t for t in journal if t.get("pending_check") and t.get("status") in ("open", "watching")]
            changed = False
            for trade in pending:
                sym = trade["sym"]
                tf = trade.get("tf", "1h")
                entry = float(trade["entry"])
                direction = trade.get("direction", "BUY")
                sl_price = trade.get("sl_price")
                mul = get_pip_multiplier(sym)
                is_watching = trade.get("status") == "watching"   # TP قبلاً خورده
                if sl_price:
                    risk = abs(entry - float(sl_price)) * mul
                    if direction == "BUY":
                        tp3 = entry + 3 * risk / mul
                    else:
                        tp3 = entry - 3 * risk / mul
                else:
                    tp3 = trade.get("tp_price")
                try:
                    tp_for_check = None if is_watching else trade.get("tp_price")
                    last_poll = trade.get("last_poll")  # آخرین چک — فقط کندل‌های جدیدتر بررسی میشن
                    res = check_sltp_hit_with_details(
                        sym, tf, trade["entryTime"], direction, entry,
                        sl_price, tp_for_check, 1.0, r3_override=tp3,
                        from_time_str=last_poll
                    )
                    (hit, hit_price, hit_time, last_close, pnl, mfe_pip, mae_pip, candle_lines,
                     found_3r, fr_possible, fr_saved, fr_at, pullback,
                     post_max, post_1r, post_1_5r, post_2r, post_3r,
                     mfe_before_sl, passed_1r, snapshot_bars) = res

                    if is_watching:
                        # ====== حالت watching: TP قبلاً خورده، منتظر SL/3R ======
                        if hit == "sl":
                            # SL خورد → بسته شو، بیشترین سود (mfe_pip) آپدیت
                            trade["status"] = "closed"
                            trade["pending_check"] = False
                            trade["mfe_pip"] = round(mfe_pip, 1)
                            trade["mae_pip"] = round(mae_pip, 1)
                            trade["free_risk_was_possible"] = fr_possible
                            trade["pullback_after_1r"] = pullback
                            trade["passed_1r"] = passed_1r
                            trade["candle_snapshot"] = snapshot_bars
                            trade["snapshot_locked"] = True
                            # exitNote آپدیت، outcome و exit همون TP قبلیه
                            trade["exitNote"] = "خودکار: تارگت — بعد SL خورد (چارت کامل شد)"
                            changed = True
                            print(f"[poll_watching] {sym} SL خورد بعد از TP — چارت بسته شد")
                        elif hit in ("tp3",) or (found_3r or (mfe_pip and sl_price and abs(entry - float(sl_price)) * mul > 0 and mfe_pip >= abs(entry - float(sl_price)) * mul * 3.0)):
                            # 3R زده شد → آپدیت به tp3
                            trade["status"] = "closed"
                            trade["pending_check"] = False
                            trade["found_3r"] = True
                            trade["exit_type"] = "tp3"
                            trade["mfe_pip"] = round(mfe_pip, 1)
                            trade["mae_pip"] = round(mae_pip, 1)
                            trade["free_risk_was_possible"] = fr_possible
                            trade["pullback_after_1r"] = pullback
                            trade["passed_1r"] = passed_1r
                            trade["candle_snapshot"] = snapshot_bars
                            trade["snapshot_locked"] = True
                            trade["exitNote"] = "خودکار: 3R کامل (بعد از تارگت)"
                            # exit_price و outcome همون win می‌مونه (یا می‌تونیم tp3_price بزاریم)
                            if tp3:
                                trade["exit"] = round(tp3, 5)
                            changed = True
                            print(f"[poll_watching] {sym} 3R زده شد بعد از TP — tp3")
                        else:
                            # هنوز تعیین تکلیف نشده — snapshot و آمار رو آپدیت کن
                            if not trade.get("snapshot_locked"):
                                trade["candle_snapshot"] = merge_snapshot(trade.get("candle_snapshot", []), snapshot_bars)
                            # آمارهای تجمعی رو با کندل‌های جدید آپدیت کن
                            if mfe_pip > float(trade.get("mfe_pip") or 0):
                                trade["mfe_pip"] = round(mfe_pip, 1)
                            trade["passed_1r"] = passed_1r or trade.get("passed_1r", False)
                            trade["free_risk_was_possible"] = fr_possible or trade.get("free_risk_was_possible", False)
                            trade["pullback_after_1r"] = pullback or trade.get("pullback_after_1r", False)
                            trade["last_poll"] = now_teh()
                            changed = True
                    else:
                        # ====== حالت open: هنوز هیچ چیز نخورده ======
                        if hit == "sl":
                            trade["exit"] = hit_price
                            trade["exitTime"] = trade.get("exitTime") or hit_time or now_teh()
                            trade["outcome"] = "loss"
                            trade["exit_type"] = "sl"
                            trade["status"] = "closed"
                            trade["pending_check"] = False
                            trade["mfe_pip"] = round(mfe_pip, 1)
                            trade["mae_pip"] = round(mae_pip, 1)
                            trade["found_3r"] = False
                            trade["free_risk_was_possible"] = fr_possible
                            trade["free_risk_saved"] = fr_saved
                            trade["pullback_after_1r"] = pullback
                            trade["mfe_before_sl_pip"] = round(mfe_before_sl, 1) if mfe_before_sl else 0
                            trade["passed_1r"] = passed_1r
                            trade["exitNote"] = "خودکار polling: SL"
                            diff = (hit_price - entry) if direction == "BUY" else (entry - hit_price)
                            trade["pnl"] = round(diff, 4)
                            trade["candle_snapshot"] = merge_snapshot(trade.get("candle_snapshot", []), snapshot_bars)
                            trade["snapshot_locked"] = True
                            changed = True
                            print(f"[poll_open] {sym} SL خورد")
                        elif hit == "tp3":
                            # 3R مستقیم → بسته
                            trade["exit"] = hit_price
                            trade["exitTime"] = trade.get("exitTime") or hit_time or now_teh()
                            trade["outcome"] = "win"
                            trade["exit_type"] = "tp3"
                            trade["status"] = "closed"
                            trade["pending_check"] = False
                            trade["mfe_pip"] = round(mfe_pip, 1)
                            trade["mae_pip"] = round(mae_pip, 1)
                            trade["found_3r"] = True
                            trade["free_risk_was_possible"] = fr_possible
                            trade["free_risk_saved"] = fr_saved
                            trade["pullback_after_1r"] = pullback
                            trade["mfe_before_sl_pip"] = round(mfe_before_sl, 1) if mfe_before_sl else 0
                            trade["passed_1r"] = passed_1r
                            trade["exitNote"] = "خودکار polling: 3R"
                            diff = (hit_price - entry) if direction == "BUY" else (entry - hit_price)
                            trade["pnl"] = round(diff, 4)
                            trade["candle_snapshot"] = merge_snapshot(trade.get("candle_snapshot", []), snapshot_bars)
                            trade["snapshot_locked"] = True
                            changed = True
                            print(f"[poll_open] {sym} 3R کامل")
                        elif hit == "tp":
                            # TP خورد → برد ثبت، watching
                            trade["exit"] = hit_price
                            trade["exitTime"] = trade.get("exitTime") or hit_time or now_teh()
                            trade["outcome"] = "win"
                            trade["exit_type"] = "tp"
                            trade["status"] = "watching"
                            trade["pending_check"] = True
                            trade["mfe_pip"] = round(mfe_pip, 1)
                            trade["mae_pip"] = round(mae_pip, 1)
                            trade["found_3r"] = False
                            trade["free_risk_was_possible"] = fr_possible
                            trade["free_risk_saved"] = fr_saved
                            trade["pullback_after_1r"] = pullback
                            trade["mfe_before_sl_pip"] = round(mfe_before_sl, 1) if mfe_before_sl else 0
                            trade["passed_1r"] = passed_1r
                            trade["exitNote"] = "خودکار polling: تارگت (watching برای SL/3R)"
                            diff = (hit_price - entry) if direction == "BUY" else (entry - hit_price)
                            trade["pnl"] = round(diff, 4)
                            trade["candle_snapshot"] = merge_snapshot(trade.get("candle_snapshot", []), snapshot_bars)
                            trade["last_poll"] = now_teh()
                            changed = True
                            print(f"[poll_open] {sym} TP خورد → watching")
                        else:
                            if not trade.get("snapshot_locked"):
                                trade["candle_snapshot"] = merge_snapshot(trade.get("candle_snapshot", []), snapshot_bars)
                            if mfe_pip > float(trade.get("mfe_pip") or 0):
                                trade["mfe_pip"] = round(mfe_pip, 1)
                            trade["passed_1r"] = passed_1r or trade.get("passed_1r", False)
                            trade["free_risk_was_possible"] = fr_possible or trade.get("free_risk_was_possible", False)
                            trade["pullback_after_1r"] = pullback or trade.get("pullback_after_1r", False)
                            trade["last_poll"] = now_teh()
                            changed = True
                except Exception as e:
                    log_error(f"[poll_open] {sym}: {e}")
            if changed:
                save_journal(journal)
        except Exception as e:
            log_error(f"poll_open_trades: {e}")
        time.sleep(900)

# =====================================================================
# MT4/MT5 TEST ENDPOINT
# =====================================================================
@app.route("/api/mt4/test", methods=["POST"])
def mt4_test():
    body = request.json or {}
    candle_count = len(body.get("candle_snapshot", []))
    print("=" * 50)
    print("[MT4 TEST] دریافت شد")
    print(f"  sym      = {body.get('sym')}")
    print(f"  direction= {body.get('direction')}")
    print(f"  outcome  = {body.get('outcome')}")
    print(f"  entry    = {body.get('entry')}")
    print(f"  exit     = {body.get('exit')}")
    print(f"  ticket   = {body.get('mt4_ticket')}")
    print(f"  candles  = {candle_count}")
    print("=" * 50)
    return jsonify({"ok": True, "received": True, "candles": candle_count})

# =====================================================================
# MT4/MT5 LIVE ENDPOINT
# =====================================================================
@app.route("/api/journal/mt4", methods=["POST"])
def add_journal_mt4():
    body = request.json or {}
    sym = body.get("sym", "").upper().strip()
    if not sym:
        return jsonify({"ok": False, "error": "sym الزامی است"}), 400

    mt4_ticket  = body.get("mt4_ticket")
    position_id = body.get("mt4_position_id")
    check_id    = position_id or mt4_ticket

    journal = load_journal()
    if check_id:
        for t in journal:
            if t.get("mt4_position_id") == check_id or t.get("mt4_ticket") == check_id:
                return jsonify({"ok": True, "skipped": True, "id": t["id"]})

    direction  = body.get("direction", "BUY")
    entry      = float(body.get("entry", 0))
    exit_price = body.get("exit")
    sl_price   = body.get("sl_price")
    tp_price   = body.get("tp_price")
    lots       = float(body.get("size", 1.0))
    outcome    = body.get("outcome", "")
    pnl_money  = body.get("pnl")
    entry_time = body.get("entryTime", now_teh())
    exit_time  = body.get("exitTime",  now_teh())
    comment    = body.get("note", "").strip()
    candles    = body.get("candle_snapshot", [])

    mul       = get_pip_multiplier(sym)
    sl_pips   = round(abs(entry - float(sl_price)) * mul, 1) if sl_price else None
    tp_pips   = round(abs(float(tp_price) - entry) * mul, 1) if tp_price else None

    # تشخیص exit_type دقیق
    _exit_type_from_ea = body.get("exit_type")
    if _exit_type_from_ea in ("sl", "tp", "tp3", "manual"):
        exit_type = _exit_type_from_ea
    elif outcome == "loss":
        exit_type = "sl"
    elif outcome == "win":
        if exit_price and tp_price and sl_price:
            _tp_f   = float(tp_price)
            _sl_f   = float(sl_price)
            _exit_f = float(exit_price)
            _risk   = abs(entry - _sl_f) * mul
            _diff_from_tp = abs(_exit_f - _tp_f) * mul
            if _diff_from_tp <= max(2.0, _risk * 0.10):
                exit_type = "tp"
            else:
                exit_type = "manual"
            print(f"[MT5] exit_type auto: exit={_exit_f} tp={_tp_f} diff={_diff_from_tp:.1f}pip risk={_risk:.1f}pip -> {exit_type}")
        else:
            exit_type = "tp"
    else:
        exit_type = None

    trade = {
        "id":           generate_id(),
        "sym":          sym,
        "tf":           body.get("tf", "15m"),
        "direction":    direction,
        "entry":        entry,
        "size":         lots,
        "sl_price":     float(sl_price)   if sl_price   else None,
        "tp_price":     float(tp_price)   if tp_price   else None,
        "sl_pips":      sl_pips,
        "tp_pips":      tp_pips,
        "entryTime":    entry_time,
        "exitTime":     exit_time,
        "exit":         float(exit_price) if exit_price else None,
        "exit_type":    exit_type,
        "outcome":      outcome or None,
        "pnl":          float(pnl_money)  if pnl_money  else None,
        "note":         comment,
        "exitNote":     f"MT5 pos #{position_id}" if position_id else f"MT5 ticket #{mt4_ticket}",
        "createdAt":    now_teh(),
        "status":       "closed" if outcome else "open",
        "pending_check": False,
        "mt4_ticket":   mt4_ticket,
        "mt4_position_id": position_id,
        "mt4_magic":    body.get("mt4_magic"),
        "mt4_profit":   float(pnl_money)  if pnl_money  else None,
        "source":       "mt5_ea",
        "tf2":          body.get("tf2"),
        "candle_snapshot_tf2": body.get("candle_snapshot_tf2"),
        "mfe_pip": 0, "mae_pip": 0,
        "found_3r": False, "free_risk_was_possible": False,
        "free_risk_saved": False, "pullback_after_1r": False,
        "mfe_before_sl_pip": 0, "passed_1r": False,
        "is_missed_zone": False, "ai_analysis": None, "ai_summary": None,
        "review_mfe": None, "review_mae": None, "review_pullback": None,
        "review_note": None, "review_reversal_occurred": None,
        "review_reversal_from_sl": None, "review_reversal_target_pips": None,
        "candle_snapshot":  candles,
        "snapshot_locked":  len(candles) > 0,
    }

    # محاسبه MFE/MAE از candle_snapshot
    if candles and sl_price and entry:
        try:
            sl_f   = float(sl_price)
            is_buy = (direction == "BUY")
            risk_pips = abs(entry - sl_f) * mul
            mfe_pip = mae_pip = mfe_before_sl = 0.0
            passed_1r = found_3r = free_risk_was_possible = pullback_after_1r = False
            mae_stopped = False
            reached_1r_at = None

            entry_ts = None
            try:
                from datetime import datetime as _dt, timedelta as _td
                et = _dt.strptime(entry_time[:16], "%Y-%m-%d %H:%M")
                entry_ts = int((et - _td(hours=3, minutes=30)).timestamp())
            except: pass

            entry_idx = 0
            if entry_ts:
                for i, c in enumerate(candles):
                    ct = c.get("t", 0)
                    if isinstance(ct, str):
                        try: ct = int(_dt.strptime(ct[:16], "%Y-%m-%d %H:%M").timestamp())
                        except: ct = 0
                    if ct >= entry_ts: entry_idx = i; break

            for i, c in enumerate(candles):
                if i < entry_idx: continue
                high = float(c.get("h", 0)); low = float(c.get("l", 0))
                if is_buy:
                    profit_now = (high - entry) * mul
                    adverse    = (entry - low)  * mul
                else:
                    profit_now = (entry - low)  * mul
                    adverse    = (high - entry) * mul
                if not mae_stopped and adverse > 0:
                    if adverse > mae_pip: mae_pip = adverse
                mfe_pip = max(mfe_pip, profit_now)
                if risk_pips and mfe_pip >= risk_pips*3: found_3r = True
                if not passed_1r: mfe_before_sl = max(mfe_before_sl, profit_now)
                if risk_pips and not passed_1r and profit_now >= risk_pips:
                    passed_1r = True; reached_1r_at = i; mae_stopped = True
                if passed_1r and reached_1r_at and i > reached_1r_at:
                    free_risk_was_possible = True
                    if is_buy and low <= entry: pullback_after_1r = True
                    elif not is_buy and high >= entry: pullback_after_1r = True

            trade.update({
                "mfe_pip": round(mfe_pip, 1), "mae_pip": round(mae_pip, 1),
                "mfe_before_sl_pip": round(mfe_before_sl, 1),
                "passed_1r": passed_1r, "found_3r": found_3r,
                "free_risk_was_possible": free_risk_was_possible,
                "pullback_after_1r": pullback_after_1r,
            })
            print(f"[MT5] MFE={mfe_pip:.1f} MAE={mae_pip:.1f} 1R={passed_1r} 3R={found_3r}")
        except Exception as e:
            print(f"[MT5] MFE calc error: {e}")

    journal.insert(0, trade)
    save_trade(trade)
    print(f"[MT5] ✅ {sym} {direction} {outcome} candles={len(candles)} pos={position_id}")
    return jsonify({"ok": True, "id": trade["id"]})


@app.route("/api/journal/mt4/status", methods=["GET"])
def mt4_status():
    journal = load_journal()
    ids = [t.get("mt4_position_id") or t.get("mt4_ticket") for t in journal if t.get("source") == "mt5_ea"]
    return jsonify({"sent": ids, "total": len(ids)})

# =====================================================================
# Watching endpoints — EA این‌ها رو هر بار ران میشه چک میکنه
# =====================================================================

@app.route("/api/journal/watching", methods=["GET"])
def get_watching_trades():
    """لیست تریدهایی که هنوز تعیین تکلیف نشدن (watching)"""
    journal = load_journal()
    watching = []
    for t in journal:
        if t.get("status") == "watching" or (t.get("source") == "mt5_ea" and not t.get("found_3r") and t.get("outcome") == "win"):
            watching.append({
                "id":           t.get("id"),
                "sym":          t.get("sym"),
                "tf":           t.get("tf", "15m"),
                "direction":    t.get("direction"),
                "entry":        t.get("entry"),
                "exit":         t.get("exit"),         # قیمت TP
                "exitTime":     t.get("exitTime"),      # آخرین کندل موجود از اینجاست
                "sl_price":     t.get("sl_price"),
                "tp_price":     t.get("tp_price"),
                "outcome":      t.get("outcome"),
                "found_3r":     t.get("found_3r", False),
                "mt4_position_id": t.get("mt4_position_id"),
            })
    print(f"[WATCHING] {len(watching)} ترید در جریان")
    return jsonify({"watching": watching, "total": len(watching)})


@app.route("/api/journal/mt4/update-watching", methods=["POST"])
def update_watching_trade():
    """
    EA کندل‌های جدید یه ترید watching رو میفرسته.
    سرور چک میکنه SL خورده یا 3R زده شده.
    """
    body = request.json or {}
    trade_id     = body.get("id")
    new_candles  = body.get("candle_snapshot", [])

    if not trade_id or not new_candles:
        return jsonify({"ok": False, "error": "id و candle_snapshot الزامی"}), 400

    journal = load_journal()
    trade = next((t for t in journal if t.get("id") == trade_id), None)
    if not trade:
        return jsonify({"ok": False, "error": "ترید یافت نشد"}), 404

    sym       = trade.get("sym", "")
    direction = trade.get("direction", "BUY")
    entry     = float(trade.get("entry", 0))
    sl_price  = trade.get("sl_price")
    exit_price= float(trade.get("exit", 0))
    mul       = get_pip_multiplier(sym)
    is_buy    = direction == "BUY"

    if not sl_price:
        return jsonify({"ok": False, "error": "sl_price ندارد"}), 400

    sl_f      = float(sl_price)
    risk_pips = abs(entry - sl_f) * mul
    tp3_price = (entry + 3 * risk_pips / mul) if is_buy else (entry - 3 * risk_pips / mul)

    # merge کندل‌های جدید با snapshot موجود
    existing  = trade.get("candle_snapshot", [])
    merged    = merge_snapshot(existing, new_candles)

    # چک 3R و SL روی کندل‌های بعد از خروج
    hit_3r = False
    hit_sl = False
    new_mfe = float(trade.get("mfe_pip") or 0)

    # فقط کندل‌های بعد از exitTime رو چک کن
    exit_ts = None
    try:
        from datetime import datetime as _dt
        et = _dt.strptime(str(trade.get("exitTime",""))[:16], "%Y-%m-%d %H:%M")
        exit_ts = int(et.timestamp())
    except: pass

    for c in new_candles:
        ct = c.get("t", 0)
        if exit_ts and ct < exit_ts: continue
        h = float(c.get("h", 0))
        l = float(c.get("l", 0))

        # MFE بعد از خروج
        post_profit = (h - exit_price) * mul if is_buy else (exit_price - l) * mul
        if post_profit > new_mfe: new_mfe = post_profit

        # 3R چک
        if is_buy and h >= tp3_price:  hit_3r = True; break
        if not is_buy and l <= tp3_price: hit_3r = True; break

        # SL برگشت چک
        if is_buy and l <= sl_f:  hit_sl = True; break
        if not is_buy and h >= sl_f: hit_sl = True; break

    # آپدیت ترید
    trade["candle_snapshot"] = merged
    trade["mfe_pip"] = round(new_mfe, 1)

    if hit_3r:
        trade["status"]       = "closed"
        trade["found_3r"]     = True
        trade["exit_type"]    = "tp3"
        trade["pending_check"]= False
        trade["snapshot_locked"] = True
        trade["exitNote"]     = "EA: 3R کامل شد بعد از تارگت"
        print(f"[WATCHING] ✅ {sym} 3R زده شد — closed")
    elif hit_sl:
        trade["status"]       = "closed"
        trade["pending_check"]= False
        trade["snapshot_locked"] = True
        trade["exitNote"]     = "EA: SL خورد بعد از تارگت (چارت کامل)"
        print(f"[WATCHING] ✅ {sym} SL برگشت — closed")
    else:
        trade["last_poll"]    = now_teh()
        print(f"[WATCHING] ⏳ {sym} هنوز تعیین تکلیف نشده — کندل‌ها آپدیت شد")

    save_trade(trade)
    return jsonify({"ok": True, "status": trade["status"], "found_3r": trade.get("found_3r", False), "hit_sl": hit_sl, "hit_3r": hit_3r})

print("=" * 60)
print(f"[STARTUP] 🚀 سرور در حال راه‌اندازی...")
print(f"[STARTUP] GROQ_API_KEY: {'✅ موجود' if GROQ_API_KEY else '❌ ندارد'}")
_init_journal = load_journal()
print(f"[STARTUP] ✅ journal لود شد — {len(_init_journal)} ترید")
print("=" * 60)
threading.Thread(target=check_alerts, daemon=True).start()
print("[STARTUP] thread check_alerts شروع شد")

# ── بازیابی reminder‌ها از Supabase بعد از restart ──
def _restore_reminders():
    """همه reminder‌های فعال رو از Supabase بخون و loop‌هاشون رو restart کن"""
    time.sleep(5)  # صبر تا bot token آماده بشه
    rows = _sb_load_reminders()
    if not rows:
        print("[STARTUP] reminder: هیچ reminder فعالی نیست")
        return
    token = BOT_TOKEN_ENV or load_alerts().get("telegram", {}).get("bot_token", "")
    if not token:
        print("[STARTUP] reminder: token نیست، restore نشد")
        return
    count = 0
    for row in rows:
        cid = str(row["chat_id"])
        sym = row["symbol"]
        interval_sec = int(row["interval_sec"])
        tf_sec = int(row.get("tf_sec") or interval_sec)
        _schedule_reminder(token, cid, sym, interval_sec, persist=False, tf_sec=tf_sec)
        count += 1
    print(f"[STARTUP] reminder: {count} هشدار بازیابی شد")
threading.Thread(target=_restore_reminders, daemon=True).start()
threading.Thread(target=daily_news_scheduler, daemon=True).start()
print(f"[STARTUP] thread daily_news_scheduler شروع شد — ارسال ساعت {FF_NEWS_HOUR:02d}:{FF_NEWS_MINUTE:02d} تهران")
threading.Thread(target=poll_telegram, daemon=True).start()
print("[STARTUP] thread poll_telegram شروع شد")
threading.Thread(target=poll_open_trades, daemon=True).start()
print("[STARTUP] thread poll_open_trades شروع شد")
threading.Thread(target=_assignment_scheduler, daemon=True).start()
print("[STARTUP] thread assignment_scheduler شروع شد")

# ── بازیابی fired_msgs و counters از Supabase بعد از restart ──
_sb_load_fired_msgs()
_sb_load_sym_counters()
# بازسازی کامل state از Supabase بعد از هر restart
_sb_restore_on_startup()

# ── بازیابی notified set از Supabase — جلوگیری از double-fire بعد از restart ──
def _restore_notified():
    """آلارم‌هایی که قبلاً fire شدن رو به notified اضافه کن تا دوباره fire نشن"""
    if not SUPABASE_KEY: return
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/alerts?status=eq.fired&select=id&limit=5000",
            headers=_sb_h(), timeout=10)
        if r.status_code == 200:
            for row in r.json():
                aid = row.get("id")
                if aid:
                    notified.add(str(aid))
                    _deleted_ids.add(str(aid))
            print(f"[STARTUP] notified بازسازی شد — {len(notified)} آلارم fired")
    except Exception as e:
        print(f"[STARTUP] notified restore error: {e}")
_restore_notified()

@app.route("/report/weekly")
def report_weekly_html():
    """گزارش هفتگی HTML — زیبا و کامل"""
    which = request.args.get("w", "this")
    now_dt = datetime.now(TEHRAN)
    days_since_sat = (now_dt.weekday() - 5) % 7
    this_week_start = (now_dt - timedelta(days=days_since_sat)).replace(hour=0, minute=0, second=0, microsecond=0)
    if which == "last":
        week_start = this_week_start - timedelta(days=7)
        week_end   = this_week_start
    else:
        week_start = this_week_start
        week_end   = None
    week_start_str = week_start.strftime("%Y-%m-%dT%H:%M:%S")
    week_label = f"{week_start.strftime('%d/%m')} — {(week_end - timedelta(days=1)).strftime('%d/%m') if week_end else now_dt.strftime('%d/%m')}"
    rows = []
    if SUPABASE_KEY:
        try:
            url = (f"{SUPABASE_URL}/rest/v1/alarm_assignments"
                   f"?fired_at=gte.{week_start_str}&select=*&order=fired_at.asc")
            if week_end:
                url += f"&fired_at=lt.{week_end.strftime('%Y-%m-%dT%H:%M:%S')}"
            r = requests.get(url, headers=_sb_h(), timeout=10)
            if r.status_code == 200:
                rows = r.json()
        except: pass
    raw = _sb_load_all_alerts()
    if raw and isinstance(raw, dict):
        all_alerts = raw.get("alarms", []) + raw.get("archive", [])
    else:
        fb = load_alerts()
        all_alerts = fb.get("alarms", []) + fb.get("archive", [])
    alerts_map = {str(a["id"]): a for a in all_alerts}
    rows = [r for r in rows if not alerts_map.get(str(r.get("id","")), {}).get("is_private")]
    rows_html = ""
    false_by_set = set()
    for row in rows:
        aid       = str(row.get("id",""))
        tag       = row.get("alarm_tag","—")
        assignee  = row.get("assigned_to","") or "⏳ منتظر"
        fired     = row.get("fired_at","")[:16]
        false_by  = row.get("false_by","") or ""
        false_at  = row.get("false_at","")[:16] if row.get("false_at") else ""
        false_rsn = row.get("false_reason","") or ""
        is_active = row.get("is_active", True)
        if false_by:
            false_by_set.add(false_by)
        # تاریخچه کامل false
        false_history = row.get("false_history") or []
        if isinstance(false_history, str):
            try: false_history = json.loads(false_history)
            except: false_history = []
        alert     = alerts_map.get(aid, {})
        sym       = alert.get("symbol","") or row.get("symbol","") or ""
        tgt_raw   = alert.get("target_price",0) or row.get("target_price",0) or 0
        target    = fmt_price(float(tgt_raw), sym) if tgt_raw else "—"
        creator   = alert.get("created_by","") or row.get("created_by","") or "—"
        created   = str(alert.get("created_at",""))[:16]
        status_cls = "active" if is_active else "false"
        status_txt = "فعال" if is_active else f"False — {false_by}"
        cond_html = alert.get("condition", "") or row.get("condition", "")
        is_buy = (cond_html == "below")
        direction_icon = "📉" if cond_html == "above" else ("📈" if cond_html == "below" else "❓")
        dir_zone_label = "ناحیه سل" if cond_html == "above" else ("ناحیه بای" if cond_html == "below" else "")
        candle_cls = "candle-up" if is_buy else "candle-down"
        false_detail = ""
        if not is_active:
            if false_history:
                hist_items = ""
                for idx_h, h in enumerate(false_history, 1):
                    h_by  = h.get("by","")
                    h_at  = str(h.get("at",""))[:16]
                    h_rsn = h.get("reason","")
                    rsn_span = f'<span class="reason">{h_rsn}</span>' if h_rsn else ""
                    hist_items += f'<span class="hist-entry"><b>{idx_h}.</b> {h_by} — 🕐 {h_at}{(" · "+rsn_span) if rsn_span else ""}</span>'
                false_detail = f'<div class="false-detail false-history">{hist_items}</div>'
            else:
                false_detail = f'<div class="false-detail"><span>🕐 {false_at}</span>{("<span class=reason>"+false_rsn+"</span>") if false_rsn else ""}</div>'
        rows_html += f"""
        <div class="card card-{status_cls}" data-search="{(assignee + ' ' + sym + ' ' + tag + ' ' + creator + ' ' + false_by).lower()}" data-falseby="{false_by.lower()}" data-status="{status_cls}">
          <div class="card-glow"></div>
          <div class="card-header">
            <div class="card-icon">{direction_icon}</div>
            <div class="card-title">
              <span class="tag">{tag}</span>
              <span class="sym">{sym}{(' • ' + dir_zone_label) if dir_zone_label else ''}</span>
            </div>
            <span class="badge badge-{status_cls}">{status_txt}</span>
          </div>
          <div class="card-target">
            <span class="target-lbl">🎯 قیمت هدف</span>
            <span class="target-val">{target}</span>
          </div>
          <div class="card-body">
            <div class="info-grid">
              <div class="info-cell"><span class="lbl">📅 ثبت</span><span class="val">{created}</span></div>
              <div class="info-cell"><span class="lbl">⏰ فایر</span><span class="val">{fired}</span></div>
              <div class="info-cell"><span class="lbl">👤 سازنده</span><span class="val">{creator}</span></div>
              <div class="info-cell"><span class="lbl">🙋 مسئول</span><span class="val highlight">{assignee}</span></div>
            </div>
            {false_detail}
          </div>
          <div class="card-rail">
            <div class="rail-dot {candle_cls}"></div>
            <div class="mini-candles">
              <span class="mc {candle_cls}" style="height:40%"></span>
              <span class="mc {candle_cls}" style="height:65%"></span>
              <span class="mc {candle_cls}" style="height:30%"></span>
              <span class="mc {candle_cls}" style="height:85%"></span>
              <span class="mc {candle_cls}" style="height:50%"></span>
            </div>
            <span class="rail-label">{dir_zone_label if dir_zone_label else ''}</span>
          </div>
        </div>"""

    false_by_options = "".join(
        f'<option value="by:{name.lower()}">👤 {name}</option>' for name in sorted(false_by_set)
    )
    active_count = sum(1 for r in rows if r.get("is_active"))
    false_count  = len(rows) - active_count
    html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>گزارش هفتگی تیم — {week_label}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  :root{{
    --bg:#070a12;--surface:#0d1424;--surface2:#10192e;
    --border:#1e293b;--border2:#2d3f5c;
    --text:#e2e8f0;--muted:#64748b;--subtle:#334155;
    --blue:#3b82f6;--blue-dim:#1e3a5f;--blue-glow:rgba(59,130,246,.35);
    --green:#22c55e;--green-dim:#052e16;--green-border:#166534;--green-glow:rgba(34,197,94,.3);
    --red:#ef4444;--red-dim:#1c0a0a;--red-border:#7f1d1d;--red-glow:rgba(239,68,68,.3);
    --gold:#fbbf24;--purple:#8b5cf6;
  }}
  [data-theme="light"]{{
    --bg:#eef2f7;--surface:#ffffff;--surface2:#ffffff;
    --border:#e2e8f0;--border2:#cbd5e1;
    --text:#1e293b;--muted:#64748b;--subtle:#94a3b8;
    --blue:#2563eb;--blue-dim:#dbeafe;--blue-glow:rgba(37,99,235,.15);
    --green:#16a34a;--green-dim:#dcfce7;--green-border:#86efac;--green-glow:rgba(22,163,74,.15);
    --red:#dc2626;--red-dim:#fee2e2;--red-border:#fca5a5;--red-glow:rgba(220,38,38,.15);
    --gold:#d97706;--purple:#7c3aed;
  }}
  html{{scroll-behavior:smooth}}
  body{{font-family:'Inter',Tahoma,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;direction:rtl;transition:background .3s,color .3s;position:relative;overflow-x:hidden}}

  /* ── Animated background particles ── */
  .bg-grid{{position:fixed;inset:0;z-index:0;opacity:.4;pointer-events:none;
    background-image:linear-gradient(var(--border) 1px,transparent 1px),linear-gradient(90deg,var(--border) 1px,transparent 1px);
    background-size:40px 40px;mask-image:radial-gradient(ellipse 60% 50% at 50% 0%,#000 0%,transparent 100%)}}
  .bg-orb{{position:fixed;border-radius:50%;filter:blur(80px);pointer-events:none;z-index:0;opacity:.35;animation:float 16s ease-in-out infinite}}
  .bg-orb.o1{{width:300px;height:300px;background:var(--blue);top:-100px;right:-80px}}
  .bg-orb.o2{{width:260px;height:260px;background:var(--purple);top:30%;left:-100px;animation-delay:-5s}}
  .bg-orb.o3{{width:240px;height:240px;background:var(--green);bottom:0;right:10%;animation-delay:-10s}}
  @keyframes float{{0%,100%{{transform:translate(0,0) scale(1)}}50%{{transform:translate(30px,-40px) scale(1.1)}}}}

  /* ── Scroll progress ── */
  .scroll-bar{{position:fixed;top:0;right:0;left:0;height:4px;background:var(--border);z-index:999;overflow:hidden}}
  .scroll-bar-fill{{height:100%;width:0%;background:linear-gradient(90deg,var(--green),var(--blue),var(--purple));transition:width .08s;position:relative}}
  .scroll-bar-fill::after{{content:'';position:absolute;inset:0;
    background:repeating-linear-gradient(90deg,transparent 0 6px,rgba(255,255,255,.3) 6px 8px)}}
  .scroll-dot{{position:fixed;top:0;width:16px;height:16px;border-radius:50%;
    background:radial-gradient(circle,#fff,var(--blue));box-shadow:0 0 14px var(--blue);z-index:1000;
    transform:translate(-50%,-6px);transition:left .08s;display:flex;align-items:center;justify-content:center;font-size:9px}}

  /* ── Theme toggle ── */
  .theme-toggle{{position:fixed;top:14px;left:14px;z-index:998;width:44px;height:44px;border-radius:50%;border:1px solid var(--border2);background:var(--surface);color:var(--text);font-size:19px;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 16px rgba(0,0,0,.25);transition:.25s}}
  .theme-toggle:hover{{transform:scale(1.1) rotate(15deg)}}

  /* ── Header ── */
  .hero{{padding:42px 20px 28px;text-align:center;position:relative;z-index:1}}
  .hero h1{{font-size:26px;font-weight:800;margin-bottom:8px;letter-spacing:-.5px;
    background:linear-gradient(135deg,var(--blue),var(--purple));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;
    animation:fadeDown .5s ease}}
  .hero .period{{font-size:13px;color:var(--muted);margin-bottom:24px;animation:fadeDown .6s ease}}

  /* ── Stats bar ── */
  .stats{{display:flex;gap:14px;justify-content:center;flex-wrap:wrap;animation:fadeUp .6s ease}}
  .stat{{background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:14px 26px;text-align:center;min-width:90px;transition:.25s;position:relative;overflow:hidden}}
  .stat::before{{content:'';position:absolute;inset:0;background:linear-gradient(135deg,var(--blue-glow),transparent);opacity:0;transition:.3s}}
  .stat:hover::before{{opacity:1}}
  .stat:hover{{transform:translateY(-4px) scale(1.03);box-shadow:0 8px 24px var(--blue-glow)}}
  .stat .num{{font-size:26px;font-weight:800;position:relative}}
  .stat .lbl2{{font-size:11px;color:var(--muted);margin-top:4px;position:relative}}
  .stat.green .num{{color:var(--green)}}
  .stat.red .num{{color:var(--red)}}
  .stat.green:hover{{box-shadow:0 8px 24px var(--green-glow)}}
  .stat.red:hover{{box-shadow:0 8px 24px var(--red-glow)}}

  /* ── Week nav ── */
  .week-nav{{display:flex;gap:10px;justify-content:center;padding:18px 20px;position:sticky;top:4px;z-index:50}}
  .week-btn{{padding:10px 26px;border-radius:12px;border:1px solid var(--border2);background:var(--surface);color:var(--muted);text-decoration:none;font-size:13px;font-weight:600;transition:.25s;backdrop-filter:blur(10px)}}
  .week-btn:hover{{border-color:var(--blue);color:var(--blue);transform:translateY(-2px)}}
  .week-btn.active{{background:linear-gradient(135deg,var(--blue),var(--purple));border-color:transparent;color:#fff;box-shadow:0 6px 20px var(--blue-glow)}}

  /* ── Search ── */
  .search-wrap{{max-width:680px;margin:0 auto;padding:0 20px 14px;position:relative;z-index:1}}
  .search-input{{width:100%;padding:13px 18px;border-radius:14px;border:1px solid var(--border2);
    background:var(--surface);color:var(--text);font-size:13px;font-family:'Inter',Tahoma,sans-serif;
    direction:rtl;outline:none;transition:.25s}}
  .search-input:focus{{border-color:var(--blue);box-shadow:0 0 0 3px var(--blue-glow)}}
  .search-input::placeholder{{color:var(--muted)}}
  .search-select{{width:100%;margin-top:10px;padding:13px 18px;border-radius:14px;border:1px solid var(--border2);
    background:var(--surface);color:var(--text);font-size:13px;font-family:'Inter',Tahoma,sans-serif;
    direction:rtl;outline:none;transition:.25s;cursor:pointer}}
  .search-select:focus{{border-color:var(--blue);box-shadow:0 0 0 3px var(--blue-glow)}}
  .search-count{{display:block;text-align:center;font-size:11px;color:var(--muted);margin-top:8px}}

  /* ── Cards ── */
  .list{{padding:10px 20px 40px;max-width:680px;margin:0 auto;position:relative;z-index:1}}
  .card{{border-radius:20px;border:1px solid var(--border);margin-bottom:18px;overflow:hidden;transition:.3s;
         opacity:0;transform:translateY(20px) scale(.98);animation:cardIn .5s ease forwards;
         background:var(--surface);position:relative}}
  .card:hover{{transform:translateY(-5px) scale(1.01);box-shadow:0 16px 40px var(--blue-glow);border-color:var(--blue)}}
  .card-false:hover{{box-shadow:0 16px 40px var(--red-glow);border-color:var(--red)}}
  .card-glow{{position:absolute;top:-50%;left:-20%;width:60%;height:200%;
    background:radial-gradient(circle,var(--blue-glow),transparent 70%);pointer-events:none;opacity:.5}}
  .card-false .card-glow{{background:radial-gradient(circle,var(--red-glow),transparent 70%)}}

  .card-header{{display:flex;align-items:center;gap:12px;padding:18px 18px 14px;position:relative;z-index:1}}
  .card-icon{{font-size:26px;width:46px;height:46px;display:flex;align-items:center;justify-content:center;
    background:var(--surface2);border-radius:12px;border:1px solid var(--border)}}
  .card-title{{display:flex;flex-direction:column;gap:3px;flex:1}}
  .tag{{font-weight:800;font-size:17px;color:var(--blue)}}
  .sym{{font-size:11px;color:var(--muted);font-weight:600;letter-spacing:.5px}}
  .badge{{font-size:11px;font-weight:700;padding:6px 14px;border-radius:24px;white-space:nowrap}}
  .badge-active{{background:var(--green-dim);color:var(--green);border:1px solid var(--green-border)}}
  .badge-false{{background:var(--red-dim);color:var(--red);border:1px solid var(--red-border)}}

  /* ── Side rail: timeline dot + mini candles ── */
  .card-rail{{display:flex;align-items:center;justify-content:flex-start;gap:10px;padding:10px 18px;
    border-top:1px solid var(--border);background:var(--surface2)}}
  .rail-label{{font-size:11px;color:var(--muted);font-weight:600;margin-right:auto}}
  .rail-dot{{width:10px;height:10px;border-radius:50%;flex-shrink:0;box-shadow:0 0 8px currentColor}}
  .rail-dot.candle-up{{background:var(--green);color:var(--green)}}
  .rail-dot.candle-down{{background:var(--red);color:var(--red)}}
  .mini-candles{{display:flex;align-items:flex-end;gap:3px;height:26px}}
  .mc{{width:4px;border-radius:2px;opacity:.55;transition:.3s}}
  .mc.candle-up{{background:var(--green)}}
  .mc.candle-down{{background:var(--red)}}
  .card:hover .mc{{opacity:1;transform:scaleY(1.15)}}

  /* ── Target highlight bar ── */
  .card-target{{display:flex;justify-content:space-between;align-items:center;
    margin:0 18px 14px;padding:14px 18px;border-radius:14px;
    background:linear-gradient(135deg,var(--blue-dim),transparent);
    border:1px solid var(--border);position:relative;overflow:hidden}}
  .card-false .card-target{{background:linear-gradient(135deg,var(--red-dim),transparent)}}
  .target-lbl{{font-size:12px;color:var(--muted);font-weight:600}}
  .target-val{{font-family:'Inter',monospace;font-size:20px;font-weight:800;color:var(--gold);letter-spacing:.5px}}

  .card-body{{padding:0 18px 18px;position:relative;z-index:1}}
  .info-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px}}
  .info-cell{{display:flex;flex-direction:column;gap:4px;padding:10px 12px;background:var(--surface2);border-radius:10px;border:1px solid var(--border)}}
  .lbl{{font-size:11px;color:var(--muted);font-weight:500}}
  .val{{font-size:13px;color:var(--text);font-weight:600}}
  .val.highlight{{color:var(--blue);font-weight:800}}
  .false-detail{{margin-top:10px;padding:10px 14px;background:var(--red-dim);border-radius:10px;border:1px solid var(--red-border);display:flex;flex-wrap:wrap;gap:8px;align-items:center;font-size:12px;color:var(--red);font-weight:500}}
  .false-detail .reason{{font-style:italic}}
  .false-history{{flex-direction:column;gap:4px}}
  .hist-entry{{display:block;font-size:11px;color:var(--red);padding:2px 0}}

  /* ── Empty ── */
  .empty{{text-align:center;padding:90px 20px;color:var(--muted);animation:fadeUp .5s ease;position:relative;z-index:1}}
  .empty .icon{{font-size:54px;margin-bottom:14px}}

  /* ── Footer ── */
  .footer{{text-align:center;padding:28px;font-size:11px;color:var(--subtle);position:relative;z-index:1}}

  /* ── Animations ── */
  @keyframes fadeDown{{from{{opacity:0;transform:translateY(-12px)}}to{{opacity:1;transform:translateY(0)}}}}
  @keyframes fadeUp{{from{{opacity:0;transform:translateY(12px)}}to{{opacity:1;transform:translateY(0)}}}}
  @keyframes cardIn{{to{{opacity:1;transform:translateY(0) scale(1)}}}}
</style>
</head>
<body data-theme="dark">
<div class="bg-grid"></div>
<div class="bg-orb o1"></div>
<div class="bg-orb o2"></div>
<div class="bg-orb o3"></div>
<div class="scroll-bar"><div class="scroll-bar-fill" id="scrollFill"></div></div>
<div class="scroll-dot" id="scrollDot">📈</div>
<button class="theme-toggle" id="themeToggle" onclick="toggleTheme()">🌙</button>
<div class="hero">
  <h1>📋 گزارش هفتگی تیم</h1>
  <div class="period">{week_label}</div>
  <div class="stats">
    <div class="stat"><div class="num">{len(rows)}</div><div class="lbl2">کل آلارم</div></div>
    <div class="stat green"><div class="num">{active_count}</div><div class="lbl2">فعال</div></div>
    <div class="stat red"><div class="num">{false_count}</div><div class="lbl2">False شده</div></div>
  </div>
</div>
<div class="week-nav">
  <a href="/report/weekly?w=this" class="week-btn {'active' if which=='this' else ''}">📅 این هفته</a>
  <a href="/report/weekly?w=last" class="week-btn {'active' if which=='last' else ''}">📅 هفته قبل</a>
</div>
<div class="search-wrap">
  <input type="text" id="searchBox" class="search-input" placeholder="🔍 جستجو بر اساس مسئول، نماد، تگ یا سازنده..." oninput="filterCards()">
  <select id="filterSelect" class="search-select" onchange="filterCards()">
    <option value="">📂 همه آلارم‌ها</option>
    <optgroup label="وضعیت">
      <option value="status:active">✅ فقط فعال</option>
      <option value="status:false">❌ فقط False شده</option>
    </optgroup>
    <optgroup label="False شده توسط">
      {false_by_options}
    </optgroup>
  </select>
  <span class="search-count" id="searchCount"></span>
</div>
<div class="list" id="cardList">
  {'<div class="empty"><div class="icon">📭</div>آلارمی ثبت نشده</div>' if not rows else rows_html}
  <div class="empty" id="noResults" style="display:none"><div class="icon">🔍</div>چیزی پیدا نشد</div>
</div>
<div class="footer">آخرین بروزرسانی: {now_dt.strftime('%H:%M — %d/%m/%Y')}</div>
<script>
  // scroll progress — chart line style
  window.addEventListener('scroll', () => {{
    const h = document.documentElement;
    const pct = (h.scrollTop / (h.scrollHeight - h.clientHeight)) * 100;
    document.getElementById('scrollFill').style.width = pct + '%';
    document.getElementById('scrollDot').style.left = pct + '%';
  }});
  // stagger card animations
  document.querySelectorAll('.card').forEach((c,i) => {{
    c.style.animationDelay = (i * 0.06) + 's';
  }});
  // search/filter
  function filterCards() {{
    const q = document.getElementById('searchBox').value.trim().toLowerCase();
    const sel = document.getElementById('filterSelect').value;
    const cards = document.querySelectorAll('#cardList .card');
    let visible = 0;
    cards.forEach(c => {{
      let match = !q || (c.dataset.search || '').includes(q);
      if (match && sel) {{
        if (sel.startsWith('status:')) {{
          match = c.dataset.status === sel.slice(7);
        }} else if (sel.startsWith('by:')) {{
          match = c.dataset.falseby === sel.slice(3);
        }}
      }}
      c.style.display = match ? '' : 'none';
      if (match) visible++;
    }});
    document.getElementById('noResults').style.display = (visible === 0) ? '' : 'none';
    document.getElementById('searchCount').textContent = (q || sel) ? `${{visible}} نتیجه پیدا شد` : '';
  }}
  // theme toggle with localStorage
  function toggleTheme() {{
    const body = document.body;
    const isDark = body.getAttribute('data-theme') === 'dark';
    body.setAttribute('data-theme', isDark ? 'light' : 'dark');
    document.getElementById('themeToggle').textContent = isDark ? '☀️' : '🌙';
    try {{ localStorage.setItem('reportTheme', isDark ? 'light' : 'dark'); }} catch(e) {{}}
  }}
  try {{
    const saved = localStorage.getItem('reportTheme');
    if (saved) {{
      document.body.setAttribute('data-theme', saved);
      document.getElementById('themeToggle').textContent = saved === 'dark' ? '🌙' : '☀️';
    }}
  }} catch(e) {{}}
</script>
</body></html>"""
    return html, 200, {"Content-Type": "text/html; charset=utf-8"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"[STARTUP] Flask روی پورت {port} اجرا میشه")
    app.run(host="0.0.0.0", port=port, debug=False)
