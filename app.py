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
GIST_TOKEN = os.environ.get("GIST_TOKEN", "")
GIST_ID_ALERTS = os.environ.get("GIST_ID", "")
GIST_ID_JOURNAL = os.environ.get("GIST_ID_JOURNAL", "")
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
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/alerts",
            headers={**_sb_h(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=payload, timeout=10)
        if r.status_code not in (200,201,204):
            print(f"[alerts] upsert {a['id']}: {r.status_code} {r.text[:80]}")
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

def _migrate_gist_to_supabase():
    """یه‌بار: داده قدیمی Gist رو به Supabase منتقل کن"""
    if not (GIST_ID_ALERTS and GIST_TOKEN): return None
    try:
        print(f"[alerts] Migrating from Gist...")
        r = requests.get(f"https://api.github.com/gists/{GIST_ID_ALERTS}",
                         headers={"Authorization": f"token {GIST_TOKEN}"}, timeout=10)
        if r.status_code != 200: return None
        content = r.json()["files"].get(ALERTS_FILE, {}).get("content","")
        if not content: return None
        data = fix_alerts(json.loads(content))
        # همه آلارم‌ها و آرشیو رو migrate کن
        for a in data.get("alerts",[]) + data.get("archive",[]):
            _sb_upsert_alert(a)
        _sb_upsert_config(data.get("telegram",{}), data.get("users",[]), data.get("errors",[]))
        print(f"[alerts] Migration done — {len(data.get('alerts',[]))} active, {len(data.get('archive',[]))} archived")
        return data
    except Exception as e:
        print(f"[alerts] migration error: {e}")
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
    # 2. Gist — migrate یه‌بار
    d = _migrate_gist_to_supabase()
    if d is not None:
        _cache_alerts = d
        return _cache_alerts
    # 3. local fallback
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
    """وقتی آلارم fire میشه — فقط همون ردیف رو آپدیت کن"""
    _sb_upsert_alert(a)

# =====================================================================
# Supabase
# =====================================================================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://erwimqqskkzcsayvhxot.supabase.co")
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

    # fallback Gist
    if GIST_ID_JOURNAL and GIST_TOKEN:
        try:
            r = requests.get(f"https://api.github.com/gists/{GIST_ID_JOURNAL}",
                             headers={"Authorization": f"token {GIST_TOKEN}"}, timeout=10)
            if r.status_code == 200:
                files = r.json().get("files", {})
                if JOURNAL_FILE in files:
                    _cache_journal = json.loads(files[JOURNAL_FILE]["content"])
                    if not isinstance(_cache_journal, list): _cache_journal = []
                    print(f"[JOURNAL:LOAD] ✅ {len(_cache_journal)} ترید از Gist")
                    return _cache_journal
        except Exception as e:
            print(f"[JOURNAL:LOAD] Gist error: {e}")

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

def send_tg(token, chat_id, text):
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}, timeout=10, headers=H)
        return r.status_code == 200
    except: return False

def broadcast(token, chat_ids, text):
    return [send_tg(token, c, text) for c in chat_ids]

def send_tg_keyboard(token, chat_id, text, keyboard):
    """ارسال پیام با inline keyboard"""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": str(chat_id), "text": text,
                  "parse_mode": "HTML", "reply_markup": {"inline_keyboard": keyboard}},
            timeout=10, headers=H)
        return r.json().get("result", {}).get("message_id")
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
# { cid: { "sym": str, "interval": int(sec), "timer": Timer } }
# _reminders = { cid: { sym: {"interval": int, "active": bool} } }
_reminders = {}

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

def _send_reminder(token, cid, sym):
    """یه پیام یادآوری بفرست با دکمه کنسل"""
    msg = f"⚠️ <b>یادآوری:</b> <code>{sym}</code> بررسی بشه!\n\n🕐 این پیام ۲ دقیقه دیگه پاک میشه."
    kb = [[{"text": f"✕ کنسل {sym}", "callback_data": f"cancel_reminder_one:{cid}:{sym}"}]]
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": cid, "text": msg, "parse_mode": "HTML",
                  "reply_markup": {"inline_keyboard": kb}},
            timeout=10, headers=H)
        mid = r.json().get("result", {}).get("message_id")
        if mid:
            _delete_msg_after(token, cid, mid, delay=120)
    except: pass

def _schedule_reminder(token, cid, sym, interval_sec):
    """هر interval_sec یه یادآوری بفرست تا کنسل نشه"""
    if cid not in _reminders:
        _reminders[cid] = {}
    _reminders[cid][sym] = {"interval": interval_sec, "active": True}
    entry = _reminders[cid][sym]
    def _loop():
        while entry.get("active") and _reminders.get(cid, {}).get(sym, {}).get("active"):
            time.sleep(interval_sec)
            if not _reminders.get(cid, {}).get(sym, {}).get("active"):
                break
            _send_reminder(token, cid, sym)
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
        lbl = labels.get(info["interval"], f"{info['interval']//60} دقیقه")
        lines.append(f"• <b>{sym}</b> — هر {lbl}")
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
    lines = ["🔒 <b>آلارم‌های شخصی تو:</b>\n"]
    keyboard = []
    for i, a in enumerate(my, 1):
        sym = a.get("symbol","")
        tgt = a.get("target_price",0)
        cond = "📈 بای" if a.get("condition") == "below" else "📉 سل"
        cmt = f" — {a['comment']}" if a.get("comment") else ""
        lines.append(f"{i}. <b>{sym}</b> {cond} @ <code>{tgt}</code>{cmt}")
        keyboard.append([{"text": f"🗑 حذف {sym} @ {tgt}", "callback_data": f"del_alert:{a['id']}"}])
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

_pending_name = {}   # cid → True  (منتظر دریافت اسم custom)

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
            for upd in r.json().get("result", []):
                last_id = upd["update_id"]

                # ── callback query (دکمه‌های inline) ─────────────────
                cbq = upd.get("callback_query", {})
                if cbq:
                    cbq_id = cbq.get("id","")
                    cbq_data = cbq.get("data","")
                    cbq_cid = str(cbq.get("from",{}).get("id","") or cbq.get("message",{}).get("chat",{}).get("id",""))
                    cbq_msg_id = cbq.get("message",{}).get("message_id")
                    token_cbq, _, _ = _get_token_and_cids()

                    if cbq_data.startswith("del_alert:"):
                        aid = cbq_data.split(":",1)[1]
                        d = load_alerts()
                        before = len(d["alerts"])
                        d["alerts"] = [a for a in d["alerts"] if a["id"] != aid]
                        if len(d["alerts"]) < before:
                            _cache_alerts = d  # cache آپدیت
                            answer_callback(token_cbq, cbq_id, "✅ آلارم حذف شد")
                            threading.Thread(target=_sb_delete_alert, args=(aid,), daemon=True).start()
                        else:
                            answer_callback(token_cbq, cbq_id, "⚠️ آلارم پیدا نشد")
                        # آپدیت لیست
                        new_text, new_kb = build_myalerts_msg(cbq_cid)
                        if new_kb:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, new_text, new_kb)
                        else:
                            edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "✅ همه آلارم‌های شخصی حذف شدن.", [])

                    elif cbq_data.startswith("set_reminder:"):
                        # set_reminder:cid:SYM — نشون بده ۴ گزینه بازه زمانی
                        parts = cbq_data.split(":", 2)
                        r_cid = parts[1] if len(parts) > 1 else cbq_cid
                        r_sym = parts[2] if len(parts) > 2 else "؟"
                        answer_callback(token_cbq, cbq_id)
                        kb = [
                            [{"text": "⏱ ۵ دقیقه",  "callback_data": f"reminder_go:{r_cid}:{r_sym}:300"}],
                            [{"text": "⏱ ۱۵ دقیقه", "callback_data": f"reminder_go:{r_cid}:{r_sym}:900"}],
                            [{"text": "⏱ ۱ ساعت",   "callback_data": f"reminder_go:{r_cid}:{r_sym}:3600"}],
                            [{"text": "⏱ ۴ ساعت",   "callback_data": f"reminder_go:{r_cid}:{r_sym}:14400"}],
                            [{"text": "✕ نه ممنون",  "callback_data": "close_myalerts"}],
                        ]
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"⏰ هر چند وقت یادآوری بیاد برای <b>{r_sym}</b>؟", kb)

                    elif cbq_data.startswith("reminder_go:"):
                        # reminder_go:cid:SYM:interval_sec
                        parts = cbq_data.split(":")
                        r_cid = parts[1] if len(parts) > 1 else cbq_cid
                        r_sym = parts[2] if len(parts) > 2 else "؟"
                        r_int = int(parts[3]) if len(parts) > 3 else 900
                        labels = {300:"۵ دقیقه", 900:"۱۵ دقیقه", 3600:"۱ ساعت", 14400:"۴ ساعت"}
                        label = labels.get(r_int, f"{r_int//60} دقیقه")
                        # کنسل قبلی برای همین نماد اگه بود
                        if _reminders.get(r_cid, {}).get(r_sym):
                            _reminders[r_cid][r_sym]["active"] = False
                            del _reminders[r_cid][r_sym]
                        answer_callback(token_cbq, cbq_id, f"✅ هر {label} یادآوری میاد")
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id,
                            f"✅ هشدار دوره‌ای <b>{r_sym}</b> هر <b>{label}</b> فعال شد.\nبرای کنسل: /cancel_reminder", [])
                        _schedule_reminder(token_cbq, r_cid, r_sym, r_int)

                    elif cbq_data.startswith("cancel_reminder_one:"):
                        # حذف یه هشدار مشخص
                        parts = cbq_data.split(":", 2)
                        r_cid = parts[1] if len(parts) > 1 else cbq_cid
                        r_sym = parts[2] if len(parts) > 2 else ""
                        if r_sym and _reminders.get(r_cid, {}).get(r_sym):
                            _reminders[r_cid][r_sym]["active"] = False
                            del _reminders[r_cid][r_sym]
                            if not _reminders[r_cid]:
                                del _reminders[r_cid]
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
                            answer_callback(token_cbq, cbq_id, "✅ همه هشدارها کنسل شد")
                        else:
                            answer_callback(token_cbq, cbq_id, "هشداری فعال نبود")
                        edit_tg_keyboard(token_cbq, cbq_cid, cbq_msg_id, "✅ همه هشدارهای دوره‌ای کنسل شدن.", [])

                    elif cbq_data == "close_myalerts":
                        answer_callback(token_cbq, cbq_id, "بسته شد")
                        try:
                            requests.post(
                                f"https://api.telegram.org/bot{token_cbq}/deleteMessage",
                                json={"chat_id": cbq_cid, "message_id": cbq_msg_id},
                                timeout=10, headers=H)
                        except: pass
                    continue

                msg = upd.get("message", {})
                raw_txt = msg.get("text", "") or ""
                # normalize: /cmd@botname → /cmd
                txt = raw_txt.split("@")[0] if raw_txt.startswith("/") else raw_txt
                ch = msg.get("chat", {})
                cid = str(ch.get("id", ""))
                uname = ch.get("username", "") or ch.get("first_name", "")

                # ── /start ──────────────────────────────────────────
                if txt.startswith("/start") and cid:
                    data = load_alerts()
                    users = data.get("users", [])
                    if cid not in [str(u["chat_id"]) for u in users]:
                        users.append({"chat_id": cid, "username": uname, "joined_at": now_teh(), "custom_name": ""})
                        data["users"] = users
                        ids = data.get("telegram", {}).get("chat_ids", [])
                        if cid not in [str(x) for x in ids]:
                            ids.append(cid)
                        data["telegram"]["chat_ids"] = ids
                        save_alerts(data)
                    _pending_name[cid] = True
                    send_tg(token, cid,
                        f"👋 سلام <b>{uname}</b>!\n\n"
                        f"لطفاً <b>اسمی که در سایت استفاده می‌کنی</b> رو بنویس:\n"
                        f"(آلارم‌های شخصیت با همین اسم شناسایی میشن)")

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
                        send_tg(token, cid,
                            f"✅ اسم <b>{custom_name}</b> ذخیره شد!\n"
                            f"از این به بعد آلارم‌هات با این اسم ثبت میشن.\n"
                            f"توی سایت هم همین اسم رو وارد کن.")

                # ── /setname — تغییر اسم ────────────────────────────
                elif txt.startswith("/setname"):
                    _pending_name[cid] = True
                    data = load_alerts()
                    users = data.get("users", [])
                    cur = next((u.get("custom_name","") for u in users if str(u.get("chat_id",""))==cid), "")
                    cur_info = f"\nاسم فعلی: <b>{cur}</b>" if cur else ""
                    send_tg(token, cid, f"✏️ اسم جدیدت رو بنویس:{cur_info}")

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
                        hashtag = "#" + re.sub(r'[^\w]', '_', sender_name).strip('_')
                        out_msg = (
                            f"🚨 <b>آلارم فوری!</b>\n\n"
                            f"💰 <b>{sym}</b> — {arrow}\n"
                            f"👤 {hashtag}\n\n"
                            f"📊 قیمت لحظه‌ای: <b>{price_text}</b>"
                            f"{cmt}\n\n⏰ {now_pretty()} (تهران)"
                        )
                        _, all_cids, _ = _get_token_and_cids()
                        targets = all_cids if BROADCAST_MODE else [YOUR_CHAT_ID]
                        for tc in targets:
                            send_tg(token, tc, out_msg)
                        d = load_alerts()
                        arch = d.get("archive", [])
                        arch.append({"id": str(int(time.time()*1000)), "symbol": sym, "type": atype,
                            "condition": condition, "comment": comment, "created_by": sender_name,
                            "active": False, "fired_at": now_teh(), "fired_price": cur,
                            "instant": True, "created_at": now_teh()})
                        d["archive"] = arch
                        save_alerts(d)
                        mode_txt = f"به {len(targets)} نفر" if BROADCAST_MODE else "فقط برای شما"
                        send_tg(token, cid, f"✅ آلارم فوری {sym} ارسال شد ({mode_txt})")

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
                            arrow = "سل 📈" if condition == "above" else "بای 📉"
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
                            # اول save کن — سریع
                            _sb_upsert_alert(new_alert)
                            _cache_alerts = d
                            # فوری پیام تأیید بده
                            send_tg(token, cid,
                                f"✅ <b>آلارم ثبت شد</b>\n\n"
                                f"💰 <b>{sym}</b> — {arrow}\n"
                                f"🎯 هدف: <code>{fmt_price(tgt_f, sym)}</code>"
                                + (f"\n💬 <i>{comment}</i>" if comment else "") +
                                f"\n\n⏰ {now_pretty()} (تهران)")
                            # در background قیمت فعلی رو بگیر و آپدیت کن
                            def _bg_price(alert=new_alert, s=sym, t=atype, tok=token, c=cid):
                                try:
                                    cur = get_price(s, t)
                                    if cur:
                                        alert["last_price"] = cur
                                        alert["last_checked"] = now_teh()
                                        _sb_upsert_alert(alert)
                                except: pass
                            threading.Thread(target=_bg_price, daemon=True).start()

                # ── /mealarm — آلارم شخصی (فقط برای خود فرستنده) ───
                elif txt.startswith("/mealarm"):
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
                            send_tg(token, cid,
                                f"✅ <b>آلارم شخصی ثبت شد</b>\n\n"
                                f"💰 <b>{sym}</b> — {arrow}\n"
                                f"🎯 هدف: <code>{fmt_price(tgt_f, sym)}</code>"
                                + (f"\n💬 <i>{comment}</i>" if comment else "") +
                                f"\n\n🔒 فقط شما این آلارم رو میبینید\n⏰ {now_pretty()} (تهران)")
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
                    text_msg, keyboard = build_myalerts_msg(cid)
                    if keyboard:
                        send_tg_keyboard(token, cid, text_msg, keyboard)
                    else:
                        send_tg(token, cid, text_msg)

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
            print(f"[poll] {e}")
        time.sleep(5)

notified = set()
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
            active = [a for a in data.get("alerts", []) if a.get("active")]
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
                if triggered and a["id"] not in notified:
                    notified.add(a["id"])
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
                        hashtag = "#" + re.sub(r'[^\w]', '_', creator).strip('_')
                        fired_msg = (
                            f"🚨 <b>آلارم قیمت!</b>\n\n"
                            f"💰 <b>{sym}</b> — {arrow}\n"
                            f"👤 {hashtag}\n\n"
                            f"🎯 هدف: <code>{fmt_price(tgt,sym)}</code>\n"
                            f"📊 قیمت لحظه‌ای: <b>{fmt_price(cur,sym)}</b>\n"
                            f"📏 فاصله: <b>{dist}</b>"
                            f"{cmt}"
                            f"{private_label}\n\n⏰ {now_pretty()} (تهران)"
                        )
                        if a.get("is_private") and a.get("notify_only"):
                            # آلارم شخصی — با دکمه تنظیم هشدار دوره‌ای
                            priv_cid = str(a["notify_only"])
                            kb = [[{"text": "⏰ تنظیم هشدار دوره‌ای", "callback_data": f"set_reminder:{priv_cid}:{sym}"}]]
                            send_tg_keyboard(token, priv_cid, fired_msg, kb)
                        else:
                            broadcast(token, notify_cids, fired_msg)
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
    hashtag = "#" + re.sub(r'[^\w]', '_', _creator).strip('_')
    out_msg = (
        f"🚨 <b>{'آلارم قیمت' if target_price else 'آلارم فوری'}!</b>\n\n"
        f"💰 <b>{sym}</b> — {arrow}\n"
        f"👤 {hashtag}\n\n"
        + (f"🎯 هدف: <code>{fmt_price(target_price, sym)}</code>\n" if target_price else "")
        + f"📊 قیمت لحظه‌ای: <b>{price_text}</b>"
        f"{cmt}\n\n⏰ {now_pretty()} (تهران)"
    )

    results = [send_tg(token, cid, out_msg) for cid in targets]
    sent_count = sum(results)

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

@app.route("/api/status")
def status():
    alerts = load_alerts()
    journal = load_journal()
    return jsonify({
        "status": "ok", "last_update": alerts.get("last_update"),
        "errors": alerts.get("errors", [])[-5:], "time_tehran": now_teh(),
        "alert_count": len(alerts.get("alerts",[])), "forex_open": is_forex_market_open(),
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
print(f"[STARTUP] GIST_TOKEN: {'✅ موجود' if GIST_TOKEN else '❌ ندارد'}")
print(f"[STARTUP] GIST_ID_JOURNAL: {'✅ ' + GIST_ID_JOURNAL[:8] + '...' if GIST_ID_JOURNAL else '❌ ندارد'}")
print(f"[STARTUP] GIST_ID_ALERTS: {'✅ موجود' if GIST_ID_ALERTS else '❌ ندارد'}")
print(f"[STARTUP] GROQ_API_KEY: {'✅ موجود' if GROQ_API_KEY else '❌ ندارد'}")
_init_journal = load_journal()
print(f"[STARTUP] ✅ journal لود شد — {len(_init_journal)} ترید")
print("=" * 60)
threading.Thread(target=check_alerts, daemon=True).start()
print("[STARTUP] thread check_alerts شروع شد")
threading.Thread(target=daily_news_scheduler, daemon=True).start()
print(f"[STARTUP] thread daily_news_scheduler شروع شد — ارسال ساعت {FF_NEWS_HOUR:02d}:{FF_NEWS_MINUTE:02d} تهران")
threading.Thread(target=poll_telegram, daemon=True).start()
print("[STARTUP] thread poll_telegram شروع شد")
threading.Thread(target=poll_open_trades, daemon=True).start()
print("[STARTUP] thread poll_open_trades شروع شد")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"[STARTUP] Flask روی پورت {port} اجرا میشه")
    app.run(host="0.0.0.0", port=port, debug=False)