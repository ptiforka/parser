#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, math, json, re, random, socket
from datetime import datetime, timezone
import requests
import redis
from fake_useragent import UserAgent

# --------- Конфиг ---------
API_BASE = "https://api-manager.upbit.com/api/v1/announcements.json"
REQUEST_TIMEOUT = (2, 3)   # (connect, read)
PERIOD_SEC = 4             # частота одного сервера = 1 раз в 4 сек
CHANNEL = os.getenv("REDIS_CHAN", "announces")

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASS = os.getenv("REDIS_PASS", "")
SERVER_NAME = os.getenv("SERVER_NAME", socket.gethostname())

START_TARGET_ID = os.getenv("START_TARGET_ID")  # строка из env
SLOT_ENV = os.getenv("SLOT")                    # опционально 0..3

BRAND_MARKER = "업비트(Upbit)"

# Тикер из заголовка
TICKER_RE = re.compile(r"\(([A-Z0-9._-]+)\)[^\n]*?(?:신규\s*거래지원\s*안내|디지털\s*자산\s*추가)")

ua = UserAgent()

def now_iso_ms() -> str:
    # ISO-8601 UTC с миллисекундами (RFC3339-like)
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def rand_headers():
    return {
        "User-Agent": ua.random,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    }

def build_url():
    params = {
        "os": "android",
        "page": "1",
        "per_page": "1",
        "category": "a",
        # анти-кешовый параметр
        "bc": str(int(time.time() * 1000)) + str(random.randint(100, 999)),
    }
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{API_BASE}?{qs}"

def fetch_top_notice():
    url = build_url()
    headers = rand_headers()
    t0 = time.perf_counter()
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=False)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        sc = resp.status_code
        etag = resp.headers.get("ETag", "-")
        xrt  = resp.headers.get("X-Runtime", "-")
        size = len(resp.content)

        print(f"[req] status={sc} | {dt_ms:.1f} ms | size={size} | etag={etag} | xrt={xrt}")

        top = None
        if sc == 200:
            data = resp.json()
            notices = (data.get("data") or {}).get("notices") or []
            top = notices[0] if notices else None

        return {"top": top, "code": sc, "rt_ms": dt_ms, "etag": etag, "x_runtime": xrt, "size": size, "url": url}
    except requests.RequestException as e:
        dt_ms = (time.perf_counter() - t0) * 1000.0
        print(f"[req] ERROR after {dt_ms:.1f} ms: {e}")
        # унифицированная структура для ошибок сети
        return {"top": None, "code": None, "rt_ms": dt_ms, "etag": "-", "x_runtime": "-", "size": 0, "url": url, "error": str(e)}

def extract_ticker(title: str):
    if not title:
        return None
    m = TICKER_RE.search(title)
    return m.group(1) if m else None

def get_slot():
    # для периода 4 сек слоты 0..3
    if SLOT_ENV is not None:
        try:
            s = int(SLOT_ENV)
            if 0 <= s <= 3:
                return s
        except ValueError:
            pass
    return random.SystemRandom().randint(0, 3)

def align_next_tick(slot: int):
    """
    Ждём ближайший момент, где (sec % PERIOD_SEC) == slot.
    """
    now = time.time()
    now_floor = math.floor(now)
    base = now_floor - (now_floor % PERIOD_SEC)
    first = base + slot
    if first <= now:
        first += PERIOD_SEC
    sleep_s = max(0.0, first - now)
    if sleep_s > 0:
        time.sleep(sleep_s)
    return first

def main():
    # Redis
    rds = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS or None,
        decode_responses=True, socket_keepalive=True
    )

    try:
        last_seen_id = int(START_TARGET_ID) if START_TARGET_ID is not None else 0
    except ValueError:
        last_seen_id = 0

    slot = get_slot()
    print(f"[i] SERVER={SERVER_NAME} uses SLOT={slot} (period={PERIOD_SEC}s)")
    next_fire = align_next_tick(slot)

    while True:
        meta = fetch_top_notice()

        # --- Шлём статус только если ошибка (не-200 или исключение) ---
        if meta.get("code") != 200 or "error" in meta:
            status_payload = {
                "type": "status",
                "found_at": now_iso_ms(),
                "server": SERVER_NAME,
                "slot": slot,
                "http_code": meta.get("code"),          # 429, 500, None (если исключение)
                "rt_ms": round(meta.get("rt_ms", 0.0), 1),
                "etag": meta.get("etag"),
                "x_runtime": meta.get("x_runtime"),
                "resp_size": meta.get("size"),
                "request_url": meta.get("url"),
            }
            if "error" in meta:
                status_payload["error"] = meta["error"]

            rds.publish(CHANNEL, json.dumps(status_payload, ensure_ascii=False))

        # --- Публикуем новый анонс ---
        top = meta.get("top")
        if meta.get("code") == 200 and top:
            try:
                cur_id = int(top.get("id", 0))
            except (TypeError, ValueError):
                cur_id = 0

            title = (top.get("title") or "").strip()
            url   = f"https://upbit.com/service_center/notice?id={cur_id}"

            # отбрасываем брендовые заголовки
            if not (BRAND_MARKER and BRAND_MARKER in title):
                if cur_id > last_seen_id:
                    announce_payload = {
                        "type": "announcement",
                        "found_at": now_iso_ms(),
                        "server": SERVER_NAME,
                        "notice_id": str(cur_id),
                        "title": title,
                        "url": url,
                        "ticker": extract_ticker(title) or "",
                    }
                    n = rds.publish(CHANNEL, json.dumps(announce_payload, ensure_ascii=False))
                    print(f"[NEW] id={cur_id} | subs={n} | {title}")
                    last_seen_id = cur_id

        # строгое выравнивание по слоту
        next_fire += PERIOD_SEC
        sleep_s = max(0.0, next_fire - time.time())
        time.sleep(sleep_s)

if __name__ == "__main__":
    main()
