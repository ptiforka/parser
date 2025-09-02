#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, math, json, re, random, socket
from datetime import datetime, timezone
import requests
import redis
from fake_useragent import UserAgent

# --------- Конфиг ---------
API_BASE = "https://api-manager.upbit.com/api/v1/announcements.json"
REQUEST_TIMEOUT = (2, 3)  # (connect, read)
PERIOD_SEC = 5            # частота одного сервера = 1 раз в 5 сек
CHANNEL = os.getenv("REDIS_CHAN", "announces")

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASS = os.getenv("REDIS_PASS", "")
SERVER_NAME = os.getenv("SERVER_NAME", socket.gethostname())

# Стартовый ID (если не задан — возьмём текущую верхнюю запись как базовую)
START_TARGET_ID = os.getenv("START_TARGET_ID")

# Слот (0..4) для точного распределения запросов по секундам; если не задан — авто.
SLOT_ENV = os.getenv("SLOT")
AUTO_SLOT = None

# Игнорировать брендовые заголовки (как в твоей логике)
BRAND_MARKER = "업비트(Upbit)"

# Тикер из заголовка
TICKER_RE = re.compile(r"\(([A-Z0-9._-]+)\)[^\n]*?(?:신규\s*거래지원\s*안내|디지털\s*자산\s*추가)")

ua = UserAgent()

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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
    # минимальный вариант + рандомный bc чтобы лучше обходить кеши
    params = {
        "os": "android",
        "page": "1",
        "per_page": "1",
        "category": "a",
        "bc": str(int(time.time()*1000)) + str(random.randint(100,999)),
    }
    qs = "&".join(f"{k}={v}" for k,v in params.items())
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

        # лог запроса
        print(f"[req] status={sc} | {dt_ms:.1f} ms | size={size} | etag={etag} | xrt={xrt}")

        top = None
        if sc == 200:
            data = resp.json()
            notices = (data.get("data") or {}).get("notices") or []
            top = notices[0] if notices else None

        return top, sc, dt_ms, etag, xrt
    except requests.RequestException as e:
        dt_ms = (time.perf_counter() - t0) * 1000.0
        print(f"[req] ERROR after {dt_ms:.1f} ms: {e}")
        return None, None, dt_ms, "-", "-"

def extract_ticker(title: str):
    if not title:
        return None
    m = TICKER_RE.search(title)
    return m.group(1) if m else None

def get_slot():
    s = random.SystemRandom().randint(0, 4)
    return s

def align_next_tick(slot: int):
    """
    Вычисляет ближайшее время запуска вида ... секунд, где (sec % 5) == slot,
    затем ждём до него; дальше в цикле всегда +5 секунд.
    """
    now = time.time()
    # граница текущего целого секунда
    now_floor = math.floor(now)
    # секунда-мишень в пределах текущих 5 сек
    base = now_floor - (now_floor % PERIOD_SEC)
    first = base + slot
    # если уже прошли это окно — берём следующее через 5 сек
    if first <= now:
        first += PERIOD_SEC
    # спим до точки выстрела
    sleep_s = max(0.0, first - now)
    if sleep_s > 0:
        time.sleep(sleep_s)
    return first  # момент первого выстрела (epoch seconds)

def main():
    # Redis
    rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                      password=REDIS_PASS or None, decode_responses=True,
                      socket_keepalive=True)

    last_seen_id = int(START_TARGET_ID)

    slot = get_slot()
    print(f"[i] SERVER={SERVER_NAME} uses SLOT={slot} (period={PERIOD_SEC}s)")
    next_fire = align_next_tick(slot)

    while True:
        t0 = time.perf_counter()
        try:
            top, sc, dt_ms, etag, xrt = fetch_top_notice()  # ← теперь есть мета
            if top:
                cur_id = int(top.get("id", 0))
                title = top.get("title", "") or ""
                url = f"https://upbit.com/service_center/notice?id={cur_id}"

                # игнорировать брендовые заголовки
                if BRAND_MARKER and BRAND_MARKER in title:
                    pass
                else:
                    if cur_id > last_seen_id:
                        # нашёл НОВОЕ — публикуем в Redis PUB/SUB
                        payload = {
                            "title": title,
                            "found_at": now_iso(),
                            "server": SERVER_NAME,
                            "notice_id": str(cur_id),
                            "url": url,
                            "ticker": extract_ticker(title) or "",
                        }
                        n = rds.publish(CHANNEL, json.dumps(payload, ensure_ascii=False))
                        print(f"[NEW] id={cur_id} title={title} | subs={n}")

                        last_seen_id = cur_id
            else:
                print("[i] empty notices")

        except requests.HTTPError as e:
            print(f"[http] {e}")
        except Exception as e:
            print(f"[err] {e}")

        # рассчитываем строго следующий выстрел через 5 сек от предыдущего слота
        next_fire += PERIOD_SEC
        now = time.time()
        sleep_s = max(0.0, next_fire - now)
        time.sleep(sleep_s)

if __name__ == "__main__":
    main()
