"""
Онлайн-монитор CRMP/SA-MP сервера: сбор + хранение + сайт с графиками.
Замер каждые POLL_SEC секунд (по умолчанию 5), история по минутам.
Работает на Render (бесплатный Web Service). Только psycopg2 для базы.
"""
import bisect
import gzip
import html
import json
import os
import re
import signal
import socket
import struct
import sys
import threading
import time
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SERVER = os.environ.get("SERVER_ADDR", "80.66.71.92:7777")
# MODE=web: брать онлайн со страницы сайта-монитора (работает с хостингов)
# MODE=udp: спрашивать игровой сервер напрямую (работает только с «домашнего» адреса)
MODE = os.environ.get("MODE", "web").lower()
WEB_URL = os.environ.get("WEB_URL", "https://libertycity.ru/servers/radmir-rp/")
WEB_ROMAN = os.environ.get("WEB_SERVER_NAME", "XXI")  # как сервер подписан на сайте
POLL = float(os.environ.get("POLL_SEC", "60" if MODE == "web" else "5"))
FLUSH = int(os.environ.get("FLUSH_MIN", "30")) * 60
DB_URL = os.environ.get("DATABASE_URL", "")
PORT = int(os.environ.get("PORT", "10000"))
HERE = os.path.dirname(os.path.abspath(__file__))

lock = threading.Lock()
live = deque(maxlen=int(3600 / POLL) + 60)  # (unix_sec, online) ; online=-1 = нет ответа
m_ts, m_avg, m_max = [], [], []  # история по минутам (отсортирована)
pending = []  # минуты, ещё не записанные в базу
cur = {"min": None, "sum": 0, "n": 0, "mx": 0}
state = {"last_ok": None, "last_max": None, "last_err": "", "db": "off", "db_err": "",
         "started": int(time.time()), "samples": 0, "fails": 0, "loaded": 0}


# ---------- запрос к игровому серверу ----------
def query_info(host, port, timeout=2.0):
    ip = socket.gethostbyname(host)
    packet = b"SAMP" + socket.inet_aton(ip) + struct.pack("<H", port) + b"i"
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        s.sendto(packet, (ip, port))
        data, _ = s.recvfrom(2048)
    return struct.unpack_from("<HH", data, 12)


# ---------- онлайн со страницы сайта-монитора ----------
def _web_text():
    req = urllib.request.Request(WEB_URL, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept-Language": "ru-RU,ru;q=0.9", "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=15) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        charset = r.headers.get_content_charset() or "utf-8"
    text = raw.decode(charset, errors="replace")
    text = re.sub(r"<(script|style)\b.*?</\1>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text))


def _web_matches(plain):
    pat = r"(?<![A-Za-zА-Яа-я])" + re.escape(WEB_ROMAN) + r"(?![A-Za-zА-Яа-я])\s*\|?\s*(\d{1,5})(?!\d)"
    return [int(m.group(1)) for m in re.finditer(pat, plain)]


def web_sample():
    plain = _web_text()
    vals = [v for v in _web_matches(plain) if v <= 5000]
    if not vals:
        raise RuntimeError(f"на странице не найден сервер {WEB_ROMAN}")
    u = re.search(r"Обновлено\s+(\d{1,2}:\d{2})\s*МСК", plain)
    state["src_updated"] = u.group(1) if u else ""
    return vals[0], None


def web_debug():
    try:
        plain = _web_text()
    except Exception as e:
        return {"ошибка": str(e) or e.__class__.__name__}
    i = plain.find(WEB_ROMAN)
    return {"найдено_чисел": _web_matches(plain), "длина_текста": len(plain),
            "фрагмент": plain[max(0, i - 80):i + 160] if i >= 0 else plain[:300]}


# ---------- минутные агрегаты ----------
def _finalize_minute():
    if cur["min"] is not None and cur["n"] > 0:
        ts = cur["min"] * 60
        avg = cur["sum"] / cur["n"]
        m_ts.append(ts); m_avg.append(avg); m_max.append(cur["mx"])
        pending.append((ts, avg, cur["mx"]))
    cur["min"], cur["sum"], cur["n"], cur["mx"] = None, 0, 0, 0


def _roll(now, online=None):
    mnt = now // 60
    if cur["min"] is not None and cur["min"] != mnt:
        _finalize_minute()
    if online is not None:
        if cur["min"] is None:
            cur["min"] = mnt
        cur["sum"] += online; cur["n"] += 1
        cur["mx"] = max(cur["mx"], online)


def collector():
    host, port = SERVER.rsplit(":", 1)
    port = int(port)
    while True:
        t0 = time.time()
        try:
            players, maxp = web_sample() if MODE == "web" else query_info(host, port)
            ok, err = True, ""
        except Exception as e:
            ok, err, players, maxp = False, str(e) or e.__class__.__name__, -1, -1
        now = int(t0)
        with lock:
            state["samples"] += 1
            live.append((now, players if ok else -1))
            if ok:
                state["last_ok"], state["last_err"] = now, ""
                if maxp is not None:
                    state["last_max"] = maxp
                _roll(now, players)
            else:
                state["fails"] += 1
                state["last_err"] = err
                _roll(now)
        time.sleep(max(0.5, POLL - (time.time() - t0)))


# ---------- база данных (Neon / любой Postgres) ----------
def db_connect():
    import psycopg2
    return psycopg2.connect(DB_URL, connect_timeout=15)


def db_load():
    if not DB_URL:
        return
    try:
        conn = db_connect()
        with conn, conn.cursor() as c:
            c.execute("CREATE TABLE IF NOT EXISTS minutes (ts BIGINT PRIMARY KEY, avg REAL, mx INTEGER)")
            c.execute("SELECT ts, avg, mx FROM minutes ORDER BY ts")
            rows = c.fetchall()
        conn.close()
        with lock:
            for ts, avg, mx in rows:
                m_ts.append(int(ts)); m_avg.append(float(avg)); m_max.append(int(mx))
            state["loaded"] = len(rows)
            state["db"] = "ok"
    except Exception as e:
        state["db"], state["db_err"] = "error", str(e)[:200]


def flush():
    if not DB_URL:
        return
    with lock:
        batch = list(pending)
    if not batch:
        return
    try:
        conn = db_connect()
        with conn, conn.cursor() as c:
            c.execute("CREATE TABLE IF NOT EXISTS minutes (ts BIGINT PRIMARY KEY, avg REAL, mx INTEGER)")
            c.executemany("INSERT INTO minutes (ts, avg, mx) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", batch)
        conn.close()
        with lock:
            del pending[:len(batch)]
        state["db"], state["db_err"] = "ok", ""
    except Exception as e:
        state["db"], state["db_err"] = "error", str(e)[:200]


def flusher():
    while True:
        time.sleep(FLUSH)
        flush()


def on_term(signum, frame):
    try:
        with lock:
            _finalize_minute()
        flush()
    finally:
        os._exit(0)


# ---------- данные для графиков ----------
def build(sec):
    now = int(time.time())
    with lock:
        use_live = bool(sec) and sec <= 3600 and len(live) >= 10
        if use_live:
            frm = now - sec
            src = [(t, v, v) for t, v in live if t >= frm and v >= 0]
            step = POLL
        else:
            frm = now - sec if sec else (m_ts[0] if m_ts else now - 3600)
            i = bisect.bisect_left(m_ts, frm)
            src = list(zip(m_ts[i:], m_avg[i:], m_max[i:]))
            step = 60
    span = max(1, now - frm)
    bsz = max(step, span / 500)
    bsz = -(-bsz // step) * step  # шаг группировки кратен интервалу замеров, иначе будут дырки
    n = int(span // bsz) + 1
    sums, cnts = [0.0] * n, [0] * n
    for t, a, _ in src:
        k = min(n - 1, int((t - frm) // bsz))
        sums[k] += a; cnts[k] += 1
    points = [[int(frm + k * bsz + bsz / 2), round(sums[k] / cnts[k]) if cnts[k] else None] for k in range(n)]
    stats = None
    if src:
        mx_row = max(src, key=lambda r: r[2])
        avgs = [r[1] for r in src]
        stats = {"max": mx_row[2], "max_t": mx_row[0], "min": round(min(avgs)),
                 "avg": round(sum(avgs) / len(avgs)), "n": len(src)}
    return {"from": frm, "to": now, "points": points, "stats": stats, "live": use_live}


def now_info():
    t = int(time.time())
    with lock:
        last = next(((tt, v) for tt, v in reversed(live) if v >= 0), None)
        return {"t": t, "online": last[1] if last else None, "sample_t": last[0] if last else None,
                "max": state["last_max"], "last_err": state["last_err"],
                "poll": POLL, "mode": MODE, "src_updated": state.get("src_updated", "")}


def udp_test():
    """Проверка: пропускает ли хостинг исходящий UDP вообще (запрос к публичным DNS)."""
    query = (b"\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00"
             b"\x07example\x03com\x00\x00\x01\x00\x01")
    result = {}
    for ip in ("8.8.8.8", "1.1.1.1"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.settimeout(3)
                s.sendto(query, (ip, 53))
                data, _ = s.recvfrom(512)
            result[ip] = f"ответ получен ({len(data)} байт)"
        except Exception as e:
            result[ip] = "нет ответа: " + (str(e) or e.__class__.__name__)
    result["вывод"] = ("UDP наружу работает — значит, игровой сервер не отвечает хостингу"
                       if any(v.startswith("ответ") for v in result.values())
                       else "UDP наружу, похоже, заблокирован на хостинге")
    return result


def status_info():
    with lock:
        return dict(state, minutes_in_memory=len(m_ts), pending=len(pending), server=SERVER,
                    poll_sec=POLL, mode=MODE, uptime_sec=int(time.time()) - state["started"])


# ---------- веб-сервер ----------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype, head=False):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head:
            self.wfile.write(body)

    def _route(self, head):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as f:
                return self._send(200, f.read(), "text/html; charset=utf-8", head)
        if u.path == "/health":
            return self._send(200, b"ok", "text/plain", head)
        if u.path == "/api/now":
            data = now_info()
        elif u.path == "/api/history":
            try:
                sec = max(0, int(q.get("sec", ["86400"])[0]))
            except ValueError:
                sec = 86400
            data = build(sec)
        elif u.path == "/api/status":
            data = status_info()
        elif u.path == "/api/udptest":
            data = udp_test()
        elif u.path == "/api/source":
            data = web_debug()
        else:
            return self._send(404, b"not found", "text/plain", head)
        self._send(200, json.dumps(data).encode(), "application/json", head)

    def do_GET(self):
        self._route(False)

    def do_HEAD(self):
        self._route(True)


def main():
    db_load()
    signal.signal(signal.SIGTERM, on_term)
    threading.Thread(target=collector, daemon=True).start()
    threading.Thread(target=flusher, daemon=True).start()
    src = WEB_URL if MODE == "web" else SERVER
    print(f"Старт: режим {MODE}, источник {src}, опрос каждые {POLL} с, порт {PORT}, база: {state['db']}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
