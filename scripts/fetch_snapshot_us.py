#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KOSMAP 미장 스냅샷 생성기 (P0 데이터 자동 갱신)

동작:
  1) index.html 의 __US_SNAPSHOT__ 에서 종목 목록(81개)·한글명·섹터를 그대로 유지
  2) 1차 소스: Yahoo Finance spark (20심볼 배치) — 현재가·등락률
  3) 지수: Yahoo ^GSPC / ^IXIC, 실패 시 네이버 해외지수(.INX / .IXIC) 폴백 (코드에 실측 기록)
     환율(USD/KRW)은 항상 네이버 FX 엔드포인트 사용(국장 스냅샷과 동일 소스)
  4) 시총: Yahoo 는 시총을 주지 않으므로
       (a) 나스닥 폴백을 탄 종목은 Nasdaq summary 의 실제 MarketCap 사용
       (b) 그 외에는 기존 스냅샷 시총 × (신규가/기존가) 비례 보정
  5) data/snapshot_us.json 원자적 저장. 실패 시 기존 파일 보존, exit 1

검증된 엔드포인트 (2026-11-07 샌드박스 실측):
  · Yahoo: GET https://query2.finance.yahoo.com/v7/finance/spark?symbols=AAPL,MSFT&range=1d&interval=1d
    -> spark.result[].response[0].meta: regularMarketPrice, regularMarketChangePercent,
       chartPreviousClose, regularMarketTime(epoch)
    (주의: 샌드박스 테스트 중 query1/query2 모두 HTTP 429 레이트리밋이 걸린 사례 있음 → 폴백 필요)
  · Nasdaq 폴백: GET https://api.nasdaq.com/api/quote/AAPL/info?assetclass=stocks
    -> data.primaryData.lastSalePrice("$342.37"), percentageChange("-0.52%"), lastTradeTimestamp("Nov 6, 2026")
    GET https://api.nasdaq.com/api/quote/AAPL/summary?assetclass=stocks
    -> data.summaryData.MarketCap(4,975,383,151,340 USD), Sector, Industry
    (하이픈 심볼은 점 표기: BRK-B -> BRK.B)
  · 네이버 해외지수: GET https://api.stock.naver.com/index/.INX/basic , /.IXIC/basic
  · 네이버 환율: GET https://m.stock.naver.com/front-api/marketIndex/prices?category=exchange&reutersCode=FX_USDKRW

사용법:
  python3 scripts/fetch_snapshot_us.py [--out data/snapshot_us.json] [--best-effort] [--verbose]
종료 코드: 0 성공, 1 실패(기존 파일 보존), 2 검증 실패
"""

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zoneinfo

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_HTML = os.path.join(ROOT, "index.html")
OUT_PATH = os.path.join(ROOT, "data", "snapshot_us.json")
BATCH = 20
ET = zoneinfo.ZoneInfo("America/New_York")
MAX_PCT = 60.0
YAHOO_HOSTS = ["https://query2.finance.yahoo.com", "https://query1.finance.yahoo.com"]


# ─────────────────────────── HTTP ───────────────────────────
def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def http_json(url, tries=2, timeout=20, sleep=1.0):
    last = None
    for i in range(tries):
        try:
            return json.loads(http_get(url, timeout))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
            last = e
            if i < tries - 1:
                time.sleep(sleep)
    raise RuntimeError("HTTP 실패 %s (%s)" % (url, last))


def num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        f = float(v)
    else:
        s = str(v).strip().replace(",", "").replace("%", "").replace("$", "")
        if s in ("", "-", "N/A", "NaN", "null"):
            return None
        try:
            f = float(s)
        except ValueError:
            return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


# ─────────────────────────── 임베디드 기준값 ───────────────────────────
def load_embedded_us(path=INDEX_HTML):
    if not os.path.exists(path):
        return None
    marker = "window.__US_SNAPSHOT__="
    line = None
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for ln in f:
            if ln.startswith(marker):
                line = ln
                break
    if not line:
        return None
    start, end = line.find("{"), line.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(line[start:end + 1].rstrip().rstrip(";"))
    except json.JSONDecodeError:
        return None


# ─────────────────────────── 1차: Yahoo ───────────────────────────
def yahoo_batch(chunk):
    q = urllib.parse.quote(",".join(chunk), safe=",^=.-")
    path = "/v7/finance/spark?symbols=%s&range=1d&interval=1d" % q
    last = None
    for attempt in range(2):
        for host in YAHOO_HOSTS:
            try:
                return http_json(host + path, tries=1)
            except RuntimeError as e:
                last = e
        time.sleep(3 + attempt * 4)
    raise RuntimeError("Yahoo 조회 실패(%d심볼): %s" % (len(chunk), last))


def yahoo_spark(symbols, verbose=False):
    out = {}
    total = (len(symbols) + BATCH - 1) // BATCH
    for i in range(0, len(symbols), BATCH):
        chunk = symbols[i:i + BATCH]
        d = yahoo_batch(chunk)
        for r in ((d.get("spark") or {}).get("result") or []):
            meta = ((r.get("response") or [{}])[0] or {}).get("meta") or {}
            if meta.get("symbol"):
                out[meta["symbol"]] = meta
        if verbose:
            print("      yahoo batch %d/%d -> 누적 %d심볼" % (i // BATCH + 1, total, len(out)))
        time.sleep(0.5)
    return out


# ─────────────────────────── 2차 폴백: Nasdaq ───────────────────────────
def nasdaq_symbol(sym):
    return sym.replace("-", ".")


def nasdaq_quote(sym):
    """{'price','pct','prev','date','mcap'} 또는 None."""
    s = urllib.parse.quote(nasdaq_symbol(sym))
    out = {}
    try:
        info = http_json("https://api.nasdaq.com/api/quote/%s/info?assetclass=stocks" % s, tries=2, sleep=1.5)
        pd = ((info.get("data") or {}).get("primaryData") or {})
        out["price"] = num(pd.get("lastSalePrice"))
        out["pct"] = num(pd.get("percentageChange"))
        out["prev"] = None if out["price"] is None or num(pd.get("netChange")) is None \
            else out["price"] - num(pd.get("netChange"))
        out["date"] = pd.get("lastTradeTimestamp")     # 예: "Nov 6, 2026"
    except RuntimeError:
        return None
    if out.get("price") is None:
        return None
    try:
        summ = http_json("https://api.nasdaq.com/api/quote/%s/summary?assetclass=stocks" % s, tries=1, timeout=12)
        mc = num(((summ.get("data") or {}).get("summaryData") or {}).get("MarketCap"))
        if mc:
            # 앱 스냅샷의 시총 단위는 USD 1억(=1e8) 달러 기준 (fmtB: v>=10000 -> v/10000 'T')
            out["mcap"] = mc / 1e8
    except RuntimeError:
        pass
    return out


# ─────────────────────────── 지수·환율 ───────────────────────────
def naver_index(sym):
    """네이버 해외지수 (.INX = S&P500, .IXIC = 나스닥). 실패 시 None."""
    try:
        d = http_json("https://api.stock.naver.com/index/%s/basic" % sym, tries=2)
    except RuntimeError:
        return None
    v = num(d.get("closePrice"))
    if v is None:
        return None
    return {"v": round(v, 2),
            "d": num(d.get("compareToPreviousClosePrice")),
            "p": num(d.get("fluctuationsRatio"))}


def naver_usdkrw():
    d = http_json("https://m.stock.naver.com/front-api/marketIndex/prices"
                  "?category=exchange&reutersCode=FX_USDKRW", tries=3)
    r = (d.get("result") or [{}])[0]
    v = num(r.get("closePrice"))
    if v is None:
        raise RuntimeError("환율 파싱 실패")
    return {"v": v, "d": num(r.get("fluctuations")), "p": num(r.get("fluctuationsRatio"))}, r.get("localTradedAt")


def month_day_year(s):
    """'Nov 6, 2026' -> date(2026,11,6). 실패 시 None."""
    if not s:
        return None
    try:
        return dt.datetime.strptime(str(s).strip(), "%b %d, %Y").date()
    except ValueError:
        return None


# ─────────────────────────── main ───────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT_PATH)
    ap.add_argument("--best-effort", action="store_true",
                    help="일부 심볼 미수신이어도 직전값으로 채워 저장(기본은 실패 처리)")
    ap.add_argument("--allow-older", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()

    print("[1/5] 기존 __US_SNAPSHOT__ 로드")
    base = load_embedded_us()
    if not base or not base.get("rows"):
        print("✗ 실패: index.html 에서 __US_SNAPSHOT__ 를 찾지 못했습니다.", file=sys.stderr)
        return 1
    base_rows = {str(r[0]): r for r in base["rows"]}
    symbols = [str(r[0]) for r in base["rows"]]
    print("      기존 %d종목, ts=%s" % (len(symbols), base.get("ts")))

    print("[2/5] Yahoo 시세 수집")
    quotes, yahoo_ok, yahoo_err = {}, 0, None
    try:
        metas = yahoo_spark(symbols + ["^GSPC", "^IXIC"], a.verbose)
        for sym in symbols:
            m = metas.get(sym)
            if not m:
                continue
            price, pct = num(m.get("regularMarketPrice")), num(m.get("regularMarketChangePercent"))
            if price is None or price <= 0 or pct is None or abs(pct) > MAX_PCT:
                continue
            quotes[sym] = {"price": price, "pct": pct,
                           "prev": num(m.get("chartPreviousClose")),
                           "time": num(m.get("regularMarketTime")), "src": "yahoo"}
            yahoo_ok += 1
        idx_raw = {k: metas.get(k) for k in ("^GSPC", "^IXIC")}
    except RuntimeError as e:
        yahoo_err = str(e)
        idx_raw = {}
        print("      경고: Yahoo 실패 -> Nasdaq 폴백 사용 (%s)" % str(e)[:120], file=sys.stderr)
    print("      Yahoo 수신 %d/%d" % (yahoo_ok, len(symbols)))

    missing = [s for s in symbols if s not in quotes]
    if missing:
        print("[3/5] Nasdaq 폴백 %d심볼" % len(missing))
        for i, sym in enumerate(missing):
            q = nasdaq_quote(sym)
            if q:
                quotes[sym] = {"price": q["price"], "pct": q["pct"], "prev": q.get("prev"),
                               "date": month_day_year(q.get("date")),
                               "mcap": q.get("mcap"), "src": "nasdaq"}
            if a.verbose and (i + 1) % 20 == 0:
                print("      폴백 %d/%d" % (i + 1, len(missing)))
            time.sleep(0.2)
        got = sum(1 for s in missing if s in quotes)
        print("      Nasdaq 수신 %d/%d" % (got, len(missing)))
    still = [s for s in symbols if s not in quotes]
    if still:
        msg = "미수신 %d심볼: %s" % (len(still), ",".join(still[:20]))
        if not a.best_effort:
            print("✗ 실패: " + msg + " (— --best-effort 로 직전값 유지 가능)", file=sys.stderr)
            return 1
        print("      경고: " + msg + " → 직전값 유지", file=sys.stderr)

    print("[4/5] 지수·환율")
    def idx_from_yahoo(sym, prev_last):
        m = idx_raw.get(sym)
        if not m:
            return None
        v = num(m.get("regularMarketPrice"))
        if v is None:
            return None
        prev = num(m.get("chartPreviousClose"))
        return {"v": round(v, 2),
                "d": round(v - prev, 2) if prev else num(m.get("regularMarketChange")),
                "p": num(m.get("regularMarketChangePercent"))}

    spx = idx_from_yahoo("^GSPC", None) or naver_index(".INX")
    nasdaq = idx_from_yahoo("^IXIC", None) or naver_index(".IXIC")
    if not spx or not nasdaq:
        print("✗ 실패: 지수 수신 실패 (spx=%r nasdaq=%r)" % (spx, nasdaq), file=sys.stderr)
        return 2
    try:
        fx, fx_at = naver_usdkrw()
    except RuntimeError as e:
        fx = (base.get("indexes") or {}).get("usdkrw")
        print("      경고: 환율 실패 → 기존값 유지 (%s)" % e, file=sys.stderr)
    print("      S&P500 %.2f (%+.2f%%) · 나스닥 %.2f (%+.2f%%) · USD/KRW %.2f (%+.2f%%)"
          % (spx["v"], spx["p"] or 0, nasdaq["v"], nasdaq["p"] or 0, fx["v"], fx["p"] or 0))

    print("[5/5] 행 조립·검증·저장")
    rows, stale = [], []
    for sym in symbols:
        b = base_rows[sym]
        q = quotes.get(sym)
        if not q:
            q = {"price": float(b[3]), "pct": float(b[4]), "prev": None, "src": "stale"}
        old_price = num(b[3]) or q["price"]
        old_mcap = num(b[5]) or 0.0
        if q.get("mcap"):
            mcap = q["mcap"]                       # Nasdaq 실측 시총 (USD 1e8 단위)
        else:
            mcap = old_mcap * (q["price"] / old_price) if old_price > 0 else old_mcap
        if q["src"] == "stale":
            stale.append(sym)
        rows.append([sym, str(b[1]), str(b[2]),
                     round(q["price"], 2), round(q["pct"], 2), round(mcap, 1)])
    rows.sort(key=lambda r: -r[5])

    # 기준일: yahoo regularMarketTime(ET) / nasdaq lastTradeTimestamp 중 가장 최근
    dates = []
    for q in quotes.values():
        if q.get("time"):
            dates.append(dt.datetime.fromtimestamp(q["time"], tz=ET).date())
        elif isinstance(q.get("date"), dt.date):
            dates.append(q["date"])
    if not dates:
        print("✗ 실패: 거래일 정보 없음", file=sys.stderr)
        return 2
    ts = max(dates).isoformat()

    snapshot = {
        "ts": ts,
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "source": "yahoo+nasdaq" if (yahoo_ok and missing) else ("yahoo" if yahoo_ok else "nasdaq"),
        "indexes": {"spx": spx, "nasdaq": nasdaq, "usdkrw": fx},
        "rows": rows,
    }

    errs = []
    if len(rows) != len(symbols):
        errs.append("종목수 %d != 기대 %d" % (len(rows), len(symbols)))
    for r in rows:
        if not (isinstance(r, list) and len(r) == 6):
            errs.append("행 구조 오류: %r" % (r,))
            break
        if not all(isinstance(x, (int, float)) for x in r[3:6]):
            errs.append("숫자 타입 오류: %r" % (r,))
            break
        if r[3] <= 0 or r[5] <= 0 or r[3] != r[3] or r[5] != r[5]:
            errs.append("가격/시총 이상: %r" % (r,))
            break
    if len({r[0] for r in rows}) != len(rows):
        errs.append("심볼 중복")
    if errs:
        print("✗ 검증 실패:\n  - " + "\n  - ".join(errs), file=sys.stderr)
        return 2

    old_ts = base.get("ts")
    if old_ts and ts < old_ts and not a.allow_older:
        print("✗ 실패: 새 데이터(%s)가 기존(%s)보다 오래됨. --allow-older 로 강제." % (ts, old_ts), file=sys.stderr)
        return 1

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    tmp = a.out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp, a.out)

    print("✓ %s  ts=%s  %d종목  (%.1f KB)  source=%s"
          % (a.out, ts, len(rows), os.path.getsize(a.out) / 1024.0, snapshot["source"]))
    print("  대표값: %s %s $%s / %s $%s"
          % (rows[0][1], rows[0][0], rows[0][3], rows[1][1], rows[1][3]))
    if stale:
        print("  주의: 직전값 유지 %d종목 %s" % (len(stale), ",".join(stale[:10])), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
