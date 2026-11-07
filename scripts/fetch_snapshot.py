#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KOSMAP 국장 스냅샷 생성기 (P0 데이터 자동 갱신)

동작:
  1) 네이버 모바일 증권 API에서 시가총액 상위 종목을 페이지네이션으로 수집
  2) 지수(코스피/코스닥)와 원달러 환율을 네이버 API에서 취득
  3) 섹터는 index.html 안의 기존 SNAPSHOT(코드→섹터)을 재사용, 신규 종목은 "기타"
  4) data/snapshot.json 원자적 저장 (실패 시 기존 파일 보존, exit 1)

검증된 엔드포인트 (2026-11-07 샌드박스 실측):
  · 종목: GET https://m.stock.naver.com/api/stocks/marketValue/{KOSPI|KOSDAQ}?page=1&pageSize=100
          -> stocks[]: itemCode, stockName, closePrice("193,500"), fluctuationsRatio("-3.97"),
             marketValue("11,312,549" = 억원), localTradedAt("2026-11-06T20:20:24+09:00"), marketStatus("CLOSE")
          totalCount 필드로 전체 종목 수 확인 가능(KOSPI 2487). pageSize 최대 100, page 1-based.
  · 지수: GET https://m.stock.naver.com/api/index/{KOSPI|KOSDAQ}/basic
          -> closePrice("5,059.69"), compareToPreviousClosePrice("-324.11"), fluctuationsRatio("-6.02"),
             localTradedAt("2026-11-06T20:15:00+09:00"), marketStatus("CLOSE")
          (참고: https://m.stock.naver.com/api/index/USDKRW/basic 는 {"code":"StockConflict"} 로 실패)
  · 환율: GET https://m.stock.naver.com/front-api/marketIndex/prices?category=exchange&reutersCode=FX_USDKRW
          -> result[0] = 최신: localTradedAt("2026-11-06" 날짜만), closePrice("1,364.00"),
             fluctuations("7.00"), fluctuationsRatio("0.52")

사용법:
  python3 scripts/fetch_snapshot.py                      # 기본 140/39 종목
  KOSPI_COUNT=200 KOSDAQ_COUNT=100 python3 scripts/fetch_snapshot.py
  python3 scripts/fetch_snapshot.py --kospi 140 --kosdaq 39 --allow-open --verbose

종료 코드: 0 성공, 1 실패(기존 data/snapshot.json 보존), 2 데이터 없음/검증 실패
"""

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

UA = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 "
      "(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_HTML = os.path.join(ROOT, "index.html")
OUT_PATH = os.path.join(ROOT, "data", "snapshot.json")
SECTORS_PATH = os.path.join(ROOT, "data", "sectors.json")

DEFAULT_KOSPI = int(os.environ.get("KOSPI_COUNT", "140"))
DEFAULT_KOSDAQ = int(os.environ.get("KOSDAQ_COUNT", "39"))
PAGE_SIZE = 100
SECTOR_FALLBACK = "기타"
MAX_PCT = 35.0        # 검증 상한 (액면분할/이상치 방어)


# ─────────────────────────── HTTP ───────────────────────────
def http_json(url, tries=3, timeout=20):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://m.stock.naver.com/",
            })
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read().decode("utf-8")
            return json.loads(body)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
            last = e
            if i < tries - 1:
                time.sleep(1.5 * (i + 1))
    raise RuntimeError("HTTP 실패 %s (%s)" % (url, last))


# ─────────────────────────── 파싱 유틸 ───────────────────────────
def num(v):
    """'1,234.5' / '-3.97' / 1234 같은 값을 float 로. 실패 시 None."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        f = float(v)
    else:
        s = str(v).strip().replace(",", "").replace("%", "")
        if s in ("", "-", "N/A", "NaN", "null"):
            return None
        try:
            f = float(s)
        except ValueError:
            return None
    if f != f or f in (float("inf"), float("-inf")):   # NaN/Inf 방어
        return None
    return f


def load_embedded_snapshot(path=INDEX_HTML):
    """index.html 안의 `const SNAPSHOT = {...};` 한 줄을 읽어 dict 로 반환."""
    if not os.path.exists(path):
        return None
    marker = "const SNAPSHOT ="
    line = None
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for ln in f:
            if ln.startswith(marker):
                line = ln
                break
    if not line:
        return None
    start = line.find("{")
    end = line.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(line[start:end + 1])
    except json.JSONDecodeError:
        return None


def load_sector_store(path, embedded):
    """코드→섹터 저장소. index.html SNAPSHOT 을 씨앗으로, data/sectors.json 이 우선.
    (사람이 신규 종목 섹터를 채워 넣어도 다음 실행에서 유지되게)"""
    store = {}
    if embedded:
        for key in ("kospi", "kosdaq"):
            for row in embedded.get(key) or []:
                try:
                    store[str(row[0])] = str(row[2])
                except (IndexError, TypeError):
                    continue
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                saved = json.load(f)
            if isinstance(saved, dict):
                for k, v in saved.items():
                    if isinstance(k, str) and isinstance(v, str) and v:
                        store[k] = v
        except (OSError, json.JSONDecodeError):
            pass
    return store


def save_sector_store(path, store):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({k: store[k] for k in sorted(store)}, f, ensure_ascii=False, indent=0)
            f.write("\n")
        os.replace(tmp, path)
    except OSError:
        pass


def sector_map_from_embedded(embedded):
    """코드 → 섹터. 기존 종목의 섹터가 바뀌지 않도록 재사용."""
    m = {}
    if not embedded:
        return m
    for key in ("kospi", "kosdaq"):
        for row in embedded.get(key) or []:
            try:
                m[str(row[0])] = str(row[2])
            except (IndexError, TypeError):
                continue
    return m


# ─────────────────────────── 수집 ───────────────────────────
PREF_RE = re.compile(r"\d?우[A-C]?$")   # 삼성전자우 / 현대차2우B 같은 우선주


def keep_stock(s):
    """개별 보통주만 유지: ETF/ETN/우선주 제외 (히트맵 종목 구성 유지)."""
    if str(s.get("stockEndType") or "stock") != "stock":
        return False
    name = str(s.get("stockName") or "")
    if PREF_RE.search(name):
        return False
    if str(s.get("stockType") or "domestic") != "domestic":
        return False
    return True


def fetch_market_list(category, want, verbose=False, include_etf=False):
    """marketValue API 를 100개 단위로 페이지네이션하며 상위 want 종목 수집.

    ETF/우선주를 걸러내므로 want 개를 채울 때까지 페이지를 더 넘긴다.
    주의: 네이버 API 의 페이지 오프셋은 (page-1)*pageSize 이므로
          마지막 페이지에서 작은 pageSize 를 쓰면 앞 페이지와 겹친다.
          → 항상 PAGE_SIZE=100 으로 요청한다.
    """
    out, total, page = [], None, 1
    while len(out) < want and page <= 20:
        url = ("https://m.stock.naver.com/api/stocks/marketValue/%s?page=%d&pageSize=%d"
               % (category, page, PAGE_SIZE))
        d = http_json(url)
        if page == 1:
            total = d.get("totalCount")
        stocks = d.get("stocks") or []
        if not stocks:
            break
        kept = 0
        for s in stocks:
            if include_etf or keep_stock(s):
                out.append(s)
                kept += 1
        if verbose:
            print("  %s page=%d 수신=%d 채택=%d 누적=%d (total=%s)"
                  % (category, page, len(stocks), kept, len(out), total))
        if len(stocks) < PAGE_SIZE:
            break
        page += 1
    return out[:want], total


def fetch_index(symbol):
    d = http_json("https://m.stock.naver.com/api/index/%s/basic" % symbol)
    v = num(d.get("closePrice"))
    diff = num(d.get("compareToPreviousClosePrice"))
    pct = num(d.get("fluctuationsRatio"))
    if v is None:
        raise RuntimeError("%s 지수 값 없음: %r" % (symbol, d.get("closePrice")))
    return {"v": v, "d": diff, "p": pct}, d.get("localTradedAt"), d.get("marketStatus")


def fetch_usdkrw():
    d = http_json("https://m.stock.naver.com/front-api/marketIndex/prices"
                  "?category=exchange&reutersCode=FX_USDKRW")
    rows = d.get("result") or []
    if not rows:
        raise RuntimeError("환율 응답 비어 있음")
    r = rows[0]
    v = num(r.get("closePrice"))
    if v is None:
        raise RuntimeError("환율 값 파싱 실패: %r" % r.get("closePrice"))
    return ({"v": v, "d": num(r.get("fluctuations")), "p": num(r.get("fluctuationsRatio"))},
            r.get("localTradedAt"))


def date_of(local_traded_at):
    if not local_traded_at:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", str(local_traded_at))
    return m.group(1) if m else None


# ─────────────────────────── 조립 ───────────────────────────
def build_rows(raw_stocks, sectors, verbose=False):
    rows, seen, skipped = [], set(), 0
    for s in raw_stocks:
        code = str(s.get("itemCode") or "").strip()
        name = str(s.get("stockName") or "").strip()
        price = num(s.get("closePrice"))
        pct = num(s.get("fluctuationsRatio"))
        mcap = num(s.get("marketValue"))     # 억원
        if not code or not name or price is None or pct is None or mcap is None:
            skipped += 1
            if verbose:
                print("  skip %s %s price=%r pct=%r mcap=%r" % (code, name, price, pct, mcap))
            continue
        if price <= 0 or mcap <= 0 or abs(pct) > MAX_PCT:
            skipped += 1
            if verbose:
                print("  skip(범위) %s %s price=%s pct=%s mcap=%s" % (code, name, price, pct, mcap))
            continue
        if code in seen:
            if verbose:
                print("  skip(중복코드) %s %s" % (code, name))
            continue
        seen.add(code)
        rows.append([code, name, sectors.get(code, SECTOR_FALLBACK),
                     round(price, 2), round(pct, 2), round(mcap, 1)])
    rows.sort(key=lambda r: -r[5])
    if skipped and verbose:
        print("  제외 %d건" % skipped)
    return rows


def validate(snapshot, want_kospi, want_kosdaq):
    errs = []
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(snapshot.get("ts", ""))):
        errs.append("ts 형식 오류: %r" % snapshot.get("ts"))
    for key, want in (("kospi", want_kospi), ("kosdaq", want_kosdaq)):
        rows = snapshot.get(key)
        if not isinstance(rows, list) or not rows:
            errs.append("%s 비어 있음" % key)
            continue
        if len(rows) != want:
            errs.append("%s 종목수 %d != 요청 %d" % (key, len(rows), want))
        codes = set()
        prev = None
        for r in rows:
            if not (isinstance(r, list) and len(r) == 6):
                errs.append("%s 행 구조 오류: %r" % (key, r))
                break
            code, name, sector, price, pct, mcap = r
            if not isinstance(code, str) or not isinstance(name, str) or not isinstance(sector, str):
                errs.append("%s 문자열 필드 오류: %r" % (key, r))
                break
            for label, v in (("price", price), ("pct", pct), ("mcap", mcap)):
                if not isinstance(v, float) and not isinstance(v, int):
                    errs.append("%s %s 타입 오류: %r" % (key, label, v))
                    break
                if v != v:
                    errs.append("%s %s NaN" % (key, label))
                    break
            if code in codes:
                errs.append("%s 코드 중복: %s" % (key, code))
                break
            codes.add(code)
            if prev is not None and mcap > prev + 1e-6:
                errs.append("%s 시총 정렬 깨짐: %s" % (key, code))
                break
            prev = mcap
    idx = snapshot.get("indexes") or {}
    for k in ("kospi", "kosdaq"):
        o = idx.get(k) or {}
        if not isinstance(o.get("v"), (int, float)) or o["v"] <= 0:
            errs.append("indexes.%s 값 오류: %r" % (k, o))
    return errs


def write_atomic(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kospi", type=int, default=DEFAULT_KOSPI)
    ap.add_argument("--kosdaq", type=int, default=DEFAULT_KOSDAQ)
    ap.add_argument("--out", default=OUT_PATH)
    ap.add_argument("--allow-open", action="store_true",
                    help="장중(CLOSE 아님)에도 저장. 기본은 장 마감 확인 실패 시 중단")
    ap.add_argument("--allow-older", action="store_true",
                    help="기존 스냅샷보다 오래된 ts 여도 저장")
    ap.add_argument("--include-etf", action="store_true",
                    help="ETF/ETN/우선주도 포함(기본은 개별 보통주만)")
    ap.add_argument("--verbose", action="store_true")
    a = ap.parse_args()

    print("[1/5] 기존 스냅샷에서 섹터 매핑 로드")
    embedded = load_embedded_snapshot()
    sectors = load_sector_store(SECTORS_PATH, embedded)
    if sectors:
        print("      섹터 저장소 %d종목 (index.html SNAPSHOT + data/sectors.json), 기존 ts=%s"
              % (len(sectors), (embedded or {}).get("ts")))
    else:
        print("      경고: 섹터 정보 없음. 모든 종목 섹터는 '%s'" % SECTOR_FALLBACK)

    print("[2/5] 종목 수집")
    kospi_raw, kospi_total = fetch_market_list("KOSPI", a.kospi, a.verbose, a.include_etf)
    kosdaq_raw, kosdaq_total = fetch_market_list("KOSDAQ", a.kosdaq, a.verbose, a.include_etf)
    print("      KOSPI %d건(전체 %s), KOSDAQ %d건(전체 %s)"
          % (len(kospi_raw), kospi_total, len(kosdaq_raw), kosdaq_total))

    print("[3/5] 지수·환율")
    kospi_idx, kospi_at, kospi_status = fetch_index("KOSPI")
    kosdaq_idx, kosdaq_at, kosdaq_status = fetch_index("KOSDAQ")
    usdkrw, fx_at = fetch_usdkrw()
    print("      KOSPI %.2f (%+.2f) / KOSDAQ %.2f (%+.2f) / USDKRW %.2f (%+.2f)"
          % (kospi_idx["v"], kospi_idx["p"] or 0, kosdaq_idx["v"], kosdaq_idx["p"] or 0,
             usdkrw["v"], usdkrw["p"] or 0))

    print("[4/5] 행 조립·검증")
    kospi_rows = build_rows(kospi_raw, sectors, a.verbose)
    kosdaq_rows = build_rows(kosdaq_raw, sectors, a.verbose)

    # 데이터 기준일 = 종목 응답의 세션 날짜(가장 많이 등장한 값)
    dates = [date_of(s.get("localTradedAt")) for s in kospi_raw + kosdaq_raw]
    dates = [d for d in dates if d]
    if not dates:
        print("✗ 실패: 종목 응답에 거래일(localTradedAt)이 없습니다.", file=sys.stderr)
        return 2
    ts = max(set(dates), key=dates.count)

    statuses = [s.get("marketStatus") for s in kospi_raw + kosdaq_raw]
    closed = sum(1 for s in statuses if s == "CLOSE")
    is_closed = closed >= max(1, int(len(statuses) * 0.9))
    if not is_closed and not a.allow_open:
        print("✗ 실패: 장 마감 상태가 아닙니다(CLOSE %d/%d, 지수 %s/%s). 장 마감 후 실행하거나 --allow-open."
              % (closed, len(statuses), kospi_status, kosdaq_status), file=sys.stderr)
        return 1

    snapshot = {
        "ts": ts,
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "source": "naver",
        "indexes": {"kospi": kospi_idx, "kosdaq": kosdaq_idx, "usdkrw": usdkrw},
        "kospi": kospi_rows,
        "kosdaq": kosdaq_rows,
    }

    errs = validate(snapshot, a.kospi, a.kosdaq)
    if errs:
        print("✗ 검증 실패:\n  - " + "\n  - ".join(errs), file=sys.stderr)
        return 2

    if embedded and not a.allow_older:
        old_ts = embedded.get("ts")
        if old_ts and ts < old_ts:
            print("✗ 실패: 새 데이터(%s)가 기존 스냅샷(%s)보다 오래되었습니다. --allow-older 로 강제 가능."
                  % (ts, old_ts), file=sys.stderr)
            return 1

    print("[5/5] 저장")
    try:
        write_atomic(a.out, snapshot)
    except OSError as e:
        print("✗ 저장 실패(기존 파일 유지): %s" % e, file=sys.stderr)
        return 1

    for code, _n, sector, *_ in kospi_rows + kosdaq_rows:
        sectors.setdefault(code, sector)
        if sector != SECTOR_FALLBACK:
            sectors[code] = sector
    save_sector_store(SECTORS_PATH, sectors)

    print("✓ %s  ts=%s  KOSPI %d종목 / KOSDAQ %d종목  (%.1f KB)"
          % (a.out, ts, len(kospi_rows), len(kosdaq_rows), os.path.getsize(a.out) / 1024.0))
    print("  코스피 %.2f (%+.2f%%) · 코스닥 %.2f (%+.2f%%) · USD/KRW %.2f (%+.2f%%)"
          % (kospi_idx["v"], kospi_idx["p"] or 0, kosdaq_idx["v"], kosdaq_idx["p"] or 0,
             usdkrw["v"], usdkrw["p"] or 0))
    print("  대표값: %s %s / %s" % (kospi_rows[0][1], kospi_rows[0][3], kosdaq_rows[0][1] + " " + str(kosdaq_rows[0][3])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
