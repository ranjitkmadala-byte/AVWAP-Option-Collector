"""Railway worker: 09:20 option money leaders and 3m-vs-1h AVWAP scanner."""
from __future__ import annotations

import gzip, json, logging, os, time
from collections import defaultdict
from datetime import date, datetime, time as dtime, timedelta
from urllib.parse import quote
from zoneinfo import ZoneInfo

import psycopg
import requests

IST = ZoneInfo("Asia/Kolkata")
OPEN, BASELINE, SELECT, FREEZE, CLOSE = dtime(9,15), dtime(9,15), dtime(9,20), dtime(10,15), dtime(15,30)
DB = os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL")
TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN") or os.getenv("UPSTOX_TOKEN")
STRIKES_EACH_SIDE = int(os.getenv("OPTION_STRIKES_EACH_SIDE", "3"))
MASTER_URL = os.getenv("UPSTOX_INSTRUMENTS_URL", "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz")
API = "https://api.upstox.com"
LOG = logging.getLogger("option-avwap")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

DDL = """
CREATE TABLE IF NOT EXISTS public.option_avwap_universe (
 trading_date date NOT NULL, symbol text NOT NULL, option_key text NOT NULL,
 trading_symbol text NOT NULL, option_type text NOT NULL, strike numeric NOT NULL,
 expiry date NOT NULL, lot_size integer NOT NULL, selection_tag text NOT NULL,
 baseline_volume bigint, volume_0920 bigint, volume_delta bigint,
 baseline_oi bigint, oi_0920 bigint, oi_delta bigint, premium_0920 numeric,
 traded_money_cr numeric, fresh_oi_money_cr numeric, selected_at timestamptz NOT NULL,
 future_key text, PRIMARY KEY(trading_date,symbol,option_key)
);
CREATE TABLE IF NOT EXISTS public.option_avwap_3m (
 trading_date date NOT NULL, symbol text NOT NULL, option_key text NOT NULL,
 trading_symbol text NOT NULL, selection_tag text NOT NULL, option_type text NOT NULL,
 strike numeric NOT NULL, expiry date NOT NULL, candle_start timestamptz NOT NULL,
 candle_end timestamptz NOT NULL, open numeric, high numeric, low numeric, close numeric,
 volume bigint, oi bigint, avwap_high numeric, avwap_low numeric,
 hourly_avwap_high numeric, hourly_avwap_low numeric, high_cross text, low_cross text,
 future_price numeric, updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(trading_date,option_key,candle_start)
);
CREATE INDEX IF NOT EXISTS option_avwap_cross_idx ON public.option_avwap_3m(trading_date,candle_end)
 WHERE high_cross IS NOT NULL OR low_cross IS NOT NULL;
CREATE TABLE IF NOT EXISTS public.option_avwap_heartbeat (
 service_name text PRIMARY KEY, trading_date date, status text NOT NULL,
 candidates integer DEFAULT 0, selected integer DEFAULT 0, last_cycle_at timestamptz,
 message text, updated_at timestamptz NOT NULL DEFAULT now()
);
"""

def headers(): return {"Accept":"application/json","Authorization":f"Bearer {TOKEN}"}
def market_dt(day, t): return datetime.combine(day,t,tzinfo=IST)

def sleep_until(t):
    while True:
        seconds=(t-datetime.now(IST)).total_seconds()
        if seconds<=0:return
        time.sleep(min(seconds,60))

def api_get(path, params=None, retries=4):
    for attempt in range(retries):
        r=requests.get(API+path,params=params,headers=headers(),timeout=30)
        if r.status_code==200:return r.json()
        if r.status_code in (429,500,502,503,504):
            time.sleep(2**attempt); continue
        raise RuntimeError(f"Upstox {r.status_code}: {r.text[:300]}")
    raise RuntimeError(f"Upstox request failed after {retries} attempts: {path}")

def master():
    r=requests.get(MASTER_URL,timeout=60); r.raise_for_status(); raw=r.content
    if raw[:2]==b"\x1f\x8b":raw=gzip.decompress(raw)
    return json.loads(raw)

def expiry(v):
    try:
        if isinstance(v,(int,float)):
            return datetime.fromtimestamp(v/(1000 if v>10_000_000_000 else 1),tz=IST).date()
        return date.fromisoformat(str(v)[:10])
    except Exception:return None

def discover(rows, today):
    futures, options = defaultdict(list), defaultdict(list)
    for x in rows:
        if str(x.get("segment","")).upper() not in {"NSE_FO","NSE_F&O","NFO"}:continue
        typ=str(x.get("instrument_type","")).upper(); exp=expiry(x.get("expiry"))
        if not exp or exp<today:continue
        sym=str(x.get("underlying_symbol") or x.get("asset_symbol") or x.get("name") or "").upper().strip()
        key=x.get("instrument_key"); ts=x.get("trading_symbol") or x.get("tradingsymbol")
        if not sym or not key or not ts:continue
        if typ=="FUT" and str(x.get("underlying_type") or "").upper() not in {"INDEX","IDX"}:
            futures[sym].append((exp,str(key)))
        elif typ in {"CE","PE"}:
            options[sym].append({"key":str(key),"ts":str(ts),"type":typ,"strike":float(x.get("strike_price") or 0),"expiry":exp,"lot":int(x.get("lot_size") or x.get("minimum_lot") or 1)})
    fut={s:min(v)[1] for s,v in futures.items()}
    opt={}
    for s,items in options.items():
        if s not in fut:continue
        nearest=min(i["expiry"] for i in items)
        opt[s]=[i for i in items if i["expiry"]==nearest]
    return fut,opt

def quote_batches(keys):
    out={}
    for i in range(0,len(keys),500):
        data=api_get("/v2/market-quote/quotes",{"instrument_key":",".join(keys[i:i+500])}).get("data",{})
        for _,q in data.items():
            k=str(q.get("instrument_token") or q.get("instrument_key") or "")
            if k:out[k]=q
        time.sleep(.25)
    return out

def qget(quotes,key):
    if key in quotes:return quotes[key]
    alt=key.replace("|",":")
    for k,v in quotes.items():
        if k==alt or str(v.get("instrument_token"))==key:return v
    return {}

def candidates(futures, options, fut_quotes):
    result=[]
    for sym,items in options.items():
        px=float(qget(fut_quotes,futures[sym]).get("last_price") or 0)
        strikes=sorted({i["strike"] for i in items})
        if not px or not strikes:continue
        atm=min(range(len(strikes)),key=lambda n:abs(strikes[n]-px))
        allowed=set(strikes[max(0,atm-STRIKES_EACH_SIDE):atm+STRIKES_EACH_SIDE+1])
        result.extend([{**i,"symbol":sym,"future_key":futures[sym]} for i in items if i["strike"] in allowed])
    return result

def select_winners(items, base, end, day):
    by=defaultdict(list)
    for i in items:
        a,b=qget(base,i["key"]),qget(end,i["key"])
        vol0,vol1=int(a.get("volume") or 0),int(b.get("volume") or 0)
        oi0,oi1=int(a.get("oi") or 0),int(b.get("oi") or 0)
        premium=float(b.get("last_price") or 0); lot=i["lot"]
        i={**i,"vol0":vol0,"vol1":vol1,"vdelta":max(0,vol1-vol0),"oi0":oi0,"oi1":oi1,"oidelta":oi1-oi0,"premium":premium}
        i["traded"] = i["vdelta"]*premium*lot/10_000_000
        i["fresh"] = max(0,i["oidelta"])*premium*lot/10_000_000
        by[i["symbol"]].append(i)
    chosen={}
    for sym,rows in by.items():
        tv=max(rows,key=lambda x:x["traded"]); fo=max(rows,key=lambda x:x["fresh"])
        for row,tag in ((tv,"TRADED_MONEY"),(fo,"FRESH_OI_MONEY")):
            if row["key"] in chosen:chosen[row["key"]]["tag"]="BOTH"
            else:chosen[row["key"]]={**row,"tag":tag}
    return list(chosen.values())

def candle_data(key):
    path=f"/v3/historical-candle/intraday/{quote(key,safe='')}/minutes/3"
    raw=api_get(path).get("data",{}).get("candles",[])
    result=[]
    for c in raw:
        ts=datetime.fromisoformat(str(c[0]).replace("Z","+00:00")).astimezone(IST)
        result.append((ts,*map(float,c[1:5]),int(c[5] or 0),int(c[6] or 0)))
    return sorted(result)

def calculated(item, candles, future_price):
    day=datetime.now(IST).date(); now=datetime.now(IST)
    start,end=market_dt(day,OPEN),market_dt(day,CLOSE)
    bars=[c for c in candles if start<=c[0]<end and c[0]+timedelta(minutes=3)<=now]
    total=hi_num=lo_num=0.0; prev_hi=prev_lo=None; result=[]
    hour=[c for c in bars if c[0]<market_dt(day,FREEZE)]
    frozen_hi=max((c[2] for c in hour),default=None) if now>=market_dt(day,FREEZE) else None
    frozen_lo=min((c[3] for c in hour),default=None) if now>=market_dt(day,FREEZE) else None
    for ts,o,h,l,cl,v,oi in bars:
        if v>0:total+=v;hi_num+=h*v;lo_num+=l*v
        ah=hi_num/total if total else None; al=lo_num/total if total else None
        hc=lc=None
        if frozen_hi is not None and prev_hi is not None:
            if prev_hi<=frozen_hi<ah:hc="CROSS_ABOVE"
            elif prev_hi>=frozen_hi>ah:hc="CROSS_BELOW"
        if frozen_lo is not None and prev_lo is not None:
            if prev_lo<=frozen_lo<al:lc="CROSS_ABOVE"
            elif prev_lo>=frozen_lo>al:lc="CROSS_BELOW"
        result.append((day,item["symbol"],item["key"],item["ts"],item["tag"],item["type"],item["strike"],item["expiry"],ts,ts+timedelta(minutes=3),o,h,l,cl,v,oi,ah,al,frozen_hi,frozen_lo,hc,lc,future_price))
        prev_hi,prev_lo=ah,al
    return result

def save_universe(conn, selected, day):
    sql="""INSERT INTO public.option_avwap_universe VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(trading_date,symbol,option_key) DO UPDATE SET selection_tag=excluded.selection_tag,selected_at=excluded.selected_at"""
    now=datetime.now(IST)
    rows=[(day,x["symbol"],x["key"],x["ts"],x["type"],x["strike"],x["expiry"],x["lot"],x["tag"],x["vol0"],x["vol1"],x["vdelta"],x["oi0"],x["oi1"],x["oidelta"],x["premium"],x["traded"],x["fresh"],now,x["future_key"]) for x in selected]
    # explicit column list avoids ordering errors across schema upgrades
    sql=sql.replace("INSERT INTO public.option_avwap_universe VALUES", "INSERT INTO public.option_avwap_universe (trading_date,symbol,option_key,trading_symbol,option_type,strike,expiry,lot_size,selection_tag,baseline_volume,volume_0920,volume_delta,baseline_oi,oi_0920,oi_delta,premium_0920,traded_money_cr,fresh_oi_money_cr,selected_at,future_key) VALUES")
    with conn.cursor() as cur:cur.executemany(sql,rows)
    conn.commit()

def save_bars(conn, rows):
    if not rows:return
    sql="""INSERT INTO public.option_avwap_3m (trading_date,symbol,option_key,trading_symbol,selection_tag,option_type,strike,expiry,candle_start,candle_end,open,high,low,close,volume,oi,avwap_high,avwap_low,hourly_avwap_high,hourly_avwap_low,high_cross,low_cross,future_price)
    VALUES ("""+",".join(["%s"]*23)+""") ON CONFLICT(trading_date,option_key,candle_start) DO UPDATE SET close=excluded.close,volume=excluded.volume,oi=excluded.oi,avwap_high=excluded.avwap_high,avwap_low=excluded.avwap_low,hourly_avwap_high=excluded.hourly_avwap_high,hourly_avwap_low=excluded.hourly_avwap_low,high_cross=excluded.high_cross,low_cross=excluded.low_cross,future_price=excluded.future_price,updated_at=now()"""
    with conn.cursor() as cur:cur.executemany(sql,rows)
    conn.commit()

def heartbeat(conn,day,status,candidates=0,selected=0,message=None):
    with conn.cursor() as cur:cur.execute("""INSERT INTO public.option_avwap_heartbeat(service_name,trading_date,status,candidates,selected,last_cycle_at,message) VALUES('option_avwap_collector',%s,%s,%s,%s,now(),%s) ON CONFLICT(service_name) DO UPDATE SET trading_date=excluded.trading_date,status=excluded.status,candidates=excluded.candidates,selected=excluded.selected,last_cycle_at=now(),message=excluded.message,updated_at=now()""",(day,status,candidates,selected,message))
    conn.commit()

def run_day(conn,day):
    rows=master(); futures,options=discover(rows,day)
    sleep_until(market_dt(day,BASELINE)); fq=quote_batches(list(futures.values()))
    pool=candidates(futures,options,fq); LOG.info("Candidate options: %d",len(pool))
    base=quote_batches([x["key"] for x in pool])
    sleep_until(market_dt(day,SELECT)); end=quote_batches([x["key"] for x in pool])
    selected=select_winners(pool,base,end,day);save_universe(conn,selected,day)
    heartbeat(conn,day,"RUNNING",len(pool),len(selected)); LOG.info("Frozen selections: %d",len(selected))
    while datetime.now(IST)<=market_dt(day,CLOSE)+timedelta(minutes=3):
        future_quotes=quote_batches(sorted({x["future_key"] for x in selected}))
        all_rows=[]
        for n,x in enumerate(selected,1):
            try:
                fp=float(qget(future_quotes,x["future_key"]).get("last_price") or 0)
                all_rows.extend(calculated(x,candle_data(x["key"]),fp))
            except Exception as exc:LOG.warning("%s failed: %s",x["ts"],exc)
            time.sleep(.15)
        save_bars(conn,all_rows);heartbeat(conn,day,"RUNNING",len(pool),len(selected))
        next_bar=(datetime.now(IST).replace(second=8,microsecond=0)+timedelta(minutes=3))
        next_bar=next_bar.replace(minute=(next_bar.minute//3)*3)
        sleep_until(next_bar)
    heartbeat(conn,day,"MARKET_CLOSED",len(pool),len(selected))

def main():
    if not DB or not TOKEN:raise RuntimeError("NEON_DATABASE_URL and UPSTOX_ACCESS_TOKEN are required")
    with psycopg.connect(DB) as conn:
        conn.execute(DDL);conn.commit()
        while True:
            now=datetime.now(IST);day=now.date()
            if now.weekday()>=5 or now>market_dt(day,CLOSE):
                nxt=day+timedelta(days=1)
                while nxt.weekday()>=5:nxt+=timedelta(days=1)
                sleep_until(market_dt(nxt,dtime(9,10)));continue
            sleep_until(market_dt(day,dtime(9,10)))
            try:run_day(conn,day)
            except Exception as exc:
                LOG.exception("Daily run failed");heartbeat(conn,day,"ERROR",message=str(exc));time.sleep(60)

if __name__=="__main__":main()
