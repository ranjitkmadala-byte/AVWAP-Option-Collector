"""One-time historical backfill for the Option AVWAP project."""
from __future__ import annotations

import argparse, time
from datetime import date, datetime, timedelta
from urllib.parse import quote

import psycopg

import option_avwap_collector as live

def historical(key, interval, day):
    encoded=quote(key,safe=""); ds=day.isoformat()
    path=f"/v3/historical-candle/{encoded}/minutes/{interval}/{ds}/{ds}"
    raw=live.api_get(path).get("data",{}).get("candles",[])
    bars=[]
    for c in raw:
        ts=datetime.fromisoformat(str(c[0]).replace("Z","+00:00")).astimezone(live.IST)
        if ts.date()==day:
            bars.append((ts,float(c[1]),float(c[2]),float(c[3]),float(c[4]),int(c[5] or 0),int(c[6] or 0)))
    return sorted(bars)

def at_or_before(bars, cutoff):
    rows=[b for b in bars if b[0] <= cutoff]
    return rows[-1] if rows else None

def discover_candidates(rows,futures,options,day):
    pseudo={}
    future_bars={}
    cutoff=live.market_dt(day,live.SELECT)-timedelta(minutes=1)
    for n,(sym,key) in enumerate(futures.items(),1):
        try:
            bars=historical(key,1,day);future_bars[sym]=bars
            bar=at_or_before(bars,cutoff)
            if bar:pseudo[key]={"instrument_token":key,"last_price":bar[4]}
        except Exception as exc:live.LOG.warning("Future history %s failed: %s",sym,exc)
        time.sleep(.12)
    return live.candidates(futures,options,pseudo),future_bars

def reconstructed_selection(pool,day):
    base,end={},{}
    start=live.market_dt(day,live.OPEN); cutoff=live.market_dt(day,live.SELECT)
    for n,item in enumerate(pool,1):
        try:
            bars=historical(item["key"],1,day)
            window=[b for b in bars if start<=b[0]<cutoff]
            if not window:continue
            first,last=window[0],window[-1]
            volume=sum(b[5] for b in window)
            base[item["key"]]={"instrument_token":item["key"],"volume":0,"oi":first[6]}
            end[item["key"]]={"instrument_token":item["key"],"volume":volume,"oi":last[6],"last_price":last[4]}
        except Exception as exc:live.LOG.warning("Candidate history %s failed: %s",item["ts"],exc)
        if n%100==0:live.LOG.info("Selection history %d/%d",n,len(pool))
        time.sleep(.12)
    usable=[x for x in pool if x["key"] in end]
    return live.select_winners(usable,base,end,day)

def calculate(item,bars,future_3m,day):
    start=live.market_dt(day,live.OPEN); close=live.market_dt(day,live.CLOSE)
    bars=[b for b in bars if start<=b[0]<close]
    hour=[b for b in bars if b[0]<live.market_dt(day,live.FREEZE)]
    frozen_hi=max((b[2] for b in hour),default=None);frozen_lo=min((b[3] for b in hour),default=None)
    fp={b[0]:b[4] for b in future_3m};total=hn=ln=0.0;ph=pl=None;out=[]
    for ts,o,h,l,cl,v,oi in bars:
        if v>0:total+=v;hn+=h*v;ln+=l*v
        ah=hn/total if total else None;al=ln/total if total else None;hc=lc=None
        if ts+timedelta(minutes=3)>live.market_dt(day,live.FREEZE):
            if ph is not None and ph<=frozen_hi<ah:hc="CROSS_ABOVE"
            elif ph is not None and ph>=frozen_hi>ah:hc="CROSS_BELOW"
            if pl is not None and pl<=frozen_lo<al:lc="CROSS_ABOVE"
            elif pl is not None and pl>=frozen_lo>al:lc="CROSS_BELOW"
        out.append((day,item["symbol"],item["key"],item["ts"],item["tag"],item["type"],item["strike"],item["expiry"],ts,ts+timedelta(minutes=3),o,h,l,cl,v,oi,ah,al,frozen_hi,frozen_lo,hc,lc,fp.get(ts)))
        ph,pl=ah,al
    return out

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--date",required=True);args=ap.parse_args()
    day=date.fromisoformat(args.date)
    if not live.DB or not live.TOKEN:raise RuntimeError("NEON_DATABASE_URL and UPSTOX_ACCESS_TOKEN are required")
    rows=live.master();futures,options=live.discover(rows,day)
    pool,future_1m=discover_candidates(rows,futures,options,day)
    live.LOG.info("Historical candidate options: %d",len(pool))
    selected=reconstructed_selection(pool,day)
    with psycopg.connect(live.DB) as conn:
        conn.execute(live.DDL);conn.commit()
        live.save_universe(conn,selected,day,live.market_dt(day,live.SELECT))
        all_rows=[]
        for n,item in enumerate(selected,1):
            try:
                option_bars=historical(item["key"],3,day)
                future_bars=historical(item["future_key"],3,day)
                all_rows.extend(calculate(item,option_bars,future_bars,day))
            except Exception as exc:live.LOG.warning("Selected history %s failed: %s",item["ts"],exc)
            if n%25==0:live.LOG.info("AVWAP history %d/%d",n,len(selected))
            time.sleep(.12)
        live.save_bars(conn,all_rows)
        live.heartbeat(conn,day,"BACKFILL_COMPLETE",len(pool),len(selected),f"Rows written: {len(all_rows)}")
    print(f"BACKFILL COMPLETE | date={day} candidates={len(pool)} selected={len(selected)} rows={len(all_rows)}")

if __name__=="__main__":main()
