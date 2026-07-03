#!/usr/bin/env python3
"""Rebuild feeds-tool/data.json from the public venue APIs.

Pulls Binance spot/futures + Lighter + Hyperliquid universes, prices, 24h volume,
and (optionally) order-book depth at +/-0.5/1/2%. Normalizes tickers, price-validates
merges (splitting scale/identity collisions), classifies by region/type, and writes data.json.

Usage:
  python3 refresh_data.py            # full refresh incl. depth (slow, ~8-10 min)
  python3 refresh_data.py --quick    # volume+price only, reuse cached depth (fast)
"""
import json, urllib.request, time, statistics, sys, os, argparse
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, Counter

HERE=os.path.dirname(os.path.abspath(__file__))
DATA=os.path.join(HERE,"data.json")
UA={"User-Agent":"Mozilla/5.0","Accept":"application/json"}

def get(url):
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=30) as r: return json.loads(r.read())
def post(url,body):
    req=urllib.request.Request(url,data=json.dumps(body).encode(),headers={**UA,"Content-Type":"application/json"})
    with urllib.request.urlopen(req,timeout=30) as r: return json.loads(r.read())

ALIAS={"XAU":"GOLD","XAG":"SILVER","XPT":"PLATINUM","XPD":"PALLADIUM","BZ":"BRENTOIL","XCU":"COPPER","WTI":"CL",
 "USA500":"SP500","US500":"SP500","USTECH":"XYZ100","US100":"XYZ100","SPACEX":"SPCX","SAMSUNG":"SMSN","SKHYNIX":"SKHX","USDJPY":"JPY"}
def normalize(sym,venue):
    s=sym
    if venue=="lighter":
        for suf in ("/USDC","/USDT","/USD"):
            if s.endswith(suf): s=s[:-len(suf)]; break
        else:
            if s.endswith("USD") and len(s)>4 and not s.startswith("USD"): s=s[:-3]
    if len(s)>1 and s[0]=="k" and s[1:].isalpha() and s[1:].isupper(): s="1000"+s[1:]
    return ALIAS.get(s,s)

ETF={"EWJ","EWT","EWY","EWZ","IWM","QQQ","SPY","DIA","SOXL","SOXX","SQQQ","TQQQ","UVXY","VIXY","XLE","XLF","SMH","URNM","URA","KORU","BOTZ","ROBO","MAGS","STXX","ASHR","CHAU","CWEB","KWEB","TLT","GLD","SLV","ARKK","IWB","IWF","IWD","IWR","VONG","MCHI"}
ASIA_EQ={"BABA","TSM","SMSN","SKHX","HYUNDAI","KIOXIA","SOFTBANK","SONY","BYD","TENCENT","XIAOMI","SMIC","POPMART","HANMI","MINIMAX","ZHIPU","KRCOMP"}
EUROPE_EQ={"ASML","NOK","NVO","SAP","SIE"}
CRYPTO_INDEX={"TOTAL2","OTHERS","BTCD","TOTAL","TOTAL3"}
CRYPTO_ON_XYZ={"BIRD","PURRDAT"}
PREIPO={"SPCX","OPENAI","ANTHROPIC","MINIMAX","ZHIPU","STRIPE","DATABRICKS","REVOLUT","CANVA","SHEIN","EPIC","DISCORD"}
ASIA_INDEX={"JP225","KR200"}; US_INDEX={"SP500","XYZ100","DXY","VIX","RUT","SOX"}
COMMOD={"GOLD","SILVER","PLATINUM","PALLADIUM","COPPER","CL","BRENTOIL","NATGAS"}; FX={"EUR","GBP","JPY","AUD","CAD","CHF","NZD"}
NAMES={"BTC":"Bitcoin","ETH":"Ethereum","SOL":"Solana","NVDA":"NVIDIA","TSLA":"Tesla","AAPL":"Apple","MSFT":"Microsoft","GOOGL":"Alphabet","AMZN":"Amazon","META":"Meta Platforms","GOLD":"Gold","SILVER":"Silver","PLATINUM":"Platinum","PALLADIUM":"Palladium","COPPER":"Copper","CL":"WTI Crude Oil","BRENTOIL":"Brent Crude Oil","NATGAS":"Natural Gas","SP500":"S&P 500","XYZ100":"Nasdaq 100","JP225":"Nikkei 225","KR200":"KOSPI 200","COIN":"Coinbase","HOOD":"Robinhood","MSTR":"MicroStrategy","CRCL":"Circle","BABA":"Alibaba","TSM":"TSMC","QNT":"Quant","EUR":"Euro","GBP":"British Pound","JPY":"Japanese Yen (USD/JPY)","NOK":"Nokia","SMSN":"Samsung Electronics","SKHX":"SK Hynix","SPCX":"SpaceX (pre-IPO)","HYUNDAI":"Hyundai","KIOXIA":"Kioxia","SOFTBANK":"SoftBank","ASML":"ASML","NVO":"Novo Nordisk","BYD":"BYD","TENCENT":"Tencent","XIAOMI":"Xiaomi","SMIC":"SMIC","POPMART":"Pop Mart","SONY":"Sony","EWJ":"iShares MSCI Japan ETF","EWY":"iShares MSCI S.Korea ETF","EWT":"iShares MSCI Taiwan ETF","QQQ":"Invesco QQQ","SPY":"SPDR S&P 500 ETF","SMH":"VanEck Semiconductor ETF"}

def classify(t,uts,dexes):
    if t in CRYPTO_INDEX: return "crypto index"
    if t in CRYPTO_ON_XYZ: return "crypto"
    if t in ETF: return "ETF"
    if t in ASIA_INDEX: return "index (Asia)"
    if t in US_INDEX: return "index (US)"
    if t in ASIA_EQ: return "equity (Asia)"
    if t in EUROPE_EQ: return "equity (Europe)"
    if t in PREIPO: return "equity (pre-IPO)"
    if t in COMMOD: return "commodity"
    if t in FX: return "FX"
    if "EQUITY" in uts or "KR_EQUITY" in uts: return "equity (US)"
    if "COMMODITY" in uts: return "commodity"
    if "INDEX" in uts: return "index (US)"
    if "PREMARKET" in uts: return "equity (pre-IPO)"
    if dexes & {"xyz","cash","para","mkts"}: return "equity (US)"
    return "crypto"

def fetch_universes():
    log("fetching universes…")
    spot=get("https://api.binance.com/api/v3/exchangeInfo")
    binance_spot={s["baseAsset"]:s["symbol"] for s in spot["symbols"] if s.get("status")=="TRADING" and s.get("quoteAsset")=="USDT"}
    fut=get("https://fapi.binance.com/fapi/v1/exchangeInfo")
    binance_fut={s["baseAsset"]:(s["symbol"],s.get("underlyingType") or "") for s in fut["symbols"]
                 if s.get("status")=="TRADING" and s.get("quoteAsset")=="USDT" and s.get("contractType") in ("PERPETUAL","TRADIFI_PERPETUAL")}
    lt=get("https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails")
    lighter={}; lighter_vol={}
    for ob in (lt.get("order_book_details") or []):
        s=ob.get("symbol"); m=ob.get("market_id")
        if s is not None and m is not None:
            lighter[s]=m
            try: lighter_vol[s]=float(ob.get("daily_quote_token_volume") or 0)
            except: lighter_vol[s]=0
    INFO="https://api.hyperliquid.xyz/info"
    hl={}
    for a in post(INFO,{"type":"meta"})["universe"]:
        if not a.get("isDelisted"): hl[a["name"]]={"dex":"(main)"}
    for d in post(INFO,{"type":"perpDexs"}):
        if not d or not d.get("name"): continue
        try:
            for a in post(INFO,{"type":"meta","dex":d["name"]}).get("universe",[]):
                if not a.get("isDelisted"): hl[a["name"]]={"dex":d["name"]}
        except: pass
    return binance_spot,binance_fut,lighter,lighter_vol,hl

def fetch_prices_vol():
    log("fetching prices + volume…")
    def bt(url):
        m={}
        for e in get(url):
            try:
                b=float(e["bidPrice"]);a=float(e["askPrice"])
                if b>0 and a>0: m[e["symbol"]]=(b+a)/2
            except: pass
        return m
    bspot_px=bt("https://api.binance.com/api/v3/ticker/bookTicker"); bfut_px=bt("https://fapi.binance.com/fapi/v1/ticker/bookTicker")
    def qv(url):
        m={}
        for e in get(url):
            try: m[e["symbol"]]=float(e["quoteVolume"])
            except: pass
        return m
    bspot_v=qv("https://api.binance.com/api/v3/ticker/24hr"); bfut_v=qv("https://fapi.binance.com/fapi/v1/ticker/24hr")
    hl_px={}; hl_v={}
    for dex in [None,"xyz","cash","para","mkts","hyna"]:
        body={"type":"metaAndAssetCtxs"} if dex is None else {"type":"metaAndAssetCtxs","dex":dex}
        try:
            resp=post("https://api.hyperliquid.xyz/info",body); meta,ctxs=resp[0],resp[1]
            for i,a in enumerate(meta["universe"]):
                ctx=ctxs[i] or {}
                if ctx.get("oraclePx") is not None: hl_px[a["name"]]=float(ctx["oraclePx"])
                if ctx.get("dayNtlVlm") is not None: hl_v[a["name"]]=float(ctx["dayNtlVlm"])
        except: pass
    return bspot_px,bfut_px,bspot_v,bfut_v,hl_px,hl_v

def depth_bands(bids,asks):
    if not bids or not asks: return {50:0,100:0,200:0}
    mid=(bids[0][0]+asks[0][0])/2; out={}
    for bp in (50,100,200):
        lo,hi=mid*(1-bp/1e4),mid*(1+bp/1e4); t=0.0
        for p,q in bids:
            if p>=lo: t+=p*q
        for p,q in asks:
            if p<=hi: t+=p*q
        out[bp]=t
    return out

def fetch_depth(assets):
    """assets: list of dicts with ids{binance,binanceFutures,lighter,hydromancer}. Adds .dep per band per venue."""
    log("fetching order-book depth (this is the slow part)…")
    # unique targets
    sp=set();fu=set();li=set();hlc=set()
    for a in assets:
        i=a["ids"]
        if i["binance"]:sp.add(i["binance"])
        if i["binanceFutures"]:fu.add(i["binanceFutures"])
        if i["lighter"]:li.add(i["lighter"])
        if i["hydromancer"]:hlc.add(i["hydromancer"])
    D={"spot":{},"fut":{},"lighter":{},"hl":{}}
    def do_hl(c):
        try:
            d=post("https://api.hyperliquid.xyz/info",{"type":"l2Book","coin":c}); lv=d["levels"]
            b=[(float(x["px"]),float(x["sz"])) for x in lv[0]];a=[(float(x["px"]),float(x["sz"])) for x in lv[1]]
            return c,depth_bands(b,a)
        except Exception as e: return c,{"err":1}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for c,v in ex.map(do_hl,sorted(hlc)): D["hl"][c]=v
    for m in sorted(li):
        for _ in range(2):
            try:
                d=get(f"https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders?market_id={m}&limit=100")
                b=[(float(x["price"]),float(x["remaining_base_amount"])) for x in d["bids"]]
                a=[(float(x["price"]),float(x["remaining_base_amount"])) for x in d["asks"]]
                D["lighter"][str(m)]=depth_bands(b,a);break
            except Exception: time.sleep(1)
        time.sleep(0.35)
    def binance(dst,host,path_,syms):
        for s in sorted(syms):
            for _ in range(4):
                try:
                    d=get(f"{host}{path_}?symbol={s}&limit=500")
                    b=[(float(x[0]),float(x[1])) for x in d["bids"]];a=[(float(x[0]),float(x[1])) for x in d["asks"]]
                    D[dst][s]=depth_bands(b,a);break
                except Exception as e:
                    if "418" in str(e) or "429" in str(e): time.sleep(90)
                    else: D[dst][s]={"err":1};break
            time.sleep(0.5)
    binance("spot","https://api.binance.com","/api/v3/depth",sp)
    binance("fut","https://fapi.binance.com","/fapi/v1/depth",fu)
    # attach
    for a in assets:
        i=a["ids"]; dep={"50":{},"100":{},"200":{}}
        srcmap=[("binance","spot",i["binance"]),("binanceFutures","fut",i["binanceFutures"]),
                ("lighter","lighter",str(i["lighter"]) if i["lighter"] else ""),("hydromancer","hl",i["hydromancer"])]
        for vkey,dkey,ident in srcmap:
            r=D[dkey].get(ident) if ident else None
            for band in (50,100,200):
                dep[str(band)][vkey]=(r.get(band) if r and "err" not in r else None)
        a["dep"]=dep
    return assets

def build(quick=False):
    bspot,bfut,lighter,lighter_vol,hl=fetch_universes()
    bspot_px,bfut_px,bspot_v,bfut_v,hl_px,hl_v=fetch_prices_vol()
    raw=defaultdict(list)
    for base,sym in bspot.items():
        raw[normalize(base,"binance")].append({"venue":"binance","id":sym,"base":base,"ut":"","px":bspot_px.get(sym),"vol":bspot_v.get(sym,0)})
    for base,(sym,ut) in bfut.items():
        raw[normalize(base,"binanceFutures")].append({"venue":"binanceFutures","id":sym,"base":base,"ut":ut,"px":bfut_px.get(sym),"vol":bfut_v.get(sym,0)})
    for sym,mid in lighter.items():
        raw[normalize(sym,"lighter")].append({"venue":"lighter","id":mid,"lsym":sym,"ut":"","px":hl_px.get(sym) if False else None,"vol":lighter_vol.get(sym,0)})
    # lighter price: use its own book mid later; for clustering use HL/binance price. keep None ok.
    for coin in hl:
        base=coin.split(":",1)[1] if ":" in coin else coin
        raw[normalize(base,"hl")].append({"venue":"hydromancer","id":coin,"ut":"","px":hl_px.get(coin),"vol":hl_v.get(coin,0)})
    # price-cluster split
    def cluster(entries):
        priced=sorted([e for e in entries if e["px"] and e["px"]>0],key=lambda e:e["px"])
        noprice=[e for e in entries if not(e["px"] and e["px"]>0)]
        if not priced: return [entries]
        cl=[[priced[0]]]
        for e in priced[1:]:
            (cl.append([e]) if e["px"]/cl[-1][-1]["px"]>1.15 else cl[-1].append(e))
        if noprice:
            best=max(range(len(cl)),key=lambda i:sum((x["vol"] or 0) for x in cl[i])); cl[best].extend(noprice)
        return cl
    TAG={"binance":"BinSpot","binanceFutures":"BinFut","lighter":"Lighter","hydromancer":"HL"}
    def pick_hl(ents):
        coins=[e["id"] for e in ents if e["venue"]=="hydromancer"]
        if not coins: return ""
        bare=[c for c in coins if ":" not in c]; xyzc=[c for c in coins if c.startswith("xyz:")]
        return bare[0] if bare else (xyzc[0] if xyzc else sorted(coins)[0])
    assets=[]
    for canon,entries in raw.items():
        for cl in cluster(entries):
            byv={}
            for e in cl: byv.setdefault(e["venue"],e)
            lts=[e for e in cl if e["venue"]=="lighter"]
            if lts: byv["lighter"]=max(lts,key=lambda e:e["vol"] or 0)
            venues=[v for v in ["binance","binanceFutures","lighter","hydromancer"] if v in byv]
            if not venues: continue
            b=byv.get("binance");f=byv.get("binanceFutures");l=byv.get("lighter")
            uts={e.get("ut") for e in cl if e.get("ut")}; dexes={e["id"].split(":",1)[0] for e in cl if e["venue"]=="hydromancer" and ":" in e["id"]}
            med=statistics.median([e["px"] for e in cl if e["px"]]) if any(e["px"] for e in cl) else None
            cls=classify(canon,uts,dexes)
            name=NAMES.get(canon,"")
            if canon=="BB": name="BounceBit" if (med or 0)<1 else "BlackBerry"; cls="crypto" if name=="BounceBit" else "equity (US)"
            bbase=(b or f or {}).get("base","") or canon
            hp=pick_hl(cl)
            path=f"multi/{bbase if (b or f) else canon}/{(l or {}).get('id','-') if l else '-'}/{hp or '-'}"
            assets.append({"ticker":canon,"name":name,"cls":cls,"coverage":" · ".join(TAG[v] for v in venues),
                "nsrc":len(venues),"path":path,"ex":venues,
                "ids":{"binance":(b or {}).get("id","") if b else "","binanceFutures":(f or {}).get("id","") if f else "",
                       "lighter":str((l or {}).get("id","")) if l else "","hydromancer":hp},
                "vol":{"binance":round((b or {}).get("vol") or 0) if b else None,"binanceFutures":round((f or {}).get("vol") or 0) if f else None,
                       "lighter":round((l or {}).get("vol") or 0) if l else None,
                       "hydromancer":round(sum(e["vol"] or 0 for e in cl if e["venue"]=="hydromancer")) if "hydromancer" in venues else None}})
    if quick:
        old={a["ticker"]+"|"+a["path"]:a for a in json.load(open(DATA))["assets"]} if os.path.exists(DATA) else {}
        for a in assets:
            o=old.get(a["ticker"]+"|"+a["path"])
            a["dep"]=o["dep"] if o and "dep" in o else {"50":{},"100":{},"200":{}}
    else:
        fetch_depth(assets)
    assets.sort(key=lambda a:-(a["vol"].get("binanceFutures") or 0)-(a["vol"].get("hydromancer") or 0))
    return {"generated":stamp(),"count":len(assets),"assets":assets}

def stamp():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
def log(m): print(f"[refresh] {m}",flush=True)

if __name__=="__main__":
    ap=argparse.ArgumentParser(); ap.add_argument("--quick",action="store_true"); a=ap.parse_args()
    d=build(quick=a.quick)
    json.dump(d,open(DATA,"w"))
    open(os.path.join(HERE,"data.js"),"w").write("window.SEED_DATA="+json.dumps(d)+";")  # file:// fallback
    log(f"wrote {DATA} (+data.js): {d['count']} assets")
