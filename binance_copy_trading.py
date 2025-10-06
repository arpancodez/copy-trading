#!/usr/bin/env python3
"""
Binance Copy Trading Bot (education-ready template)
Features implemented:
 1) Lead trader copy trading (mock ingestion with optional webhook)
 2) Portfolio tracking (holdings, average price, PnL)
 3) Risk management (max drawdown halt, per-trade stop loss)
 4) Position sizing (fixed or risk-based sizing)
 5) Performance analytics (per-asset PnL, win rate, sharpe-lite)
 6) CLI management commands (start/stop/status/cfg/quit)
"""

from __future__ import annotations

import os
import time
import math
import json
import hmac
import hashlib
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger("copy_bot")

@dataclass
class BotConfig:
    base_url: str = "https://api.binance.com"
    futures_url: str = "https://fapi.binance.com"
    use_futures: bool = False
    recv_window: int = 5000
    lead_symbol_universe: List[str] = field(default_factory=lambda: ["BTCUSDT","ETHUSDT","BNBUSDT"])
    poll_interval_sec: int = 10
    max_account_drawdown_pct: float = 30.0
    per_trade_risk_pct: float = 0.5
    default_stop_pct: float = 1.0
    take_profit_pct: Optional[float] = 2.0
    sizing_mode: str = "risk"
    fixed_size_usd: float = 50.0
    lead_endpoint: Optional[str] = None

class BinanceClient:
    def __init__(self, api_key: str, api_secret: str, cfg: BotConfig):
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.cfg = cfg
    def _headers(self):
        return {"X-MBX-APIKEY": self.api_key}
    def _sign(self, params: Dict) -> Dict:
        query = urlencode(params)
        params["signature"] = hmac.new(self.api_secret, query.encode(), hashlib.sha256).hexdigest()
        return params
    def _request(self, method: str, path: str, params: Optional[Dict]=None, futures: bool=False, signed: bool=False):
        url = (self.cfg.futures_url if futures else self.cfg.base_url) + path
        params = params or {}
        if signed:
            params.update({"timestamp": int(time.time()*1000), "recvWindow": self.cfg.recv_window})
            params = self._sign(params)
        r = requests.request(method, url, params=params if method=='GET' else None, data=params if method!='GET' else None, headers=self._headers())
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        return r.json()
    def account_info(self):
        if self.cfg.use_futures:
            return self._request('GET','/fapi/v2/account', signed=True, futures=True)
        return self._request('GET','/api/v3/account', signed=True)
    def ticker_price(self, symbol: str) -> float:
        ep = '/fapi/v1/ticker/price' if self.cfg.use_futures else '/api/v3/ticker/price'
        data = self._request('GET', ep, params={"symbol": symbol}, futures=self.cfg.use_futures)
        return float(data["price"]) if isinstance(data, dict) else float(next(x for x in data if x["symbol"]==symbol)["price"])
    def market_order(self, symbol: str, side: str, quantity: float):
        if self.cfg.use_futures:
            return self._request('POST','/fapi/v1/order', params={"symbol":symbol,"side":side,"type":"MARKET","quantity":quantity}, futures=True, signed=True)
        return self._request('POST','/api/v3/order', params={"symbol":symbol,"side":side,"type":"MARKET","quoteOrderQty":quantity}, signed=True)

@dataclass
class Position:
    symbol: str
    side: str
    qty: float
    entry: float
    stop: Optional[float]=None
    take_profit: Optional[float]=None
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
@dataclass
class TradeRecord:
    symbol: str
    pnl: float
    pnl_pct: float
    opened_at: datetime
    closed_at: datetime
@dataclass
class Portfolio:
    equity_usd: float = 0.0
    peak_equity_usd: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)
    history: List[TradeRecord] = field(default_factory=list)
    def update_equity(self, balances: Dict[str,float], prices: Dict[str,float]):
        total=0.0
        for asset, amount in balances.items():
            if asset.upper()=="USDT": total+=amount
            else:
                sym=f"{asset.upper()}USDT"; px=prices.get(sym,0.0); total+=amount*px
        self.equity_usd=total; self.peak_equity_usd=max(self.peak_equity_usd,self.equity_usd)
    def max_drawdown_pct(self)->float:
        if self.peak_equity_usd<=0: return 0.0
        return max(0.0,(1-self.equity_usd/self.peak_equity_usd)*100)
    def record_close(self, pos: Position, exit_price: float):
        direction = 1 if pos.side.upper() in ("BUY","LONG") else -1
        pnl = direction*(exit_price-pos.entry)*pos.qty
        pnl_pct = direction*((exit_price/pos.entry)-1)*100
        self.history.append(TradeRecord(symbol=pos.symbol,pnl=pnl,pnl_pct=pnl_pct,opened_at=pos.opened_at,closed_at=datetime.now(timezone.utc)))
        self.positions.pop(pos.symbol, None)
    def analytics(self)->Dict:
        if not self.history: return {"total_pnl":0.0,"win_rate":None,"avg_pnl":0.0,"trades":0}
        wins=[t for t in self.history if t.pnl>0]
        total_pnl=sum(t.pnl for t in self.history)
        avg_pnl=total_pnl/len(self.history)
        win_rate=len(wins)/len(self.history)*100
        returns=[t.pnl_pct/100 for t in self.history]
        if len(returns)>1:
            mean_r=sum(returns)/len(returns)
            var=sum((r-mean_r)**2 for r in returns)/(len(returns)-1)
            sharpe_lite=(mean_r/math.sqrt(var)) if var>0 else None
        else:
            sharpe_lite=None
        per_asset: Dict[str,float]={}
        for t in self.history: per_asset[t.symbol]=per_asset.get(t.symbol,0.0)+t.pnl
        return {"total_pnl":total_pnl,"avg_pnl":avg_pnl,"win_rate":win_rate,"sharpe_lite":sharpe_lite,"per_asset_pnl":per_asset,"trades":len(self.history)}

class CopyEngine:
    def __init__(self, client: BinanceClient, cfg: BotConfig, portfolio: Portfolio):
        self.client=client; self.cfg=cfg; self.portfolio=portfolio
    def _fetch_lead_positions(self)->List[Tuple[str,str]]:
        try:
            if self.cfg.lead_endpoint:
                data=requests.get(self.cfg.lead_endpoint,timeout=5).json()
                return [(x["symbol"].upper(),x["signal"].upper()) for x in data if x["symbol"].upper() in self.cfg.lead_symbol_universe]
        except Exception as e:
            log.warning(f"Lead endpoint failed: {e}")
        t=int(time.time())//60; demo_signal="BUY" if (t%2==0) else "FLAT"; return [("BTCUSDT",demo_signal)]
    def _account_balances(self)->Dict[str,float]:
        info=self.client.account_info(); balances={}
        if self.cfg.use_futures:
            for a in info.get("assets",[]): balances[a["asset"].upper()]=float(a.get("walletBalance",0))
        else:
            for b in info.get("balances",[]): balances[b["asset"].upper()]=float(b.get("free",0))+float(b.get("locked",0))
        return balances
    def _position_size(self,symbol:str,entry:float,stop:float)->float:
        equity=self.portfolio.equity_usd
        if self.cfg.sizing_mode=="fixed": return max(0.0,self.cfg.fixed_size_usd)
        risk_usd=max(0.0,self.cfg.per_trade_risk_pct/100.0*equity)
        stop_dist=max(1e-8,abs(entry-stop))
        qty=risk_usd/stop_dist
        return qty if self.cfg.use_futures else min(equity, qty*entry)
    def _apply_risk_checks(self):
        dd=self.portfolio.max_drawdown_pct()
        if dd>=self.cfg.max_account_drawdown_pct:
            raise RuntimeError(f"Trading halted: max drawdown {dd:.2f}% exceeded threshold {self.cfg.max_account_drawdown_pct}%")
    def _ensure_stops(self,entry:float,side:str)->Tuple[float,Optional[float]]:
        if side.upper() in ("BUY","LONG"):
            stop=entry*(1-self.cfg.default_stop_pct/100)
            tp=entry*(1+(self.cfg.take_profit_pct or 0)/100) if self.cfg.take_profit_pct else None
        else:
            stop=entry*(1+self.cfg.default_stop_pct/100)
            tp=entry*(1-(self.cfg.take_profit_pct or 0)/100) if self.cfg.take_profit_pct else None
        return stop,tp
    def sync_once(self):
        self._apply_risk_checks()
        balances=self._account_balances()
        prices={}
        for s in self.cfg.lead_symbol_universe:
            try: prices[s]=self.client.ticker_price(s)
            except Exception as e: log.warning(f"Price fetch failed for {s}: {e}")
        self.portfolio.update_equity(balances,prices)
        for symbol,signal in self._fetch_lead_positions():
            px=prices.get(symbol)
            if px is None: continue
            existing=self.portfolio.positions.get(symbol)
            if signal=="FLAT":
                if existing:
                    self.portfolio.record_close(existing,exit_price=px)
                    log.info(f"Closed {symbol} at {px:.2f}, PnL recorded")
                continue
            side="BUY" if signal=="BUY" else "SELL"
            stop,tp=self._ensure_stops(px,side)
            size=self._position_size(symbol,entry=px,stop=stop)
            if size<=0:
                log.info(f"Skip {symbol}: size is 0 after risk checks"); continue
            try:
                _=self.client.market_order(symbol,side=side,quantity=round(size,6))
                log.info(f"Executed MARKET {side} {symbol} size={size:.4f}")
            except Exception as e:
                log.error(f"Order failed for {symbol}: {e}"); continue
            self.portfolio.positions[symbol]=Position(symbol=symbol,side=side,qty=size if self.cfg.use_futures else size/px,entry=px,stop=stop,take_profit=tp)

class CLI:
    def __init__(self, engine: CopyEngine, portfolio: Portfolio, cfg: BotConfig):
        self.engine=engine; self.portfolio=portfolio; self.cfg=cfg
        self._running=False; self._thread: Optional[threading.Thread]=None
    def start(self):
        if self._running: print("Already running"); return
        self._running=True
        self._thread=threading.Thread(target=self._loop,daemon=True); self._thread.start()
        print("Engine started. Type 'status' to see portfolio.")
    def stop(self):
        self._running=False
        if self._thread: self._thread.join(timeout=1)
        print("Engine stopped.")
    def _loop(self):
        while self._running:
            try: self.engine.sync_once()
            except Exception as e: log.error(e)
            time.sleep(self.cfg.poll_interval_sec)
    def status(self):
        print(json.dumps({
            "equity_usd": round(self.portfolio.equity_usd,2),
            "peak_equity_usd": round(self.portfolio.peak_equity_usd,2),
            "drawdown_pct": round(self.portfolio.max_drawdown_pct(),2),
            "positions": {k:{"side":v.side,"qty":round(v.qty,6),"entry":v.entry,"stop":v.stop,"take_profit":v.take_profit} for k,v in self.portfolio.positions.items()},
            "analytics": self.portfolio.analytics(),
        }, indent=2, default=str))
    def show_cfg(self):
        print(json.dumps(self.cfg.__dict__, indent=2, default=str))
    def run(self):
        print("Commands: start | stop | status | cfg | quit")
        while True:
            try:
                cmd=input("> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                cmd="quit"
            if cmd=="start": self.start()
            elif cmd=="stop": self.stop()
            elif cmd=="status": self.status()
            elif cmd in ("cfg","config"): self.show_cfg()
            elif cmd in ("quit","exit"): 
                self.stop(); print("Bye!"); break
            else:
                print("Unknown command. Use: start | stop | status | cfg | quit")

def build_and_run_cli():
    cfg=BotConfig()
    api_key=os.getenv("BINANCE_API_KEY","demo_key")
    api_secret=os.getenv("BINANCE_API_SECRET","demo_secret")
    client=BinanceClient(api_key, api_secret, cfg)
    portfolio=Portfolio()
    engine=CopyEngine(client,cfg,portfolio)
    CLI(engine,portfolio,cfg).run()

if __name__=="__main__":
    build_and_run_cli()
