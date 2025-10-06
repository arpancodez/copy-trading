#!/usr/bin/env python3
"""
Binance Copy Trading Bot with Advanced Portfolio Analytics and Logging
Additions in this version:
- Advanced analytics: Sharpe Ratio, ROI, Max Drawdown, Allocation by asset
- In-memory logs: trade executions and periodic portfolio snapshots
- CSV export: analytics summary, trade history, snapshots
- CLI additions: analytics, export_logs <path>, snapshot_now
"""
from __future__ import annotations
import os
import time
import math
import json
import hmac
import csv
import hashlib
import logging
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlencode
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger("copy_bot")

# =========================
# Config and HTTP client
# =========================
@dataclass
class BotConfig:
    base_url: str = "https://api.binance.com"
    futures_url: str = "https://fapi.binance.com"
    use_futures: bool = False
    recv_window: int = 5000

    lead_symbol_universe: List[str] = field(default_factory=lambda: ["BTCUSDT", "ETHUSDT", "BNBUSDT"])
    poll_interval_sec: int = 10

    max_account_drawdown_pct: float = 30.0
    per_trade_risk_pct: float = 0.5
    default_stop_pct: float = 1.0
    take_profit_pct: Optional[float] = 2.0

    sizing_mode: str = "risk"  # "risk" or "fixed"
    fixed_size_usd: float = 50.0

    lead_endpoint: Optional[str] = None

    # Snapshot cadence in seconds (for periodic portfolio snapshots)
    snapshot_interval_sec: int = 60

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
        # spot: use quoteOrderQty as USD notional
        return self._request('POST','/api/v3/order', params={"symbol":symbol,"side":side,"type":"MARKET","quoteOrderQty":quantity}, signed=True)

# =========================
# Domain models and logging
# =========================
@dataclass
class Position:
    symbol: str
    side: str  # BUY/LONG or SELL/SHORT
    qty: float  # base qty for futures, or approximate base size for spot (calculated)
    entry: float
    stop: Optional[float]=None
    take_profit: Optional[float]=None
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

@dataclass
class TradeRecord:
    symbol: str
    side: str
    qty: float
    entry: float
    exit: float
    pnl: float
    pnl_pct: float
    opened_at: datetime
    closed_at: datetime

@dataclass
class ExecutionLog:
    ts: datetime
    symbol: str
    side: str
    action: str  # OPEN/CLOSE
    price: float
    qty: float
    note: str = ""

@dataclass
class Snapshot:
    ts: datetime
    equity_usd: float
    peak_equity_usd: float
    drawdown_pct: float
    allocation: Dict[str, float]  # symbol -> pct of equity based on current estimation

@dataclass
class Portfolio:
    equity_usd: float = 0.0
    peak_equity_usd: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)
    history: List[TradeRecord] = field(default_factory=list)

    # In-memory logs
    executions: List[ExecutionLog] = field(default_factory=list)
    snapshots: List[Snapshot] = field(default_factory=list)

    # ===== Equity and risk metrics =====
    def update_equity(self, balances: Dict[str,float], prices: Dict[str,float]):
        total = 0.0
        alloc_calc: Dict[str, float] = {}
        for asset, amount in balances.items():
            if asset.upper() == "USDT":
                total += amount
                alloc_calc['USDT'] = alloc_calc.get('USDT', 0.0) + amount
            else:
                sym = f"{asset.upper()}USDT"
                px = prices.get(sym, 0.0)
                total += amount * px
                alloc_calc[sym] = alloc_calc.get(sym, 0.0) + amount * px
        self.equity_usd = total
        self.peak_equity_usd = max(self.peak_equity_usd, self.equity_usd)
        # Save a snapshot if time-based will be handled externally
        return alloc_calc

    def max_drawdown_pct(self) -> float:
        if self.peak_equity_usd <= 0:
            return 0.0
        return max(0.0, (1 - self.equity_usd / self.peak_equity_usd) * 100)

    def record_open_exec(self, symbol: str, side: str, price: float, qty: float, note: str = ""):
        self.executions.append(ExecutionLog(ts=datetime.now(timezone.utc), symbol=symbol, side=side, action="OPEN", price=price, qty=qty, note=note))

    def record_close_exec(self, symbol: str, side: str, price: float, qty: float, note: str = ""):
        self.executions.append(ExecutionLog(ts=datetime.now(timezone.utc), symbol=symbol, side=side, action="CLOSE", price=price, qty=qty, note=note))

    def record_snapshot(self, allocation_value: Dict[str, float]):
        total = self.equity_usd if self.equity_usd > 0 else 1e-9
        alloc_pct = {k: (v / total) * 100 for k, v in allocation_value.items()}
        self.snapshots.append(Snapshot(ts=datetime.now(timezone.utc), equity_usd=self.equity_usd, peak_equity_usd=self.peak_equity_usd, drawdown_pct=self.max_drawdown_pct(), allocation=alloc_pct))

    def record_close(self, pos: Position, exit_price: float):
        direction = 1 if pos.side.upper() in ("BUY", "LONG") else -1
        pnl = direction * (exit_price - pos.entry) * pos.qty
        pnl_pct = direction * ((exit_price / pos.entry) - 1) * 100
        tr = TradeRecord(symbol=pos.symbol, side=pos.side, qty=pos.qty, entry=pos.entry, exit=exit_price, pnl=pnl, pnl_pct=pnl_pct, opened_at=pos.opened_at, closed_at=datetime.now(timezone.utc))
        self.history.append(tr)
        self.positions.pop(pos.symbol, None)
        self.record_close_exec(pos.symbol, pos.side, exit_price, pos.qty, note="closed via signal")

    # ===== Advanced analytics =====
    def analytics(self) -> Dict[str, Any]:
        # ROI based on equity peak or initial? We'll compute simple ROI from first snapshot or first equity we saw
        roi = None
        if self.snapshots:
            start_eq = self.snapshots[0].equity_usd
            if start_eq > 0:
                roi = ((self.equity_usd / start_eq) - 1) * 100

        total_pnl = sum(t.pnl for t in self.history)
        trades_n = len(self.history)
        wins = [t for t in self.history if t.pnl > 0]
        win_rate = (len(wins) / trades_n * 100) if trades_n > 0 else None
        avg_pnl = (total_pnl / trades_n) if trades_n > 0 else 0.0

        # Sharpe Ratio estimation: use trade-level returns as proxy for period returns
        returns = [t.pnl_pct / 100.0 for t in self.history]
        sharpe = None
        if len(returns) > 1:
            mean_r = sum(returns) / len(returns)
            var = sum((r - mean_r)**2 for r in returns) / (len(returns) - 1)
            std = math.sqrt(var)
            if std > 0:
                sharpe = mean_r / std

        # Allocation (latest snapshot if exists)
        allocation = self.snapshots[-1].allocation if self.snapshots else {}

        # Max drawdown already tracked against equity peak
        max_dd = self.max_drawdown_pct()

        per_asset_pnl: Dict[str, float] = {}
        for t in self.history:
            per_asset_pnl[t.symbol] = per_asset_pnl.get(t.symbol, 0.0) + t.pnl

        return {
            "equity_usd": self.equity_usd,
            "peak_equity_usd": self.peak_equity_usd,
            "roi_pct": roi,
            "max_drawdown_pct": max_dd,
            "total_pnl": total_pnl,
            "avg_pnl": avg_pnl,
            "win_rate": win_rate,
            "sharpe": sharpe,
            "per_asset_pnl": per_asset_pnl,
            "trades": trades_n,
            "open_positions": {k: asdict(v) for k, v in self.positions.items()},
            "allocation_pct": allocation,
        }

    # ===== CSV Export =====
    def export_csv(self, folder_path: str):
        os.makedirs(folder_path, exist_ok=True)
        # 1) Analytics summary
        summary_path = os.path.join(folder_path, 'analytics_summary.csv')
        analytics = self.analytics().copy()
        # Flatten dict fields
        per_asset = analytics.pop('per_asset_pnl', {})
        allocation = analytics.pop('allocation_pct', {})
        with open(summary_path, 'w', newline='') as f:
            w = csv.writer(f)
            for k, v in analytics.items():
                w.writerow([k, v])
        # per-asset pnl
        with open(os.path.join(folder_path, 'per_asset_pnl.csv'), 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['symbol', 'pnl'])
            for sym, pnl in per_asset.items():
                w.writerow([sym, pnl])
        # allocation
        with open(os.path.join(folder_path, 'allocation_pct.csv'), 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['symbol', 'allocation_pct'])
            for sym, pct in allocation.items():
                w.writerow([sym, pct])
        # 2) Executions
        with open(os.path.join(folder_path, 'executions.csv'), 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['ts', 'symbol', 'side', 'action', 'price', 'qty', 'note'])
            for e in self.executions:
                w.writerow([e.ts.isoformat(), e.symbol, e.side, e.action, e.price, e.qty, e.note])
        # 3) Trade history
        with open(os.path.join(folder_path, 'trades.csv'), 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['symbol', 'side', 'qty', 'entry', 'exit', 'pnl', 'pnl_pct', 'opened_at', 'closed_at'])
            for t in self.history:
                w.writerow([t.symbol, t.side, t.qty, t.entry, t.exit, t.pnl, t.pnl_pct, t.opened_at.isoformat(), t.closed_at.isoformat()])
        # 4) Snapshots
        with open(os.path.join(folder_path, 'snapshots.csv'), 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['ts', 'equity_usd', 'peak_equity_usd', 'drawdown_pct', 'allocation_json'])
            for s in self.snapshots:
                w.writerow([s.ts.isoformat(), s.equity_usd, s.peak_equity_usd, s.drawdown_pct, json.dumps(s.allocation)])

# =========================
# Engine
# =========================
class CopyEngine:
    def __init__(self, client: BinanceClient, cfg: BotConfig, portfolio: Portfolio):
        self.client = client
        self.cfg = cfg
        self.portfolio = portfolio
        self._last_snapshot_ts = 0.0

    def _fetch_lead_positions(self) -> List[Tuple[str,str]]:
        try:
            if self.cfg.lead_endpoint:
                data = requests.get(self.cfg.lead_endpoint, timeout=5).json()
                return [(x["symbol"].upper(), x["signal"].upper()) for x in data if x["symbol"].upper() in self.cfg.lead_symbol_universe]
        except Exception as e:
            log.warning(f"Lead endpoint failed: {e}")
        # Demo fallback: alternate BUY/FLAT
        t = int(time.time()) // 60
        demo_signal = "BUY" if (t % 2 == 0) else "FLAT"
        return [("BTCUSDT", demo_signal)]

    def _account_balances(self) -> Dict[str,float]:
        info = self.client.account_info()
        balances = {}
        if self.cfg.use_futures:
            for a in info.get("assets", []):
                balances[a["asset"].upper()] = float(a.get("walletBalance", 0))
        else:
            for b in info.get("balances", []):
                balances[b["asset"].upper()] = float(b.get("free", 0)) + float(b.get("locked", 0))
        return balances

    def _position_size(self, symbol: str, entry: float, stop: float) -> float:
        equity = self.portfolio.equity_usd
        if self.cfg.sizing_mode == "fixed":
            return max(0.0, self.cfg.fixed_size_usd)
        risk_usd = max(0.0, self.cfg.per_trade_risk_pct/100.0 * equity)
        stop_dist = max(1e-8, abs(entry - stop))
        qty = risk_usd / stop_dist
        return qty if self.cfg.use_futures else min(equity, qty * entry)

    def _apply_risk_checks(self):
        dd = self.portfolio.max_drawdown_pct()
        if dd >= self.cfg.max_account_drawdown_pct:
            raise RuntimeError(f"Trading halted: max drawdown {dd:.2f}% >= threshold {self.cfg.max_account_drawdown_pct}%")

    def _ensure_stops(self, entry: float, side: str) -> Tuple[float,
