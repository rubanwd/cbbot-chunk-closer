#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance USDT-M Futures chunk-closer bot

Главное изменение:
- По умолчанию PNL_GAP_MODE=plus: gap = pnl_a + pnl_b (суммарный PnL пары).
  Закрываем ТОЛЬКО если gap > PNL_DIFF_USD (т.е. закрытие в плюс).
- Для совместимости доступны режимы:
  - PNL_GAP_MODE=magnitude -> gap = | |p1| - |p2| |
  - PNL_GAP_MODE=signed    -> gap = | p1 - p2 |

Остальное:
- Снапшоты перезаписываются в два файла (balance.json, positions.json).
- Ежедневная очистка папки snapshots/.
- Чанки по 2 позиции, закрытие рыночными reduceOnly.
"""

import os
import time
import csv
import json
import math
import signal
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from dotenv import load_dotenv
from binance.um_futures import UMFutures
from binance.error import ClientError, ServerError

APP_NAME = "binance_futures_chunk_closer"

BASE_DIR = Path(os.getenv("OUTPUT_DIR", "./data")).resolve()
LOGS_DIR = BASE_DIR / "logs"
SNAP_DIR = BASE_DIR / "snapshots"

SNAP_BALANCE_FILE = SNAP_DIR / "balance.json"
SNAP_POSITIONS_FILE = SNAP_DIR / "positions.json"

CHUNKS_CSV = BASE_DIR / "positions_chunks.csv"
POSITIONS_CSV = BASE_DIR / "positions.csv"
CLOSURES_LOG = LOGS_DIR / "closures.log"
CLOSURES_CSV = BASE_DIR / "closures.csv"

DEFAULT_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com")
POLL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

def setup_loggers(verbose: bool = True):
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(APP_NAME)
    logger.setLevel(logging.INFO if verbose else logging.WARNING)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)
    fh = RotatingFileHandler(LOGS_DIR / f"{APP_NAME}.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt); logger.addHandler(fh)

    closures_logger = logging.getLogger(APP_NAME + ".closures")
    closures_logger.setLevel(logging.INFO)
    fh2 = RotatingFileHandler(CLOSURES_LOG, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh2.setFormatter(fmt); closures_logger.addHandler(fh2)

    return logger, closures_logger

def ts_ms_to_iso(ms: int) -> str:
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone().isoformat()
    except Exception:
        return ""

def round_step(quantity: float, step_size: float) -> float:
    if step_size <= 0:
        return quantity
    precision = int(round(-math.log10(step_size), 0)) if step_size < 1 else 0
    q = math.floor(quantity / step_size) * step_size
    return float(f"{q:.{precision}f}")

class BinanceClient:
    def __init__(self, key: str, secret: str, base_url: str = DEFAULT_BASE_URL):
        self.client = UMFutures(key=key, secret=secret, base_url=base_url)
        self._exchange_info = None

    def exchange_info(self) -> Dict[str, dict]:
        if self._exchange_info is None:
            self._exchange_info = self.client.exchange_info()
        result = {}
        for s in self._exchange_info.get("symbols", []):
            sym = s["symbol"]
            info = {"pricePrecision": s.get("pricePrecision"), "quantityPrecision": s.get("quantityPrecision")}
            for f in s.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    info["stepSize"] = float(f["stepSize"])
                if f.get("filterType") == "MARKET_LOT_SIZE":
                    info["marketStepSize"] = float(f["stepSize"])
            result[sym] = info
        return result

    def balance(self) -> List[dict]:
        return self.client.balance()

    def position_risk(self) -> List[dict]:
        return self.client.get_position_risk()

    def market_close(self, symbol: str, position_amt: float, position_side: str = "BOTH") -> dict:
        if position_amt == 0:
            return {"skipped": True, "reason": "zero position"}
        side = "SELL" if position_amt > 0 else "BUY"
        exch = self.exchange_info().get(symbol, {})
        step = exch.get("marketStepSize") or exch.get("stepSize") or 0.0
        qty = round_step(abs(position_amt), step) if step else abs(position_amt)
        params = dict(symbol=symbol, side=side, type="MARKET", quantity=qty, reduceOnly="true")
        if position_side and position_side != "BOTH":
            params["positionSide"] = position_side
        return self.client.new_order(**params)

class ChunkCloserBot:
    def __init__(self):
        load_dotenv()
        BASE_DIR.mkdir(parents=True, exist_ok=True)
        SNAP_DIR.mkdir(parents=True, exist_ok=True)

        self.logger, self.closures_logger = setup_loggers(verbose=True)

        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        self.base_url = DEFAULT_BASE_URL

        # --- ключевые настройки ---
        self.pnl_diff_usd = float(os.getenv("PNL_DIFF_USD", "40"))
        # Новый дефолт: закрываем только «в плюс»
        self.pnl_gap_mode = os.getenv("PNL_GAP_MODE", "plus").lower().strip()
        if self.pnl_gap_mode not in ("plus", "magnitude", "signed"):
            self.pnl_gap_mode = "plus"

        self.dry_run = os.getenv("DRY_RUN", "true").lower() == "true"

        if not self.api_key or not self.api_secret:
            self.logger.error("API ключи не заданы. Укажите BINANCE_API_KEY и BINANCE_API_SECRET в .env")
            raise SystemExit(2)

        self.client = BinanceClient(self.api_key, self.api_secret, self.base_url)

        self.logger.info(
            "Старт бота. DRY_RUN=%s, PNL_DIFF_USD=%.2f, GAP_MODE=%s, outdir=%s",
            self.dry_run, self.pnl_diff_usd, self.pnl_gap_mode, str(BASE_DIR)
        )

        # CSV заголовки
        if not CHUNKS_CSV.exists():
            with open(CHUNKS_CSV, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(
                    ["ts", "chunk_id", "symbol", "positionSide", "open_time_iso", "pnl_unrealized"]
                )
        if not POSITIONS_CSV.exists():
            with open(POSITIONS_CSV, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(
                    ["ts", "symbol", "positionSide", "positionAmt", "entryPrice", "pnl_unrealized", "updateTime_iso"]
                )
        if not CLOSURES_CSV.exists():
            with open(CLOSURES_CSV, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(
                    ["close_ts", "mode", "symbol1", "symbol2", "posSide1", "posSide2",
                     "open_time1", "open_time2", "pnl1", "pnl2", "gap_value",
                     "dry_run", "order_id_1", "order_id_2"]
                )

        self._stop = False
        self._last_cleanup_day = None
        self.daily_cleanup(force=True)

        try:
            signal.signal(signal.SIGINT, self._sig_stop)
            signal.signal(signal.SIGTERM, self._sig_stop)
        except Exception:
            pass

    def _sig_stop(self, *args):
        self._stop = True
        self.logger.info("Остановка по сигналу...")

    # --- ежедневная очистка snapshots ---
    def daily_cleanup(self, force: bool = False):
        today = datetime.now().date()
        if force or self._last_cleanup_day != today:
            SNAP_DIR.mkdir(parents=True, exist_ok=True)
            keep = {"balance.json", "positions.json"}
            for p in SNAP_DIR.glob("*"):
                if p.is_file() and p.name not in keep:
                    try:
                        p.unlink()
                    except Exception as e:
                        self.logger.warning("Не удалось удалить %s: %s", p, e)
            self._last_cleanup_day = today
            self.logger.info("Ежедневная очистка snapshots выполнена.")

    # --- шаг 1: получить и залогировать аккаунт ---
    def fetch_and_log_account(self) -> Tuple[dict, List[dict]]:
        try:
            bal_list = self.client.balance()
        except (ClientError, ServerError) as e:
            self.logger.error("Balance error: %s", e)
            bal_list = []
        balance_summary = {b.get("asset"): b for b in bal_list}
        usdt_total = balance_summary.get("USDT", {}).get("balance")
        usdt_avail = balance_summary.get("USDT", {}).get("availableBalance")
        self.logger.info("Баланс (USDT): total=%s, available=%s", usdt_total, usdt_avail)

        try:
            pr_list = self.client.position_risk()
        except (ClientError, ServerError) as e:
            self.logger.error("Position risk error: %s", e)
            pr_list = []

        positions_open = []
        for p in pr_list:
            try:
                amt = float(p.get("positionAmt", "0"))
                if abs(amt) < 1e-12:
                    continue
                positions_open.append({
                    "symbol": p.get("symbol"),
                    "positionAmt": amt,
                    "entryPrice": float(p.get("entryPrice", "0")),
                    "unRealizedProfit": float(p.get("unRealizedProfit", "0")),
                    "updateTime": int(p.get("updateTime", 0)),
                    "positionSide": p.get("positionSide", "BOTH"),
                })
            except Exception as ex:
                self.logger.warning("Skip malformed position: %s | err=%s", p, ex)

        # перезаписываем 2 снапшота и, при смене дня, чистим каталог
        self.daily_cleanup()
        with open(SNAP_BALANCE_FILE, "w", encoding="utf-8") as f:
            json.dump(balance_summary, f, ensure_ascii=False, indent=2)
        with open(SNAP_POSITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(positions_open, f, ensure_ascii=False, indent=2)

        # CSV позиций (строка на каждую позицию)
        with open(POSITIONS_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            now_iso = datetime.now().isoformat()
            for po in positions_open:
                w.writerow([now_iso, po["symbol"], po["positionSide"], po["positionAmt"],
                            po["entryPrice"], po["unRealizedProfit"], ts_ms_to_iso(po["updateTime"])])

        if positions_open:
            self.logger.info("Открытые позиции (%d):", len(positions_open))
            for po in positions_open:
                self.logger.info("  %s %-5s amt=%s entry=%s PnL=%s upd=%s",
                                 po["symbol"], po["positionSide"], po["positionAmt"],
                                 po["entryPrice"], po["unRealizedProfit"], ts_ms_to_iso(po["updateTime"]))
        else:
            self.logger.info("Открытых позиций нет.")
        return balance_summary, positions_open

    # --- шаг 2: чанки по 2 ---
    @staticmethod
    def make_chunks(positions_open: List[dict]) -> List[List[dict]]:
        pos_sorted = sorted(positions_open, key=lambda x: x.get("updateTime", 0))
        return [pos_sorted[i:i+2] for i in range(0, len(pos_sorted) - 1, 2)]

    def log_chunks(self, chunks: List[List[dict]]):
        if not chunks:
            self.logger.info("Чанков для сравнения нет.")
            return
        self.logger.info("Чанки (по 2): всего %d", len(chunks))
        now_iso = datetime.now().isoformat()
        with open(CHUNKS_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for idx, pair in enumerate(chunks, start=1):
                a, b = pair
                self.logger.info("  #%d: %s(%s) PnL=%.2f | %s(%s) PnL=%.2f",
                                 idx, a["symbol"], a["positionSide"], a["unRealizedProfit"],
                                 b["symbol"], b["positionSide"], b["unRealizedProfit"])
                for po in pair:
                    w.writerow([now_iso, idx, po["symbol"], po["positionSide"],
                                ts_ms_to_iso(po["updateTime"]), po["unRealizedProfit"]])

    # --- формула "гэп" ---
    def _calc_gap(self, pnl_a: float, pnl_b: float) -> float:
        mode = self.pnl_gap_mode
        if mode == "plus":
            # закрываем только в плюс: gap = суммарный PnL пары
            return pnl_a + pnl_b
        if mode == "magnitude":
            return abs(abs(pnl_a) - abs(pnl_b))
        # signed
        return abs(pnl_a - pnl_b)

    # --- шаг 3: возможно закрыть пару ---
    def maybe_close_chunk(self, pair: List[dict]):
        if len(pair) != 2:
            return
        a, b = pair
        pnl_a = float(a["unRealizedProfit"])
        pnl_b = float(b["unRealizedProfit"])
        gap = self._calc_gap(pnl_a, pnl_b)

        self.logger.info("Gap(%s)=%0.2f vs threshold=%0.2f  |  pair=(%s:%0.2f, %s:%0.2f)",
                         self.pnl_gap_mode, gap, self.pnl_diff_usd,
                         a["symbol"], pnl_a, b["symbol"], pnl_b)

        # В режиме 'plus' gap может быть отрицательным — это сигнал «не закрывать».
        if gap <= self.pnl_diff_usd:
            self.logger.info("Условия НЕ выполнены — не закрываем.")
            return

        self.logger.warning("Условия выполнены ⇒ закрываем обе позиции: %s(%s) и %s(%s)",
                            a["symbol"], a["positionSide"], b["symbol"], b["positionSide"])

        order1 = order2 = None
        if self.dry_run:
            self.logger.warning("DRY_RUN=true — имитация без отправки ордеров.")
        else:
            try:
                order1 = self.client.market_close(
                    symbol=a["symbol"], position_amt=a["positionAmt"], position_side=a["positionSide"]
                )
                time.sleep(0.2)
                order2 = self.client.market_close(
                    symbol=b["symbol"], position_amt=b["positionAmt"], position_side=b["positionSide"]
                )
            except (ClientError, ServerError) as e:
                self.logger.error("Ошибка при закрытии: %s", e)

        close_iso = datetime.now().isoformat()
        self.closures_logger.info(
            "CLOSE | mode=%s | %s & %s | posSides: %s/%s | gap=%.2f | openA=%s openB=%s | dry_run=%s",
            self.pnl_gap_mode, a["symbol"], b["symbol"], a["positionSide"], b["positionSide"],
            gap, ts_ms_to_iso(a["updateTime"]), ts_ms_to_iso(b["updateTime"]), self.dry_run
        )
        with open(CLOSURES_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                close_iso, self.pnl_gap_mode,
                a["symbol"], b["symbol"],
                a["positionSide"], b["positionSide"],
                ts_ms_to_iso(a["updateTime"]), ts_ms_to_iso(b["updateTime"]),
                pnl_a, pnl_b, gap,
                self.dry_run,
                (order1 or {}).get("orderId") if isinstance(order1, dict) else None,
                (order2 or {}).get("orderId") if isinstance(order2, dict) else None
            ])

    def run_once(self):
        _, positions = self.fetch_and_log_account()
        if len(positions) > 1:
            chunks = self.make_chunks(positions)
            self.log_chunks(chunks)
            for pair in chunks:
                self.maybe_close_chunk(pair)
        else:
            self.logger.info("Для чанков нужно >1 открытая позиция.")

    def run_loop(self):
        self.logger.info("Запуск цикла опроса каждые %s сек.", POLL_SECONDS)
        while not self._stop:
            start = time.time()
            try:
                self.run_once()
            except Exception as e:
                self.logger.exception("Ошибка в итерации: %s", e)
            time.sleep(max(0.0, POLL_SECONDS - (time.time() - start)))

def main():
    try:
        bot = ChunkCloserBot()
    except SystemExit:
        return
    bot.run_loop()

if __name__ == "__main__":
    main()
