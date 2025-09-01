#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance USDT‑M Futures chunk-closer bot

Функции:
1) Каждую минуту получает баланс фьючерсного счёта и открытые позиции.
   Печатает в консоль и пишет снапшоты в файлы.
2) Если открытых позиций > 1, сортирует их по времени (updateTime) и делит на чанки по 2.
   Логирует чанки в консоль и в отдельный CSV.
3) Если внутри чанка разница по PnL двух позиций > ENV(PNL_DIFF_USD),
   закрывает ОБЕ позиции рыночными ордерами (reduceOnly) и пишет отдельный лог/CSV.
   
Важные заметки
- Бот работает с UM (USDT‑Margined) фьючерсами через официальный SDK: binance-um-futures.
- По умолчанию DRY_RUN=true (ничего не закрывает). Чтобы реально торговать, выставьте DRY_RUN=false в .env.
- В регионах с ограничениями (HTTP 451) запросы могут падать. Бот это логирует и продолжит попытки.
- "Дата открытия" берётся из поля updateTime позиции (это приблизительно время последнего изменения позиции).
  Для строгой даты открытия нужен разбор истории сделок; при желании можно доработать (см. TODO в коде).

Автор: ChatGPT (GPT-5 Thinking)
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
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

# Официальный SDK от Binance для UM фьючерсов
# pip install binance-um-futures
from binance.um_futures import UMFutures
from binance.error import ClientError, ServerError

# ---------------------------- Константы/настройка ----------------------------

APP_NAME = "binance_futures_chunk_closer"
BASE_DIR = Path(os.getenv("OUTPUT_DIR", "./data")).resolve()
LOGS_DIR = BASE_DIR / "logs"
SNAP_DIR = BASE_DIR / "snapshots"
CHUNKS_CSV = BASE_DIR / "positions_chunks.csv"
POSITIONS_CSV = BASE_DIR / "positions.csv"
CLOSURES_LOG = LOGS_DIR / "closures.log"
CLOSURES_CSV = BASE_DIR / "closures.csv"

DEFAULT_BASE_URL = "https://fapi.binance.com"  # UM фьючерсы
POLL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# ---------------------------- Утилиты ----------------------------

def setup_loggers(verbose: bool = True) -> Tuple[logging.Logger, logging.Logger]:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(APP_NAME)
    logger.setLevel(logging.INFO if verbose else logging.WARNING)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = RotatingFileHandler(LOGS_DIR / f"{APP_NAME}.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    closures_logger = logging.getLogger(APP_NAME + ".closures")
    closures_logger.setLevel(logging.INFO)
    fh2 = RotatingFileHandler(CLOSURES_LOG, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh2.setFormatter(fmt)
    closures_logger.addHandler(fh2)

    return logger, closures_logger


def ts_ms_to_iso(ms: int) -> str:
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone().isoformat()
    except Exception:
        return ""


def round_step(quantity: float, step_size: float) -> float:
    """
    Округление количества под LOT_SIZE (stepSize).
    """
    if step_size <= 0:
        return quantity
    precision = int(round(-math.log10(step_size), 0)) if step_size < 1 else 0
    # Безопасное "обрезание" до шага
    q = math.floor(quantity / step_size) * step_size
    return float(f"{q:.{precision}f}")


# ---------------------------- Binance обёртки ----------------------------

class BinanceClient:
    def __init__(self, key: str, secret: str, base_url: str = DEFAULT_BASE_URL):
        self.client = UMFutures(key=key, secret=secret, base_url=base_url)
        self._exchange_info = None  # кэш фильтров

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
        # Возвращает список по всем символам (и сторонам в хедж-режиме)
        return self.client.get_position_risk()

    def market_close(self, symbol: str, position_amt: float, position_side: str = "BOTH") -> dict:
        """
        Закрывает позицию рыночным ордером reduceOnly.
        Для one-way: side противоположная знаку position_amt.
        Для hedge: нужен корректный positionSide ("LONG" или "SHORT").
        """
        if position_amt == 0:
            return {"skipped": True, "reason": "zero position"}

        side = "SELL" if position_amt > 0 else "BUY"
        exch = self.exchange_info().get(symbol, {})
        step = exch.get("marketStepSize") or exch.get("stepSize") or 0.0
        qty = round_step(abs(position_amt), step) if step else abs(position_amt)
        params = dict(symbol=symbol, side=side, type="MARKET", quantity=qty, reduceOnly="true")
        # В режиме хеджа явно укажем позиционную сторону
        if position_side and position_side != "BOTH":
            params["positionSide"] = position_side
        return self.client.new_order(**params)


# ---------------------------- Основная логика ----------------------------

class ChunkCloserBot:
    def __init__(self):
        load_dotenv()
        BASE_DIR.mkdir(parents=True, exist_ok=True)
        SNAP_DIR.mkdir(parents=True, exist_ok=True)

        self.logger, self.closures_logger = setup_loggers(verbose=True)

        self.api_key = os.getenv("BINANCE_API_KEY", "").strip()
        self.api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        self.base_url = os.getenv("BINANCE_BASE_URL", DEFAULT_BASE_URL)

        self.pnl_diff_usd = float(os.getenv("PNL_DIFF_USD", "40"))
        self.dry_run = os.getenv("DRY_RUN", "true").lower() == "true"

        if not self.api_key or not self.api_secret:
            self.logger.error("API ключи не заданы. Укажите BINANCE_API_KEY и BINANCE_API_SECRET в .env")
            raise SystemExit(2)

        self.client = BinanceClient(self.api_key, self.api_secret, self.base_url)

        self.logger.info("Старт бота. DRY_RUN=%s, PNL_DIFF_USD=%.2f", self.dry_run, self.pnl_diff_usd)
        self.logger.info("База данных/логов: %s", str(BASE_DIR))

        # CSV заголовки
        if not CHUNKS_CSV.exists():
            with open(CHUNKS_CSV, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["ts", "chunk_id", "symbol", "positionSide", "open_time_iso", "pnl_unrealized"])
        if not POSITIONS_CSV.exists():
            with open(POSITIONS_CSV, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["ts", "symbol", "positionSide", "positionAmt", "entryPrice", "pnl_unrealized", "updateTime_iso"])
        if not CLOSURES_CSV.exists():
            with open(CLOSURES_CSV, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["close_ts", "symbol1", "symbol2", "posSide1", "posSide2", "open_time1", "open_time2",
                            "pnl1", "pnl2", "pnl_diff_abs", "dry_run", "order_id_1", "order_id_2"])

        self._stop = False
        signal.signal(signal.SIGINT, self._sig_stop)
        signal.signal(signal.SIGTERM, self._sig_stop)

    def _sig_stop(self, *args):
        self._stop = True
        self.logger.info("Остановка по сигналу...")

    # ------------------------ шаг 1: снимок аккаунта ------------------------

    def fetch_and_log_account(self) -> Tuple[dict, List[dict]]:
        """
        Возвращает (balance_summary, positions_open)
        balance_summary: {'USDT': {...}, ...}
        positions_open: список открытых позиций (abs(positionAmt)>0) с ключами:
          symbol, positionAmt(float), entryPrice(float), unRealizedProfit(float), updateTime(int), positionSide(str)
        """
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
                pos = {
                    "symbol": p.get("symbol"),
                    "positionAmt": amt,
                    "entryPrice": float(p.get("entryPrice", "0")),
                    "unRealizedProfit": float(p.get("unRealizedProfit", "0")),
                    "updateTime": int(p.get("updateTime", 0)),
                    "positionSide": p.get("positionSide", "BOTH"),
                }
                positions_open.append(pos)
            except Exception as ex:
                self.logger.warning("Skip malformed position: %s | err=%s", p, ex)

        # Снапшоты
        ts = int(time.time())
        SNAP_DIR.mkdir(parents=True, exist_ok=True)
        with open(SNAP_DIR / f"balance_{ts}.json", "w", encoding="utf-8") as f:
            json.dump(balance_summary, f, ensure_ascii=False, indent=2)
        with open(SNAP_DIR / f"positions_{ts}.json", "w", encoding="utf-8") as f:
            json.dump(positions_open, f, ensure_ascii=False, indent=2)

        # CSV позиций
        with open(POSITIONS_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for po in positions_open:
                w.writerow([datetime.now().isoformat(), po["symbol"], po["positionSide"], po["positionAmt"],
                            po["entryPrice"], po["unRealizedProfit"], ts_ms_to_iso(po["updateTime"])])

        # Консольный вывод
        if positions_open:
            self.logger.info("Открытые позиции (%d):", len(positions_open))
            for po in positions_open:
                self.logger.info("  %s %-5s amt=%s entry=%s PnL=%s upd=%s",
                                 po["symbol"], po["positionSide"], po["positionAmt"],
                                 po["entryPrice"], po["unRealizedProfit"], ts_ms_to_iso(po["updateTime"]))
        else:
            self.logger.info("Открытых позиций нет.")

        return balance_summary, positions_open

    # ------------------------ шаг 2: чанки по 2 -----------------------------

    @staticmethod
    def make_chunks(positions_open: List[dict]) -> List[List[dict]]:
        """
        Сортировка по updateTime (как приблизительный "время открытия") и разбиение на пары.
        """
        pos_sorted = sorted(positions_open, key=lambda x: x.get("updateTime", 0))
        chunks = []
        for i in range(0, len(pos_sorted) - 1, 2):
            chunks.append([pos_sorted[i], pos_sorted[i + 1]])
        return chunks

    def log_chunks(self, chunks: List[List[dict]]):
        if not chunks:
            self.logger.info("Чанков для сравнения нет.")
            return
        self.logger.info("Чанки (по 2): всего %d", len(chunks))
        now_iso = datetime.now().isoformat()
        with open(CHUNKS_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for idx, pair in enumerate(chunks, start=1):
                self.logger.info("  #%d: %s(%s) PnL=%.2f | %s(%s) PnL=%.2f",
                                 idx,
                                 pair[0]["symbol"], pair[0]["positionSide"], pair[0]["unRealizedProfit"],
                                 pair[1]["symbol"], pair[1]["positionSide"], pair[1]["unRealizedProfit"])
                for po in pair:
                    w.writerow([now_iso, idx, po["symbol"], po["positionSide"],
                                ts_ms_to_iso(po["updateTime"]), po["unRealizedProfit"]])

    # ------------------------ шаг 3: закрытие пар ---------------------------

    def maybe_close_chunk(self, pair: List[dict]):
        if len(pair) != 2:
            return
        a, b = pair
        pnl_a = float(a["unRealizedProfit"])
        pnl_b = float(b["unRealizedProfit"])
        diff = abs(pnl_a - pnl_b)
        if diff <= self.pnl_diff_usd:
            self.logger.info("ΔPnL=%.2f ≤ %.2f — закрывать не будем (%s vs %s).",
                             diff, self.pnl_diff_usd, a["symbol"], b["symbol"])
            return

        self.logger.warning("ΔPnL=%.2f > %.2f — закрываем ОБЕ позиции: %s(%s) и %s(%s)",
                            diff, self.pnl_diff_usd, a["symbol"], a["positionSide"], b["symbol"], b["positionSide"])

        order1 = order2 = None
        if self.dry_run:
            self.logger.warning("DRY_RUN=true — имитируем закрытие без отправки ордеров.")
        else:
            try:
                order1 = self.client.market_close(
                    symbol=a["symbol"],
                    position_amt=a["positionAmt"],
                    position_side=a["positionSide"]
                )
                time.sleep(0.2)  # небольшая пауза
                order2 = self.client.market_close(
                    symbol=b["symbol"],
                    position_amt=b["positionAmt"],
                    position_side=b["positionSide"]
                )
            except (ClientError, ServerError) as e:
                self.logger.error("Ошибка при закрытии: %s", e)

        # Логи закрытия
        close_iso = datetime.now().isoformat()
        self.closures_logger.info(
            "CLOSE | %s & %s | posSides: %s/%s | ΔPnL=%.2f | openA=%s openB=%s | dry_run=%s",
            a["symbol"], b["symbol"], a["positionSide"], b["positionSide"], diff,
            ts_ms_to_iso(a["updateTime"]), ts_ms_to_iso(b["updateTime"]), self.dry_run
        )

        # CSV закрытий
        with open(CLOSURES_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([close_iso,
                        a["symbol"], b["symbol"],
                        a["positionSide"], b["positionSide"],
                        ts_ms_to_iso(a["updateTime"]), ts_ms_to_iso(b["updateTime"]),
                        pnl_a, pnl_b, diff,
                        self.dry_run,
                        (order1 or {}).get("orderId") if isinstance(order1, dict) else None,
                        (order2 or {}).get("orderId") if isinstance(order2, dict) else None])

    # ------------------------ основной цикл ---------------------------------

    def run_once(self):
        bal, positions = self.fetch_and_log_account()
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
            # Досыпаем до целевой периодичности
            elapsed = time.time() - start
            sleep_s = max(0.0, POLL_SECONDS - elapsed)
            time.sleep(sleep_s)


def main():
    try:
        bot = ChunkCloserBot()
    except SystemExit:
        return
    bot.run_loop()


if __name__ == "__main__":
    main()
