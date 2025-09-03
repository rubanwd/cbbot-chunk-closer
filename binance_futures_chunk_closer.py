#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance USDT-M Futures chunk-closer bot (+ Telegram notify on close)

Новое:
- Динамический порог закрытия по GAP: PNL_DIFF_PCT (% от общего USDT баланса).
  На каждой итерации порог считается как: threshold = balance_total_usdt * PNL_DIFF_PCT / 100.
  Если PNL_DIFF_PCT не задан или баланс недоступен — используется запасной PNL_DIFF_USD.

Ключевые моменты:
- Снапшоты перезаписываются (balance.json, positions.json) + ежедневная очистка каталога snapshots/.
- Логика «чанков» по 2 позиции. Закрытие по порогу GAP:
    PNL_GAP_MODE=plus (по умолчанию): gap = pnl_a + pnl_b → закрываем только если gap > threshold.
    magnitude: gap = | |p1| - |p2| |
    signed:    gap = | p1 - p2 |
- После закрытия ПАРЫ отправляется Telegram-отчёт:
    • пары (символы, стороны, PnL)
    • дата/время закрытия (локальное)
    • фактический порог (динамический)
    • «время жизни» чанка = now - max(updateTime обеих позиций)

ENV:
- BINANCE_API_KEY, BINANCE_API_SECRET (обязательные)
- OUTPUT_DIR=./data
- POLL_INTERVAL_SECONDS=60
- BINANCE_BASE_URL=https://fapi.binance.com
- PNL_GAP_MODE=plus | magnitude | signed
- PNL_DIFF_PCT=2            # <-- новый способ задать порог (проценты от баланса)
- PNL_DIFF_USD=40           # запасной фиксированный порог в USD
- DRY_RUN=true/false
- TELEGRAM_BOT_TOKEN=<token>
- TELEGRAM_CHAT_ID=<id>
- TELEGRAM_SEND_IN_DRY_RUN=false   # если true — шлём отчёт даже в dry-run
"""

import os
import time
import csv
import json
import math
import signal
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from dotenv import load_dotenv
from binance.um_futures import UMFutures
from binance.error import ClientError, ServerError

from telegram_notifier import send_telegram_message

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

def fmt_local(ts: datetime) -> str:
    return ts.astimezone().strftime("%d %b %Y, %H:%M:%S %Z")

def fmt_duration(secs: float) -> str:
    if secs < 0: secs = 0
    td = timedelta(seconds=int(secs))
    days = td.days
    hours, rem = divmod(td.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

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

        # --- логика закрытия
        # запасной фиксированный порог (USD), если проценты недоступны
        self.pnl_diff_usd = float(os.getenv("PNL_DIFF_USD", "40"))
        # новый: проценты от общего баланса USDT
        self.pnl_diff_pct: Optional[float] = None
        _pct = os.getenv("PNL_DIFF_PCT", "").strip()
        if _pct:
            try:
                self.pnl_diff_pct = float(_pct)
            except ValueError:
                self.pnl_diff_pct = None

        self.pnl_gap_mode = os.getenv("PNL_GAP_MODE", "plus").lower().strip()
        if self.pnl_gap_mode not in ("plus", "magnitude", "signed"):
            self.pnl_gap_mode = "plus"

        self.dry_run = os.getenv("DRY_RUN", "true").lower() == "true"

        # --- Telegram
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.tg_send_in_dry_run = os.getenv("TELEGRAM_SEND_IN_DRY_RUN", "false").lower() == "true"
        self.tg_enabled = bool(self.tg_token and self.tg_chat_id)

        if not self.api_key or not self.api_secret:
            self.logger.error("API ключи не заданы. Укажите BINANCE_API_KEY и BINANCE_API_SECRET в .env")
            raise SystemExit(2)

        self.client = BinanceClient(self.api_key, self.api_secret, self.base_url)

        self.logger.info(
            "Старт. DRY_RUN=%s, GAP_MODE=%s, PNL_DIFF_PCT=%s, PNL_DIFF_USD(backup)=%.2f, Telegram=%s",
            self.dry_run, self.pnl_gap_mode,
            f"{self.pnl_diff_pct}%" if self.pnl_diff_pct is not None else "N/A",
            self.pnl_diff_usd,
            "on" if self.tg_enabled else "off"
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

    # -------- housekeeping --------
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

    # -------- helpers --------
    def _compute_threshold_usd(self, balance_summary: Dict) -> float:
        """
        Возвращает фактический порог закрытия в USD.
        Если задан PNL_DIFF_PCT и удалось прочитать общий USDT баланс — берем проценты.
        Иначе — возвращаем запасной PNL_DIFF_USD.
        """
        if self.pnl_diff_pct is not None and self.pnl_diff_pct > 0:
            usdt = balance_summary.get("USDT", {})
            total_str = usdt.get("balance")  # Binance UM Futures: total balance в поле "balance"
            try:
                total = float(total_str) if total_str is not None else None
            except (TypeError, ValueError):
                total = None
            if total is not None:
                th = total * (self.pnl_diff_pct / 100.0)
                # защита от отрицательных/NaN
                try:
                    if not (th >= 0 or th <= 0):  # NaN check
                        th = self.pnl_diff_usd
                except Exception:
                    pass
                return max(0.0, th)
            else:
                self.logger.warning("Не удалось получить общий USDT баланс — использую PNL_DIFF_USD=%.2f", self.pnl_diff_usd)
        return max(0.0, self.pnl_diff_usd)

    # -------- data fetch --------
    def fetch_and_log_account(self) -> Tuple[dict, List[dict]]:
        try:
            bal_list = self.client.balance()
        except (ClientError, ServerError) as e:
            self.logger.error("Balance error: %s", e)
            bal_list = []
        balance_summary = {b.get("asset"): b for b in bal_list}
        usdt_total = balance_summary.get("USDT", {}).get("balance")
        usdt_avail = balance_summary.get("USDT", {}).get("availableBalance")
        # посчитаем и залогируем фактический порог
        threshold_usd = self._compute_threshold_usd(balance_summary)
        pct_info = f" ({self.pnl_diff_pct}% от {usdt_total})" if self.pnl_diff_pct is not None and usdt_total is not None else ""
        self.logger.info("Баланс (USDT): total=%s, available=%s | Порог закрытия: %.2f%s",
                         usdt_total, usdt_avail, threshold_usd, pct_info)

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

        self.daily_cleanup()
        with open(SNAP_BALANCE_FILE, "w", encoding="utf-8") as f:
            json.dump(balance_summary, f, ensure_ascii=False, indent=2)
        with open(SNAP_POSITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(positions_open, f, ensure_ascii=False, indent=2)

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

    # -------- chunks --------
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

    def _calc_gap(self, pnl_a: float, pnl_b: float) -> float:
        mode = self.pnl_gap_mode
        if mode == "plus":
            return pnl_a + pnl_b
        if mode == "magnitude":
            return abs(abs(pnl_a) - abs(pnl_b))
        return abs(pnl_a - pnl_b)

    # -------- close pair + Telegram notify --------
    def maybe_close_chunk(self, pair: List[dict], threshold_usd: float):
        if len(pair) != 2:
            return
        a, b = pair
        pnl_a = float(a["unRealizedProfit"])
        pnl_b = float(b["unRealizedProfit"])
        gap = self._calc_gap(pnl_a, pnl_b)

        self.logger.info("Gap(%s)=%.2f (threshold=%.2f) | pair=(%s:%.2f, %s:%.2f)",
                         self.pnl_gap_mode, gap, threshold_usd,
                         a["symbol"], pnl_a, b["symbol"], pnl_b)

        if gap <= threshold_usd:
            self.logger.info("Условия НЕ выполнены — не закрываем.")
            return

        self.logger.warning("Закрываем обе позиции: %s(%s) и %s(%s)",
                            a["symbol"], a["positionSide"], b["symbol"], b["positionSide"])

        order1 = order2 = None
        if self.dry_run:
            self.logger.warning("DRY_RUN=true — имитация без ордеров.")
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

        close_dt_local = datetime.now().astimezone()
        close_iso = close_dt_local.isoformat()

        # лог и CSV закрытий
        self.closures_logger.info(
            "CLOSE | mode=%s | %s & %s | posSides: %s/%s | gap=%.2f | threshold=%.2f | openA=%s openB=%s | dry_run=%s",
            self.pnl_gap_mode, a["symbol"], b["symbol"], a["positionSide"], b["positionSide"],
            gap, threshold_usd, ts_ms_to_iso(a["updateTime"]), ts_ms_to_iso(b["updateTime"]), self.dry_run
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

        # -------- Telegram отчёт --------
        if self.tg_enabled and (not self.dry_run or self.tg_send_in_dry_run):
            last_upd_ms = max(int(a.get("updateTime", 0)), int(b.get("updateTime", 0)))
            last_upd_dt = datetime.fromtimestamp(last_upd_ms / 1000, tz=timezone.utc).astimezone()
            chunk_age = fmt_duration((close_dt_local - last_upd_dt).total_seconds())

            msg_lines = [
                "Binance — Чанк закрыт",
                f"Время закрытия: {fmt_local(close_dt_local)}",
                f"Gap({self.pnl_gap_mode}) = {gap:.2f}",
                f"Порог закрытия: {threshold_usd:.2f}"
                + (f" (из {self.pnl_diff_pct}% от баланса)" if self.pnl_diff_pct is not None else ""),
                f"Время существования чанка: {chunk_age}",
                "",
                f"• {a['symbol']}  {a['positionSide']}  amt={a['positionAmt']}  uPnL={pnl_a:.2f}",
                f"• {b['symbol']}  {b['positionSide']}  amt={b['positionAmt']}  uPnL={pnl_b:.2f}",
                "",
                f"Dry-run: {self.dry_run}",
            ]
            _ok = send_telegram_message(self.tg_token, self.tg_chat_id, "\n".join(msg_lines))
            if _ok:
                self.logger.info("Отчёт отправлен в Telegram.")
            else:
                self.logger.warning("Не удалось отправить отчёт в Telegram.")

    # -------- loop --------
    def run_once(self):
        balance, positions = self.fetch_and_log_account()
        threshold_usd = self._compute_threshold_usd(balance)
        if len(positions) > 1:
            chunks = self.make_chunks(positions)
            self.log_chunks(chunks)
            for pair in chunks:
                self.maybe_close_chunk(pair, threshold_usd)
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
