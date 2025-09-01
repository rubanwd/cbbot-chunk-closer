# Binance Futures Chunk Closer Bot

## Быстрый старт
```bash
python -m venv .venv && . .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env  # заполните API ключи
python binance_futures_chunk_closer.py
```

## Что делает бот
- Каждую минуту опрашивает Binance UM Futures: баланс и открытые позиции.
- Печатает их в консоль и пишет снапшоты в `data/snapshots/`.
- Если позиций > 1, сортирует по времени (updateTime) и делит на пары.
- Для каждой пары проверяет |PnL1 - PnL2| > PNL_DIFF_USD — если да, закрывает обе позиции **рыночными reduceOnly ордерами**.
- Все события логируются в `data/logs/` и CSV: `positions.csv`, `positions_chunks.csv`, `closures.csv`.

## Важно
- По умолчанию `DRY_RUN=true` — ничего не закрывает. Чтобы реально закрывать — `DRY_RUN=false`.
- В странах/регионах с ограничениями Binance может отвечать 451. В таком случае используйте VPN/прокси или другой доступный регион и убедитесь, что ваш доступ соответствует условиям Binance.
- Поле `updateTime` используется как приблизительная дата открытия. Для точной даты можно доработать бота разбором истории сделок.
