# -*- coding: utf-8 -*-
"""
Простой Telegram notifier без внешних зависимостей.
Использование:
    from telegram_notifier import send_telegram_message
    send_telegram_message(token, chat_id, "text")
"""

from urllib import request, parse
import json
import time


def _split_chunks(text: str, limit: int = 4096):
    # мягко режем по строкам, чтобы не ломать формат
    lines = text.splitlines(True)
    buf, size = [], 0
    for ln in lines:
        ln_len = len(ln)
        if size + ln_len > limit and buf:
            yield "".join(buf)
            buf, size = [ln], ln_len
        else:
            buf.append(ln)
            size += ln_len
    if buf:
        yield "".join(buf)


def send_telegram_message(
    token: str,
    chat_id: str,
    text: str,
    parse_mode: str | None = None,
    disable_web_page_preview: bool = True,
    retries: int = 2,
    timeout: int = 10,
) -> bool:
    """
    Отправляет text в Telegram чат. Возвращает True/False.
    Автоматически делит длинные сообщения >4096 символов.
    """
    if not token or not chat_id or not text:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    ok_total = True
    for chunk in _split_chunks(text):
        data = {
            "chat_id": chat_id,
            "text": chunk,
            "disable_web_page_preview": "true" if disable_web_page_preview else "false",
        }
        if parse_mode:
            data["parse_mode"] = parse_mode

        body = parse.urlencode(data).encode("utf-8")
        req = request.Request(url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"})
        last_err = None
        for _ in range(max(1, retries)):
            try:
                with request.urlopen(req, timeout=timeout) as resp:
                    raw = resp.read()
                    js = json.loads(raw.decode("utf-8"))
                    if not js.get("ok", False):
                        last_err = js
                        time.sleep(0.8)
                        continue
                    last_err = None
                    break
            except Exception as e:
                last_err = e
                time.sleep(0.8)
        if last_err is not None:
            ok_total = False
    return ok_total
