import os
import random
import string
import logging
from datetime import datetime, timedelta, timezone
import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import requests

# ---------------------------------------------------------
# Configuração básica
# ---------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")

# ---------------------------------------------------------
# Função utilitária para gerar chaves de licença
# ---------------------------------------------------------
def _gen_key():
    parts = ["".join(random.choices(string.ascii_uppercase + string.digits, k=4)) for _ in range(3)]
    return f"GF-{'-'.join(parts)}"

# ---------------------------------------------------------
# Função auxiliar para rodar queries com retry
# ---------------------------------------------------------
def _exec_with_retry(sql, params=()):
    for attempt in range(3):
        try:
            with psycopg.connect(DATABASE_URL, sslmode="require") as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    conn.commit()
            return
        except Exception as e:
            logger.warning(f"Tentativa {attempt+1}/3 falhou: {e}")
            if attempt == 2:
                raise

# ---------------------------------------------------------
# Simulação de escrita no Google Sheets (placeholder)
# ---------------------------------------------------------
def gs_append_license_row(key, days, email, created_at, expires_at, status):
    try:
        logger.info(f"[Sheets] Licença {key} criada ({days} dias) para {email} — expira em {expires_at}")
    except Exception as e:
        logger.warning(f"Erro ao enviar para Google Sheets: {e}")

# ---------------------------------------------------------
# Função principal de criação de licença
# ---------------------------------------------------------
def create_license(days: int = 30, max_files: int = 1,
                   notes: str = None, custom_key: str = None,
                   email_for_sheet: str = None):
    key = custom_key or _gen_key()
    created_at = datetime.now(timezone.utc)
    expires_at = (created_at + timedelta(days=days)) if days else None

    logger.info("create_license: USANDO_COLUNAS_COM_UNDERSCORE + EMAIL")

    _exec_with_retry(
        """
        INSERT INTO licenses(license_key, status, max_files, expires_at, notes, email)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (key, "active", max_files, expires_at, notes, email_for_sheet),
    )

    try:
        gs_append_license_row(key, days, email_for_sheet, created_at, expires_at, "active")
    except Exception:
        logger.exception("Falha ao escrever licença no Sheets (ignorado).")

    return key, expires_at.isoformat(timespec="seconds") if expires_at else None

# ---------------------------------------------------------
# Endpoint Webhook do Telegram
# ---------------------------------------------------------
@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    logger.info(f"Mensagem recebida: {data}")

    if "message" not in data:
        return JSONResponse(content={"ok": True})

    message = data["message"]
    chat_id = message["chat"]["id"]
    text = message.get("text", "").strip()

    if text.startswith("/start"):
        send_message(chat_id, "🤖 Bot ativo! Use /licenca nova <dias> [email]")
        return JSONResponse(content={"ok": True})

    elif text.startswith("/whoami"):
        send_message(chat_id, f"• chatid: {chat_id}\n• admin: {'true' if str(chat_id) == ADMIN_TELEGRAM_ID else 'false'}")
        return JSONResponse(content={"ok": True})

    elif text.startswith("/licenca nova"):
        parts = text.split()
        if len(parts) < 3:
            send_message(chat_id, "❌ Use: /licenca nova <dias> [email]")
            return JSONResponse(content={"ok": True})

        try:
            days = int(parts[2])
            email = parts[3] if len(parts) > 3 else None
            key, expires = create_license(days=days, email_for_sheet=email)
            msg = f"✅ Licença criada!\n🔑 {key}\n📅 Expira em: {expires}\n📧 {email or '(sem email)'}"
            send_message(chat_id, msg)
        except Exception as e:
            logger.exception("Erro ao criar licença")
            send_message(chat_id, f"❌ Erro ao criar licença: {e}")

        return JSONResponse(content={"ok": True})

    return JSONResponse(content={"ok": True})

# ---------------------------------------------------------
# Função para enviar mensagem ao Telegram
# ---------------------------------------------------------
def send_message(chat_id, text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": chat_id, "text": text}
        requests.post(url, json=payload)
    except Exception as e:
        logger.warning(f"Erro ao enviar mensagem Telegram: {e}")

# ---------------------------------------------------------
# Health check
# ---------------------------------------------------------
@app.get("/")
async def root():
    return {"status": "ok"}
