# app.py — Bot Telegram + FastAPI usando Google Sheets como fonte de verdade
# Colunas esperadas no Sheets (aba "Licencas"): A=Licença, B=Validade, C=Data de inicio, D=Data final, E=email, F=status

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import requests
from fastapi import FastAPI, Request, Header
from fastapi.responses import JSONResponse

# Google
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials

# ----------------- logging -----------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("sheetsbot")

# ----------------- envs --------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
ADMIN_TELEGRAM_ID = (os.getenv("ADMIN_TELEGRAM_ID") or "").strip()
ADMIN_TOKEN = (os.getenv("ADMIN_TOKEN") or "").strip()  # opcional (para /admin <TOKEN>)
TELEGRAM_WEBHOOK_SECRET = (os.getenv("TELEGRAM_WEBHOOK_SECRET") or "").strip()

LICENSE_SHEET_ID = (os.getenv("LICENSE_SHEET_ID") or "").strip()  # obrigatório
LICENSE_SHEET_TAB = os.getenv("LICENSE_SHEET_TAB", "Licencas")
LICENSE_SHEET_RANGE = os.getenv("LICENSE_SHEET_RANGE", f"{LICENSE_SHEET_TAB}!A:F")

GOOGLE_USE_OAUTH = os.getenv("GOOGLE_USE_OAUTH", "0") == "1"
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON") or os.getenv("GOOGLE_SHEETS_CREDENTIALS")
GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

if not TELEGRAM_TOKEN:
    raise RuntimeError("Defina TELEGRAM_TOKEN")
if not LICENSE_SHEET_ID:
    raise RuntimeError("Defina LICENSE_SHEET_ID")

# --------------- app -----------------------
app = FastAPI()

# ------------- Google Sheets client ----------
_sheets_service = None
def get_sheets():
    """Retorna cliente do Google Sheets (cacheado em memória)."""
    global _sheets_service
    if _sheets_service:
        return _sheets_service
    if GOOGLE_USE_OAUTH:
        if not os.path.exists(GOOGLE_TOKEN_PATH):
            raise RuntimeError("GOOGLE_TOKEN_PATH não encontrado.")
        with open(GOOGLE_TOKEN_PATH, "r") as f:
            info = json.load(f)
        creds = Credentials.from_authorized_user_info(info, GOOGLE_SCOPES)
    else:
        if not GOOGLE_SA_JSON:
            raise RuntimeError("GOOGLE_SA_JSON não definido.")
        info = json.loads(GOOGLE_SA_JSON)
        creds = service_account.Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
    _sheets_service = build("sheets", "v4", credentials=creds).spreadsheets()
    return _sheets_service

# ------------- helpers ----------------------
def _now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def send_telegram(chat_id: int, text: str):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    except Exception as e:
        log.warning("Erro enviando Telegram: %s", e)

def _read_sheet_all() -> list:
    """Retorna todas as linhas (a partir da 1) do range A:F."""
    srv = get_sheets()
    resp = srv.values().get(spreadsheetId=LICENSE_SHEET_ID, range=LICENSE_SHEET_RANGE).execute()
    return resp.get("values", [])

def _append_sheet_row(row: list):
    srv = get_sheets()
    srv.values().append(
        spreadsheetId=LICENSE_SHEET_ID,
        range=LICENSE_SHEET_RANGE,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()

def _update_sheet_row_by_index(row_index_1based: int, row: list):
    rng = f"{LICENSE_SHEET_TAB}!A{row_index_1based}:F{row_index_1based}"
    srv = get_sheets()
    srv.values().update(
        spreadsheetId=LICENSE_SHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": [row]},
    ).execute()

def _find_license_row(license_key: str) -> Optional[Tuple[int, list]]:
    """
    Busca pela chave na coluna A. Retorna (row_index_1based, row_values) ou None.
    """
    vals = _read_sheet_all()
    for idx, r in enumerate(vals, start=1):
        if len(r) >= 1 and r[0].strip() == license_key:
            return idx, r
    return None

def _gen_key() -> str:
    import secrets, string
    alphabet = string.ascii_uppercase + string.digits
    def part(n): return "".join(secrets.choice(alphabet) for _ in range(n))
    return f"GF-{part(4)}-{part(4)}"

def _promove_admin(chat_id: int):
    """Atualiza ADMIN_TELEGRAM_ID de forma segura (corrige problema de 'global')."""
    global ADMIN_TELEGRAM_ID
    ADMIN_TELEGRAM_ID = str(chat_id)
    os.environ["ADMIN_TELEGRAM_ID"] = ADMIN_TELEGRAM_ID

# ------------- business logic ----------------
def create_license_in_sheet(days: int = 30, email: Optional[str] = None) -> Tuple[str, str]:
    key = _gen_key()
    start = datetime.now(timezone.utc)
    end = (start + timedelta(days=days)) if days and days > 0 else None
    validade_cell = "vitalícia" if not days or days == 0 else str(days)
    start_s = start.strftime("%Y-%m-%d %H:%M:%S")
    end_s = end.strftime("%Y-%m-%d %H:%M:%S") if end else ""
    status = "active"
    row = [key, validade_cell, start_s, end_s, email or "", status]
    _append_sheet_row(row)
    return key, (end_s or "vitalícia")

def set_license_status_in_sheet(license_key: str, new_status: str) -> bool:
    found = _find_license_row(license_key)
    if not found:
        return False
    idx, row = found
    while len(row) < 6:
        row.append("")
    row[5] = new_status
    _update_sheet_row_by_index(idx, row)
    return True

def get_license_info_from_sheet(license_key: str) -> Optional[dict]:
    found = _find_license_row(license_key)
    if not found:
        return None
    idx, row = found
    def col(i): return row[i] if i < len(row) else ""
    return {
        "row": idx,
        "license_key": col(0),
        "validade": col(1),
        "data_inicio": col(2),
        "data_final": col(3),
        "email": col(4),
        "status": col(5),
    }

# ------------- Telegram webhook -------------
@app.post("/telegram/webhook")
async def webhook(request: Request, x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)):
    # validação opcional do secret
    if TELEGRAM_WEBHOOK_SECRET:
        got = (x_telegram_bot_api_secret_token or "").strip()
        if got != TELEGRAM_WEBHOOK_SECRET:
            log.warning("Webhook secret mismatch")
            return JSONResponse({"ok": True})

    body = await request.json()
    msg = body.get("message") or {}
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id or not text:
        return JSONResponse({"ok": True})

    log.info("msg from %s: %s", chat_id, text)

    if text.startswith("/start"):
        send_telegram(
            chat_id,
            "🤖 Bot ativo!\n"
            "Comandos:\n"
            "/whoami\n"
            "/licenca nova <dias> [email]\n"
            "/licenca set <CHAVE> <status>\n"
            "/licenca info <CHAVE>\n"
            "/admin <TOKEN> (opcional)"
        )
        return JSONResponse({"ok": True})

    if text.startswith("/whoami"):
        is_admin = 'true' if str(chat_id) == ADMIN_TELEGRAM_ID else 'false'
        send_telegram(chat_id, f"• chatid: `{chat_id}`\n• admin: `{is_admin}`")
        return JSONResponse({"ok": True})

    # promover admin com token (sem 'global' no meio do handler)
    if text.startswith("/admin"):
        parts = text.split(maxsplit=1)
        if len(parts) == 2 and ADMIN_TOKEN and parts[1].strip() == ADMIN_TOKEN:
            _promove_admin(chat_id)
            send_telegram(chat_id, "✅ Você foi promovido a admin neste chat.")
        else:
            send_telegram(chat_id, "Uso: /admin <TOKEN>")
        return JSONResponse({"ok": True})

    # criar licença
    if text.lower().startswith("/licenca nova"):
        if str(chat_id) != ADMIN_TELEGRAM_ID:
            send_telegram(chat_id, "❌ Apenas admin pode criar licenças.")
            return JSONResponse({"ok": True})

        parts = text.split()
        if len(parts) < 3:
            send_telegram(chat_id, "Uso: /licenca nova <dias> [email]")
            return JSONResponse({"ok": True})

        try:
            days = int(parts[2])
        except Exception:
            send_telegram(chat_id, "Dias inválidos. Ex.: /licenca nova 30 email@dominio.com")
            return JSONResponse({"ok": True})

        email = parts[3] if len(parts) > 3 else None
        try:
            key, exp = create_license_in_sheet(days=days, email=email)
            send_telegram(chat_id, f"✅ Licença criada\n`{key}`\nValidade: {exp}\nEmail: {email or '(sem email)'}")
        except Exception as e:
            log.exception("Erro criando licença")
            send_telegram(chat_id, f"❌ Erro ao criar licença: {e}")
        return JSONResponse({"ok": True})

    # alterar status
    if text.lower().startswith("/licenca set"):
        if str(chat_id) != ADMIN_TELEGRAM_ID:
            send_telegram(chat_id, "❌ Apenas admin pode alterar status.")
            return JSONResponse({"ok": True})

        parts = text.split()
        if len(parts) < 4:
            send_telegram(chat_id, "Uso: /licenca set <CHAVE> <novo_status>")
            return JSONResponse({"ok": True})

        chave = parts[2]
        novo = parts[3]
        ok = set_license_status_in_sheet(chave, novo)
        if ok:
            send_telegram(chat_id, f"✅ Status atualizado: {chave} -> {novo}")
        else:
            send_telegram(chat_id, f"❌ Licença não encontrada: {chave}")
        return JSONResponse({"ok": True})

    # info
    if text.lower().startswith("/licenca info"):
        parts = text.split()
        if len(parts) < 3:
            send_telegram(chat_id, "Uso: /licenca info <CHAVE>")
            return JSONResponse({"ok": True})
        chave = parts[2]
        info = get_license_info_from_sheet(chave)
        if not info:
            send_telegram(chat_id, f"❌ Licença não encontrada: {chave}")
        else:
            msg = (
                f"*Licença:* `{info['license_key']}`\n"
                f"*Validade:* {info['validade']}\n"
                f"*Início:* {info['data_inicio']}\n"
                f"*Final:* {info['data_final']}\n"
                f"*Email:* {info['email']}\n"
                f"*Status:* {info['status']}"
            )
            send_telegram(chat_id, msg)
        return JSONResponse({"ok": True})

    # fallback
    send_telegram(chat_id, "Comando não reconhecido. Use /start")
    return JSONResponse({"ok": True})

# health
@app.get("/ping")
def ping():
    return {"pong": True, "time": datetime.now(timezone.utc).isoformat(timespec="seconds")}
