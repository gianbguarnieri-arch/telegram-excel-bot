import os
import json
import secrets
import string
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

import httpx
from fastapi import FastAPI, Request, Header
from fastapi.responses import RedirectResponse, PlainTextResponse, JSONResponse

# Google OAuth / Sheets
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

app = FastAPI(title="Telegram + Sheets (OAuth)")

# ============================ ENVs ================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

# Google OAuth (obrigatório neste modo)
GOOGLE_USE_OAUTH = True
GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = os.getenv("GOOGLE_OAUTH_REDIRECT_URI")
GOOGLE_OAUTH_SCOPES = (os.getenv("GOOGLE_OAUTH_SCOPES") or
    "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive").split()
GOOGLE_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "/tmp/google_oauth_token.json")

# Planilha de Licenças
# Colunas: A:Licença | B:Validade | C:Data de inicio | D:Data final | E:email | F:status
LICENSE_SHEET_ID   = os.getenv("LICENSE_SHEET_ID", "").strip()      # ID da planilha
LICENSE_SHEET_TAB  = os.getenv("LICENSE_SHEET_TAB", "Licencas").strip()
LICENSE_SHEET_RANGE = os.getenv("LICENSE_SHEET_RANGE", f"{LICENSE_SHEET_TAB}!A:F").strip()

# Compartilhamento ao criar links (não usamos aqui, mas mantido caso evolua)
SHARE_LINK_ROLE = os.getenv("SHARE_LINK_ROLE", "writer")

# ============================ Helpers Gerais ======================
def _now_tz() -> datetime:
    return datetime.now(timezone.utc)

def _fmt_date(dt: datetime) -> str:
    # Mostra em YYYY-MM-DD HH:MM (UTC) para padronizar
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")

def _gen_license(prefix="GF") -> str:
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

def _is_admin(chat_id) -> bool:
    return ADMIN_TELEGRAM_ID != "" and str(chat_id).strip() == ADMIN_TELEGRAM_ID

async def tg_send(chat_id, text):
    if not TELEGRAM_TOKEN:
        log.warning("TELEGRAM_TOKEN vazio; não enviando mensagem.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
        except Exception as e:
            log.error(f"Falha Telegram: {e}")

# ============================ OAuth Google ========================
def _flow() -> Flow:
    # client_config no formato "web"
    return Flow(
        client_type="web",
        client_config={
            "web": {
                "client_id": GOOGLE_OAUTH_CLIENT_ID,
                "client_secret": GOOGLE_OAUTH_CLIENT_SECRET,
                "redirect_uris": [GOOGLE_OAUTH_REDIRECT_URI],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        },
        scopes=GOOGLE_OAUTH_SCOPES,
        redirect_uri=GOOGLE_OAUTH_REDIRECT_URI,
    )

def _credentials_from_disk() -> Optional[Credentials]:
    # Lê o arquivo salvo pelo callback
    if not os.path.exists(GOOGLE_TOKEN_PATH):
        return None
    try:
        with open(GOOGLE_TOKEN_PATH, "r") as f:
            data = json.load(f)
        return Credentials.from_authorized_user_info(data)
    except Exception as e:
        log.error(f"Erro lendo GOOGLE_TOKEN_PATH: {e}")
        return None

def _save_credentials(creds: Credentials):
    data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }
    with open(GOOGLE_TOKEN_PATH, "w") as f:
        json.dump(data, f)

def _sheets_service() -> build:
    creds = _credentials_from_disk()
    if not creds:
        raise RuntimeError("Token OAuth do Google não encontrado. Acesse /oauth/start.")
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(httpx.Client())  # não é padrão; alternativa simples é refazer o fluxo se expirar
        except Exception:
            pass
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

# ============================ Acesso ao Sheets ====================
def sheets_upsert_license(key: str, days: int, email: Optional[str]) -> Tuple[bool, str]:
    """
    Cria/atualiza a linha da licença no intervalo A:F.
    Colunas: Licença | Validade | Data de inicio | Data final | email | status
    """
    service = _sheets_service()
    sheet = service.spreadsheets()

    start_dt = _now_tz()
    end_dt = start_dt + timedelta(days=days)
    values = [key, str(days), _fmt_date(start_dt), _fmt_date(end_dt), (email or ""), "active"]

    try:
        # Ler todas as linhas existentes
        resp = sheet.values().get(spreadsheetId=LICENSE_SHEET_ID, range=LICENSE_SHEET_RANGE).execute()
        rows: List[List[str]] = resp.get("values", [])

        # Cabeçalho esperado (opcional; se você tem header na primeira linha, pule-a)
        # Vamos procurar pela coluna A (Licença)
        found_row_idx = None
        for idx, row in enumerate(rows, start=1):
            if len(row) >= 1 and row[0].strip().upper() == key.strip().upper():
                found_row_idx = idx
                break

        if found_row_idx:
            # Atualiza a linha existente
            target_range = f"{LICENSE_SHEET_TAB}!A{found_row_idx}:F{found_row_idx}"
            sheet.values().update(
                spreadsheetId=LICENSE_SHEET_ID,
                range=target_range,
                valueInputOption="RAW",
                body={"values": [values]},
            ).execute()
            return True, f"Licença *{key}* atualizada na linha {found_row_idx}."
        else:
            # Append nova linha
            sheet.values().append(
                spreadsheetId=LICENSE_SHEET_ID,
                range=LICENSE_SHEET_RANGE,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [values]},
            ).execute()
            return True, f"Licença *{key}* criada."
    except HttpError as e:
        log.exception("Erro Sheets")
        return False, f"Erro no Google Sheets: {e}"
    except Exception as e:
        log.exception("Erro geral Sheets")
        return False, f"Erro ao gravar na planilha: {e}"

# ============================ Rotas HTTP ==========================
@app.get("/ping")
def ping():
    return {"pong": True, "time": _fmt_date(_now_tz())}

@app.get("/oauth/start")
def oauth_start():
    if not all([GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, GOOGLE_OAUTH_REDIRECT_URI]):
        return PlainTextResponse("Vars do OAuth ausentes.", status_code=500)
    flow = _flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    # (Opcional) guardar state em storage; aqui pulamos
    return RedirectResponse(auth_url)

@app.get("/oauth/callback")
def oauth_callback(request: Request):
    flow = _flow()
    flow.fetch_token(authorization_response=str(request.url))
    creds = flow.credentials
    _save_credentials(creds)
    return PlainTextResponse(f"✅ Google OAuth concluído. Token salvo em {GOOGLE_TOKEN_PATH}")

@app.get("/oauth/status")
def oauth_status():
    try:
        with open(GOOGLE_TOKEN_PATH) as f:
            data = json.load(f)
        ok = "refresh_token" in data and data.get("token_uri")
        return {"exists": True, "ok": bool(ok), "scopes": data.get("scopes")}
    except Exception:
        return {"exists": False}

# ============================ Telegram Webhook ====================
@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(default=None)
):
    # (opcional) valida segredo do webhook
    if TELEGRAM_WEBHOOK_SECRET:
        if (x_telegram_bot_api_secret_token or "").strip() != TELEGRAM_WEBHOOK_SECRET:
            return JSONResponse({"ok": True})  # ignora silenciosamente

    body = await request.json()
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    norm = text.lower()
    # === /start
    if norm == "/start":
        await tg_send(chat_id,
            "Olá! 👋\n"
            "Este bot usa Google OAuth. Para configurar o Google Sheets:\n"
            "1) Acesse *SEU_DOMÍNIO*/oauth/start, autorize e salve o token.\n"
            "2) Depois, use /whoami ou /licenca."
        )
        return {"ok": True}

    # === /whoami
    if norm.startswith("/whoami"):
        await tg_send(chat_id,
            f"*whoami*\n• chatid: `{chat_id}`\n• admin: {'true' if _is_admin(chat_id) else 'false'}"
        )
        return {"ok": True}

    # === /licenca nova <dias> [email]
    if norm.startswith("/licenca") and "nova" in norm:
        if not _is_admin(chat_id):
            await tg_send(chat_id, "❌ Você não é admin.")
            return {"ok": True}

        parts = text.split()
        # formatos aceitos:
        # /licenca nova 30
        # /licenca nova 30 email@dominio.com
        days = None
        email = None
        for p in parts[2:]:
            if p.isdigit():
                days = int(p)
            elif "@" in p and "." in p:
                email = p

        if not days:
            await tg_send(chat_id, "Use: `/licenca nova <dias> [email]`")
            return {"ok": True}

        # Gera key e atualiza Sheets
        key = _gen_license()
        try:
            ok, msg = sheets_upsert_license(key, days, email)
            if ok:
                await tg_send(chat_id,
                    "🔑 *Licença criada/atualizada*\n"
                    f"*Chave:* `{key}`\n"
                    f"*Validade (dias):* {days}\n"
                    f"*Email:* {email or '-'}\n\n{msg}"
                )
            else:
                await tg_send(chat_id, f"❌ Erro ao criar licença: {msg}")
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao criar licença: {e}")
        return {"ok": True}

    # Fallback
    await tg_send(chat_id, "❗ Comando não reconhecido.")
    return {"ok": True}
