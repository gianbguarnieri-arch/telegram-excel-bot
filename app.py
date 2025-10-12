import os
import re
import json
import sqlite3
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import requests
import httpx
from fastapi import FastAPI, Request, Header

# Google APIs
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =========================
# FASTAPI + ENV
# =========================
app = FastAPI()

# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET")

# SQLite (Render Free => /tmp)
SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/db.sqlite")

# Google Service Account (um dos dois):
# 1) GOOGLE_SA_JSON = conteúdo JSON (inteiro) da chave; OU
# 2) GOOGLE_SA_FILE = caminho do arquivo (ex.: /opt/render/project/src/sa.json)
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")
GOOGLE_SA_FILE = os.getenv("GOOGLE_SA_FILE")

# Google Drive/Sheets
GS_TEMPLATE_ID = os.getenv("GS_TEMPLATE_ID")       # ID da planilha modelo (no Drive)
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID") # ID da pasta de destino onde ficarão as cópias

# Compartilhamento do Google Drive
# Roles: writer | commenter | reader
SHARE_LINK_ROLE = os.getenv("SHARE_LINK_ROLE", "writer")

# Excel/Sheets — usamos a primeira aba, mas se quiser uma específica:
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME")  # se não setar, usa a 1ª aba

# =========================
# ESTADO EFÊMERO DE CONVERSA
# =========================
# chat_id -> { step: "ask_license"|"ask_email", "license_key": str }
pending: Dict[str, Dict] = {}

def reset_pending(chat_id: str):
    pending.pop(str(chat_id), None)

# =========================
# BANCO
# =========================
def _db():
    return sqlite3.connect(SQLITE_PATH)

def db_init():
    con = _db()
    con.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        license_key TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        expires_at TEXT
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        chat_id TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        sheet_id TEXT,
        created_at TEXT,
        last_seen_at TEXT
    )""")
    con.commit(); con.close()

@app.on_event("startup")
def _auto_init():
    try:
        os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
    except Exception:
        pass
    db_init()
    google_init()
    print(f"✅ DB pronto em {SQLITE_PATH}")

# =========================
# GOOGLE AUTH / HELPERS
# =========================
SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
_gs_creds = None
_gs_client = None
_drive = None

def google_init():
    """Inicializa credenciais do Google a partir de GOOGLE_SA_JSON ou GOOGLE_SA_FILE."""
    global _gs_creds, _gs_client, _drive

    if GOOGLE_SA_JSON:
        # salva em arquivo temporário para bibliotecas que esperam arquivo
        sa_path = "/tmp/sa.json"
        try:
            # valida JSON
            data = json.loads(GOOGLE_SA_JSON)
            with open(sa_path, "w", encoding="utf-8") as f:
                json.dump(data, f)
        except Exception as e:
            raise RuntimeError(f"GOOGLE_SA_JSON inválido: {e}")
        sa_file = sa_path
    else:
        if not GOOGLE_SA_FILE:
            raise RuntimeError("Configure GOOGLE_SA_JSON (conteúdo) ou GOOGLE_SA_FILE (caminho).")
        sa_file = GOOGLE_SA_FILE

    _gs_creds = Credentials.from_service_account_file(sa_file, scopes=SCOPES)
    _gs_client = gspread.authorize(_gs_creds)
    _drive = build("drive", "v3", credentials=_gs_creds)

def gs_client():
    if _gs_client is None:
        google_init()
    return _gs_client

def drive_service():
    if _drive is None:
        google_init()
    return _drive

def drive_search_by_name_in_folder(name: str, folder_id: str) -> Optional[str]:
    """Retorna o ID do primeiro arquivo com nome exato dentro da pasta."""
    try:
safe_name = name.replace("'", "\\'")
q = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
        resp = drive_service().files().list(q=q, spaces="drive", fields="files(id, name)", pageSize=1).execute()
        files = resp.get("files", [])
        if files:
            return files[0]["id"]
        return None
    except HttpError as e:
        raise RuntimeError(f"Drive search error: {e}")

def drive_copy_file(template_id: str, new_name: str, dest_folder_id: str) -> str:
    """Copia um arquivo do Drive para a pasta destino e retorna o ID da cópia."""
    meta = {"name": new_name, "parents": [dest_folder_id]}
    copied = drive_service().files().copy(fileId=template_id, body=meta, fields="id,name").execute()
    return copied["id"]

def drive_set_anyone_permission(file_id: str, role: str = "writer"):
    """Cria permissão 'qualquer pessoa com o link' com role desejada."""
    if role not in ("writer", "commenter", "reader"):
        role = "writer"
    perm = {
        "type": "anyone",
        "role": role,
        "allowFileDiscovery": False
    }
    drive_service().permissions().create(fileId=file_id, body=perm).execute()

def sheets_open_by_id(sheet_id: str):
    return gs_client().open_by_key(sheet_id)

def sheets_append_row(sheet_id: str, values: List):
    sh = sheets_open_by_id(sheet_id)
    if WORKSHEET_NAME:
        ws = sh.worksheet(WORKSHEET_NAME)
    else:
        ws = sh.get_worksheet(0)
    # gspread: append_row usa USER_ENTERED por padrão se value_input_option não for informado
    ws.append_row(values, value_input_option="USER_ENTERED")

# =========================
# CLIENTS / LICENSES
# =========================
def upsert_client(chat_id: str, email: str, sheet_id: Optional[str]):
    con = _db()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    con.execute("INSERT OR IGNORE INTO clients(chat_id, email, created_at, last_seen_at) VALUES(?,?,?,?)",
                (chat_id, email.lower().strip(), now, now))
    con.execute("UPDATE clients SET email=?, sheet_id=?, last_seen_at=? WHERE chat_id=?",
                (email.lower().strip(), sheet_id, now, chat_id))
    con.commit(); con.close()

def get_client(chat_id: str):
    con = _db()
    cur = con.execute("SELECT chat_id,email,sheet_id,created_at,last_seen_at FROM clients WHERE chat_id=?",
                      (chat_id,))
    row = cur.fetchone(); con.close()
    if not row: return None
    return {"chat_id": row[0], "email": row[1], "sheet_id": row[2], "created_at": row[3], "last_seen_at": row[4]}

def create_license(code: str, email: str, days: int = 7):
    exp = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds")
    con = _db()
    con.execute("""
        INSERT OR REPLACE INTO licenses(license_key, email, status, expires_at)
        VALUES (?, ?, 'active', ?)
    """, (code.upper().strip(), email.lower().strip(), exp))
    con.commit(); con.close()

def get_license(code: str):
    con = _db()
    cur = con.execute("SELECT license_key,email,status,expires_at FROM licenses WHERE license_key=?",
                      (code.upper().strip(),))
    row = cur.fetchone(); con.close()
    if not row: return None
    return {"license_key": row[0], "email": row[1], "status": row[2], "expires_at": row[3]}

def is_license_valid_for_email(code: str, email: str):
    lic = get_license(code)
    if not lic: return False, "Licença não encontrada."
    if lic["status"] != "active": return False, "Licença inativa."
    if lic["email"].lower().strip() != email.lower().strip():
        return False, "E-mail não corresponde à licença."
    if lic["expires_at"]:
        try:
            if datetime.now(timezone.utc) > datetime.fromisoformat(lic["expires_at"]):
                return False, "Licença expirada."
        except Exception:
            return False, "Validade inválida."
    return True, None

# =========================
# TELEGRAM
# =========================
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
        )

# =========================
# PROVISIONAMENTO (Google)
# =========================
async def provision_for_client(chat_id: str, email: str) -> Tuple[bool, Optional[str], Optional[str]]:
    safe_email = re.sub(r"[^A-Za-z0-9._@+-]", "_", email.strip())
    file_name = f"Lancamentos - {safe_email}"

    # 1) Reusar se já existir com mesmo nome na pasta
    try:
        existing_id = drive_search_by_name_in_folder(f"{file_name}", GS_DEST_FOLDER_ID)
        if existing_id:
            # garantir permissões "anyone"
            try:
                drive_set_anyone_permission(existing_id, SHARE_LINK_ROLE)
            except Exception:
                pass
            upsert_client(chat_id, email, existing_id)
            share_url = f"https://docs.google.com/spreadsheets/d/{existing_id}/edit?usp=sharing"
            return True, None, share_url
    except Exception:
        pass

    # 2) Copiar template
    try:
        new_id = drive_copy_file(GS_TEMPLATE_ID, file_name, GS_DEST_FOLDER_ID)
    except HttpError as e:
        return False, f"Falha ao copiar template no Google Drive: {e}", None
    except Exception as e:
        return False, f"Falha ao copiar template: {e}", None

    # 3) Definir link "qualquer pessoa com link"
    try:
        drive_set_anyone_permission(new_id, SHARE_LINK_ROLE)
    except Exception as e:
        # Não bloqueia — ainda assim dá o link (pode exigir login)
        pass

    upsert_client(chat_id, email, new_id)
    share_url = f"https://docs.google.com/spreadsheets/d/{new_id}/edit?usp=sharing"
    return True, None, share_url

# =========================
# NLP simples + lançamento
# =========================
def parse_money(text: str) -> Optional[float]:
    m = re.search(r"(\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+(?:\.\d{2})?)", text)
    if not m: return None
    val = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(val)
    except:
        return None

def parse_date(text: str) -> Optional[str]:
    t = text.lower()
    today = datetime.now().date()
    if "hoje" in t: return today.strftime("%Y-%m-%d")
    if "ontem" in t: return (today - timedelta(days=1)).strftime("%Y-%m-%d")
    m = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{4}))?\b", text)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3) or str(today.year)
        try:
            dt = datetime.strptime(f"{d}/{mo}/{y}", "%d/%m/%Y").date()
            return dt.strftime("%Y-%m-%d")
        except: return None
    return None

def detect_payment(text: str) -> str:
    t = text.lower()
    if "pix" in t: return "Pix"
    if "dinheiro" in t or "cash" in t: return "Dinheiro"
    if "débito" in t or "debito" in t: return "Débito"
    if "crédito" in t or "credito" in t: return "💳 cartão"
    return "Outros"

def detect_installments(text: str) -> str:
    t = text.lower()
    m = re.search(r"(\d{1,2})x", t)
    if m: return f"{m.group(1)}x"
    if "parcelad" in t: return "parcelado"
    if "à vista" in t or "a vista" in t or "avista" in t: return "à vista"
    return "à vista"

CATEGORIES = {
    "Restaurante": ["restaurante", "almoço", "jantar", "lanche", "pizza", "hamburg", "sushi"],
    "Mercado": ["mercado", "supermercado", "rancho", "hortifruti"],
    "Farmácia": ["farmácia", "remédio", "medicamento", "drogaria"],
    "Combustível": ["gasolina", "álcool", "etanol", "diesel", "posto", "combustível"],
    "Ifood": ["ifood", "i-food"],
    "Passeio em família": ["passeio", "parque", "cinema", "lazer"],
    "Viagem": ["hotel", "passagem", "viagem", "airbnb"],
    "Assinatura": ["netflix", "amazon", "disney", "spotify", "premiere"],
    "Aluguel": ["aluguel", "condomínio"], "Água": ["água"], "Energia": ["energia","luz"],
    "Internet": ["internet","fibra"], "Escola": ["escola","mensalidade"], "Imposto": ["iptu","ipva"]
}

def map_group(category: str) -> str:
    if category in ["Aluguel","Água","Energia","Internet","Plano de Saúde","Escola","Assinatura"]: return "Gastos Fixos"
    if category in ["Imposto","Financiamento","Empréstimo"]: return "Despesas Temporárias"
    return "Gastos Variáveis"

def detect_category_and_desc(text: str) -> Tuple[str, Optional[str]]:
    t = text.lower()
    for cat, kws in CATEGORIES.items():
        for kw in kws:
            if kw in t:
                return cat, None
    return "Outros", None

def parse_natural(text: str) -> Tuple[Optional[List], Optional[str]]:
    valor = parse_money(text)
    if valor is None:
        return None, "Não achei o valor. Ex.: 45,90"
    data_iso = parse_date(text) or datetime.now().strftime("%Y-%m-%d")
    forma = detect_payment(text)
    cond = detect_installments(text)
    cat, desc = detect_category_and_desc(text)
    tipo = "Entrada" if re.search(r"\b(ganhei|recebi|sal[aá]rio|renda)\b", text.lower()) else "Saída"
    grupo = map_group(cat)
    # 8 colunas, como você usava:
    return [data_iso, tipo, grupo, cat, (desc or ""), float(valor), forma, cond], None

# =========================
# TELEGRAM WEBHOOK
# =========================
@app.post("/telegram/webhook")
async def telegram_webhook(
    req: Request,
    x_telegram_bot_api_secret_token: Optional[str] = Header(None)
):
    # valida secret do webhook (se configurado)
    if TELEGRAM_WEBHOOK_SECRET and (x_telegram_bot_api_secret_token != TELEGRAM_WEBHOOK_SECRET):
        return {"ok": True}

    body = await req.json()
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()
    low = text.lower()

    if not chat_id or not text:
        return {"ok": True}

    chat_id_str = str(chat_id)

    # ===== ADMIN =====
    if ADMIN_TELEGRAM_ID and chat_id_str == str(ADMIN_TELEGRAM_ID):
        if low.startswith("/licenca nova"):
            parts = text.split()
            # /licenca nova CODIGO EMAIL [DIAS]
            if len(parts) < 4:
                await tg_send(chat_id, "Uso: `/licenca nova CODIGO EMAIL [DIAS]`\nEx.: `/licenca nova GF-ABCD-1234 cliente@gmail.com 7`")
                return {"ok": True}
            code = parts[2].strip().upper()
            email = parts[3].strip()
            days = int(parts[4]) if (len(parts) >= 5 and parts[4].isdigit()) else 7
            try:
                create_license(code, email, days)
                await tg_send(chat_id, f"✅ Licença criada:\n• Chave: `{code}`\n• Email: `{email}`\n• Validade: {days} dia(s)")
            except Exception as e:
                await tg_send(chat_id, f"❌ Erro ao criar licença: {e}")
            return {"ok": True}

        if low.startswith("/db init"):
            try:
                db_init()
                await tg_send(chat_id, f"✅ DB inicializado em `{SQLITE_PATH}`")
            except Exception as e:
                await tg_send(chat_id, f"❌ Erro ao inicializar DB: {e}")
            return {"ok": True}

    # ===== FLUXO AMIGÁVEL =====
    if low.startswith("/start"):
        reset_pending(chat_id_str)
        pending[chat_id_str] = {"step": "ask_license"}
        await tg_send(chat_id, "🔑 *Informe sua licença* (ex.: `GF-ABCD-1234`)\n\nDigite */cancel* para cancelar.")
        return {"ok": True}

    state = pending.get(chat_id_str)
    if state:
        if low.startswith("/cancel"):
            reset_pending(chat_id_str)
            await tg_send(chat_id, "✅ Cancelado. Envie */start* para começar novamente.")
            return {"ok": True}

        if state.get("step") == "ask_license":
            license_key = text.strip().upper()
            lic = get_license(license_key)
            if not lic:
                await tg_send(chat_id, "❌ Licença não encontrada. Tente novamente ou digite */cancel*.")
                return {"ok": True}
            if lic["status"] != "active":
                await tg_send(chat_id, "❌ Licença inativa. Fale com o suporte.")
                return {"ok": True}
            if lic["expires_at"]:
                try:
                    if datetime.now(timezone.utc) > datetime.fromisoformat(lic["expires_at"]):
                        await tg_send(chat_id, "❌ Licença expirada. Fale com o suporte.")
                        return {"ok": True}
                except Exception:
                    await tg_send(chat_id, "❌ Validade da licença inválida. Fale com o suporte.")
                    return {"ok": True}

            pending[chat_id_str] = {"step": "ask_email", "license_key": license_key}
            await tg_send(chat_id, "📧 *Informe seu e-mail* (ex.: `cliente@gmail.com`)")
            return {"ok": True}

        if state.get("step") == "ask_email":
            email = text.strip()
            if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
                await tg_send(chat_id, "❌ E-mail inválido. Tente novamente (ex.: `cliente@gmail.com`) ou digite */cancel*.")
                return {"ok": True}

            license_key = state.get("license_key")
            ok, err = is_license_valid_for_email(license_key, email)
            if not ok:
                await tg_send(chat_id, f"❌ {err}")
                return {"ok": True}

            await tg_send(chat_id, "⏳ Configurando sua planilha…")
            ok2, err2, share = await provision_for_client(chat_id_str, email)
            reset_pending(chat_id_str)

            if not ok2:
                await tg_send(chat_id, f"❌ {err2}")
                return {"ok": True}

            if share:
                await tg_send(chat_id, f"🚀 *Sua planilha foi criada!*\n\nAcesse:\n{share}\n\nAgora é só me mandar mensagens como:\n_\"gastei 45,90 no mercado via pix hoje\"_ que eu lanço pra você 😉")
            else:
                await tg_send(chat_id, "🚀 *Sua planilha foi criada!* (não consegui gerar o link agora).")
            return {"ok": True}

    # ===== LANÇAMENTOS (modo uso) =====
    if not low.startswith("/"):
        cli = get_client(chat_id_str)
        if not cli or not cli.get("sheet_id"):
            await tg_send(chat_id, "Para começar, envie */start* e faça a configuração inicial.")
            return {"ok": True}

        row, perr = parse_natural(text)
        if perr:
            await tg_send(chat_id, f"❗ {perr}\nEx.: _gastei 45,90 no mercado via pix hoje_")
            return {"ok": True}

        try:
            sheets_append_row(cli["sheet_id"], row)
            await tg_send(chat_id, "✅ Lançado!")
        except HttpError as e:
            await tg_send(chat_id, f"❌ Erro ao lançar na planilha (Google): {e}")
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao lançar: {e}")
        return {"ok": True}

    # ===== fallback =====
    await tg_send(chat_id, "Use */start* para configurar ou me envie um lançamento, ex.: _gastei 45,90 no mercado via pix hoje_.")
    return {"ok": True}

@app.get("/")
def health():
    return {"status": "ok"}
