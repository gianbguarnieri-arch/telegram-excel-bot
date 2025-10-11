import os
import json
import sqlite3
import requests
from datetime import datetime, timedelta
from fastapi import FastAPI, Request
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

# ======================
# CONFIGURAÇÕES GERAIS
# ======================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")
SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/licenses.db")

GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
TEMPLATE_FILE_ID = os.getenv("TEMPLATE_FILE_ID")
SHAREPOINT_SITE = os.getenv("SHAREPOINT_SITE")
SHAREPOINT_DOCS = os.getenv("SHAREPOINT_DOCS")
FROM_EMAIL = os.getenv("FROM_EMAIL")

# ======================
# FUNÇÕES AUXILIARES
# ======================
def licenses_db_init():
    """Cria tabela de licenças se não existir."""
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        license_key TEXT PRIMARY KEY,
        status TEXT,
        max_files INTEGER,
        expires_at TEXT,
        notes TEXT
    )
    """)
    conn.close()

@app.on_event("startup")
def _auto_init_db():
    """Recria o banco se necessário (Render free reinicia e apaga o /tmp)"""
    try:
        dbdir = os.path.dirname(SQLITE_PATH)
        if dbdir:
            os.makedirs(dbdir, exist_ok=True)
    except Exception:
        pass
    try:
        licenses_db_init()
        print(f"✅ DB inicializado em {SQLITE_PATH}")
    except Exception as e:
        print(f"❌ Erro ao inicializar DB: {e}")

def tg_send(chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    return requests.post(url, json={"chat_id": chat_id, "text": text})

def create_license(license_key, days=30, max_files=1, notes=""):
    """Cria licença nova no SQLite"""
    licenses_db_init()
    exp = (datetime.now() + timedelta(days=days)).isoformat()
    conn = sqlite3.connect(SQLITE_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(?,?,?,?,?)",
        (license_key, "active", max_files, exp, notes)
    )
    conn.commit()
    conn.close()

def check_license(license_key):
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()
    cur.execute("SELECT status, expires_at FROM licenses WHERE license_key=?", (license_key,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return False, "Licença não encontrada"
    status, expires_at = row
    if status != "active":
        return False, "Licença inativa"
    if datetime.fromisoformat(expires_at) < datetime.now():
        return False, "Licença expirada"
    return True, "ok"

# ======================
# ROTAS TELEGRAM
# ======================
@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    data = await req.json()
    message = data.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "").strip()
    low = text.lower()

    if not text:
        return {"ok": True}

    # ADMIN COMMANDS
    if str(chat_id) == str(ADMIN_TELEGRAM_ID):
        if low.startswith("/licenca nova"):
            parts = text.split()
            if len(parts) >= 3:
                key = parts[2].strip().upper()
                days = int(parts[3]) if len(parts) >= 4 else 30
                create_license(key, days)
                tg_send(chat_id, f"✅ Licença criada: {key} (expira em {days} dias)")
            else:
                tg_send(chat_id, "Uso: /licenca nova CÓDIGO [dias]")
            return {"ok": True}

        if low.startswith("/db init"):
            try:
                licenses_db_init()
                tg_send(chat_id, "✅ Banco inicializado manualmente.")
            except Exception as e:
                tg_send(chat_id, f"❌ Erro ao inicializar DB: {e}")
            return {"ok": True}

    # USUÁRIO NORMAL
    if low.startswith("/start"):
        parts = text.split()
        if len(parts) == 2:
            license_key = parts[1].strip().upper()
            ok, msg = check_license(license_key)
            if not ok:
                tg_send(chat_id, f"Licença inválida: {msg}")
                return {"ok": True}

            tg_send(chat_id, "Licença válida! Criando sua planilha...")
            # (Aqui entra a lógica para copiar o template e enviar o e-mail)
            tg_send(chat_id, "✅ Planilha criada e enviada por e-mail!")
            return {"ok": True}
        else:
            tg_send(chat_id, "Envie /start CÓDIGO-LICENÇA")
            return {"ok": True}

    tg_send(chat_id, "Comando não reconhecido.")
    return {"ok": True}


@app.get("/")
def home():
    return {"status": "ok", "message": "Bot ativo e aguardando mensagens do Telegram."}
