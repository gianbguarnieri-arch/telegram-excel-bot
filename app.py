import os
import re
import json
import sqlite3
import secrets
import string
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

import requests
import httpx
import msal
from fastapi import FastAPI, Request

# =========================================================
# FastAPI
# =========================================================
app = FastAPI()

# =========================================================
# ENVs (Telegram + Graph app-only)
# =========================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# === Excel destino ===
# Forma 1 (global): use EXCEL_PATH (funciona agora)
#   - exemplo por caminho: /users/.../drive/root:/Planilhas/Lancamentos.xlsx
#   - exemplo por ID:      /users/.../drive/items/01ABCD...
EXCEL_PATH = os.getenv("EXCEL_PATH")  # fallback global se não houver arquivo específico por cliente
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")

# (Opcional) Estrutura por Drive/Item (para quem já usa IDs em site/SharePoint)
DRIVE_ID = os.getenv("DRIVE_ID")  # ex.: b!_GPz2s5...
# item_id por cliente será salvo no SQLite (clients.item_id)

# =========================================================
# [LICENÇAS] ENVs / DB
# =========================================================
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")  # seu chat id (número)
SQLITE_PATH = os.getenv("SQLITE_PATH", "/data/db.sqlite")  # use um Disk no Render pra persistir
LICENSE_ENFORCE = os.getenv("LICENSE_ENFORCE", "1") == "1"  # exige licença no /start e /add

def _db():
    return sqlite3.connect(SQLITE_PATH)

def licenses_db_init():
    con = _db()
    con.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        license_key TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'active',  -- active | revoked | expired
        max_files INTEGER NOT NULL DEFAULT 1,
        expires_at TEXT,                        -- ISO8601 ou NULL (vitalícia)
        notes TEXT
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS clients (
        chat_id TEXT PRIMARY KEY,
        license_key TEXT,
        file_scope TEXT,        -- 'drive' (seu SharePoint) | 'me' (futuro per-user)
        drive_id TEXT,          -- se 'drive': DRIVE_ID; se 'me': vazio (futuro)
        item_id TEXT,           -- item da planilha do cliente
        created_at TEXT,
        last_seen_at TEXT,
        FOREIGN KEY (license_key) REFERENCES licenses(license_key)
    )""")
    con.execute("""
    CREATE TABLE IF NOT EXISTS usage (
        chat_id TEXT,
        event TEXT,
        ts TEXT
    )""")
    con.commit()
    con.close()

licenses_db_init()

def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def record_usage(chat_id, event):
    con = _db()
    con.execute("INSERT INTO usage(chat_id, event, ts) VALUES(?,?,?)",
                (str(chat_id), event, _now_iso()))
    con.commit(); con.close()

def _gen_key(prefix="GF"):
    alphabet = string.ascii_uppercase + string.digits
    part = lambda n: "".join(secrets.choice(alphabet) for _ in range(n))
    return f"{prefix}-{part(4)}-{part(4)}"

def create_license(days: int|None = 30, max_files: int = 1, notes: str|None=None):
    key = _gen_key()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat(timespec="seconds") if days else None
    con = _db()
    con.execute("INSERT INTO licenses(license_key,status,max_files,expires_at,notes) VALUES(?,?,?,?,?)",
                (key, "active", max_files, expires_at, notes))
    con.commit(); con.close()
    return key, expires_at

def get_license(license_key: str):
    con = _db()
    cur = con.execute("SELECT license_key,status,max_files,expires_at,notes FROM licenses WHERE license_key=?",
                      (license_key,))
    row = cur.fetchone()
    con.close()
    if not row: return None
    return {"license_key": row[0], "status": row[1], "max_files": row[2], "expires_at": row[3], "notes": row[4]}

def is_license_valid(lic: dict):
    if not lic: return False, "Licença não encontrada."
    if lic["status"] != "active": return False, "Licença não está ativa."
    if lic["expires_at"]:
        try:
            if datetime.now(timezone.utc) > datetime.fromisoformat(lic["expires_at"]):
                return False, "Licença expirada."
        except Exception:
            return False, "Validade da licença inválida."
    return True, None

def bind_license_to_chat(chat_id: str, license_key: str):
    con = _db()
    # bloqueia reuso em outro chat
    cur = con.execute("SELECT chat_id FROM clients WHERE license_key=? AND chat_id<>? LIMIT 1",
                      (license_key, str(chat_id)))
    conflict = cur.fetchone()
    if conflict:
        con.close()
        return False, "Essa licença já foi usada por outro Telegram."

    con.execute("""
        INSERT INTO clients(chat_id, license_key, created_at, last_seen_at)
        VALUES(?,?,?,?,)
    """.replace("?,?,?,?,", "?,?,?,?"),
        (str(chat_id), license_key, _now_iso(), _now_iso())
    )
    # upsert se já existia
    con.execute("""
        UPDATE clients SET license_key=?, last_seen_at=? WHERE chat_id=?
    """, (license_key, _now_iso(), str(chat_id)))
    con.commit(); con.close()
    return True, None

def get_client(chat_id: str):
    con = _db()
    cur = con.execute("""SELECT chat_id, license_key, file_scope, drive_id, item_id, created_at, last_seen_at
                         FROM clients WHERE chat_id=?""", (str(chat_id),))
    row = cur.fetchone()
    con.close()
    if not row: return None
    return {"chat_id": row[0], "license_key": row[1], "file_scope": row[2], "drive_id": row[3],
            "item_id": row[4], "created_at": row[5], "last_seen_at": row[6]}

def set_client_file(chat_id: str, file_scope: str, drive_id: Optional[str], item_id: str):
    con = _db()
    con.execute("""UPDATE clients SET file_scope=?, drive_id=?, item_id=?, last_seen_at=? WHERE chat_id=?""",
                (file_scope, drive_id, item_id, _now_iso(), str(chat_id)))
    con.commit(); con.close()

def require_active_license(chat_id: str):
    cli = get_client(chat_id)
    if not cli:
        return False, "Para usar o bot você precisa **ativar sua licença**. Envie /start SEU-CÓDIGO (ex.: /start GF-ABCD-1234)."
    lic = get_license(cli["license_key"]) if cli["license_key"] else None
    ok, err = is_license_valid(lic)
    if not ok:
        return False, f"Licença inválida: {err}\nFale com o suporte para renovar/ativar."
    return True, None

# =========================================================
# Telegram helper
# =========================================================
async def tg_send(chat_id, text):
    async with httpx.AsyncClient(timeout=12) as client:
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )

# =========================================================
# MSAL (app-only) + Graph helpers
# =========================================================
SCOPE = ["https://graph.microsoft.com/.default"]

def msal_token():
    app_msal = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    )
    result = app_msal.acquire_token_silent(SCOPE, account=None)
    if not result:
        result = app_msal.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise RuntimeError(f"MSAL error: {result}")
    return result["access_token"]

def _build_workbook_rows_add_url(excel_path: str) -> str:
    # aceita /drive/items/{id} OU /drive/root:/...xlsx
    if "/drive/items/" in excel_path:
        # por ID (sem ':')
        return (
            f"{GRAPH_BASE}{excel_path}"
            f"/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
        )
    else:
        # por caminho (com ':')
        return (
            f"{GRAPH_BASE}{excel_path}"
            f":/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
        )

def excel_add_row(values: List):
    """Insere uma linha (8 colunas) na tabela 'Lancamentos'."""
    if len(values) != 8:
        raise RuntimeError(f"Esperava 8 colunas, recebi {len(values)}.")
    # escolhe o caminho do arquivo:
    excel_path = excel_path_for_chat(values[-1] if False else None)  # não usado; manter assinatura
    # acima, mantemos o EXCEL_PATH global por simplicidade:
    if not EXCEL_PATH:
        raise RuntimeError("Caminho do Excel não definido (EXCEL_PATH).")

    token = msal_token()
    url = _build_workbook_rows_add_url(EXCEL_PATH)
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json={"values": [values]},
        timeout=25
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()

def excel_path_for_chat(_unused=None):
    """
    Futuro: procurar no SQLite se o cliente tem item_id específico.
    Hoje: usa EXCEL_PATH global (sua planilha).
    """
    if not EXCEL_PATH:
        raise RuntimeError("EXCEL_PATH não definido.")
    return EXCEL_PATH

# =========================================================
# NLP simples (PT-BR) → 8 colunas
# =========================================================
def parse_money(text: str) -> Optional[float]:
    # captura 123,45 / 1.234,56 / 123.45 / 123
    m = re.search(r"(\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+(?:\.\d{2})?)", text)
    if not m: return None
    val = m.group(1).replace(".", "").replace(",", ".")
    try:
        return float(val)
    except:
        return None

def parse_date(text: str) -> Optional[str]:
    # hoje / ontem / dd/mm/aaaa / dd/mm
    t = text.lower()
    today = datetime.now().date()
    if "hoje" in t:
        return today.strftime("%Y-%m-%d")
    if "ontem" in t:
        return (today - timedelta(days=1)).strftime("%Y-%m-%d")
    m = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{4}))?\b", text)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3) or str(today.year)
        try:
            dt = datetime.strptime(f"{d}/{mo}/{y}", "%d/%m/%Y").date()
            return dt.strftime("%Y-%m-%d")
        except:
            return None
    return None

def detect_payment(text: str) -> str:
    t = text.lower()
    # Cartões nomeados
    m = re.search(r"cart[aã]o\s+([a-z0-9 ]+)", t)
    if m:
        brand = m.group(1).strip()
        brand = re.sub(r"\s+", " ", brand).strip()
        return f"💳 cartão {brand}"
    if "pix" in t:
        return "Pix"
    if "dinheiro" in t or "cash" in t:
        return "Dinheiro"
    if "débito" in t or "debito" in t:
        return "Débito"
    if "crédito" in t or "credito" in t:
        return "💳 cartão"
    return "Outros"

def detect_installments(text: str) -> str:
    t = text.lower()
    m = re.search(r"(\d{1,2})x", t)
    if m:
        return f"{m.group(1)}x"
    if "parcelad" in t:
        return "parcelado"
    if "à vista" in t or "a vista" in t or "avista" in t:
        return "à vista"
    # por padrão, à vista
    return "à vista"

CATEGORIES = {
    "Restaurante": ["restaurante", "almoço", "jantar", "lanche", "pizza", "hamburg", "sushi"],
    "Mercado": ["mercado", "supermercado", "compras de mercado", "rancho", "hortifruti"],
    "Farmácia": ["farmácia", "remédio", "medicamento", "drogaria"],
    "Combustível": ["gasolina", "álcool", "etanol", "diesel", "posto", "combustível"],
    "Ifood": ["ifood", "i-food"],
    "Passeio em família": ["passeio", "parque", "cinema", "lazer"],
    "Viagem": ["hotel", "passagem", "viagem", "airbnb"],
    "Assinatura": ["netflix", "amazon", "disney", "spotify", "premiere"],
    "Aluguel": ["aluguel", "condomínio"],
    "Água": ["água", "sabesp"],
    "Energia": ["energia", "luz", "enel", "cpfl", "cemig"],
    "Internet": ["internet", "banda larga", "fibra", "vivo", "claro", "oi"],
    "Plano de Saúde": ["plano de saúde", "unimed", "amil", "bradesco saúde", "hapvida"],
    "Escola": ["escola", "mensalidade", "faculdade", "curso"],
    "Imposto": ["iptu", "ipva"],
    "Financiamento": ["financiamento", "parcela do carro", "parcela da casa"],
}

def map_group(category: str) -> str:
    if category in ["Aluguel","Água","Energia","Internet","Plano de Saúde","Escola","Assinatura"]:
        return "Gastos Fixos"
    if category in ["Imposto","Financiamento","Empréstimo"]:
        return "Despesas Temporárias"
    if category in ["Mercado","Farmácia","Combustível","Passeio em família","Ifood","Viagem","Restaurante"]:
        return "Gastos Variáveis"
    if category in ["Salário","Vale","Renda Extra 1","Renda Extra 2","Pró labore"]:
        return "Ganhos"
    if category in ["Renda Fixa","Renda Variável","Fundos imobiliários"]:
        return "Investimento"
    if category in ["Trocar de carro","Viagem pra Disney"]:
        return "Reserva"
    return "Gastos Variáveis"

def detect_category_and_desc(text: str) -> Tuple[str, Optional[str]]:
    t = text.lower()
    for cat, kws in CATEGORIES.items():
        for kw in kws:
            if kw in t:
                # descrição opcional: tenta pegar algo simples após verbo
                m = re.search(r"(comprei|paguei|gastei)\s+(.*?)(?:\s+na\s+|\s+no\s+|\s+via\s+|$)", t)
                desc = None
                if m:
                    raw = m.group(2)
                    # tira preço/data das sobras
                    raw = re.sub(r"\d{1,3}(?:\.\d{3})*(?:,\d{2})|\d+(?:\.\d{2})?", "", raw)
                    raw = re.sub(r"\b(hoje|ontem|\d{1,2}/\d{1,2}(?:/\d{4})?)\b", "", raw)
                    raw = raw.strip(" .,-")
                    if raw and len(raw) < 60:
                        desc = raw
                return cat, (desc if desc else None)
    # fallback
    return "Outros", None

def parse_natural(text: str) -> Tuple[Optional[List], Optional[str]]:
    """
    Retorna valores (8 colunas) ou erro:
    [DataISO, Tipo, Grupo, Categoria, Descrição, Valor, FormaPgto, CondiçãoPgto]
    """
    valor = parse_money(text)
    if valor is None:
        return None, "Não achei o valor. Ex.: 45,90"

    data_iso = parse_date(text) or datetime.now().strftime("%Y-%m-%d")
    forma = detect_payment(text)
    cond = detect_installments(text)
    cat, desc = detect_category_and_desc(text)

    # Tipo: se contiver 'ganhei', 'recebi' => Entrada; senão Saída
    tipo = "Entrada" if re.search(r"\b(ganhei|recebi|sal[aá]rio|renda)\b", text.lower()) else "Saída"
    grupo = map_group(cat)

    return [data_iso, tipo, grupo, cat, (desc or ""), float(valor), forma, cond], None

# =========================================================
# Routes
# =========================================================
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/diag")
def diag():
    # diagnóstico simples de envs
    envs = {
        "TENANT_ID": bool(TENANT_ID),
        "CLIENT_ID": bool(CLIENT_ID),
        "CLIENT_SECRET": bool(CLIENT_SECRET),
        "TELEGRAM_TOKEN": bool(TELEGRAM_TOKEN),
        "EXCEL_PATH": bool(EXCEL_PATH),
        "WORKSHEET_NAME": WORKSHEET_NAME,
        "TABLE_NAME": TABLE_NAME,
        "SQLITE_PATH": SQLITE_PATH,
        "LICENSE_ENFORCE": LICENSE_ENFORCE,
    }
    # testes básicos
    checks = {"graph_token": False, "table_ready": False}
    try:
        tok = msal_token()
        checks["graph_token"] = bool(tok)
        if EXCEL_PATH:
            url = _build_workbook_rows_add_url(EXCEL_PATH).replace("/rows/add", "")
            r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=15)
            checks["table_ready"] = r.status_code < 300
    except Exception:
        pass
    return {"envs": envs, "checks": checks}

@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    body = await req.json()
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    # ===== [ADMIN] comandos de licença =====
    if ADMIN_TELEGRAM_ID and str(chat_id) == str(ADMIN_TELEGRAM_ID):
        low = text.lower()
        if low.startswith("/licenca nova"):
            try:
                parts = text.split()
                days = int(parts[2]) if len(parts) >= 3 else 30
            except:
                days = 30
            key, exp = create_license(days=None if days == 0 else days, max_files=1)
            msg = f"🔑 Licença criada: {key}\nValidade: {'vitalícia' if not exp else exp}"
            await tg_send(chat_id, msg); return {"ok": True}

        if low.startswith("/licenca revogar"):
            parts = text.split()
            if len(parts) >= 3:
                key = parts[2].strip()
                con = _db()
                con.execute("UPDATE licenses SET status='revoked' WHERE license_key=?", (key,))
                con.commit(); con.close()
                await tg_send(chat_id, f"♻️ Licença revogada: {key}")
            else:
                await tg_send(chat_id, "Uso: /licenca revogar CHAVE")
            return {"ok": True}

        if low.startswith("/licenca info"):
            parts = text.split()
            if len(parts) >= 3:
                key = parts[2].strip()
                lic = get_license(key)
                await tg_send(chat_id, f"ℹ️ {lic if lic else 'Não encontrei.'}")
            else:
                await tg_send(chat_id, "Uso: /licenca info CHAVE")
            return {"ok": True}

    # ===== /start (com token de licença) =====
    if text.lower().startswith("/start"):
        record_usage(chat_id, "start")
        parts = text.split(" ", 1)
        token = parts[1].strip() if len(parts) > 1 else None

        if LICENSE_ENFORCE and not token:
            await tg_send(chat_id,
                "Bem-vindo! Para ativar, envie /start SEU-CÓDIGO.\nEx.: /start GF-ABCD-1234")
            return {"ok": True}

        if token:
            lic = get_license(token)
            ok, err = is_license_valid(lic)
            if not ok:
                await tg_send(chat_id, f"❌ Licença inválida: {err}")
                return {"ok": True}

            ok2, err2 = bind_license_to_chat(chat_id, token)
            if not ok2:
                await tg_send(chat_id, f"❌ {err2}")
                return {"ok": True}

            await tg_send(chat_id, "✅ Licença ativada com sucesso!")

        # sua mensagem de boas-vindas
        reply = (
            "Olá! Pode me contar seus gastos/recebimentos em linguagem natural.\n"
            "Exemplos:\n"
            "• gastei 45,90 no mercado via cartão hoje\n"
            "• comprei remédio 34 na farmácia via pix\n"
            "• ganhei 800 de salário\n"
            "Se preferir: /add 07/10/2025;Compra;Mercado;Almoço;45,90;Cartão"
        )
        await tg_send(chat_id, reply)
        return {"ok": True}

    # ===== exige licença para qualquer uso (se habilitado) =====
    if LICENSE_ENFORCE:
        ok, msg = require_active_license(chat_id)
        if not ok:
            await tg_send(chat_id, f"❗ {msg}")
            return {"ok": True}

    # ===== /add DD/MM/AAAA;Tipo;Categoria;Descricao;Valor;FormaPagamento =====
    if text.lower().startswith("/add"):
        m = re.match(r"^/add\s+(.+)$", text, flags=re.I)
        if not m:
            await tg_send(chat_id, "Formato: /add DD/MM/AAAA;Tipo;Categoria;Descricao;Valor;FormaPagamento")
            return {"ok": True}
        parts = [p.strip() for p in m.group(1).split(";")]
        if len(parts) != 6:
            await tg_send(chat_id, "Faltam campos. Use 6 campos separados por ;")
            return {"ok": True}

        data_br, tipo, categoria, descricao, valor_str, forma = parts
        try:
            dt = datetime.strptime(data_br, "%d/%m/%Y").date()
            data_iso = dt.strftime("%Y-%m-%d")
        except:
            await tg_send(chat_id, "Data inválida. Use DD/MM/AAAA.")
            return {"ok": True}

        valor = None
        try:
            valor = float(valor_str.replace(".", "").replace(",", "."))
        except:
            await tg_send(chat_id, "Valor inválido. Ex.: 123,45")
            return {"ok": True}

        # inferir grupo
        grupo = map_group(categoria)
        cond = "à vista"
        row = [data_iso, tipo, grupo, categoria, descricao, valor, forma, cond]

        try:
            excel_add_row(row)
            await tg_send(chat_id, "✅ Lançado!")
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro: {e}")
        return {"ok": True}

    # ===== NLP livre =====
    row, err = parse_natural(text)
    if err:
        await tg_send(chat_id, f"❗ {err}")
        return {"ok": True}

    try:
        excel_add_row(row)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro: {e}")

    return {"ok": True}
