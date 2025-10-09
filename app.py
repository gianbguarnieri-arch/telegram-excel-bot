import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List

import httpx
import msal
import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# -----------------------------------------------------------
# Config
# -----------------------------------------------------------
app = FastAPI()

# Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# Graph / Auth
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

# Template e destino (SharePoint do site)
DRIVE_ID = os.getenv("DRIVE_ID", "").strip()
TEMPLATE_ITEM_ID = os.getenv("TEMPLATE_ITEM_ID", "").strip()          # modelo Lancamentos.xlsx
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID", "").strip()    # pasta Planilhas

# Planilha de Clientes (onde registramos o chat_id -> excel_path)
CLIENTS_TABLE_PATH = os.getenv("CLIENTS_TABLE_PATH", "").strip()      # /drives/{driveId}/items/{itemId}
CLIENTS_WORKSHEET_NAME = os.getenv("CLIENTS_WORKSHEET_NAME", "Plan1")
CLIENTS_TABLE_NAME = os.getenv("CLIENTS_TABLE_NAME", "Clientes")

# Padrões da planilha de lançamentos do cliente
DEFAULT_WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
DEFAULT_TABLE_NAME = os.getenv("TABLE_NAME", "Lançamentos")

DEBUG = os.getenv("DEBUG", "0") == "1"

# -----------------------------------------------------------
# Auth & HTTP helpers
# -----------------------------------------------------------
def msal_token() -> str:
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

def headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {msal_token()}", "Content-Type": "application/json"}

def gget(url: str, **kw) -> requests.Response:
    if DEBUG:
        print("GET", url)
    return requests.get(url, headers=headers(), timeout=30, **kw)

def gpost(url: str, json: Optional[dict] = None, extra_headers: Optional[dict] = None) -> requests.Response:
    h = headers()
    if extra_headers:
        h.update(extra_headers)
    if DEBUG:
        print("POST", url, "json=", json)
    return requests.post(url, headers=h, json=json, timeout=30)

# -----------------------------------------------------------
# Excel helpers (tabelas)
# -----------------------------------------------------------
def excel_add_row_by_item_path(item_path: str, worksheet: str, table: str, values: list) -> dict:
    """
    item_path: '/drives/{driveId}/items/{itemId}'  (SEM '/workbook' e SEM ':')
    """
    if not item_path:
        raise RuntimeError("Caminho do Excel não definido (item_path vazio).")

    url = f"{GRAPH_BASE}{item_path}/workbook/worksheets('{worksheet}')/tables('{table}')/rows/add"
    payload = {"values": [values]}
    r = gpost(url, json=payload)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()

def excel_list_rows(item_path: str, worksheet: str, table: str, top: int = 500) -> List[List[Any]]:
    url = f"{GRAPH_BASE}{item_path}/workbook/worksheets('{worksheet}')/tables('{table}')/rows?$top={top}"
    r = gget(url)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph list rows {r.status_code}: {r.text}")
    data = r.json()
    rows = []
    for it in data.get("value", []):
        rows.append(it.get("values", [[]])[0])
    return rows

# -----------------------------------------------------------
# Cópia do modelo para criar planilha do cliente
# -----------------------------------------------------------
def copy_template_for_user(file_name: str) -> Dict[str, Any]:
    """
    POST /drives/{DRIVE_ID}/items/{TEMPLATE_ITEM_ID}/copy  ->  202 + Location
    Faz poll no Location, retorna metadata do novo item.
    """
    if not (DRIVE_ID and TEMPLATE_ITEM_ID and DEST_FOLDER_ITEM_ID):
        raise RuntimeError("Faltam DRIVE_ID/TEMPLATE_ITEM_ID/DEST_FOLDER_ITEM_ID.")

    copy_url = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{TEMPLATE_ITEM_ID}/copy"
    body = {
        "name": file_name,
        "parentReference": {"driveId": DRIVE_ID, "id": DEST_FOLDER_ITEM_ID},
        "conflictBehavior": "rename",
    }
    r = gpost(copy_url, json=body, extra_headers={"Prefer": "respond-async"})
    if r.status_code not in (202, 201, 200):
        raise RuntimeError(f"Graph POST {r.status_code}: {r.text}")

    location = r.headers.get("Location") or r.headers.get("location")
    if not location:
        # às vezes já retorna o item no body
        try:
            data = r.json()
            if "id" in data:
                return data
        except Exception:
            pass
        raise RuntimeError("Cópia sem Location (monitor).")

    # poll
    for _ in range(30):  # até ~60s
        time.sleep(2)
        rr = requests.get(location, headers=headers(), timeout=30)
        if rr.status_code in (200, 201):
            try:
                data = rr.json()
            except Exception:
                data = {}
            # se vier resourceId
            if "resourceId" in data:
                new_id = data["resourceId"]
                meta = gget(f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{new_id}")
                return meta.json() if meta.status_code == 200 else {"id": new_id}
            # ou já veio id/webUrl
            if "id" in data:
                return data
            return data
        if rr.status_code >= 400:
            raise RuntimeError(f"Monitor error {rr.status_code}: {rr.text}")
    raise RuntimeError("Timeout no monitor da cópia.")

# -----------------------------------------------------------
# Planilha de Clientes: upsert e lookup
# -----------------------------------------------------------
def clients_item_path() -> str:
    """
    Retorna o item_path padrão da planilha Clientes.xlsx:
      /drives/{driveId}/items/{itemId}
    """
    if not CLIENTS_TABLE_PATH:
        raise RuntimeError("CLIENTS_TABLE_PATH não definido.")
    return CLIENTS_TABLE_PATH  # já deve estar no formato /drives/.../items/...

def clients_get_by_chat_id(chat_id: int) -> Optional[Dict[str, Any]]:
    """
    Busca na tabela Clientes (Plan1) uma linha com o chat_id informado.
    Espera colunas: chat_id | nome | excel_path | worksheet_name | table_name | created_at
    """
    rows = excel_list_rows(clients_item_path(), CLIENTS_WORKSHEET_NAME, CLIENTS_TABLE_NAME, top=2000)
    for row in rows:
        # proteger contra linhas curtas
        cols = row + [""] * 6
        cid, nome, excel_path, ws, tbl, created_at = cols[:6]
        # chat_id na planilha fica como texto; comparo string
        if str(cid).strip() == str(chat_id):
            return {
                "chat_id": str(cid).strip(),
                "nome": str(nome).strip(),
                "excel_path": str(excel_path).strip(),
                "worksheet_name": (ws or DEFAULT_WORKSHEET_NAME),
                "table_name": (tbl or DEFAULT_TABLE_NAME),
                "created_at": str(created_at).strip(),
            }
    return None

def clients_add_row(chat_id: int, nome: str, excel_item_id: str, worksheet: str, table: str):
    """
    Insere uma nova linha na tabela Clientes com os dados do cliente.
    excel_item_id -> montamos o item_path do arquivo do cliente
    """
    item_path = f"/drives/{DRIVE_ID}/items/{excel_item_id}"
    created_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    values = [str(chat_id), nome, item_path, worksheet, table, created_at]
    excel_add_row_by_item_path(clients_item_path(), CLIENTS_WORKSHEET_NAME, CLIENTS_TABLE_NAME, values)

def ensure_client_workbook(chat_id: int, nome: str) -> Tuple[str, str, str]:
    """
    Garante que o cliente tem uma planilha própria:
      - Se já existir na Clientes.xlsx, retorna (item_path, worksheet, table)
      - Senão, copia template -> cria registro na Clientes.xlsx -> retorna
    """
    # 1) tentar achar cliente
    found = clients_get_by_chat_id(chat_id)
    if found and found.get("excel_path"):
        return found["excel_path"], found["worksheet_name"], found["table_name"]

    # 2) copiar template para o cliente
    safe_name = (nome or "Cliente").split()[0]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"Planilha_{safe_name}_{ts}.xlsx"
    meta = copy_template_for_user(file_name)
    new_item_id = meta.get("id")
    if not new_item_id:
        raise RuntimeError(f"Falha ao obter id da planilha criada: {meta}")

    # 3) gravar registro na Clientes.xlsx
    clients_add_row(chat_id, safe_name, new_item_id, DEFAULT_WORKSHEET_NAME, DEFAULT_TABLE_NAME)

    # 4) retornar caminho do arquivo do cliente
    item_path = f"/drives/{DRIVE_ID}/items/{new_item_id}"
    return item_path, DEFAULT_WORKSHEET_NAME, DEFAULT_TABLE_NAME

# -----------------------------------------------------------
# NLP simples
# -----------------------------------------------------------
ADD_REGEX = re.compile(r"^/add\s+(.+)$", re.IGNORECASE)
FMT_REGEX = re.compile(
    r"""(?P<data>(\d{1,2}/\d{1,2}/\d{2,4}|hoje|ontem))?
        .*?
        (?P<tipo>(compra|pagamento|recebimento|ganho|gastei|paguei))?
        .*?
        (?P<valor>\d+[.,]?\d*)
        .*?
        (?P<formapag>(pix|dinheiro|debito|débito|credito|crédito|cartao|cartão|boleto|transfer(ê|e)ncia)(\s+\w+)*)?
        .*?
        (?P<parcelas>\d{1,2}x)?
    """,
    re.IGNORECASE | re.VERBOSE,
)

def parse_amount(val_str: str) -> float:
    s = val_str.strip().replace("R$", "").replace(" ", "")
    if "," in s and s.count(",") == 1 and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    return float(s)

def normalize_date(s: Optional[str]) -> str:
    if not s:
        return datetime.now().strftime("%Y-%m-%d")
    s = s.strip().lower()
    if s == "hoje":
        return datetime.now().strftime("%Y-%m-%d")
    if s == "ontem":
        return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")

def guess_group_and_category(desc: str) -> Tuple[str, str]:
    d = (desc or "").lower()
    grupos = {
        "Gastos Fixos": ["aluguel", "água", "agua", "energia", "internet", "plano de saúde", "escola"],
        "Assinatura": ["netflix", "amazon", "disney", "premiere", "spotify"],
        "Gastos Variáveis": ["mercado", "farmácia", "farmacia", "combustível", "combustivel", "passeio", "ifood", "viagem", "restaurante"],
        "Despesas Temporárias": ["financiamento", "iptu", "ipva", "empréstimo", "emprestimo"],
        "Pagamento de Fatura": ["fatura", "cartão", "cartao"],
        "Ganhos": ["salário", "salario", "vale", "renda extra", "pró labore", "pro labore"],
        "Investimento": ["renda fixa", "renda variável", "renda variavel", "fii", "fundos imobiliários", "fundos imobiliarios"],
        "Reserva": ["disney", "trocar de carro", "reserva"],
    }
    for g, keys in grupos.items():
        for k in keys:
            if k in d:
                if g == "Gastos Variáveis" and "restaurante" in d:
                    return g, "Restaurante"
                if g == "Assinatura" and "netflix" in d:
                    return g, "Netflix"
                if "farmácia" in d or "farmacia" in d:
                    return g, "Farmácia"
                if "mercado" in d:
                    return g, "Mercado"
                return g, k.capitalize()
    return "Gastos Variáveis", "Outros"

def parse_free_text(text: str) -> Tuple[List[Any], Optional[str]]:
    m = FMT_REGEX.search(text)
    if not m:
        return [], "Não entendi. Ex.: 'gastei 45,90 no mercado via cartão hoje'."

    data = normalize_date(m.group("data"))
    valor = parse_amount(m.group("valor"))

    tipo = "Saída"
    if m.group("tipo") and m.group("tipo").lower() in ("recebimento", "ganho"):
        tipo = "Entrada"

    forma_bruta = (m.group("formapag") or "").strip()
    forma = ""
    if forma_bruta:
        f = forma_bruta.lower().replace("cartao", "cartão")
        if "crédito" in f or "credito" in f or "cartão" in f:
            forma = "💳 " + f
        else:
            forma = f.capitalize()

    condicao = "À vista" if tipo == "Saída" else ""
    parc = m.group("parcelas")
    if parc:
        condicao = parc  # 12x

    desc = text.strip()
    grupo, categoria = guess_group_and_category(desc)

    values = [data, tipo, grupo, categoria, desc, valor, forma, condicao]
    return values, None

# -----------------------------------------------------------
# Telegram helpers
# -----------------------------------------------------------
async def tg_send(chat_id: int, text: str):
    if not TELEGRAM_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text},
            )
    except Exception as e:
        if DEBUG:
            print("tg_send error:", e)

# -----------------------------------------------------------
# Rotas
# -----------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/diag")
def diag():
    out = {
        "envs": {
            "DRIVE_ID": bool(DRIVE_ID),
            "TEMPLATE_ITEM_ID": bool(TEMPLATE_ITEM_ID),
            "DEST_FOLDER_ITEM_ID": bool(DEST_FOLDER_ITEM_ID),
            "CLIENTS_TABLE_PATH": bool(CLIENTS_TABLE_PATH),
        }
    }
    try:
        if DRIVE_ID and TEMPLATE_ITEM_ID:
            u = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{TEMPLATE_ITEM_ID}"
            r = gget(u); out["template_item"] = {"status": r.status_code, "ok": r.ok}
        if DRIVE_ID and DEST_FOLDER_ITEM_ID:
            u = f"{GRAPH_BASE}/drives/{DRIVE_ID}/items/{DEST_FOLDER_ITEM_ID}"
            r = gget(u); out["dest_folder"] = {"status": r.status_code, "ok": r.ok}
        if CLIENTS_TABLE_PATH:
            u = f"{GRAPH_BASE}{CLIENTS_TABLE_PATH}"
            r = gget(u); out["clients_file"] = {"status": r.status_code, "ok": r.ok}
        return JSONResponse(content=out)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    body = await req.json()
    msg = body.get("message") or {}
    chat = msg.get("chat") or {}
    from_user = msg.get("from") or {}
    chat_id = chat.get("id")
    text = (msg.get("text") or "").strip()
    first_name = (from_user.get("first_name") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    # /start: garante planilha do cliente e cadastra na "Clientes"
    if text.lower().startswith("/start"):
        try:
            _item_path, _ws, _tbl = ensure_client_workbook(chat_id, first_name or "Cliente")
            await tg_send(chat_id,
                "Olá! Pode me contar seus gastos/recebimentos em linguagem natural.\n"
                "Exemplos:\n"
                "• gastei 45,90 no mercado via cartão hoje\n"
                "• comprei remédio 34 na farmácia via pix\n"
                "• ganhei 800 de salário\n"
                "Se preferir: /add 07/10/2025;Compra;Mercado;Almoço;45,90;Cartão"
            )
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao preparar sua planilha: {e}")
        return {"ok": True}

    # /add DD/MM/AAAA;Tipo;Categoria;Descrição;Valor;FormaPagamento
    m = ADD_REGEX.match(text)
    if m:
        parts = [p.strip() for p in m.group(1).split(";")]
        if len(parts) != 6:
            await tg_send(chat_id, "❗ Formato: /add DD/MM/AAAA;Tipo;Categoria;Descrição;Valor;FormaPagamento")
            return {"ok": True}
        data_br, tipo, categoria, descricao, valor_str, forma = parts
        try:
            dt = datetime.strptime(data_br, "%d/%m/%Y")
            data_iso = dt.strftime("%Y-%m-%d")
        except Exception:
            await tg_send(chat_id, "❗ Data inválida. Use DD/MM/AAAA.")
            return {"ok": True}
        try:
            valor = parse_amount(valor_str)
        except Exception:
            await tg_send(chat_id, "❗ Valor inválido. Ex.: 123,45")
            return {"ok": True}
        grupo, _ = guess_group_and_category(descricao)
        condicao = ""
        values = [data_iso, tipo, grupo, categoria or "Outros", descricao, valor, forma, condicao]

        try:
            item_path, ws, tbl = ensure_client_workbook(chat_id, first_name or "Cliente")
            excel_add_row_by_item_path(item_path, ws, tbl, values)
            await tg_send(chat_id, "✅ Lançado!")
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao lançar no Excel: {e}")
        return {"ok": True}

    # Linguagem natural
    values, err = parse_free_text(text)
    if err:
        await tg_send(chat_id, err)
        return {"ok": True}

    try:
        item_path, ws, tbl = ensure_client_workbook(chat_id, first_name or "Cliente")
        excel_add_row_by_item_path(item_path, ws, tbl, values)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro ao lançar no Excel: {e}")

    return {"ok": True}
