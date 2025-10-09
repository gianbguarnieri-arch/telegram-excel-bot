import os
import re
import time
from datetime import datetime
from typing import Dict, Any, Optional

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
TENANT_ID = os.getenv("TENANT_ID")  # ex: 5fe42c67-f15f-4a48-8dbd-faff326ab0d4
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SCOPE = ["https://graph.microsoft.com/.default"]

# Excel padrão (planilha-mãe para lançamento)
# Formatos aceitos:
#   1) /users/<UPN>/drive/items/<ITEM_ID>
#   2) /sites/<host>,<siteId>,<webId>/drive/items/<ITEM_ID>
EXCEL_PATH = os.getenv("EXCEL_PATH", "").strip()
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")
TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos")

# Pasta e arquivo modelo para criação de planilhas de clientes
# Estes trabalham com o "drive" do site/biblioteca onde você criou "Planilhas".
DRIVE_ID = os.getenv("DRIVE_ID", "").strip()                 # b!_GPz2... (o drive da biblioteca)
TEMPLATE_ITEM_ID = os.getenv("TEMPLATE_ITEM_ID", "").strip() # 01GSQYRM... (id do arquivo modelo .xlsx)
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID", "").strip()  # 01GSQYRM... (id da pasta Planilhas)

# Tabela de clientes (opcional) – onde você registra as planilhas de cada cliente
CLIENTS_TABLE_PATH = os.getenv("CLIENTS_TABLE_PATH", "").strip()  # /users/.../drive/items/<ID>
CLIENTS_TABLE_NAME = os.getenv("CLIENTS_TABLE_NAME", "Clientes")
CLIENTS_WORKSHEET_NAME = os.getenv("CLIENTS_WORKSHEET_NAME", "Plan1")

DEBUG = os.getenv("DEBUG", "0") == "1"


# -----------------------------------------------------------
# Utilidades
# -----------------------------------------------------------
def msal_token() -> str:
    """Obtém token app-only (client credentials)."""
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


def g_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {msal_token()}", "Content-Type": "application/json"}


def graph_get(url: str, **kw) -> requests.Response:
    if DEBUG:
        print("GET", url)
    r = requests.get(url, headers=g_headers(), timeout=30, **kw)
    return r


def graph_post(url: str, json: Optional[dict] = None) -> requests.Response:
    if DEBUG:
        print("POST", url, "->", json)
    r = requests.post(url, headers=g_headers(), json=json, timeout=30)
    return r


# -----------------------------------------------------------
# Excel helpers
# -----------------------------------------------------------
def excel_add_row_generic(base_path: str, worksheet: str, table: str, values: list) -> dict:
    """
    Adiciona `values` em uma tabela de Excel (Graph).
    base_path: caminho base do arquivo (sem '/workbook/...').
               Ex.: '/users/<UPN>/drive/items/<ID>'  ou '/sites/<host>,<siteId>,<webId>/drive/items/<ID>'
    """
    if not base_path:
        raise RuntimeError("Caminho do Excel não definido.")

    # Quando é por items/<ID> não usamos ':'
    uses_items = "/items/" in base_path

    if uses_items:
        url = f"https://graph.microsoft.com/v1.0{base_path}/workbook/worksheets('{worksheet}')/tables('{table}')/rows/add"
    else:
        url = f"https://graph.microsoft.com/v1.0{base_path}:/workbook/worksheets('{worksheet}')/tables('{table}')/rows/add"

    payload = {"values": [values]}
    r = graph_post(url, json=payload)
    if r.status_code >= 300:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()


def parse_amount(val_str: str) -> float:
    # aceita "1.234,56" ou "1234.56" etc
    s = val_str.strip().replace("R$", "").replace(" ", "")
    if "," in s and s.count(",") == 1 and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    return float(s)


# -----------------------------------------------------------
# Bot helpers
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


ADD_REGEX = re.compile(r"^/add\s+(.+)$", re.IGNORECASE)
FMT_REGEX = re.compile(
    r"""(?P<data>(\d{1,2}/\d{1,2}/\d{2,4}|hoje|ontem))?    # data opcional
        .*?
        (?P<tipo>(compra|pagamento|recebimento|ganho|gastei|paguei))?  # tipo opcional
        .*?
        (?P<cat>\w[\w\s\-_]+?)?                               # categoria solta
        .*?
        (?P<valor>\d+[.,]?\d*)                                # valor
        .*?
        (?P<formapag>(pix|dinheiro|debito|débito|credito|crédito|cartao|cartão|boleto|transferência|transferencia)
        (\s+\w+)*)?                                           # forma pagto opcional
        .*?
        (?P<parcelas>\d{1,2}x)?                               # ex.: 12x
    """,
    re.IGNORECASE | re.VERBOSE,
)


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


def guess_group_and_category(desc: str) -> (str, str):
    d = desc.lower()

    # grupos declarados pelo usuário
    grupos = {
        "gastos fixos": [
            "aluguel", "água", "agua", "energia", "internet", "plano de saúde", "escola"
        ],
        "assinatura": ["netflix", "amazon", "disney", "premiere", "spotify"],
        "gastos variáveis": [
            "mercado", "farmácia", "farmacia", "combustível", "combustivel", "passeio", "ifood", "viagem", "restaurante"
        ],
        "despesas temporárias": ["financiamento", "iptu", "ipva", "empréstimo", "emprestimo"],
        "pagamento de fatura": ["fatura", "cartão", "cartao"],
        "ganhos": ["salário", "salario", "vale", "renda extra", "pró labore", "pro labore"],
        "investimento": ["renda fixa", "renda variável", "renda variavel", "fii", "fundos imobiliários", "fundos imobiliarios"],
        "reserva": ["reserva", "disney", "trocar de carro"],
    }

    for g, keys in grupos.items():
        for k in keys:
            if k in d:
                # categoria = primeira palavra "relevante" encontrada
                if g == "gastos variáveis" and "restaurante" in d:
                    return g, "Restaurante"
                if g == "assinatura" and "netflix" in d:
                    return g, "Netflix"
                if "farmácia" in d or "farmacia" in d:
                    return g, "Farmácia"
                if "mercado" in d:
                    return g, "Mercado"
                return g, k.capitalize()

    # fallback
    return "Gastos variáveis", "Outros"


def parse_free_text(text: str) -> (list, Optional[str]):
    """
    Retorna:
      values = [DataISO, Tipo(Saída/Entrada), Grupo, Categoria, Descrição, Valor, Forma, Condição]
      err = string ou None
    """
    m = FMT_REGEX.search(text)
    if not m:
        return [], "Não consegui entender. Tente algo como: 'gastei 45,90 no mercado via cartão hoje'."

    data = normalize_date(m.group("data"))
    valor = parse_amount(m.group("valor"))

    # saída/entrada
    tipo = "Saída"
    if m.group("tipo") and m.group("tipo").lower() in ("recebimento", "ganho"):
        tipo = "Entrada"

    # descrição livre (pequena heurística)
    desc = text.strip()
    # forma de pagamento
    forma_bruta = (m.group("formapag") or "").strip()
    forma = ""
    if forma_bruta:
        f = forma_bruta.lower()
        f = f.replace("cartao", "cartão")
        if "crédito" in f or "credito" in f or "cartão" in f:
            forma = "💳 " + f
        else:
            forma = f.capitalize()

    # parcelas
    condicao = ""
    parc = m.group("parcelas")
    if parc:
        condicao = parc  # ex.: '12x'
    else:
        condicao = "À vista" if tipo == "Saída" else ""

    # grupo & categoria
    grupo, categoria = guess_group_and_category(desc)

    values = [data, tipo, grupo, categoria, desc, valor, forma, condicao]
    return values, None


# -----------------------------------------------------------
# Telegram webhook
# -----------------------------------------------------------
@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    body = await req.json()
    message = body.get("message") or {}
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return {"ok": True}

    if text.lower().startswith("/start"):
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

    # /add clássico
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
        grupo, cat_guess = guess_group_and_category(descricao)
        if categoria.lower() == "restaurante":
            cat_final = "Restaurante"
        else:
            cat_final = categoria or cat_guess
        origem = "Telegram"
        condicao = ""  # manual no /add
        values = [data_iso, tipo, grupo, cat_final, descricao, valor, forma, condicao, origem][:8]
        try:
            excel_add_row_generic(EXCEL_PATH, WORKSHEET_NAME, TABLE_NAME, values)
            await tg_send(chat_id, "✅ Lançado!")
        except Exception as e:
            await tg_send(chat_id, f"❌ Erro ao lançar no Excel: {e}")
        return {"ok": True}

    # linguagem natural
    values, err = parse_free_text(text)
    if err:
        await tg_send(chat_id, err)
        return {"ok": True}

    try:
        excel_add_row_generic(EXCEL_PATH, WORKSHEET_NAME, TABLE_NAME, values)
        await tg_send(chat_id, "✅ Lançado!")
    except Exception as e:
        await tg_send(chat_id, f"❌ Erro ao lançar no Excel: {e}")

    return {"ok": True}


# -----------------------------------------------------------
# Diagnóstico e teste de cópia
# -----------------------------------------------------------
@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/diag")
def diag():
    """
    Verifica se as variáveis e os recursos (template/pasta) estão acessíveis.
    Usa /drives/{DRIVE_ID}/items/{ITEM_ID}.
    """
    try:
        envs = {
            "DRIVE_ID_set": bool(DRIVE_ID),
            "TEMPLATE_ITEM_ID_set": bool(TEMPLATE_ITEM_ID),
            "DEST_FOLDER_ITEM_ID_set": bool(DEST_FOLDER_ITEM_ID),
            "EXCEL_PATH_set": bool(EXCEL_PATH),
            "WORKSHEET_NAME": WORKSHEET_NAME,
            "TABLE_NAME": TABLE_NAME,
        }

        results: Dict[str, Any] = {"envs": envs}

        if DRIVE_ID and TEMPLATE_ITEM_ID:
            u1 = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{TEMPLATE_ITEM_ID}"
            r1 = graph_get(u1)
            results["template_item"] = {"status": r1.status_code, "ok": r1.ok, "url": u1}

        if DRIVE_ID and DEST_FOLDER_ITEM_ID:
            u2 = f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{DEST_FOLDER_ITEM_ID}"
            r2 = graph_get(u2)
            results["dest_folder"] = {"status": r2.status_code, "ok": r2.ok, "url": u2}

        return JSONResponse(content=results)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/test-copy")
def test_copy():
    """
    Faz uma cópia de TESTE do template para a pasta de destino (Planilhas).
    Requer: DRIVE_ID, TEMPLATE_ITEM_ID, DEST_FOLDER_ITEM_ID.
    """
    if not (DRIVE_ID and TEMPLATE_ITEM_ID and DEST_FOLDER_ITEM_ID):
        return JSONResponse(
            status_code=400,
            content={"error": "Defina DRIVE_ID, TEMPLATE_ITEM_ID e DEST_FOLDER_ITEM_ID"},
        )

    name = f"Test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    copy_url = (
        f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{TEMPLATE_ITEM_ID}/copy"
    )
    body = {
        "name": name,
        "parentReference": {"driveId": DRIVE_ID, "id": DEST_FOLDER_ITEM_ID},
        "conflictBehavior": "rename",
    }

    r = graph_post(copy_url, json=body)
    if r.status_code not in (202, 201, 200):
        return JSONResponse(
            status_code=r.status_code,
            content={"error": "copy failed", "graph": r.text},
        )

    # Se 202, acompanhar pelo cabeçalho Location
    loc = r.headers.get("Location")
    if not loc:
        # alguns tenants retornam 201 com body
        try:
            data = r.json()
        except Exception:
            data = {}
        return JSONResponse(content={"status": r.status_code, "data": data})

    # poll simples
    for _ in range(18):  # ~18 * 2s = ~36s
        time.sleep(2)
        rr = requests.get(loc, headers=g_headers(), timeout=30)
        if rr.status_code in (200, 201):
            try:
                data = rr.json()
            except Exception:
                data = {}
            return JSONResponse(content={"status": rr.status_code, "result": data})
        if rr.status_code >= 400:
            return JSONResponse(
                status_code=rr.status_code,
                content={"error": "monitor failed", "graph": rr.text},
            )

    return JSONResponse(
        status_code=504, content={"error": "Timeout ao monitorar a cópia."}
    )
