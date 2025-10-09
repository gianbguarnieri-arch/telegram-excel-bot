# app.py
import os
import re
import json
import time
import asyncio
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
TENANT_ID = os.getenv("TENANT_ID", "").strip()
CLIENT_ID = os.getenv("CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "").strip()

DRIVE_ID = os.getenv("DRIVE_ID", "").strip()
TEMPLATE_ITEM_ID = os.getenv("TEMPLATE_ITEM_ID", "").strip()
DEST_FOLDER_ITEM_ID = os.getenv("DEST_FOLDER_ITEM_ID", "").strip()

CLIENTS_TABLE_PATH = os.getenv("CLIENTS_TABLE_PATH", "").strip()
CLIENTS_TABLE_NAME = os.getenv("CLIENTS_TABLE_NAME", "Clientes").strip()
CLIENTS_WORKSHEET_NAME = os.getenv("CLIENTS_WORKSHEET_NAME", "Plan1").strip()

TABLE_NAME = os.getenv("TABLE_NAME", "Lancamentos").strip()
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1").strip()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
DEBUG = os.getenv("DEBUG", "0").strip() in ("1", "true", "True", "yes")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"

app = FastAPI(title="telegram-excel-bot", version="2.0")


def log(*args):
    if DEBUG:
        print("[DEBUG]", *args)


# ---------------------------------------------------------------------
# Auth (corrige invalidAudienceUri)
# ---------------------------------------------------------------------
_access_token_cache: Tuple[float, str] = (0.0, "")

async def get_access_token() -> str:
    """
    Client Credentials com scope '.default' (corrige invalidAudienceUri).
    """
    global _access_token_cache
    now = time.time()
    exp, token = _access_token_cache
    if token and now < exp:
        return token

    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    async with httpx.AsyncClient(timeout=40) as cli:
        r = await cli.post(url, data=data)
    if r.status_code != 200:
        raise HTTPException(
            status_code=500,
            detail=f"Token error {r.status_code}: {r.text}"
        )
    tok = r.json()
    access_token = tok["access_token"]
    expires_in = int(tok.get("expires_in", 3599))
    _access_token_cache = (now + expires_in - 60, access_token)
    return access_token


async def graph(method: str, path: str, **kwargs) -> httpx.Response:
    """
    Chamada Graph com Bearer; path deve ser relativo a /v1.0 (ex.: '/drives/{id}/items/...').
    """
    token = await get_access_token()
    url = f"{GRAPH_BASE}{path}"
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers.setdefault("Content-Type", "application/json")
    async with httpx.AsyncClient(timeout=60) as cli:
        resp = await cli.request(method, url, headers=headers, **kwargs)
    return resp


# ---------------------------------------------------------------------
# Utilitários Graph / Excel
# ---------------------------------------------------------------------
def extract_item_root_from_clients_path() -> str:
    """
    De CLIENTS_TABLE_PATH (que aponta para workbook, p.ex. '/drives/{driveId}/items/{itemId}')
    devolve o prefixo '/drives/.../items/{itemId}' sem o sufixo '/workbook...'
    para lermos metadata do arquivo.
    """
    p = CLIENTS_TABLE_PATH.strip()
    # comum: '/users/.../drive/items/{id}' ou '/drives/{driveId}/items/{id}'
    # se já terminar com '/items/{id}', devolve; se tiver '/workbook', corta antes.
    if "/workbook" in p:
        p = p.split("/workbook")[0]
    return p


async def graph_get_json(path: str) -> Dict[str, Any]:
    r = await graph("GET", path)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    return r.json()


async def ensure_clients_sheet_ok() -> Dict[str, Any]:
    """
    Checa se a Clients.xlsx é acessível. Usa CLIENTS_TABLE_PATH como base.
    """
    root = extract_item_root_from_clients_path()
    r = await graph("GET", root)
    return {"status": r.status_code, "ok": r.status_code == 200}


async def check_item_exists_by_id(item_id: str) -> Dict[str, Any]:
    r = await graph("GET", f"/drives/{DRIVE_ID}/items/{item_id}")
    return {"status": r.status_code, "ok": r.status_code == 200}


async def add_row_to_clients(chat_id: str, excel_graph_path: str,
                             worksheet_name: str, table_name: str) -> None:
    """
    Adiciona linha na tabela Clientes (colunas: chat_id, excel_path, worksheet_name, table_name, created_at).
    """
    body = {
        "values": [[
            chat_id,
            excel_graph_path,
            worksheet_name,
            table_name,
            datetime.now().strftime("%d/%m/%Y %H:%M")
        ]]
    }
    url = f"{CLIENTS_TABLE_PATH}/workbook/worksheets('{CLIENTS_WORKSHEET_NAME}')/tables('{CLIENTS_TABLE_NAME}')/rows/add"
    r = await graph("POST", url, content=json.dumps(body))
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)


async def get_client_row(chat_id: str) -> Optional[Dict[str, Any]]:
    """
    Lê linhas da tabela Clientes e busca chat_id.
    Retorna dict com colunas se achar; None se não.
    """
    url = f"{CLIENTS_TABLE_PATH}/workbook/worksheets('{CLIENTS_WORKSHEET_NAME}')/tables('{CLIENTS_TABLE_NAME}')/rows"
    r = await graph("GET", url)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)
    rows = r.json().get("value", [])
    for row in rows:
        vals = row.get("values", [[]])[0]
        if not vals:
            continue
        if str(vals[0]).strip() == str(chat_id):
            # mapear colunas
            out = {
                "chat_id": vals[0],
                "excel_path": vals[1],
                "worksheet_name": vals[2],
                "table_name": vals[3],
                "created_at": vals[4] if len(vals) > 4 else ""
            }
            return out
    return None


async def copy_template_for_user(file_name: str) -> Dict[str, Any]:
    """
    Copia o template usando /copy + monitor (corrige 202/Location).
    Retorna {item_id, webUrl, graph_path}.
    """
    body = {
        "name": file_name,
        "parentReference": {
            "driveId": DRIVE_ID,
            "id": DEST_FOLDER_ITEM_ID
        }
    }
    path = f"/drives/{DRIVE_ID}/items/{TEMPLATE_ITEM_ID}/copy"
    r = await graph("POST", path, content=json.dumps(body))

    if r.status_code not in (202, 201, 200):
        raise HTTPException(status_code=r.status_code, detail=f"Erro ao copiar modelo: {r.text}")

    # Se já veio 201/200, ótimo, pegue Location se houver ou faça um GET por nome.
    location = r.headers.get("Location")
    if not location:
        # Tenta achar por nome dentro da pasta destino
        # (fallback – raramente necessário; manter simples)
        await asyncio.sleep(2)

    # Monitora o Location (quando há)
    if location:
        async with httpx.AsyncClient(timeout=60) as cli:
            # usa o mesmo token
            token = await get_access_token()
            headers = {"Authorization": f"Bearer {token}"}
            for _ in range(40):
                res = await cli.get(location, headers=headers)
                if res.status_code == 200:
                    job = res.json()
                    status = job.get("status")
                    if status in ("completed", "succeeded", "success"):
                        resource = job.get("resourceId") or job.get("resource", {}).get("id")
                        if resource:
                            item_meta = await graph_get_json(f"/drives/{DRIVE_ID}/items/{resource}")
                            return {
                                "item_id": item_meta["id"],
                                "webUrl": item_meta.get("webUrl", ""),
                                "graph_path": f"/drives/{DRIVE_ID}/items/{item_meta['id']}"
                            }
                        break
                elif res.status_code in (201, 204):
                    # alguns tenants retornam 201/204 quando conclui
                    break
                await asyncio.sleep(1)

    # Sem Location ou monitor: procura por nome recém criado na pasta de destino
    children = await graph_get_json(f"/drives/{DRIVE_ID}/items/{DEST_FOLDER_ITEM_ID}/children?$select=id,name,webUrl")
    for it in children.get("value", []):
        if it.get("name") == file_name:
            return {
                "item_id": it["id"],
                "webUrl": it.get("webUrl", ""),
                "graph_path": f"/drives/{DRIVE_ID}/items/{it['id']}"
            }

    raise HTTPException(status_code=500, detail="Falha ao localizar cópia do modelo (monitor).")


async def append_row_to_user_excel(excel_graph_path: str, values: List[Any]) -> None:
    """
    Adiciona uma linha na tabela do Excel do cliente.
    """
    url = f"{excel_graph_path}/workbook/worksheets('{WORKSHEET_NAME}')/tables('{TABLE_NAME}')/rows/add"
    body = {"values": [values]}
    r = await graph("POST", url, content=json.dumps(body))
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text)


# ---------------------------------------------------------------------
# Parser simples (linguagem natural)
# ---------------------------------------------------------------------
PGTO_MAP = {
    "pix": "Pix",
    "cartao": "💳 cartão",
    "cartão": "💳 cartão",
    "dinheiro": "Dinheiro",
    "debito": "Débito",
    "débito": "Débito",
    "credito": "Crédito",
    "crédito": "Crédito",
    "boleto": "Boleto",
}

CATEG_MAP = {
    "mercado": "Mercado",
    "farmácia": "Farmácia",
    "farmacia": "Farmácia",
    "restaurante": "Restaurante",
    "ifood": "IFood",
    "combustível": "Combustível",
    "combustivel": "Combustível",
    "viagem": "Viagem",
    "assinatura": "Assinatura",
    "netflix": "Assinatura",
    "spotify": "Assinatura",
}

def parse_pt_br_amount(txt: str) -> Optional[float]:
    m = re.search(r"(\d{1,3}(?:\.\d{3})*|\d+)(?:[,\.](\d{1,2}))?", txt)
    if not m:
        return None
    inteiro = m.group(1).replace(".", "")
    frac = m.group(2) or "00"
    if len(frac) == 1:
        frac += "0"
    return float(f"{inteiro}.{frac}")


def guess_pgto(txt: str) -> str:
    txtl = txt.lower()
    for k, v in PGTO_MAP.items():
        if k in txtl:
            return v
    return "A vista"


def guess_categoria(txt: str) -> str:
    txtl = txt.lower()
    for k, v in CATEG_MAP.items():
        if k in txtl:
            return v
    return "Outros"


def guess_data(txt: str) -> date:
    # procura dd/mm/aaaa ou dd/mm
    m = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{4}))?\b", txt)
    if m:
        d, mth, y = m.group(1), m.group(2), m.group(3)
        if not y:
            y = str(datetime.now().year)
        return date(int(y), int(mth), int(d))
    if "ontem" in txt.lower():
        return date.fromordinal(date.today().toordinal() - 1)
    return date.today()


def parse_message_freeform(text: str) -> Dict[str, Any]:
    """
    Retorna: {data, tipo, grupo, categoria, descricao, valor, forma_pgto, condicao}
    """
    valor = parse_pt_br_amount(text)
    data_lanc = guess_data(text)
    forma = guess_pgto(text)
    categoria = guess_categoria(text)
    # heurística simples
    tipo = "Saída" if "gastei" in text.lower() or "comprei" in text.lower() else "Entrada"
    grupo = "Gastos Variáveis" if tipo == "Saída" else "Ganhos"

    # parcelas
    condicao = "A vista"
    parc = re.search(r"(\d+)\s*x", text.lower())
    if parc:
        condicao = f"{parc.group(1)}x"

    # descrição: pega trechos após 'no/na/em' se houver
    desc = ""
    md = re.search(r"\bno\s+([^\d]+)", text.lower())
    if md:
        desc = md.group(1).strip().title()
    if not desc:
        desc = categoria

    return {
        "data": data_lanc.strftime("%d/%m/%Y"),
        "tipo": tipo,
        "grupo": grupo,
        "categoria": categoria,
        "descricao": desc,
        "valor": f"{valor:.2f}" if valor is not None else "",
        "forma_pgto": forma,
        "condicao": condicao,
    }


# ---------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------
class TgMessage(BaseModel):
    update_id: Optional[int] = None
    message: Optional[Dict[str, Any]] = None
    edited_message: Optional[Dict[str, Any]] = None


async def ensure_user_excel(chat_id: str, first_name: str) -> Dict[str, Any]:
    """
    Garante que o usuário possua uma planilha. Se existir no Clientes, reaproveita.
    Se não existir, copia o template e registra.
    Retorna dict {excel_graph_path, item_id, webUrl}.
    """
    # 1) tenta buscar
    row = await get_client_row(chat_id)
    if row:
        return {
            "excel_graph_path": row["excel_path"],
            "item_id": row["excel_path"].split("/")[-1],
            "webUrl": ""  # opcional
        }

    # 2) copia template
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"Planilha_{first_name}_{ts}.xlsx"
    info = await copy_template_for_user(file_name)

    # 3) registra no Clientes
    await add_row_to_clients(
        chat_id=chat_id,
        excel_graph_path=info["graph_path"],  # ex.: /drives/{driveId}/items/{itemId}
        worksheet_name=WORKSHEET_NAME,
        table_name=TABLE_NAME
    )

    return {
        "excel_graph_path": info["graph_path"],
        "item_id": info["item_id"],
        "webUrl": info["webUrl"]
    }


async def tg_send(chat_id: str, text: str) -> None:
    if not TELEGRAM_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": chat_id, "text": text}
    async with httpx.AsyncClient(timeout=30) as cli:
        await cli.post(url, data=data)


# ---------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------
@app.get("/")
async def root():
    return {"status": "ok"}


@app.get("/diag")
async def diag():
    checks = {
        "envs": {
            "DRIVE_ID": bool(DRIVE_ID),
            "TEMPLATE_ITEM_ID": bool(TEMPLATE_ITEM_ID),
            "DEST_FOLDER_ITEM_ID": bool(DEST_FOLDER_ITEM_ID),
            "CLIENTS_TABLE_PATH": bool(CLIENTS_TABLE_PATH),
        }
    }
    # template/dest
    tpl = await check_item_exists_by_id(TEMPLATE_ITEM_ID) if DRIVE_ID and TEMPLATE_ITEM_ID else {"status": 400, "ok": False}
    dst = await check_item_exists_by_id(DEST_FOLDER_ITEM_ID) if DRIVE_ID and DEST_FOLDER_ITEM_ID else {"status": 400, "ok": False}
    cli = await ensure_clients_sheet_ok() if CLIENTS_TABLE_PATH else {"status": 400, "ok": False}
    checks["template_item"] = tpl
    checks["dest_folder"] = dst
    checks["clients_file"] = cli
    return checks


@app.post("/telegram/webhook")
async def telegram_webhook(payload: TgMessage):
    msg = payload.message or payload.edited_message
    if not msg:
        return {"ok": True}

    chat_id = str(msg["chat"]["id"])
    text = (msg.get("text") or "").strip()
    first_name = msg["chat"].get("first_name") or "User"

    try:
        # /start → garante planilha
        if text.lower().startswith("/start"):
            info = await ensure_user_excel(chat_id, first_name)
            await tg_send(chat_id, "Olá! Pode me contar seus gastos/recebimentos em linguagem natural.\n"
                                    "Exemplos:\n• gastei 45,90 no mercado via cartão hoje\n"
                                    "• comprei remédio 34 na farmácia via pix\n"
                                    "• ganhei 800 de salário\n"
                                    "Se preferir: /add 07/10/2025;Compra;Mercado;Almoço;45,90;Cartão")
            return {"ok": True}

        # /add DD/MM/AAAA;Tipo;Grupo|Categoria;Descricao;Valor;Forma
        if text.lower().startswith("/add"):
            try:
                _, rest = text.split(" ", 1)
                partes = [p.strip() for p in rest.split(";")]
                dt, tipo, categoria, descricao, valor, forma = partes[:6]
                grupo = "Ganhos" if tipo.lower().startswith("ent") else "Gastos Variáveis"
                cond = "A vista"
            except Exception:
                await tg_send(chat_id, "Formato inválido. Use:\n/add 07/10/2025;Compra;Mercado;Almoço;45,90;Cartão")
                return {"ok": True}

            # Garante excel
            info = await ensure_user_excel(chat_id, first_name)
            row = [dt, tipo, grupo, categoria, descricao, valor.replace(",", "."), forma, "A vista"]
            await append_row_to_user_excel(info["excel_graph_path"], row)
            await tg_send(chat_id, "Lançado!")
            return {"ok": True}

        # linguagem natural
        info = await ensure_user_excel(chat_id, first_name)
        parsed = parse_message_freeform(text)
        if not parsed.get("valor"):
            await tg_send(chat_id, "Não entendi o valor. Ex.: 'gastei 45,90 no mercado via cartão hoje'.")
            return {"ok": True}

        row = [
            parsed["data"],
            parsed["tipo"],
            parsed["grupo"],
            parsed["categoria"],
            parsed["descricao"],
            parsed["valor"].replace(",", "."),
            parsed["forma_pgto"],
            parsed["condicao"],
        ]
        await append_row_to_user_excel(info["excel_graph_path"], row)
        await tg_send(chat_id, "Lançado!")
        return {"ok": True}

    except HTTPException as e:
        log("HTTPException", e.status_code, e.detail)
        await tg_send(chat_id, f"❌ Erro: {e.detail}")
        return JSONResponse(status_code=200, content={"ok": True})
    except Exception as e:
        log("Exception", repr(e))
        await tg_send(chat_id, f"❌ Erro inesperado: {e}")
        return JSONResponse(status_code=200, content={"ok": True})


# Opcional: endpoint de teste de cópia manual (POST manda JSON {"name": "arquivo.xlsx"})
@app.post("/test-copy")
async def test_copy(payload: Dict[str, Any]):
    name = payload.get("name") or f"Planilha_Teste_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    info = await copy_template_for_user(name)
    return info
