from fastapi import FastAPI, Request
import requests
import os

app = FastAPI()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    data = await req.json()
    message = data.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "")

    if not chat_id or not text:
        return {"ok": True}

    if text.lower().startswith("/start"):
        reply = "Olá! Envie /add 06/10/2025;Compra;Mercado;Almoço;45,90;Cartão"
    else:
        reply = f"Você mandou: {text}"

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": reply},
    )

    return {"ok": True}
