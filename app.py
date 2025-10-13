import os
import re
import json
import sqlite3
import secrets
import string
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict

import requests
import httpx
from fastapi import FastAPI, Request, Header

# Google APIs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =========================================================
# FastAPI
# =========================================================
app = FastAPI()

# =========================================================
# ENVs (Telegram + Google)
# =========================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID")

SQLITE_PATH = os.getenv("SQLITE_PATH", "/tmp/db.sqlite")

# Google Service Account JSON (conteúdo completo)
GOOGLE_SA_JSON = os.getenv("GOOGLE_SA_JSON")

# IDs do modelo e pasta de destino
GS_TEMPLATE_ID = os.getenv("GS_TEMPLATE_ID")
GS_DEST_FOLDER_ID = os.getenv("GS_DEST_FOLDER_ID")

# Opções
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Plan1")  # pode ser "🧾"
SHARE_LINK_ROLE = os.getenv("SHARE_LINK_ROLE", "writer")  # writer|commenter|reader
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

# Scopes Google
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# =========================================================
# DB
# =========================================================
def _db():
    return sqlite3.connect(SQLITE_PATH)

def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def licenses_db_init():
    """Inicializa/atualiza o schema."""
    con = _db()
    cur = con.cursor()
    # licenses
    cur.execute("""
    CREATE TABLE IF NOT EXISTS licenses (
        license_key TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'active',
        max_files INTEGER NOT NULL DEFAULT 1,
        expires_at TEXT,
        notes TEXT
    )""")
    # clients
    cur.execute("""
    CREATE TABLE IF NOT EXISTS
