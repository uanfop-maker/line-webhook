import os, json, hashlib, hmac, base64, asyncio
from datetime import datetime, timezone
from typing import Optional
import httpx
from fastapi import FastAPI, Request, HTTPException
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()

LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
GSHEET_LINE_ID = os.environ["GSHEET_LINE_ID"]
GOOGLE_SA_JSON = os.environ["GOOGLE_SA_JSON"]

_sheets_svc = None

def get_sheets():
    global _sheets_svc
    if _sheets_svc is None:
        info = json.loads(GOOGLE_SA_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        _sheets_svc = build("sheets", "v4", credentials=creds)
    return _sheets_svc

def sheets_append(tab: str, row: list):
    get_sheets().spreadsheets().values().append(
        spreadsheetId=GSHEET_LINE_ID,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values": [row]}
    ).execute()

def sheets_get(tab: str, range_: str) -> list:
    res = get_sheets().spreadsheets().values().get(
        spreadsheetId=GSHEET_LINE_ID,
        range=f"{tab}!{range_}"
    ).execute()
    return res.get("values", [])

def sheets_update(tab: str, range_: str, values: list):
    get_sheets().spreadsheets().values().update(
        spreadsheetId=GSHEET_LINE_ID,
        range=f"{tab}!{range_}",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

def verify_signature(body: bytes, signature: str) -> bool:
    h = hmac.new(LINE_CHANNEL_SECRET.encode(), body, hashlib.sha256)
    return hmac.compare_digest(base64.b64encode(h.digest()).decode(), signature)

async def get_line_profile(user_id: str) -> dict:
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"https://api.line.me/v2/bot/profile/{user_id}",
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"}
        )
    return r.json() if r.status_code == 200 else {}

async def reply_line(reply_token: str, text: str):
    async with httpx.AsyncClient() as c:
        await c.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={
                "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                "Content-Type": "application/json"
            },
            json={"replyToken": reply_token, "messages": [{"type": "text", "text": text}]}
        )

def find_user_row(user_id: str) -> Optional[int]:
    """Returns 1-based row index in 用戶資料 sheet, or None."""
    rows = sheets_get("用戶資料", "A:A")
    for i, row in enumerate(rows):
        if row and row[0] == user_id:
            return i + 1
    return None

def get_assigned_agent(user_id: str) -> str:
    """Check 專員名單 and return agent_name if user assigned."""
    rows = sheets_get("專員名單", "A:C")
    for row in rows[1:]:  # skip header
        if len(row) >= 3 and user_id in row[2].split(","):
            return row[1] if len(row) > 1 else row[0]
    return ""

def check_keyword_reply(text: str) -> Optional[str]:
    """Check 關鍵字回應 sheet. Return rendered reply or None."""
    rows = sheets_get("關鍵字回應", "A:C")
    for row in rows[1:]:  # skip header
        if len(row) < 2:
            continue
        keyword = row[0]
        template = row[1]
        variables = json.loads(row[2]) if len(row) > 2 and row[2] else {}
        if keyword and keyword in text:
            try:
                return template.format_map(variables)
            except Exception:
                return template
    return None

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

async def handle_follow(user_id: str):
    profile = await get_line_profile(user_id)
    agent = get_assigned_agent(user_id)
    row_idx = find_user_row(user_id)
    ts = now_iso()
    if row_idx:
        # Already exists — update follow_at and clear unfollow_at
        sheets_update("用戶資料", f"F{row_idx}:H{row_idx}", [[ts, "", agent]])
    else:
        sheets_append("用戶資料", [
            user_id,
            profile.get("displayName", ""),
            profile.get("pictureUrl", ""),
            profile.get("statusMessage", ""),
            profile.get("language", ""),
            ts, "", agent
        ])
    sheets_append("動作紀錄", [ts, user_id, "follow", ""])

async def handle_unfollow(user_id: str):
    ts = now_iso()
    row_idx = find_user_row(user_id)
    if row_idx:
        sheets_update("用戶資料", f"G{row_idx}", [[ts]])
    sheets_append("動作紀錄", [ts, user_id, "unfollow", ""])

async def handle_message(user_id: str, reply_token: str, text: str):
    ts = now_iso()
    sheets_append("動作紀錄", [ts, user_id, "text", text])
    reply = check_keyword_reply(text)
    if reply:
        await reply_line(reply_token, reply)

async def handle_postback(user_id: str, data: str):
    ts = now_iso()
    sheets_append("動作紀錄", [ts, user_id, "postback", data])

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/webhook")
async def webhook(request: Request):
    body = await request.body()
    sig = request.headers.get("X-Line-Signature", "")
    if not verify_signature(body, sig):
        raise HTTPException(status_code=400, detail="Invalid signature")

    payload = json.loads(body)
    for event in payload.get("events", []):
        uid = event.get("source", {}).get("userId", "")
        etype = event.get("type")
        rtoken = event.get("replyToken", "")
        if etype == "follow":
            asyncio.create_task(handle_follow(uid))
        elif etype == "unfollow":
            asyncio.create_task(handle_unfollow(uid))
        elif etype == "message" and event.get("message", {}).get("type") == "text":
            asyncio.create_task(handle_message(uid, rtoken, event["message"]["text"]))
        elif etype == "postback":
            asyncio.create_task(handle_postback(uid, event.get("postback", {}).get("data", "")))

    return {"status": "ok"}
