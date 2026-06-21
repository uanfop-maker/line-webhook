import os, json, hashlib, hmac, base64, asyncio
import datetime as dt
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Optional
import httpx
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build

LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
GSHEET_LINE_ID = os.environ["GSHEET_LINE_ID"]
GOOGLE_SA_JSON = os.environ["GOOGLE_SA_JSON"]
GSHEET_PERSONAL_ID = os.environ.get("GSHEET_PERSONAL_ID", "")
GSHEET_DAILY_ID = os.environ.get("GSHEET_DAILY_ID", "")
LIFF_ID = os.environ.get("LIFF_ID", "")

TZ_TW = pytz.timezone("Asia/Taipei")

_sheets_svc = None

# In-memory cache for user display names to prevent duplicate 用戶資料 rows
_user_cache: dict[str, str] = {}
# Lock to prevent concurrent writes for the same user_id
_user_cache_lock = asyncio.Lock()

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

def sheets_get_from(spreadsheet_id: str, tab: str, range_: str) -> list:
    res = get_sheets().spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
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
    rows = sheets_get("用戶資料", "A:A")
    for i, row in enumerate(rows):
        if row and row[0] == user_id:
            return i + 1
    return None

def get_assigned_agent(user_id: str) -> str:
    rows = sheets_get("專員名單", "A:E")
    for row in rows[1:]:
        if len(row) >= 5 and user_id in row[4].split(","):
            return row[1] if len(row) > 1 else row[0]
    return ""

def check_keyword_reply(text: str) -> Optional[str]:
    rows = sheets_get("關鍵字回應", "A:C")
    for row in rows[1:]:
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

def _populate_cache_from_sheet():
    """Read all 用戶資料 rows and populate _user_cache. Call while holding _user_cache_lock."""
    try:
        user_rows = sheets_get("用戶資料", "A:B")
        for row in user_rows[1:]:  # skip header
            if row and row[0]:
                uid = row[0]
                dname = row[1] if len(row) > 1 and row[1] else uid
                if uid not in _user_cache:
                    _user_cache[uid] = dname
        print(f"[cache] Loaded {len(_user_cache)} users from sheet")
    except Exception as e:
        print(f"[cache] Error populating cache: {e}")

async def get_or_fetch_display_name(user_id: str) -> str:
    """Look up display_name; uses in-memory cache to prevent duplicate sheet rows."""
    # Fast path: already in cache
    if user_id in _user_cache:
        return _user_cache[user_id]

    # Slow path: acquire lock to prevent concurrent inserts for the same user
    async with _user_cache_lock:
        # Re-check after acquiring lock (another coroutine may have inserted while we waited)
        if user_id in _user_cache:
            return _user_cache[user_id]

        # Cache miss — populate cache from sheet to catch any rows not yet in cache
        _populate_cache_from_sheet()

        # Check again after full sheet load
        if user_id in _user_cache:
            return _user_cache[user_id]

        # Truly not found — fetch from LINE API and append to sheet
        profile = await get_line_profile(user_id)
        dname = profile.get("displayName", user_id)
        if profile:
            ts = now_iso()
            sheets_append("用戶資料", [
                user_id,
                dname,
                profile.get("pictureUrl", ""),
                profile.get("statusMessage", ""),
                profile.get("language", ""),
                "",  # follow_at unknown
                "",  # unfollow_at
                "舊好友"  # assigned_agent = mark as legacy
            ])
        _user_cache[user_id] = dname
        return dname

_personal_tabs: set = set()  # cache of created tab names

def log_to_personal_sheet(user_id: str, display_name: str, event_type: str, content: str, ts: str):
    if not GSHEET_PERSONAL_ID:
        return
    short_id = user_id[-8:] if len(user_id) >= 8 else user_id
    tab_name = f"{display_name[:20]}({short_id})" if display_name and display_name != user_id else f"({short_id})"
    svc = get_sheets()
    # Create tab if not exists
    if tab_name not in _personal_tabs:
        ss = svc.spreadsheets().get(spreadsheetId=GSHEET_PERSONAL_ID).execute()
        existing = {s["properties"]["title"] for s in ss["sheets"]}
        if tab_name not in existing:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=GSHEET_PERSONAL_ID,
                body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
            ).execute()
            # Write header
            svc.spreadsheets().values().update(
                spreadsheetId=GSHEET_PERSONAL_ID,
                range=f"{tab_name}!A1",
                valueInputOption="RAW",
                body={"values": [["時間(UTC)", "動作類型", "內容", "user_id"]]}
            ).execute()
        _personal_tabs.add(tab_name)
    # Append action
    svc.spreadsheets().values().append(
        spreadsheetId=GSHEET_PERSONAL_ID,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        body={"values": [[ts, event_type, content, user_id]]}
    ).execute()

async def handle_follow(user_id: str):
    profile = await get_line_profile(user_id)
    agent = get_assigned_agent(user_id)
    row_idx = find_user_row(user_id)
    ts = now_iso()
    if row_idx:
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
    display_name = profile.get("displayName", user_id)
    # Update cache on follow
    _user_cache[user_id] = display_name
    sheets_append("動作紀錄", [ts, user_id, display_name, "follow", ""])
    log_to_personal_sheet(user_id, display_name, "follow", "", ts)

async def handle_unfollow(user_id: str):
    ts = now_iso()
    row_idx = find_user_row(user_id)
    if row_idx:
        sheets_update("用戶資料", f"G{row_idx}", [[ts]])
    sheets_append("動作紀錄", [ts, user_id, "", "unfollow", ""])

async def handle_message(user_id: str, reply_token: str, text: str):
    ts = now_iso()
    dname = await get_or_fetch_display_name(user_id)
    sheets_append("動作紀錄", [ts, user_id, dname, "text", text])
    log_to_personal_sheet(user_id, dname, "text", text, ts)
    reply = check_keyword_reply(text)
    if reply:
        await reply_line(reply_token, reply)

async def handle_postback(user_id: str, data: str):
    ts = now_iso()
    dname = await get_or_fetch_display_name(user_id)
    sheets_append("動作紀錄", [ts, user_id, dname, "postback", data])
    log_to_personal_sheet(user_id, dname, "postback", data, ts)

def generate_daily_report():
    """Runs at 22:00 Asia/Taipei. Covers prev 22:00 → today 22:00."""
    try:
        now_tw = datetime.now(TZ_TW)
        end_tw = now_tw.replace(hour=22, minute=0, second=0, microsecond=0)
        start_tw = end_tw - timedelta(days=1)

        start_utc = start_tw.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
        end_utc = end_tw.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

        date_label = end_tw.strftime("%Y-%m-%d")
        sheet_tab = f"日報 {date_label}"

        user_rows = sheets_get("用戶資料", "A:H")
        new_users = {}
        for row in user_rows[1:]:
            if len(row) < 6:
                continue
            follow_at = row[5]
            if start_utc <= follow_at < end_utc:
                new_users[row[0]] = row[1] if len(row) > 1 else row[0]

        action_rows = sheets_get("動作紀錄", "A:E")
        click_details = []
        clicked_uids = set()
        for row in action_rows[1:]:
            if len(row) < 5:
                continue
            ts, uid, _dname, etype, content = row[0], row[1], row[2], row[3], row[4]
            if uid in new_users and start_utc <= ts < end_utc and etype == "postback":
                clicked_uids.add(uid)
                click_details.append([uid, new_users[uid], etype, content, ts])

        total = len(new_users)
        clicked = len(clicked_uids)
        no_click = total - clicked

        period_str = f"{start_tw.strftime('%Y-%m-%d %H:%M')} ~ {end_tw.strftime('%Y-%m-%d %H:%M')} (台灣時間)"
        report_data = [
            ["統計期間", period_str],
            ["新加入人數", total],
            ["有點擊人數", clicked],
            ["沒有點擊人數", no_click],
            [],
            ["點擊者明細"],
            ["user_id", "display_name", "event_type", "點擊內容", "時間(UTC)"],
        ] + click_details

        svc = get_sheets()
        svc.spreadsheets().batchUpdate(
            spreadsheetId=GSHEET_LINE_ID,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_tab}}}]}
        ).execute()
        svc.spreadsheets().values().update(
            spreadsheetId=GSHEET_LINE_ID,
            range=f"{sheet_tab}!A1",
            valueInputOption="RAW",
            body={"values": report_data}
        ).execute()
        print(f"[report] {sheet_tab}: {total} users, {clicked} clicked")
    except Exception as e:
        print(f"[report] ERROR: {e}")

def generate_daily_stats():
    """
    Runs at 22:00 Asia/Taipei.
    Reports stats for the period: yesterday 22:00 TW ~ today 22:00 TW
    i.e. running at 6/22 22:00 → reports stats for 6/21 22:00 ~ 6/22 22:00
    Day label: the end day (e.g. "2026/06/22").
    UTC equivalent: 14:00 UTC boundaries.
    """
    if not GSHEET_DAILY_ID:
        print("[daily_stats] GSHEET_DAILY_ID not set, skipping")
        return
    try:
        now_tw = datetime.now(TZ_TW)
        # Period end = today 22:00 TW
        end_tw = now_tw.replace(hour=22, minute=0, second=0, microsecond=0)
        # Period start = yesterday 22:00 TW
        start_tw = end_tw - timedelta(days=1)

        # UTC boundaries (22:00 TW = 14:00 UTC)
        start_utc = start_tw.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
        end_utc = end_tw.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Date label = end day
        date_label = end_tw.strftime("%Y/%m/%d")

        # Read 用戶資料 (A=user_id, F=follow_at col6, G=unfollow_at col7)
        user_rows = sheets_get("用戶資料", "A:H")

        join_count = 0
        block_same_day = 0
        block_other_day = 0

        for row in user_rows[1:]:  # skip header
            if len(row) < 6:
                continue
            follow_at = row[5] if len(row) > 5 else ""
            unfollow_at = row[6] if len(row) > 6 else ""

            followed_in_period = follow_at and start_utc <= follow_at < end_utc
            unfollowed_in_period = unfollow_at and start_utc <= unfollow_at < end_utc

            if followed_in_period:
                join_count += 1

            if unfollowed_in_period:
                if followed_in_period:
                    block_same_day += 1
                else:
                    block_other_day += 1

        # Read 動作紀錄 (A=timestamp, B=user_id, C=暱稱, D=event_type, E=content)
        action_rows = sheets_get("動作紀錄", "A:E")

        click_1x1 = 0
        click_faq = 0
        click_money = 0

        for row in action_rows[1:]:  # skip header
            if len(row) < 5:
                continue
            ts, _uid, _nick, etype, content = row[0], row[1], row[2], row[3], row[4]
            if etype == "uri_click" and start_utc <= ts < end_utc:
                if content.startswith("1x1"):
                    click_1x1 += 1
                elif content.startswith("faq"):
                    click_faq += 1
                elif content.startswith("money"):
                    click_money += 1

        new_row = [date_label, join_count, block_same_day, block_other_day, click_1x1, click_faq, click_money]

        svc = get_sheets()

        # Check if header exists in daily sheet
        existing = svc.spreadsheets().values().get(
            spreadsheetId=GSHEET_DAILY_ID,
            range="Sheet1!A1:G1"
        ).execute().get("values", [])

        header = ["日期", "加入人數", "封鎖(當天加入)", "封鎖(非當天加入)", "1x1點擊", "faq點擊", "money點擊"]

        if not existing or existing[0] != header:
            # Write header first
            svc.spreadsheets().values().update(
                spreadsheetId=GSHEET_DAILY_ID,
                range="Sheet1!A1",
                valueInputOption="RAW",
                body={"values": [header]}
            ).execute()

        # Append the stats row
        svc.spreadsheets().values().append(
            spreadsheetId=GSHEET_DAILY_ID,
            range="Sheet1!A1",
            valueInputOption="RAW",
            body={"values": [new_row]}
        ).execute()

        print(f"[daily_stats] {date_label}: join={join_count}, block_same={block_same_day}, block_other={block_other_day}, 1x1={click_1x1}, faq={click_faq}, money={click_money}")
    except Exception as e:
        print(f"[daily_stats] ERROR: {e}")

@asynccontextmanager
async def lifespan(app_: FastAPI):
    # Pre-populate user cache from existing 用戶資料 rows to prevent duplicates after restart
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _populate_cache_from_sheet)

    scheduler = AsyncIOScheduler()
    # Existing daily report job
    scheduler.add_job(generate_daily_report, CronTrigger(hour=14, minute=0, timezone="UTC"))
    # New daily stats job (also 22:00 Asia/Taipei = 14:00 UTC)
    scheduler.add_job(generate_daily_stats, CronTrigger(hour=14, minute=0, timezone="UTC"))
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def health():
    return {"status": "ok"}

@app.get("/track")
async def track_page(to: str = "", label: str = ""):
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<script charset="utf-8" src="https://static.line-scdn.net/liff/edge/2/sdk.js"></script>
</head>
<body>
<script>
const destination = {json.dumps(to)};
const liffId = {json.dumps(LIFF_ID)};
const label = {json.dumps(label)};

async function main() {{
  let userId = '';
  let displayName = '';
  try {{
    if (liffId && liffId !== 'PLACEHOLDER') {{
      await liff.init({{ liffId: liffId }});
      if (liff.isLoggedIn()) {{
        const profile = await liff.getProfile();
        userId = profile.userId;
        displayName = profile.displayName;
      }}
    }}
    fetch('/api/track', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{
        user_id: userId,
        display_name: displayName,
        destination: destination,
        label: label
      }})
    }}).catch(() => {{}});
  }} catch(e) {{
    // Still record anonymously on error
    fetch('/api/track', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{ user_id: '', display_name: '', destination: destination, label: label }})
    }}).catch(() => {{}});
  }}
  if (destination) window.location.href = destination;
}}
main();
</script>
<p style="font-family:sans-serif;color:#888;text-align:center;margin-top:40px">正在跳轉...</p>
</body>
</html>"""
    return HTMLResponse(content=html)


class TrackPayload(BaseModel):
    user_id: str = ""
    display_name: str = ""
    destination: str = ""
    label: str = ""

@app.post("/api/track")
async def api_track(payload: TrackPayload):
    ts = now_iso()
    uid = payload.user_id or "anonymous"
    content = f"{payload.label} → {payload.destination}" if payload.label else payload.destination
    sheets_append("動作紀錄", [ts, uid, payload.display_name or "", "uri_click", content])
    if payload.user_id:
        dname = payload.display_name or payload.user_id
        log_to_personal_sheet(payload.user_id, dname, "uri_click", content, ts)
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
