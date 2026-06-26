import os, json, hashlib, hmac, base64, asyncio, urllib.parse, time
from collections import deque
import datetime as dt
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Optional
import httpx
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
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
NOTIFY_TG_TOKEN = os.environ.get("NOTIFY_TG_TOKEN", "")
NOTIFY_TG_CHAT_ID = os.environ.get("NOTIFY_TG_CHAT_ID", "")

TZ_TW = pytz.timezone("Asia/Taipei")

KEYWORD_RESPONSES = {
    "__1x1__": (
        "收到您的諮詢預約了！✨\n"
        "為了提供您最精準、最詳細的規劃，我們將由經營顧問專員為您進行 1對1 線上解答。\n\n"
        "🎁 加好友專屬福利：\n"
        "現在添加專員，立即獲得：\n"
        "量身訂做的專屬電商藍圖\n"
        "上千會員的實戰成功案例\n\n"
        "👇 請立刻點擊下方圖片 👇\n"
        "轉跳加入專員的個人 LINE，發送訊息「想了解電商」，專員將優先為您安排諮詢時間！"
    ),
    "__FAQ__": (
        "常見問題 FAQ\n\n"
        "1. 需要會員費嗎？\n"
        "💡 完全不用！諮詢、輔導與合作皆不收會員費。\n"
        "2. 需要囤貨嗎？\n"
        "💡 不用囤貨！採用「雲倉模式」，有訂單再出貨、零庫存。\n"
        "3. 警示戶可以合作嗎？\n"
        "💡 可以合作！ 我們有合法結算方案，不影響您的收益。\n"
        "4. 沒有經驗可以做嗎？\n"
        "💡 完全可以！ 手把手實戰帶教，90% 賣家都是從零開始。\n"
        "5. 上班族/兼職有時間做嗎？\n"
        "💡 每天不用 1 小時！ 流程系統化，利用下班零碎時間即可。\n"
        "6. 資金很少也能開始嗎？\n"
        "💡 可以！ 不用大量批貨，啟動資金極低、低風險。\n"
        "7. 我還有其他問題？\n"
        "👇 直接點擊下方圖片 加專員 LINE，一對一為您詳細解答！"
    ),
    "__扶植金__": (
        "🎁 新手扶植金限量開放中！\n"
        "我們特別為首次合作的會員準備補助扶植金，讓您在零壓力的情況下就能開始跨境電商事業！\n"
        "無論您目前資金是否寬裕，這筆基礎資金都能幫助您快速上手、解決初期壓力，並親身體驗跨境電商的驚人魅力與收益。\n"
        "⚠️ 名額有限，先搶先贏！\n"
        "👇 點擊下方圖片立即加入專員，領取您的扶植金資格！"
    ),
}

async def notify_tg(text: str):
    if not NOTIFY_TG_TOKEN or not NOTIFY_TG_CHAT_ID:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{NOTIFY_TG_TOKEN}/sendMessage",
                json={"chat_id": NOTIFY_TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=5
            )
    except Exception:
        pass

async def notify_tg_photo(photo_url: str, caption: str):
    if not NOTIFY_TG_TOKEN or not NOTIFY_TG_CHAT_ID:
        return
    try:
        async with httpx.AsyncClient() as client:
            img_r = await client.get(photo_url, timeout=10)
            if img_r.status_code == 200:
                r = await client.post(
                    f"https://api.telegram.org/bot{NOTIFY_TG_TOKEN}/sendDocument",
                    data={"chat_id": NOTIFY_TG_CHAT_ID, "caption": caption, "parse_mode": "HTML"},
                    files={"document": ("profile.jpg", img_r.content, "image/jpeg")},
                    timeout=15
                )
                if r.status_code == 200:
                    return
            await notify_tg(caption)
    except Exception:
        await notify_tg(caption)

_sheets_svc = None
_recent_actions: deque = deque(maxlen=20)
_rr_index: int = 0

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

def sheets_user_data_next_row() -> int:
    """Return the next empty row number in 用戶資料 column A (1-indexed).
    Uses column A to avoid being confused by checkboxes in column I that extend to row 500."""
    col_a = get_sheets().spreadsheets().values().get(
        spreadsheetId=GSHEET_LINE_ID,
        range="用戶資料!A:A"
    ).execute().get("values", [])
    # Count rows that have a non-empty value in column A
    filled = len([r for r in col_a if r and r[0].strip()])
    return filled + 1  # +1 because 1-indexed; header counts as row 1

def sheets_user_data_append(row: list):
    """Append a new user row to 用戶資料 by finding the first empty A-column row.
    Avoids the append() API which gets confused by checkboxes in column I."""
    next_row = sheets_user_data_next_row()
    # Pad to 11 columns
    r = list(row)
    while len(r) < 11:
        r.append("")
    get_sheets().spreadsheets().values().update(
        spreadsheetId=GSHEET_LINE_ID,
        range=f"用戶資料!A{next_row}:K{next_row}",
        valueInputOption="USER_ENTERED",
        body={"values": [r[:11]]}
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

async def line_reply(reply_token: str, messages: list):
    """Reply with one or more message objects (any type: text, flex, etc.)"""
    async with httpx.AsyncClient() as c:
        await c.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={
                "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                "Content-Type": "application/json"
            },
            json={"replyToken": reply_token, "messages": messages}
        )

def assign_agent(user_id: str, display_name: str) -> dict:
    """Returns {'agent_name': str, 'agent_link': str} after assigning or finding existing"""
    rows = get_agent_rows()
    if not rows or len(rows) <= 1:
        return {"agent_name": "客服", "agent_link": ""}

    data_rows = rows[1:]  # skip header

    # New column layout: A=owner B=# C=agent_name D=agent_id E=agent_link F=提供次數(5) G=停用(6) H=line_user_ids(7)

    # Check if already assigned (check H column, index 7)
    for i, row in enumerate(data_rows):
        if len(row) >= 8 and user_id in row[7].split(","):
            return {
                "agent_name": row[2] if len(row) > 2 else row[0],
                "agent_link": row[4] if len(row) > 4 else ""
            }

    # --- Manual assignment logic ---
    # Read 用戶資料!A:K to check 手動指派ID (col J, index 9) and 已加入專員 (col I, index 8)
    user_rows_manual = sheets_get("用戶資料", "A:K")
    manual_agent_id = ""
    already_joined = False
    for urow in user_rows_manual[1:]:
        if urow and urow[0] == user_id:
            manual_agent_id = urow[9].strip() if len(urow) > 9 and urow[9] else ""
            already_joined = str(urow[8]).upper() == "TRUE" if len(urow) > 8 and urow[8] else False
            break

    if manual_agent_id or already_joined:
        if manual_agent_id:
            # Find agent whose B column (index 1, # field) matches 手動指派ID
            for row in data_rows:
                if len(row) > 1 and str(row[1]).strip() == manual_agent_id:
                    return {
                        "agent_name": row[2] if len(row) > 2 else row[0],
                        "agent_link": row[4] if len(row) > 4 else ""
                    }
            # If no match found for manual_agent_id, fall through to auto-assign
        else:
            # 已加入專員=TRUE but no 手動指派ID → assign first agent in 專員名單
            if data_rows:
                first = data_rows[0]
                return {
                    "agent_name": first[2] if len(first) > 2 else first[0],
                    "agent_link": first[4] if len(first) > 4 else ""
                }

    # Filter to only active (non-disabled) agents with a valid name and link
    active_agents = [
        (i, row, int(row[5]) if len(row) >= 6 and row[5] else 0)
        for i, row in enumerate(data_rows)
        if (len(row) >= 3 and row[2].strip())  # must have agent_name
        and (len(row) >= 5 and row[4].strip())  # must have agent_link
        and not (len(row) >= 7 and str(row[6]).upper() == "TRUE")  # not disabled (G col)
    ]

    if not active_agents:
        return {"agent_name": "客服", "agent_link": ""}

    # Assign to agent with fewest 提供次數; use row order as tiebreaker
    active_agents.sort(key=lambda x: (x[2], x[0]))
    idx, target, current_count = active_agents[0]

    # Add user_id to this agent's line_user_ids (H column)
    svc = get_sheets()
    SHEET_ID = GSHEET_LINE_ID
    existing_ids = target[7].strip() if len(target) >= 8 and target[7] else ""
    new_ids = f"{existing_ids},{user_id}".lstrip(",") if existing_ids else user_id
    row_num = idx + 2  # +1 for header, +1 for 1-based
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"專員名單!H{row_num}",
        valueInputOption="RAW",
        body={"values": [[new_ids]]}
    ).execute()
    # Increment 提供次數 (F column)
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"專員名單!F{row_num}",
        valueInputOption="RAW",
        body={"values": [[current_count + 1]]}
    ).execute()

    # Update 用戶資料 assigned_agent column (H)
    user_rows = sheets_get("用戶資料", "A:K")
    for j, urow in enumerate(user_rows[1:], start=2):
        if urow and urow[0] == user_id:
            svc.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range=f"用戶資料!H{j}",
                valueInputOption="RAW",
                body={"values": [[target[2] if len(target) > 2 else ""]]}
            ).execute()
            break

    # Invalidate agent list cache since we just mutated 專員名單
    invalidate_agent_cache()

    return {
        "agent_name": target[2] if len(target) > 2 else target[0],
        "agent_link": target[4] if len(target) > 4 else ""
    }

def build_assign_flex(agent_name: str, agent_link: str, user_id: str = "", src: str = "") -> dict:
    msg = {
        "type": "flex",
        "altText": f"您的專屬顧問是 {agent_name}",
        "contents": {
            "type": "bubble",
            "hero": {
                "type": "image",
                "url": "https://i.imgur.com/t7oWcNQ.jpeg",
                "size": "full",
                "aspectRatio": "20:13",
                "aspectMode": "cover"
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "text",
                        "text": "您的專屬顧問",
                        "weight": "bold",
                        "size": "xl",
                        "color": "#1a1a1a"
                    },
                    {
                        "type": "text",
                        "text": agent_name,
                        "size": "lg",
                        "color": "#666666",
                        "margin": "md"
                    }
                ]
            }
        }
    }
    if agent_link:
        msg["contents"]["footer"] = {
            "type": "box",
            "layout": "vertical",
            "contents": [
                {
                    "type": "button",
                    "style": "primary",
                    "color": "#C8A96E",
                    "action": {
                        "type": "uri",
                        "label": "➡️ 立即聯絡",
                        "uri": f"https://line-dda.zeabur.app/track?to={urllib.parse.quote(agent_link, safe='')}&label=%E5%8A%A0%E5%85%A5%E5%B0%88%E5%93%A1&uid={user_id}&agent={urllib.parse.quote(agent_name, safe='')}&src={urllib.parse.quote(src, safe='')}"
                    }
                }
            ]
        }
    return msg

def find_user_row(user_id: str) -> Optional[int]:
    rows = sheets_get("用戶資料", "A:A")
    for i, row in enumerate(rows):
        if row and row[0] == user_id:
            return i + 1
    return None

def get_assigned_agent(user_id: str) -> str:
    rows = get_agent_rows()
    for row in rows[1:]:
        if len(row) >= 8 and user_id in row[7].split(","):
            return row[2] if len(row) > 2 else row[0]
    return ""

def check_keyword_reply(text: str) -> Optional[str]:
    rows = get_keyword_rows()
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
    return datetime.now(TZ_TW).strftime("%Y-%m-%d %H:%M:%S")

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
            sheets_user_data_append([
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

# In-memory cache for Sheets-based keyword responses
_keyword_cache: Optional[list] = None
_keyword_cache_ts: float = 0.0
KEYWORD_CACHE_TTL = 60  # seconds

def get_keyword_rows() -> list:
    """Return keyword rows from cache, refreshing at most every KEYWORD_CACHE_TTL seconds."""
    global _keyword_cache, _keyword_cache_ts
    now = time.time()
    if _keyword_cache is None or (now - _keyword_cache_ts) > KEYWORD_CACHE_TTL:
        _keyword_cache = sheets_get("關鍵字回應", "A:C")
        _keyword_cache_ts = now
    return _keyword_cache

# In-memory cache for 專員名單 to avoid repeated reads in assign_agent
_agent_list_cache: Optional[list] = None
_agent_list_cache_ts: float = 0.0
AGENT_LIST_CACHE_TTL = 120  # seconds

def get_agent_rows() -> list:
    """Return agent rows from cache, refreshing at most every AGENT_LIST_CACHE_TTL seconds."""
    global _agent_list_cache, _agent_list_cache_ts
    now = time.time()
    if _agent_list_cache is None or (now - _agent_list_cache_ts) > AGENT_LIST_CACHE_TTL:
        _agent_list_cache = sheets_get("專員名單", "A:H")
        _agent_list_cache_ts = now
    return _agent_list_cache

def invalidate_agent_cache():
    """Call after mutating 專員名單 so next read is fresh."""
    global _agent_list_cache, _agent_list_cache_ts
    _agent_list_cache = None
    _agent_list_cache_ts = 0.0

_personal_tabs: set = set()  # cache of created tab names

def log_to_personal_sheet(user_id: str, display_name: str, event_type: str, content: str, ts: str, pic_url: str = ""):
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
            res = svc.spreadsheets().batchUpdate(
                spreadsheetId=GSHEET_PERSONAL_ID,
                body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
            ).execute()
            new_gid = res["replies"][0]["addSheet"]["properties"]["sheetId"]
            # Apply avatar formatting: row height, column width, freeze rows, font size
            svc.spreadsheets().batchUpdate(
                spreadsheetId=GSHEET_PERSONAL_ID,
                body={"requests": [
                    # Row 1 height = 131px
                    {"updateDimensionProperties": {
                        "range": {"sheetId": new_gid, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
                        "properties": {"pixelSize": 131}, "fields": "pixelSize"
                    }},
                    # Column A width = 131px
                    {"updateDimensionProperties": {
                        "range": {"sheetId": new_gid, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                        "properties": {"pixelSize": 131}, "fields": "pixelSize"
                    }},
                    # Freeze first 2 rows
                    {"updateSheetProperties": {
                        "properties": {"sheetId": new_gid, "gridProperties": {"frozenRowCount": 2}},
                        "fields": "gridProperties.frozenRowCount"
                    }},
                    # B1 font size = 30
                    {"repeatCell": {
                        "range": {"sheetId": new_gid, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 1, "endColumnIndex": 2},
                        "cell": {"userEnteredFormat": {"textFormat": {"fontSize": 30}}},
                        "fields": "userEnteredFormat.textFormat.fontSize"
                    }}
                ]}
            ).execute()
            # Write row 1 (avatar + name) and row 2 (header)
            avatar_formula = f'=IMAGE("{pic_url}",4,131,131)' if pic_url else ""
            svc.spreadsheets().values().update(
                spreadsheetId=GSHEET_PERSONAL_ID,
                range=f"'{tab_name}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [
                    [avatar_formula, display_name],           # row 1: avatar, name
                    ["時間(UTC)", "動作類型", "內容", "user_id"]  # row 2: header
                ]}
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
        sheets_user_data_append([
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
    _recent_actions.append((ts, display_name, "follow"))
    log_to_personal_sheet(user_id, display_name, "follow", "", ts, profile.get("pictureUrl", ""))
    # Count follows since yesterday 22:00 TW and today 00:00 TW
    now_tw = datetime.now(TZ_TW)
    yesterday_22_dt = (now_tw.replace(hour=22, minute=0, second=0, microsecond=0) - timedelta(days=1))
    today_00_dt = now_tw.replace(hour=0, minute=0, second=0, microsecond=0)
    count_22 = 0
    count_00 = 0
    try:
        user_rows = sheets_get("用戶資料", "A:F")
        for row in user_rows[1:]:
            if len(row) > 5 and row[5]:
                fa = row[5]
                try:
                    fa_dt = TZ_TW.localize(datetime.strptime(fa, "%Y-%m-%d %H:%M:%S"))
                except ValueError:
                    try:
                        fa_dt = TZ_TW.localize(datetime.strptime(fa, "%Y-%m-%d %I:%M:%S"))
                    except ValueError:
                        continue
                if fa_dt >= yesterday_22_dt:
                    count_22 += 1
                if fa_dt >= today_00_dt:
                    count_00 += 1
    except Exception:
        pass
    pic = profile.get("pictureUrl", "")
    safe_name = display_name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    caption = f"👤 新好友加入\n暱稱： <code>{safe_name}</code>\nID： <code>{user_id}</code>\n昨天22開始: {count_22}\n今天00開始: {count_00}"
    if pic:
        await notify_tg_photo(pic, caption)
    else:
        await notify_tg(caption)

async def handle_unfollow(user_id: str):
    ts = now_iso()
    row_idx = find_user_row(user_id)
    display_name = _user_cache.get(user_id, user_id)
    if row_idx:
        sheets_update("用戶資料", f"G{row_idx}", [[ts]])
    sheets_append("動作紀錄", [ts, user_id, "", "unfollow", ""])
    _recent_actions.append((ts, display_name, "unfollow"))
    await notify_tg(f"🚫 好友封鎖\n暱稱：{display_name}\nID：{user_id}")

async def handle_message(user_id: str, reply_token: str, text: str):
    ts = now_iso()
    dname = await get_or_fetch_display_name(user_id)

    if text == "__ASSIGN__":
        # assign_agent reads Sheets — do it, then reply, then log in background
        agent = assign_agent(user_id, dname)
        flex_msg = build_assign_flex(agent["agent_name"], agent["agent_link"], user_id=user_id, src="assign")
        await line_reply(reply_token, [flex_msg])
        # Log in background so we don't block
        async def _log_assign():
            sheets_append("動作紀錄", [ts, user_id, dname, "assign", agent["agent_name"]])
        _recent_actions.append((ts, dname, f"assign:{agent['agent_name']}"))
        asyncio.create_task(_log_assign())
        return

    if text in KEYWORD_RESPONSES:
        # FAST PATH: send reply immediately using cached agent list, then do heavy work in background.
        # Step 1: quick cached lookup — is this user already assigned?
        cached_rows = get_agent_rows()
        pre_assigned = None
        if cached_rows and len(cached_rows) > 1:
            for row in cached_rows[1:]:
                if len(row) >= 8 and user_id in row[7].split(","):
                    pre_assigned = {
                        "agent_name": row[2] if len(row) > 2 else row[0],
                        "agent_link": row[4] if len(row) > 4 else ""
                    }
                    break

        if pre_assigned:
            # Already assigned — reply right away, no Sheets writes needed for assignment
            text_msg = {"type": "text", "text": KEYWORD_RESPONSES[text]}
            src_label = text.strip("_")
            flex_msg = build_assign_flex(pre_assigned["agent_name"], pre_assigned["agent_link"], user_id=user_id, src=src_label)
            await line_reply(reply_token, [text_msg, flex_msg])
            _recent_actions.append((ts, dname, text))
            # Log in background
            async def _log_keyword_existing(agent=pre_assigned):
                sheets_append("動作紀錄", [ts, user_id, dname, "keyword", text])
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, log_to_personal_sheet, user_id, dname, "keyword", text, ts)
            asyncio.create_task(_log_keyword_existing())
        else:
            # New assignment needed — do assign_agent (Sheets reads/writes), reply, then log
            agent = assign_agent(user_id, dname)
            text_msg = {"type": "text", "text": KEYWORD_RESPONSES[text]}
            src_label = text.strip("_")
            flex_msg = build_assign_flex(agent["agent_name"], agent["agent_link"], user_id=user_id, src=src_label)
            await line_reply(reply_token, [text_msg, flex_msg])
            _recent_actions.append((ts, dname, text))
            # Log in background
            async def _log_keyword_new(agent=agent):
                sheets_append("動作紀錄", [ts, user_id, dname, "keyword", text])
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, log_to_personal_sheet, user_id, dname, "keyword", text, ts)
            asyncio.create_task(_log_keyword_new())
        return

    # Non-keyword message: log and check Sheets-based keyword replies
    _recent_actions.append((ts, dname, text))
    # Check Sheets keywords first (cached — fast)
    reply = check_keyword_reply(text)
    # Fire-and-forget logging
    async def _log_text():
        sheets_append("動作紀錄", [ts, user_id, dname, "text", text])
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, log_to_personal_sheet, user_id, dname, "text", text, ts)
    asyncio.create_task(_log_text())
    if reply:
        await reply_line(reply_token, reply)

async def handle_sticker(user_id: str, message: dict):
    ts = now_iso()
    sticker_id = message.get("stickerId", "")
    package_id = message.get("packageId", "")
    dname = await get_or_fetch_display_name(user_id)
    thumbnail_url = f"https://stickershop.line-scdn.net/stickershop/v1/sticker/{sticker_id}/iPhone/sticker.png"
    # Verify the thumbnail URL is reachable; fall back to text if not
    try:
        async with httpx.AsyncClient() as c:
            r = await c.head(thumbnail_url, timeout=5)
        content = thumbnail_url if r.status_code == 200 else f"[貼圖] packageId={package_id} stickerId={sticker_id}"
    except Exception:
        content = f"[貼圖] packageId={package_id} stickerId={sticker_id}"
    sheets_append("動作紀錄", [ts, user_id, dname, "sticker", content])
    _recent_actions.append((ts, dname, f"sticker:{sticker_id}"))
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, log_to_personal_sheet, user_id, dname, "sticker", content, ts, "")

async def handle_postback(user_id: str, data: str, reply_token: str = ""):
    ts = now_iso()
    dname = await get_or_fetch_display_name(user_id)

    if data == "__ASSIGN__":
        result = assign_agent(user_id, dname)
        flex_msg = build_assign_flex(result["agent_name"], result["agent_link"], user_id=user_id)
        if reply_token:
            await line_reply(reply_token, [flex_msg])
        sheets_append("動作紀錄", [ts, user_id, dname, "assign", result["agent_name"]])
        _recent_actions.append((ts, dname, f"assign:{result['agent_name']}"))
        log_to_personal_sheet(user_id, dname, "assign", result["agent_name"], ts)
        return

    sheets_append("動作紀錄", [ts, user_id, dname, "postback", data])
    _recent_actions.append((ts, dname, data))
    log_to_personal_sheet(user_id, dname, "postback", data, ts)

def generate_daily_report():
    """
    Runs at 22:00 Asia/Taipei. Covers prev 22:00 → today 22:00.
    Writes a 6-section structured 日報 with stat hyperlinks and per-user click details.
    """
    try:
        now_tw  = datetime.now(TZ_TW)
        end_tw  = now_tw.replace(hour=22, minute=0, second=0, microsecond=0)
        start_tw = end_tw - timedelta(days=1)

        start_utc = start_tw.strftime("%Y-%m-%d %H:%M:%S")
        end_utc   = end_tw.strftime("%Y-%m-%d %H:%M:%S")

        date_label = end_tw.strftime("%Y-%m-%d")
        sheet_tab  = f"日報 {date_label}"
        period_str = (
            f"{start_tw.strftime('%Y-%m-%d %H:%M')} ~ "
            f"{end_tw.strftime('%Y-%m-%d %H:%M')} (台灣時間)"
        )

        svc = get_sheets()
        ss  = svc.spreadsheets()

        # ── Read 用戶資料 ──────────────────────────────────────────────────────
        # A=user_id B=display_name C=pic_url D=status E=lang F=follow_at G=unfollow_at H=agent
        user_rows = sheets_get("用戶資料", "A:H")
        users: dict = {}
        for row in user_rows[1:]:
            if not row or not row[0]:
                continue
            uid = row[0]
            users[uid] = {
                "display_name": row[1] if len(row) > 1 else "",
                "pic_url":      row[2] if len(row) > 2 else "",
                "follow_at":    row[5] if len(row) > 5 else "",
                "unfollow_at":  row[6] if len(row) > 6 else "",
            }

        # ── Read 動作紀錄 ──────────────────────────────────────────────────────
        action_rows = sheets_get("動作紀錄", "A:E")
        period_actions = []
        for row in action_rows[1:]:
            if len(row) < 4:
                continue
            ts    = row[0]
            uid   = row[1]
            etype = row[3]
            content = row[4] if len(row) > 4 else ""
            if start_utc <= ts < end_utc:
                period_actions.append((ts, uid, etype, content))

        # ── Get personal sheet gid map ─────────────────────────────────────────
        # Personal sheets are named "display_name(last8chars)" or "(last8chars)".
        # Map last-8-char suffix → gid. If a user has no matching personal sheet,
        # the uid cell falls back to plain text (no hyperlink) to avoid "無效的範圍".
        import re as _re
        suffix_to_gid: dict = {}
        if GSHEET_PERSONAL_ID:
            personal_meta = ss.get(spreadsheetId=GSHEET_PERSONAL_ID).execute()
            for s in personal_meta.get("sheets", []):
                title = s["properties"]["title"]
                gid   = s["properties"]["sheetId"]
                m = _re.search(r'\(([A-Za-z0-9]{8})\)\s*$', title)
                if m:
                    suffix_to_gid[m.group(1)] = gid

        def get_personal_gid(uid: str):
            suffix = uid[-8:] if len(uid) >= 8 else uid
            return suffix_to_gid.get(suffix)  # None if no personal sheet exists

        # ── Classify users ─────────────────────────────────────────────────────
        new_user_ids: set = set()
        old_user_ids: set = set()
        for uid, u in users.items():
            fa = u["follow_at"]
            if fa and start_utc <= fa < end_utc:
                new_user_ids.add(uid)
            else:
                old_user_ids.add(uid)

        user_period_actions: dict = {}
        for (ts, uid, etype, content) in period_actions:
            user_period_actions.setdefault(uid, []).append((ts, etype, content))

        uids_clicked    = {uid for uid, acts in user_period_actions.items()
                           if any(e == "uri_click" for _, e, _ in acts)}
        uids_non_follow = {uid for uid, acts in user_period_actions.items()
                           if any(e != "follow" for _, e, _ in acts)}
        uids_only_text  = {uid for uid, acts in user_period_actions.items()
                           if all(e == "text" for _, e, _ in acts)}

        sec1_uids = sorted(uid for uid in new_user_ids if uid in uids_non_follow and uid not in uids_clicked)
        sec2_uids = sorted(uid for uid in old_user_ids if uid in uids_non_follow and uid not in uids_clicked)
        sec3_uids = sorted(uid for uid in new_user_ids if uid not in uids_non_follow)
        sec4_uids = sorted(uid for uid in old_user_ids if uid in uids_only_text)
        sec5_uids = sorted(uid for uid in new_user_ids if uid in uids_clicked)
        sec6_uids = sorted(uid for uid in old_user_ids if uid in uids_clicked)

        # ── Statistics ─────────────────────────────────────────────────────────
        # Use sec5+sec6 so 總人數 matches the displayed ①+② rows (ghost uids excluded)
        total_ever_clicked = len(sec5_uids) + len(sec6_uids)
        block_total    = 0
        block_same_day = 0
        for uid, u in users.items():
            ua = u.get("unfollow_at", "")
            fa = u.get("follow_at", "")
            if ua and start_utc <= ua < end_utc:
                block_total += 1
                if fa and start_utc <= fa < end_utc:
                    block_same_day += 1

        new_count          = len(new_user_ids)
        clicked_today_new  = len(sec5_uids)
        no_click_today_new = len(sec1_uids)
        silent_today_new   = len(sec3_uids)

        # ── Row position calculation ───────────────────────────────────────────
        STATS_ROWS = 8
        section_sizes = [len(sec5_uids), len(sec6_uids), len(sec1_uids),
                         len(sec2_uids), len(sec3_uids), len(sec4_uids)]

        def section_header_row(idx: int) -> int:
            row = STATS_ROWS
            for i in range(idx):
                row += 1 + 1 + 1 + section_sizes[i]  # blank + header + col-hdr + data
            row += 1 + 1  # blank + header for this section
            return row

        row_stat_click_header  = section_header_row(0)  # ① 當天點擊者明細
        row_stat_idle_header   = section_header_row(2)  # ③ 當天有互動但未點擊
        row_stat_silent_header = section_header_row(4)  # ⑤ 當天沉默成員

        # ── Find or create the 日報 sheet (in 每日統計) ────────────────────────
        line_meta   = ss.get(spreadsheetId=GSHEET_DAILY_ID).execute()
        line_sheets = line_meta.get("sheets", [])
        daily_gid   = None
        for s in line_sheets:
            if s["properties"]["title"] == sheet_tab:
                daily_gid = s["properties"]["sheetId"]
                break

        if daily_gid is not None:
            ss.values().clear(
                spreadsheetId=GSHEET_DAILY_ID,
                range=f"'{sheet_tab}'!A1:Z1000",
                body={}
            ).execute()
        else:
            res = ss.batchUpdate(
                spreadsheetId=GSHEET_DAILY_ID,
                body={"requests": [{"addSheet": {"properties": {"title": sheet_tab}}}]}
            ).execute()
            daily_gid = res["replies"][0]["addSheet"]["properties"]["sheetId"]

        # ── Move 日報 sheet to first position in 每日統計 ──────────────────────
        ss.batchUpdate(
            spreadsheetId=GSHEET_DAILY_ID,
            body={"requests": [{"updateSheetProperties": {
                "properties": {"sheetId": daily_gid, "index": 0},
                "fields": "index"
            }}]}
        ).execute()

        # ── Helper cells ───────────────────────────────────────────────────────
        def stat_hyperlink(section_row: int, label: str) -> str:
            return f'=HYPERLINK("#gid={daily_gid}&range=A{section_row}","{label}")'

        def user_id_cell(uid: str) -> str:
            gid = get_personal_gid(uid)
            if gid is not None:
                return f'=HYPERLINK("https://docs.google.com/spreadsheets/d/{GSHEET_PERSONAL_ID}/edit#gid={gid}&range=A1","{uid}")'
            return uid

        def label_click_content(content: str) -> str:
            if "[1x1]" in content or "[1X1]" in content:
                return "1x1"
            if "[FAQ]" in content or "[faq]" in content:
                return "FAQ"
            if "[扶植金]" in content:
                return "扶植金"
            return "agent"

        def user_base_row(uid: str) -> list:
            u = users.get(uid, {})
            return [user_id_cell(uid), u.get("display_name", ""),
                    u.get("pic_url", ""), u.get("follow_at", "")]

        def click_row(uid: str) -> list:
            u    = users.get(uid, {})
            acts = user_period_actions.get(uid, [])
            click_acts = [(ts, content) for (ts, etype, content) in acts if etype == "uri_click"]
            if not click_acts:
                last_btn, last_ts, total = "", "", 0
            else:
                last_ts, last_content = max(click_acts, key=lambda x: x[0])
                last_btn = label_click_content(last_content)
                total    = len(click_acts)
            return [user_id_cell(uid), u.get("display_name", ""), last_btn, last_ts, total]

        # ── Build data ─────────────────────────────────────────────────────────
        data = []
        data.append(["統計期間",                                         period_str])
        data.append(["新加入人數",                                       new_count])
        data.append(["點擊專員總人數",                                   total_ever_clicked])
        data.append([stat_hyperlink(row_stat_click_header,  "當天點擊專員人數"), clicked_today_new])
        data.append([stat_hyperlink(row_stat_idle_header,   "當天閒逛人數"),     no_click_today_new])
        data.append([stat_hyperlink(row_stat_silent_header, "當天潛水人數"),     silent_today_new])
        data.append(["封鎖人數（當天總封鎖）",                           block_total])
        data.append(["當天加入封鎖",                                     block_same_day])

        def add_section(title: str, col_headers: list, rows: list):
            data.append([])
            data.append([title])
            data.append(col_headers)
            data.extend(rows)

        add_section("① 當天點擊者明細",
                    ["user_id", "display_name", "最後點擊按鈕", "最後點擊時間", "總點擊次數"],
                    [click_row(uid) for uid in sec5_uids])
        add_section("② 點擊者明細（非當天加入）",
                    ["user_id", "display_name", "最後點擊按鈕", "最後點擊時間", "總點擊次數"],
                    [click_row(uid) for uid in sec6_uids])
        add_section("③ 當天有互動但未點擊",
                    ["user_id", "display_name", "大頭照URL", "加入時間"],
                    [user_base_row(uid) for uid in sec1_uids])
        add_section("④ 有互動但未點擊（非當天加入）",
                    ["user_id", "display_name", "大頭照URL", "加入時間"],
                    [user_base_row(uid) for uid in sec2_uids])
        add_section("⑤ 當天沉默成員（潛水）",
                    ["user_id", "display_name", "大頭照URL", "加入時間"],
                    [user_base_row(uid) for uid in sec3_uids])
        add_section("⑥ 沉默成員（非當天加入）",
                    ["user_id", "display_name", "大頭照URL", "加入時間"],
                    [user_base_row(uid) for uid in sec4_uids])

        # ── Write ──────────────────────────────────────────────────────────────
        ss.values().update(
            spreadsheetId=GSHEET_DAILY_ID,
            range=f"'{sheet_tab}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": data}
        ).execute()

        print(f"[report] {sheet_tab}: new={new_count}, clicked_today={clicked_today_new}, "
              f"①={len(sec1_uids)} ②={len(sec2_uids)} ③={len(sec3_uids)} "
              f"④={len(sec4_uids)} ⑤={len(sec5_uids)} ⑥={len(sec6_uids)}")
    except Exception as e:
        import traceback
        print(f"[report] ERROR: {e}")
        print(traceback.format_exc())

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

        # TW time boundaries for sheet comparison (sheet now stores TW time)
        start_utc = start_tw.strftime("%Y-%m-%d %H:%M:%S")
        end_utc = end_tw.strftime("%Y-%m-%d %H:%M:%S")

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
async def track_page(to: str = "", label: str = "", uid: str = "", agent: str = "", src: str = ""):
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
const uid = {json.dumps(uid)};
const agent = {json.dumps(agent)};
const src = {json.dumps(src)};

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
        user_id: userId || uid,
        display_name: displayName,
        destination: destination,
        label: label,
        agent: agent,
        src: src
      }})
    }}).catch(() => {{}});
  }} catch(e) {{
    // Still record with uid fallback on error
    fetch('/api/track', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{ user_id: userId || uid, display_name: '', destination: destination, label: label, agent: agent, src: src }})
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
    agent: str = ""
    src: str = ""

@app.post("/api/track")
async def api_track(payload: TrackPayload):
    ts = now_iso()
    uid = payload.user_id or "anonymous"
    if payload.label == "加入專員":
        name = payload.display_name or _user_cache.get(payload.user_id, "") or (f"用戶({payload.user_id[-8:]})" if payload.user_id else "匿名")
        agent_name = payload.agent or "未知"
        src = payload.src or ""
        src_tag = f" [{src}]" if src else ""
        content = f"加入專員 → {agent_name}{src_tag}"
        sheets_append("動作紀錄", [ts, uid, name, "uri_click", content])
        _recent_actions.append((ts, name, content))
        if payload.user_id:
            log_to_personal_sheet(payload.user_id, name, "uri_click", content, ts)
        src_line = f"\n來源按鈕：{src}" if src else ""
        await notify_tg(f"🔗 點擊專員連結\n用戶：{name}\n專員：{agent_name}{src_line}")
        return {"status": "ok"}
    content = f"{payload.label} → {payload.destination}" if payload.label else payload.destination
    sheets_append("動作紀錄", [ts, uid, payload.display_name or "", "uri_click", content])
    if payload.user_id:
        dname = payload.display_name or payload.user_id
        log_to_personal_sheet(payload.user_id, dname, "uri_click", content, ts)
    _recent_actions.append((ts, payload.display_name or "匿名", content))
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
        elif etype == "message" and event.get("message", {}).get("type") == "sticker":
            asyncio.create_task(handle_sticker(uid, event["message"]))
        elif etype == "postback":
            asyncio.create_task(handle_postback(uid, event.get("postback", {}).get("data", ""), rtoken))

    return {"status": "ok"}
