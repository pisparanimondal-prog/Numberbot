# === ENGINE: shared state, helpers, SMS engine, keyboards, monitoring ===
import logging
import os
import asyncio
from io import BytesIO, StringIO
from datetime import datetime, timedelta
import csv
import time
import re
import json
from urllib.parse import quote as _url_quote

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from motor.motor_asyncio import AsyncIOMotorClient
import pytz
import pycountry
import aiohttp

# Import all configurations from config.py
from config import *

# === GLOBAL VARIABLES ===
TIMEZONE = pytz.timezone(TIMEZONE_NAME)
logging.basicConfig(level=getattr(logging, LOGGING_LEVEL))

# Session management - initialize from config
CURRENT_SMS_API_COOKIE = SMS_API_COOKIE
logging.info(f"🔑 Initialized SMS API session from config: {CURRENT_SMS_API_COOKIE[:20]}...{CURRENT_SMS_API_COOKIE[-10:]}")

# Admin notification rate limiting
last_api_failure_notification = {}  # Track last notification time for each failure type

# Bot state variables
uploaded_csv = {}  # Admin user_id -> in-memory CSV bytes for the current upload
user_states = {}  # Store user states for country input
manual_numbers = {}  # Store manual numbers for each user
add_service = {}  # Admin user_id -> service tag for the current /addservice upload


def reset_add_flow(user_id: int) -> None:
    """Clear all per-admin add/addservice state."""
    user_states.pop(user_id, None)
    manual_numbers.pop(user_id, None)
    add_service.pop(user_id, None)
    add_service.pop(f"{user_id}_country", None)
    uploaded_csv.pop(user_id, None)
current_user_numbers = {}  # Track current number for each user
user_monitoring_sessions = {}  # Track multiple monitoring sessions per user
active_number_monitors = {}  # Store active monitors for each number
change_number_last_press = {}  # user_id -> unix-ts of last "Change Number" tap (6s cooldown)
CHANGE_NUMBER_COOLDOWN_SECONDS = 6

# Shared aiohttp session — reused across ALL SMS-panel calls so we don't
# pay TLS/TCP handshake cost on every poll. Created lazily on first use
# and survives for the bot's lifetime.
_shared_http_session = None
_shared_http_session_lock = asyncio.Lock()

async def get_shared_http_session():
    """Return a long-lived aiohttp.ClientSession with a sane connection pool.
    Safe to call from many concurrent coroutines."""
    global _shared_http_session
    if _shared_http_session is not None and not _shared_http_session.closed:
        return _shared_http_session
    async with _shared_http_session_lock:
        if _shared_http_session is None or _shared_http_session.closed:
            connector = aiohttp.TCPConnector(
                limit=200,             # total simultaneous sockets
                limit_per_host=50,     # per panel
                ttl_dns_cache=300,     # cache DNS 5 min
                enable_cleanup_closed=True,
            )
            _shared_http_session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=15),
            )
    return _shared_http_session

# PERFORMANCE OPTIMIZATION: Cache for country data to avoid repeated DB queries
countries_cache = None
countries_cache_time = None

def clear_countries_cache():
    """Clear the countries cache to force refresh"""
    global countries_cache, countries_cache_time
    countries_cache = None
    countries_cache_time = None
    logging.info("Countries cache cleared")

# === SESSION MANAGEMENT FUNCTIONS ===
def reload_config_session():
    """Reload SMS API session from config file"""
    global CURRENT_SMS_API_COOKIE
    try:
        import importlib
        import config
        importlib.reload(config)
        
        old_session = CURRENT_SMS_API_COOKIE
        CURRENT_SMS_API_COOKIE = config.SMS_API_COOKIE
        
        if old_session != CURRENT_SMS_API_COOKIE:
            logging.info(f"🔄 SMS session reloaded from config file")
            logging.info(f"🔑 Old: {old_session[:20]}...{old_session[-10:]}")
            logging.info(f"🔑 New: {CURRENT_SMS_API_COOKIE[:20]}...{CURRENT_SMS_API_COOKIE[-10:]}")
            return True
        return False
    except Exception as e:
        logging.error(f"❌ Failed to reload config session: {e}")
        return False

def get_current_sms_cookie():
    """Get the current active SMS API cookie"""
    return CURRENT_SMS_API_COOKIE

def update_runtime_session(new_cookie):
    """Update the runtime session without modifying config file"""
    global CURRENT_SMS_API_COOKIE
    old_session = CURRENT_SMS_API_COOKIE
    CURRENT_SMS_API_COOKIE = new_cookie
    logging.info(f"🔄 Runtime SMS session updated")
    logging.info(f"🔑 Old: {old_session[:20]}...{old_session[-10:]}")
    logging.info(f"🔑 New: {CURRENT_SMS_API_COOKIE[:20]}...{CURRENT_SMS_API_COOKIE[-10:]}")

def update_config_file_session(new_cookie):
    """Update the session in config.py file"""
    try:
        with open('config.py', 'r') as f:
            config_content = f.read()
        
        # Replace the SMS_API_COOKIE line
        import re
        config_content = re.sub(
            r'SMS_API_COOKIE = "[^"]*"',
            f'SMS_API_COOKIE = "{new_cookie}"',
            config_content
        )
        
        with open('config.py', 'w') as f:
            f.write(config_content)
        
        logging.info(f"✅ Config file updated with new session")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to update config file: {e}")
        return False

def derive_referer(base_url, endpoint):
    """Derive a sensible Referer URL from the endpoint path.
    Example: '/ints/agent/res/data_smscdr.php' -> '{base_url}/ints/agent/SMSCDRReports'
             '/agent/res/data_smscdr.php'      -> '{base_url}/agent/SMSCDRReports'
    Falls back to the standard '/ints/agent/SMSCDRReports' if the path is unfamiliar.
    """
    try:
        if "/res/" in endpoint:
            prefix = endpoint.split("/res/")[0]
            return f"{base_url}{prefix}/SMSCDRReports"
    except Exception:
        pass
    return f"{base_url}/ints/agent/SMSCDRReports"


def add_sms_api_to_config(name, base_url, cookie, endpoint="/ints/agent/res/data_smscdr.php"):
    """Append a new panel dict to the SMS_APIS list in config.py.
    Returns (True, msg) on success, (False, error_msg) on failure.
    Refuses to add a panel whose name already exists (case-insensitive)."""
    global SMS_APIS
    try:
        import re
        # Refuse duplicates
        for p in SMS_APIS:
            if p.get("name", "").lower() == name.lower():
                return False, f"A panel named '{name}' already exists."

        with open('config.py', 'r') as f:
            content = f.read()

        # Find the REAL (uncommented) SMS_APIS = [ ... ] block.
        # Must be at the start of a line — skip commented example blocks
        # like "# SMS_APIS = [" inside the admin guide.
        start_match = re.search(r'^SMS_APIS\s*=\s*\[', content, re.MULTILINE)
        if not start_match:
            return False, "Could not find SMS_APIS list in config.py."

        # Walk the brackets to find the matching closing ']'
        depth = 0
        i = start_match.end() - 1  # at the '['
        close_idx = -1
        while i < len(content):
            ch = content[i]
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    close_idx = i
                    break
            i += 1
        if close_idx == -1:
            return False, "Could not find end of SMS_APIS list in config.py."

        # Build the new dict block, with proper indentation and trailing comma.
        # Escape quotes/backslashes in the cookie/url just in case.
        def _esc(s):
            return s.replace('\\', '\\\\').replace('"', '\\"')

        new_entry = (
            "    {\n"
            f'        "name": "{_esc(name)}",\n'
            f'        "base_url": "{_esc(base_url)}",\n'
            f'        "endpoint": "{_esc(endpoint)}",\n'
            f'        "cookie": "{_esc(cookie)}",\n'
            "    },\n"
        )

        # Insert just before the closing ']'. Walk back to skip whitespace,
        # so we put the new entry on its own line right above ']'.
        insert_at = close_idx
        # Ensure there is a newline before our insert
        prefix = content[:insert_at]
        if not prefix.endswith("\n"):
            new_entry = "\n" + new_entry
        new_content = content[:insert_at] + new_entry + content[insert_at:]

        with open('config.py', 'w') as f:
            f.write(new_content)

        # Reload config so SMS_APIS picks up the new entry immediately
        try:
            import importlib
            import config as _cfg
            importlib.reload(_cfg)
            SMS_APIS = _cfg.SMS_APIS
        except Exception as reload_err:
            logging.warning(f"Config rewritten but in-memory reload failed: {reload_err}")

        logging.info(f"✅ Added panel '{name}' to SMS_APIS")
        return True, f"Panel '{name}' added."
    except Exception as e:
        logging.error(f"❌ Failed to add panel: {e}")
        return False, str(e)


def remove_sms_api_from_config(panel_name):
    """Remove a panel dict from the SMS_APIS list in config.py by name.
    Returns (True, msg) on success, (False, err) on failure."""
    global SMS_APIS
    try:
        import re

        with open('config.py', 'r') as f:
            content = f.read()

        # Restrict the search to the REAL (uncommented) SMS_APIS list,
        # so we never match a name inside the commented admin-guide examples.
        list_start = re.search(r'^SMS_APIS\s*=\s*\[', content, re.MULTILINE)
        if not list_start:
            return False, "Could not find SMS_APIS list in config.py."
        depth = 0
        i = list_start.end() - 1
        list_close = -1
        while i < len(content):
            ch = content[i]
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    list_close = i
                    break
            i += 1
        if list_close == -1:
            return False, "Could not find end of SMS_APIS list in config.py."

        list_region_start = list_start.end()
        list_region_end = list_close

        # Find the dict block whose "name" matches panel_name (within the list only)
        name_pat = re.compile(r'"name"\s*:\s*"' + re.escape(panel_name) + r'"', re.IGNORECASE)
        m = name_pat.search(content, list_region_start, list_region_end)
        if not m:
            return False, f"Panel '{panel_name}' not found in config."

        open_idx = content.rfind('{', list_region_start, m.start())
        close_idx = content.find('}', m.end(), list_region_end)
        if open_idx == -1 or close_idx == -1:
            return False, f"Could not isolate dict block for '{panel_name}'."

        # Extend to swallow a trailing comma and the surrounding whitespace/newline
        end = close_idx + 1
        while end < len(content) and content[end] in ' \t':
            end += 1
        if end < len(content) and content[end] == ',':
            end += 1
        # Swallow up to and including the next newline
        while end < len(content) and content[end] in ' \t':
            end += 1
        if end < len(content) and content[end] == '\n':
            end += 1

        # Trim leading whitespace before the block on the same line
        start = open_idx
        while start > 0 and content[start - 1] in ' \t':
            start -= 1

        new_content = content[:start] + content[end:]
        with open('config.py', 'w') as f:
            f.write(new_content)

        try:
            import importlib
            import config as _cfg
            importlib.reload(_cfg)
            SMS_APIS = _cfg.SMS_APIS
        except Exception as reload_err:
            logging.warning(f"Config rewritten but in-memory reload failed: {reload_err}")

        logging.info(f"✅ Removed panel '{panel_name}' from SMS_APIS")
        return True, f"Panel '{panel_name}' removed."
    except Exception as e:
        logging.error(f"❌ Failed to remove panel: {e}")
        return False, str(e)


def update_panel_cookie_in_config(panel_name, new_cookie):
    """Update the cookie of a named panel inside the SMS_APIS list in config.py.
    Finds the dict literal in the file whose "name" key equals panel_name
    (case-insensitive) and rewrites only its "cookie" string value.
    Robust to field ordering and to characters like `}` inside the cookie
    (we only match a quoted-string cookie value).
    Returns True on success.
    """
    global SMS_APIS
    try:
        import re
        with open('config.py', 'r') as f:
            content = f.read()

        # Restrict the search to the REAL (uncommented) SMS_APIS list, so we
        # never match a name inside the commented admin-guide examples.
        list_start = re.search(r'^SMS_APIS\s*=\s*\[', content, re.MULTILINE)
        if not list_start:
            logging.error("❌ Could not find SMS_APIS list in config.py")
            return False
        depth = 0
        i = list_start.end() - 1
        list_close = -1
        while i < len(content):
            ch = content[i]
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    list_close = i
                    break
            i += 1
        if list_close == -1:
            logging.error("❌ Could not find end of SMS_APIS list in config.py")
            return False
        region_start = list_start.end()
        region_end = list_close

        # Step 1: locate the dict block whose "name" key equals panel_name,
        # restricted to the real list region.
        name_pattern = re.compile(
            r'"name"\s*:\s*"' + re.escape(panel_name) + r'"',
            re.IGNORECASE,
        )
        match = name_pattern.search(content, region_start, region_end)
        if not match:
            logging.error(f"❌ Panel '{panel_name}' not found in SMS_APIS")
            return False

        # Step 2: from the name match, walk outward to find the enclosing { ... }.
        open_idx = content.rfind('{', region_start, match.start())
        close_idx = content.find('}', match.end(), region_end)
        if open_idx == -1 or close_idx == -1:
            logging.error(f"❌ Could not isolate dict block for panel '{panel_name}'")
            return False

        block = content[open_idx:close_idx + 1]

        # Step 3: replace ONLY a quoted-string cookie value within this block.
        # Variable-reference cookies (e.g. `"cookie": SMS_API_COOKIE,`) are
        # intentionally skipped — those are kept in sync via SMS_API_COOKIE.
        cookie_pattern = re.compile(r'("cookie"\s*:\s*")[^"]*(")')
        new_block, count = cookie_pattern.subn(
            lambda m: m.group(1) + new_cookie + m.group(2), block, count=1
        )
        if count == 0:
            logging.warning(
                f"Panel '{panel_name}' uses a variable-reference cookie; "
                f"no quoted cookie to replace in the SMS_APIS entry."
            )
            return False

        new_content = content[:open_idx] + new_block + content[close_idx + 1:]
        with open('config.py', 'w') as f:
            f.write(new_content)

        # Reload runtime SMS_APIS so the new cookie takes effect immediately
        try:
            import importlib
            import config as _cfg
            importlib.reload(_cfg)
            SMS_APIS = _cfg.SMS_APIS
        except Exception as reload_err:
            logging.warning(f"Config rewritten but in-memory reload failed: {reload_err}")

        logging.info(f"✅ Updated cookie for panel '{panel_name}' in config.py")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to update panel cookie: {e}")
        return False


def set_otp_group_in_config(group_chat_id):
    """Set / update the global `OTP_GROUP_CHAT_ID` in config.py — the single
    group that receives every OTP from every panel for every user.
    Returns (True, msg) on success, (False, error_msg) otherwise."""
    global OTP_GROUP_CHAT_ID
    try:
        import re
        with open('config.py', 'r') as f:
            content = f.read()

        pat = re.compile(r'^OTP_GROUP_CHAT_ID\s*=\s*-?\d+\s*$', re.MULTILINE)
        new_line = f"OTP_GROUP_CHAT_ID = {int(group_chat_id)}"
        if pat.search(content):
            new_content = pat.sub(new_line, content, count=1)
        else:
            new_content = content.rstrip() + f"\n\n{new_line}\n"

        with open('config.py', 'w') as f:
            f.write(new_content)

        # Hot-reload in memory
        reload_failed = None
        try:
            import importlib
            import config as _cfg
            importlib.reload(_cfg)
            OTP_GROUP_CHAT_ID = _cfg.OTP_GROUP_CHAT_ID
        except Exception as reload_err:
            reload_failed = str(reload_err)
            logging.warning(f"Config rewritten but in-memory reload failed: {reload_err}")

        if reload_failed:
            return True, (
                f"OTP group saved to config.py as <code>{int(group_chat_id)}</code>, "
                f"but the in-memory reload failed ({reload_failed}). "
                "Restart the bot to apply."
            )
        return True, f"OTP group is now <code>{int(group_chat_id)}</code>. Every captured OTP will be sent here."
    except Exception as e:
        logging.error(f"❌ Failed to set OTP group: {e}")
        return False, f"Error: {e}"


def _mask_phone_number(phone_number):
    """Mask the middle digits of a phone number, e.g. +5093XXXXXXX929."""
    s = str(phone_number or "").strip()
    if not s:
        return ""
    plus = "+" if s.startswith("+") else ""
    digits = s.lstrip("+")
    if len(digits) <= 7:
        return f"{plus}{digits}"
    head = digits[:4]
    tail = digits[-3:]
    middle = "X" * (len(digits) - len(head) - len(tail))
    return f"{plus}{head}{middle}{tail}"


def _format_rocket_otp_message(country_name, country_flag, phone_number, service, otp):
    """Build the HUNTER OTP BOT styled message used in group chats."""
    country_line = shorten_country_name((country_name or "Unknown").strip())
    if country_flag:
        country_line = f"{country_line} {country_flag}"
    return (
        "🎉 NEW OTP RECEIVED 🎉\n\n"
        f"🌍 Country: {country_line}\n"
        f"📱 Number: {_mask_phone_number(phone_number)}\n"
        f"🚨 Service: {service or 'Unknown'}\n"
        f"🔓 OTP: `{otp or 'N/A'}`"
    )


async def forward_otp_to_group(
    context,
    legacy_text=None,
    *,
    otp=None,
    phone_number="",
    service="",
    country_name="",
    country_flag="",
):
    """Forward every captured OTP to the SINGLE global OTP group
    (config.OTP_GROUP_CHAT_ID). This group also acts as the safety net:
    if a user never opened a DM with the bot, the OTP still lands here.
    Silent on failure (logged only) so it never blocks other delivery."""
    group_id = OTP_GROUP_CHAT_ID
    if not group_id:
        return
    if otp:
        text = _format_rocket_otp_message(
            country_name, country_flag, phone_number, service, otp
        )
    else:
        text = legacy_text or ""
    if not text:
        return
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(GROUP_BUTTON_COMMUNITY_TEXT, url=GROUP_BUTTON_COMMUNITY_URL),
        InlineKeyboardButton(GROUP_BUTTON_NUMBER_TEXT, url=GROUP_BUTTON_NUMBER_URL),
    ]])
    try:
        await context.bot.send_message(
            chat_id=group_id, text=text, reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN,
        )
        logging.info(f"📤 Forwarded OTP to group {group_id}")
    except Exception as e:
        logging.error(f"Failed to forward OTP to group {group_id}: {e}")

# === ADMIN NOTIFICATION FUNCTIONS ===
async def notify_admins_api_failure(failure_type):
    """Notify all admins about SMS API failure with rate limiting"""
    try:
        # Rate limiting - only send notification once per 10 minutes for same failure type
        current_time = datetime.now(TIMEZONE)
        if failure_type in last_api_failure_notification:
            time_diff = (current_time - last_api_failure_notification[failure_type]).total_seconds()
            if time_diff < 600:  # 10 minutes
                logging.info(f"🔇 API failure notification rate limited for {failure_type}")
                return
        
        last_api_failure_notification[failure_type] = current_time
        
        from telegram import Bot
        bot = Bot(token=TOKEN)
        
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        panel_count = len(SMS_APIS)

        if failure_type == "session_expired":
            message = (
                f"🚨 **All SMS Panels Failed — Session Expired**\n\n"
                f"⏰ **Time**: {current_time_str}\n"
                f"🌐 **Panels tried**: {panel_count}\n\n"
                f"❌ **Issue**: Every panel returned a login page (sessions expired).\n\n"
                f"🔧 **Required Action**:\n"
                f"• Run `/checkapi` to see which panel(s) are down\n"
                f"• Refresh a cookie: `/updatesms <Panel> PHPSESSID=...`\n"
                f"• Or edit `config.py` and run `/reloadsession`\n\n"
                f"⚠️ **Impact**: OTP detection currently not working"
            )
        elif failure_type == "connection_error":
            message = (
                f"🚨 **All SMS Panels Failed — Connection Error**\n\n"
                f"⏰ **Time**: {current_time_str}\n"
                f"🌐 **Panels tried**: {panel_count}\n\n"
                f"❌ **Issue**: Cannot reach any SMS panel\n"
                f"🔧 **Possible Causes**:\n"
                f"• Server(s) down\n"
                f"• Network connectivity issues\n"
                f"• Firewall blocking requests\n\n"
                f"💡 **Suggestions**: Run `/checkapi` to see per-panel status.\n\n"
                f"⚠️ **Impact**: OTP detection currently not working"
            )
        elif failure_type == "access_blocked":
            message = (
                f"🚨 **All SMS Panels Failed — Access Blocked**\n\n"
                f"⏰ **Time**: {current_time_str}\n"
                f"🌐 **Panels tried**: {panel_count}\n\n"
                f"❌ **Issue**: Direct script access not allowed\n"
                f"🔧 **Required Action**:\n"
                f"• Log in to the failing panel manually\n"
                f"• Refresh its cookie: `/updatesms <Panel> PHPSESSID=...`\n\n"
                f"⚠️ **Impact**: OTP detection currently not working"
            )
        else:
            message = (
                f"🚨 **SMS API Error**\n\n"
                f"⏰ **Time**: {current_time_str}\n"
                f"🌐 **Panels tried**: {panel_count}\n"
                f"❌ **Issue**: {failure_type}\n\n"
                f"🔧 **Suggestion**: Use `/checkapi` to diagnose\n"
                f"⚠️ **Impact**: OTP detection may not be working"
            )
        
        # Send to all admins
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    chat_id=admin_id,
                    text=message,
                    parse_mode=ParseMode.MARKDOWN
                )
                logging.info(f"📢 API failure notification sent to admin {admin_id}")
            except Exception as e:
                logging.error(f"❌ Failed to notify admin {admin_id}: {e}")

    except Exception as e:
        logging.error(f"❌ Failed to send admin notifications: {e}")

async def notify_admins_api_recovery():
    """Notify all admins about successful API recovery"""
    try:
        from telegram import Bot
        bot = Bot(token=TOKEN)
        
        current_time = datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
        current_session = get_current_sms_cookie()
        
        message = (
            f"✅ **SMS API Auto-Recovery Successful**\n\n"
            f"⏰ **Time**: {current_time}\n"
            f"🔑 **New Session**: `{current_session[:20]}...{current_session[-10:]}`\n"
            f"📡 **Endpoint**: {SMS_API_BASE_URL}\n\n"
            f"🔄 **What Happened**:\n"
            f"• Session expired and was detected\n"
            f"• Auto-reloaded from config.py file\n"
            f"• API connection restored\n\n"
            f"✅ **Status**: OTP detection fully operational\n"
            f"💡 **Tip**: Use `/checkapi` to verify health"
        )
        
        # Send to all admins
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    chat_id=admin_id,
                    text=message,
                    parse_mode=ParseMode.MARKDOWN
                )
                logging.info(f"📢 API recovery notification sent to admin {admin_id}")
            except Exception as e:
                logging.error(f"❌ Failed to notify admin {admin_id}: {e}")

    except Exception as e:
        logging.error(f"❌ Failed to send recovery notifications: {e}")

# === UTILITY FUNCTIONS ===
async def send_lol_message(update: Update):
    """Send a fun message when users try to use admin commands"""
    await update.message.reply_text("Lol")

def extract_otp_from_message(message):
    """Extract OTP from SMS message using patterns from config"""
    if not message:
        return None
    
    message_lower = message.lower()
    logging.info(f"Extracting OTP from message: {message}")
    
    for pattern in OTP_PATTERNS:
        match = re.search(pattern, message_lower)
        if match:
            otp = match.group(1)
            # Validate that it's actually an OTP (not just any number)
            if len(otp) >= 4 and len(otp) <= 6 and otp.isdigit():
                logging.info(f"Found OTP: {otp} using pattern: {pattern}")
                return otp
    
    logging.info(f"No OTP found in message: {message}")
    return None

def get_country_flag(country_code):
    """Get country flag emoji from country code"""
    try:
        country_code = country_code.upper()
        if country_code == 'XK':
            return '🇽🇰'
        # Handle custom country codes (like "india_ws", "india_tg")
        if "_" in country_code or len(country_code) > 2:
            # Try to extract a valid country code from the custom name
            if country_code.startswith("INDIA"):
                return '🇮🇳'
            elif country_code.startswith("SAUDI") or country_code.startswith("SA"):
                return '🇸🇦'
            elif country_code.startswith("USA") or country_code.startswith("US"):
                return '🇺🇸'
            elif country_code.startswith("UK") or country_code.startswith("GB"):
                return '🇬🇧'
            elif country_code.startswith("SRI") or country_code.startswith("LK"):
                return '🇱🇰'
            else:
                return '🌐'
        if len(country_code) != 2 or not country_code.isalpha():
            return '🌐'
        offset = ord('🇦') - ord('A')
        return chr(ord(country_code[0]) + offset) + chr(ord(country_code[1]) + offset)
    except:
        return '🌐'

def clean_number(number):
    """Convert numbers to proper string format"""
    if isinstance(number, float) and number.is_integer():
        return str(int(number))
    return str(number).replace(" ", "").replace("-", "").replace(".", "")

def extract_country_from_range(range_str):
    """Extract country name from range string using intelligent parsing"""
    if not range_str:
        return None
    
    range_str = str(range_str).lower()
    
    # Remove common non-country words and patterns
    patterns_to_remove = [
        r'\(.*?\)', r'\[.*?\]', r'\d+', r'[-–_/\\|]',
        r'\bwhatsapp\b', r'\bws\b', r'\bbmet\b', r'\bsms\b'
    ]
    
    for pattern in patterns_to_remove:
        range_str = re.sub(pattern, ' ', range_str)
    
    # Try to find country match with pycountry
    try:
        matches = pycountry.countries.search_fuzzy(range_str.strip())
        if matches:
            return matches[0].alpha_2.lower()
    except:
        pass
    
    return None

def detect_country_code(number, range_str=None):
    """Detect country code from number and range string using config prefixes"""
    # First try to detect from range string
    if range_str:
        country_code = extract_country_from_range(range_str)
        if country_code:
            return country_code
    
    # Then try to detect from number prefix
    number = clean_number(str(number))
    
    # Check if number starts with known prefix from config
    for prefix, code in COUNTRY_PREFIXES.items():
        if number.startswith(prefix):
            return code
    
    return None


# === SERVICE AUTO-DETECTION ===
# Scan the SMS body (and sender ID) for known brand keywords so the bot
# always shows a clean, consistent service name + emoji even when the
# panel sends junk sender IDs like "FB-2", "VERIFY", or a numeric short code.
# Order matters: more specific patterns first.
SERVICE_PATTERNS = [
    ("Facebook",   "📘", [r"\bfacebook\b", r"\bfb[-\s:]"]),
    ("Instagram",  "📷", [r"\binstagram\b", r"\binsta\b"]),
    ("WhatsApp",   "🟢", [r"\bwhatsapp\b", r"\bwa[-\s:]"]),
    ("Telegram",   "✈️", [r"\btelegram\b"]),
    ("Google",     "🔍", [r"\bgoogle\b", r"\bgmail\b", r"\bg-\d"]),
    ("Microsoft",  "🪟", [r"\bmicrosoft\b", r"\boutlook\b", r"\bhotmail\b", r"\bazure\b"]),
    ("Apple",      "🍎", [r"\bapple\b", r"\bicloud\b"]),
    ("Amazon",     "📦", [r"\bamazon\b"]),
    ("TikTok",     "🎵", [r"\btiktok\b"]),
    ("Snapchat",   "👻", [r"\bsnapchat\b"]),
    ("Discord",    "🎮", [r"\bdiscord\b"]),
    ("Netflix",    "🎬", [r"\bnetflix\b"]),
    ("PayPal",     "💳", [r"\bpaypal\b"]),
    ("LinkedIn",   "💼", [r"\blinkedin\b"]),
    ("Twitter",    "🐦", [r"\btwitter\b", r"\bx\.com\b"]),
    ("Uber",       "🚗", [r"\buber\b"]),
    ("Signal",     "💬", [r"\bsignal\b"]),
    ("Viber",      "💜", [r"\bviber\b"]),
    ("Binance",    "🪙", [r"\bbinance\b"]),
    ("Coinbase",   "🪙", [r"\bcoinbase\b"]),
]


def detect_service_from_message(body, sender_fallback=None):
    """Return (service_name, emoji) from the SMS body. Falls back to scanning
    the sender ID, then to the raw sender (with a generic icon), then to
    ('Unknown', '🚨'). Always returns a 2-tuple of strings."""
    if body:
        text = str(body).lower()
        for name, emoji, patterns in SERVICE_PATTERNS:
            for pat in patterns:
                if re.search(pat, text):
                    return name, emoji
    if sender_fallback:
        s = str(sender_fallback).lower()
        for name, emoji, patterns in SERVICE_PATTERNS:
            for pat in patterns:
                if re.search(pat, s):
                    return name, emoji
        return str(sender_fallback), "🚨"
    return "Unknown", "🚨"


_COUNTRY_SHORT_NAMES = {
    "united kingdom": "UK",
    "united states": "USA",
    "united states of america": "USA",
    "united arab emirates": "UAE",
    "russian federation": "Russia",
    "korea, republic of": "South Korea",
    "korea, democratic people's republic of": "North Korea",
    "iran, islamic republic of": "Iran",
    "viet nam": "Vietnam",
    "lao people's democratic republic": "Laos",
    "tanzania, united republic of": "Tanzania",
    "bolivia, plurinational state of": "Bolivia",
    "venezuela, bolivarian republic of": "Venezuela",
    "moldova, republic of": "Moldova",
    "syrian arab republic": "Syria",
    "türkiye": "Turkey",
    "turkiye": "Turkey",
    "côte d'ivoire": "Ivory Coast",
    "cote d'ivoire": "Ivory Coast",
    "macedonia, the former yugoslav republic of": "North Macedonia",
    "north macedonia": "N. Macedonia",
    "congo, the democratic republic of the": "DR Congo",
    "congo, democratic republic of the": "DR Congo",
    "saint vincent and the grenadines": "St. Vincent",
    "trinidad and tobago": "Trinidad",
    "bosnia and herzegovina": "Bosnia",
    "dominican republic": "Dominican Rep.",
    "central african republic": "CAR",
    "saudi arabia": "Saudi Arabia",
    "south africa": "South Africa",
    "papua new guinea": "Papua N.G.",
    "saint kitts and nevis": "St. Kitts",
    "saint lucia": "St. Lucia",
    "antigua and barbuda": "Antigua",
    "sao tome and principe": "Sao Tome",
    "são tomé and príncipe": "Sao Tome",
    "equatorial guinea": "Eq. Guinea",
    "burkina faso": "Burkina Faso",
    "sierra leone": "Sierra Leone",
    "sri lanka": "Sri Lanka",
    "new zealand": "New Zealand",
    "el salvador": "El Salvador",
    "costa rica": "Costa Rica",
    "puerto rico": "Puerto Rico",
    "hong kong": "Hong Kong",
    "south sudan": "S. Sudan",
    "north korea": "N. Korea",
    "south korea": "S. Korea",
    "myanmar": "Myanmar",
    "czechia": "Czech Rep.",
    "czech republic": "Czech Rep.",
}


def shorten_country_name(name):
    """Return a short, chat-friendly version of a country name.

    Looks up the lowercased input in the short-name map; otherwise returns
    the original name unchanged. Safe to call on None / empty strings.
    """
    if not name:
        return name
    return _COUNTRY_SHORT_NAMES.get(str(name).strip().lower(), name)


def resolve_country_display(phone_number, range_str=None, fallback_name=None):
    """Return (country_name, country_flag) for a phone number using the
    existing prefix table + pycountry + get_country_flag. Falls back to
    fallback_name (or 'Unknown') with a globe emoji when detection fails."""
    # Strip leading "+" / "00" so prefix matching works ("+92..." -> "92...").
    normalized = str(phone_number or "").strip().lstrip("+")
    if normalized.startswith("00"):
        normalized = normalized[2:]
    code = detect_country_code(normalized, range_str)
    if code:
        flag = get_country_flag(code)
        try:
            country = pycountry.countries.get(alpha_2=code.upper())
            name = country.name if country else (fallback_name or "Unknown")
        except Exception:
            name = fallback_name or "Unknown"
        return shorten_country_name(name), flag
    return shorten_country_name(fallback_name or "Unknown"), "🌐"


# === KEYBOARDS ===
def join_channel_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Join Channel", url=CHANNEL_LINK)],
        [InlineKeyboardButton("✅ Check Join", callback_data="check_join")]
    ])

# Service-selection reply keyboard shown under the welcome message.
SERVICE_BUTTONS = ["📘 FACEBOOK", "🟢 WHATSAPP", "✈️ TELEGRAM", "📱 OTHER"]


def service_keyboard():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(SERVICE_BUTTONS[0]), KeyboardButton(SERVICE_BUTTONS[1])],
            [KeyboardButton(SERVICE_BUTTONS[2]), KeyboardButton(SERVICE_BUTTONS[3])],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )

def _service_query_filter(service):
    """Build a Mongo filter for the `numbers` collection that matches docs
    tagged with the given service. Untagged numbers are also included so
    legacy data without a service tag remains visible to all services."""
    if not service:
        return {}
    return {"$or": [{"service": service.lower()}, {"service": {"$exists": False}}, {"service": None}]}


async def countries_keyboard(db, service=None):
    """Build the country-selection inline keyboard.

    If `service` is provided, only countries that have at least one number
    matching that service (or untagged) are shown. Otherwise all countries
    are shown."""
    global countries_cache, countries_cache_time
    from datetime import datetime, timedelta

    if service:
        # Service-filtered listing — query live; do not use the global cache.
        coll = db[COLLECTION_NAME]
        countries_coll = db[COUNTRIES_COLLECTION]
        match = _service_query_filter(service)
        pipeline = [
            {"$match": match},
            {"$group": {"_id": "$country_code", "count": {"$sum": 1}}},
        ]
        service_counts = {}
        async for doc in coll.aggregate(pipeline):
            cc = doc.get("_id")
            if cc:
                service_counts[cc] = doc.get("count", 0)
        country_codes = list(service_counts.keys())
        if not country_codes:
            countries_data = []
        else:
            countries_data = await countries_coll.find(
                {"country_code": {"$in": country_codes}}
            ).to_list(length=None)
            for c in countries_data:
                c["_live_count"] = service_counts.get(c.get("country_code"), 0)
            countries_data.sort(
                key=lambda x: x.get("display_name", x.get("country_code", ""))
            )
    else:
        # PERFORMANCE OPTIMIZATION: Use cache if available and fresh (5 minutes)
        now = datetime.now()
        if countries_cache and countries_cache_time and (now - countries_cache_time) < timedelta(minutes=5):
            logging.info("Using cached countries data")
            countries_data = countries_cache
        else:
            logging.info("Refreshing countries cache")
            countries_coll = db[COUNTRIES_COLLECTION]
            countries_data = await countries_coll.find({}).to_list(length=None)
            countries_data.sort(
                key=lambda x: x.get("display_name", x.get("country_code", ""))
            )
            countries_cache = countries_data
            countries_cache_time = now

    buttons = []
    for country_info in countries_data:
        country_code = country_info.get("country_code")
        if not country_code:
            continue

        if "display_name" in country_info:
            display_name = country_info["display_name"]
            detected_country = country_info.get("detected_country", country_code)
            flag = get_country_flag(detected_country)
        else:
            try:
                country = pycountry.countries.get(alpha_2=country_code.upper())
                display_name = country.name if country else country_code
            except:
                display_name = country_code
            flag = get_country_flag(country_code)

        # Available-number count: live per-service count when filtered,
        # otherwise the cached country-level total.
        if service:
            available_count = country_info.get("_live_count", 0)
        else:
            available_count = country_info.get("number_count", 0)

        # Skip empty buckets so users never see a "(0)" button.
        if available_count <= 0:
            continue

        buttons.append([InlineKeyboardButton(
            f"{flag} {display_name} ({available_count})",
            callback_data=f"country_{country_code}"
        )])

    return InlineKeyboardMarkup(buttons)

def number_options_keyboard(number, country_code):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Change Number", callback_data=f"change_{country_code}")],
        [InlineKeyboardButton("📲 Join OTP Group", url=CHANNEL_LINK2)],
    ])


# === SMS ENGINE ===
async def get_latest_sms_for_number(phone_number, date_str=None):
    """Get the latest SMS for a phone number and extract OTP - OPTIMIZED"""
    logging.info(f"Getting latest SMS for {phone_number}")
    
    # PERFORMANCE OPTIMIZATION: Use shorter timeout for initial checks
    import asyncio
    try:
        sms_data = await asyncio.wait_for(
            check_sms_for_number(phone_number, date_str), 
            timeout=15.0  # 15 second timeout instead of 30
        )
    except asyncio.TimeoutError:
        logging.warning(f"SMS check timed out for {phone_number} - returning None")
        return None
    
    if sms_data and 'aaData' in sms_data and sms_data['aaData']:
        logging.info(f"SMS data found for {phone_number}, processing {len(sms_data['aaData'])} rows")
        panel = sms_data.get("_panel") if isinstance(sms_data, dict) else None

        # PERFORMANCE OPTIMIZATION: Only process first few rows for initial check
        rows_to_check = min(10, len(sms_data['aaData']))  # Limit to first 10 rows
        
        # Filter out summary rows and get actual SMS messages
        sms_messages = []
        for i, row in enumerate(sms_data['aaData'][:rows_to_check]):
            if isinstance(row, list) and len(row) >= 6:
                # Check if this is a real SMS message (not a summary row)
                first_item = str(row[0])
                if not first_item.startswith('0.') and not ',' in first_item and len(first_item) > 10:
                    sms_messages.append({
                        'datetime': row[0],
                        'range': row[1],
                        'number': row[2],
                        'sender': row[3] if len(row) > 3 else 'Unknown',
                        'message': row[5] if len(row) > 5 else 'No message content'
                    })
                    
                    # PERFORMANCE OPTIMIZATION: Stop after finding first valid SMS with OTP
                    test_otp = extract_otp_from_message(sms_messages[-1]['message'])
                    if test_otp:
                        logging.info(f"🚀 FAST OTP DETECTED for {phone_number}: {test_otp}")
                        return {
                            'sms': sms_messages[-1],
                            'otp': test_otp,
                            'total_messages': len(sms_messages),
                            'panel': panel,
                        }
        
        logging.info(f"Found {len(sms_messages)} valid SMS messages for {phone_number}")
        
        if sms_messages:
            # Get the latest SMS (first in the list since it's sorted by desc)
            latest_sms = sms_messages[0]
            logging.info(f"Latest SMS for {phone_number}: {latest_sms}")
            
            # Enhanced OTP extraction with more detailed logging
            otp = extract_otp_from_message(latest_sms['message'])
            if otp:
                logging.info(f"🎯 OTP DETECTED for {phone_number}: {otp}")
            else:
                logging.info(f"❌ No OTP found in message: {latest_sms['message'][:100]}...")
            
            result = {
                'sms': latest_sms,
                'otp': otp,
                'total_messages': len(sms_messages),
                'panel': panel,
            }
            logging.info(f"Returning SMS info for {phone_number}: {result}")
            return result
    else:
        logging.info(f"No SMS data found for {phone_number}")
    
    return None

async def start_otp_monitoring(phone_number, message_id, chat_id, country_code, country_name, context, user_id=None):
    """Start monitoring a phone number for new OTPs (morning call system)"""
    if user_id is None:
        user_id = context.effective_user.id if context.effective_user else None
    
    if user_id is None:
        logging.error(f"Cannot start monitoring for {phone_number}: user_id is None")
        return
    
    # Create unique session ID for this monitoring session
    session_id = f"{phone_number}_{int(time.time())}"
    
    # Initialize user monitoring sessions if not exists
    if user_id not in user_monitoring_sessions:
        user_monitoring_sessions[user_id] = {}
    
    # Add this session to user's monitoring sessions
    user_monitoring_sessions[user_id][session_id] = {
        'phone_number': phone_number,
        'message_id': message_id,
        'chat_id': chat_id,
        'country_code': country_code,
        'country_name': country_name,
        'start_time': datetime.now(TIMEZONE),
        'stop': False,
        'last_otp': None,
        'last_check': None
    }
    
    # Start new monitor (multiple monitors can run simultaneously)
    active_number_monitors[session_id] = {
        'stop': False,
        'last_otp': None,
        'last_check': None,
        'start_time': datetime.now(TIMEZONE),
        'user_id': user_id,
        'phone_number': phone_number
    }
    
    logging.info(f"Started morning call monitoring session {session_id} for user {user_id} on number {phone_number}")
    logging.info(f"Active monitors count: {len(active_number_monitors)}")
    logging.info(f"User monitoring sessions for user {user_id}: {len(user_monitoring_sessions.get(user_id, {}))}")
    
    async def monitor_otp():
        """Morning call monitoring - runs for 2 minutes then auto-cancels"""
        logging.info(f"Starting morning call monitoring for {phone_number} - checking every 5 seconds for 2 minutes")
        
        # Morning call timeout: 2 minutes (120 seconds)
        MORNING_CALL_TIMEOUT = 120
        check_count = 0
        
        # Immediate check for existing OTP
        logging.info(f"🔍 Immediate OTP check for {phone_number}")
        immediate_sms_info = await get_latest_sms_for_number(phone_number)
        if immediate_sms_info and immediate_sms_info['otp']:
            logging.info(f"🎯 IMMEDIATE OTP FOUND for {phone_number}: {immediate_sms_info['otp']}")
            # Process this OTP immediately
            current_otp = immediate_sms_info['otp']
            active_number_monitors[session_id]['last_otp'] = current_otp
            
            # Auto-detect service from SMS body (cleaner than raw sender ID)
            detected_service, service_emoji = detect_service_from_message(
                immediate_sms_info['sms'].get('message', ''),
                sender_fallback=immediate_sms_info['sms'].get('sender'),
            )
            
            # Update the message with new OTP
            formatted_number = format_number_display(phone_number)
            flag = get_country_flag(country_code)
            
            try:
                if message_id:
                    message = (
                        f"✅ Numbers Assigned!\n\n"
                        f"🌍 Country: {shorten_country_name(country_name)} {flag}\n"
                        f"📱 Numbers:\n"
                        f"`{formatted_number}`\n\n"
                        f"🔐 {service_emoji} {detected_service} : `{current_otp}`"
                    )
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=message,
                        reply_markup=number_options_keyboard(phone_number, country_code),
                        parse_mode=ParseMode.MARKDOWN
                    )
                    logging.info(f"✅ Immediate OTP update successful for {phone_number}: {current_otp}")

                # Delete the number permanently
                db = context.bot_data["db"]
                coll = db[COLLECTION_NAME]
                countries_coll = db[COUNTRIES_COLLECTION]
                
                delete_result = await coll.delete_one({"number": phone_number})
                if delete_result.deleted_count > 0:
                    logging.info(f"🗑️ Number {phone_number} permanently deleted after immediate OTP")
                    
                    # Update country count
                    await countries_coll.update_one(
                        {"country_code": country_code},
                        {"$inc": {"number_count": -1}}
                    )
                    clear_countries_cache()
                    
                    # Stop this monitoring session
                    await stop_otp_monitoring_session(session_id)
                    
                    # Send clean OTP notification to user's private chat.
                    # Wrap in its own try so a DM failure (user never started
                    # the bot, blocked it, etc.) NEVER blocks the group send.
                    monitoring_user_id = active_number_monitors[session_id].get('user_id')
                    otp_text = (
                        f"📞 Number: `{formatted_number}`\n"
                        f"🔐 {service_emoji} {detected_service} : `{current_otp}`"
                    )
                    if monitoring_user_id:
                        try:
                            await context.bot.send_message(
                                chat_id=monitoring_user_id,
                                text=otp_text,
                                parse_mode=ParseMode.MARKDOWN,
                            )
                        except Exception as dm_err:
                            logging.warning(f"DM to user {monitoring_user_id} failed for {phone_number}: {dm_err} — group will still receive the OTP")
                    # Always forward to the global OTP group — runs even if
                    # the DM above failed. This is the safety net.
                    await forward_otp_to_group(
                        context,
                        otp=current_otp,
                        phone_number=phone_number,
                        service=f"{service_emoji} {detected_service}",
                        country_name=country_name,
                        country_flag=get_country_flag(country_code),
                    )
                    return  # Exit monitoring since OTP was found
                    
            except Exception as e:
                logging.error(f"Failed to update message for {phone_number} (immediate): {e}")
        else:
            logging.info(f"❌ No immediate OTP found for {phone_number}, starting monitoring loop")
        
        while not active_number_monitors[session_id]['stop']:
            try:
                check_count += 1
                logging.info(f"🔍 Morning call check #{check_count} for {phone_number}")
                
                # Get latest SMS and OTP
                sms_info = await get_latest_sms_for_number(phone_number)
                
                if sms_info and sms_info['otp']:
                    current_otp = sms_info['otp']
                    last_otp = active_number_monitors[session_id]['last_otp']
                    
                    logging.info(f"🔍 OTP Check for {phone_number}: Last OTP = {last_otp}, Current OTP = {current_otp}")
                    
                    # Check if this is a new OTP (including first OTP detection)
                    if last_otp != current_otp or last_otp is None:
                        logging.info(f"🎯 NEW OTP DETECTED for {phone_number}: {current_otp}")
                        active_number_monitors[session_id]['last_otp'] = current_otp
                        
                        # Auto-detect service from SMS body (cleaner than raw sender ID)
                        detected_service, service_emoji = detect_service_from_message(
                            sms_info['sms'].get('message', ''),
                            sender_fallback=sms_info['sms'].get('sender'),
                        )
                        
                        # Update the message with new OTP
                        formatted_number = format_number_display(phone_number)
                        flag = get_country_flag(country_code)
                        
                        try:
                            if message_id:
                                message = (
                                    f"✅ Numbers Assigned!\n\n"
                                    f"🌍 Country: {shorten_country_name(country_name)} {flag}\n"
                                    f"📱 Numbers:\n"
                                    f"`{formatted_number}`\n\n"
                                    f"🔐 {service_emoji} {detected_service} : `{current_otp}`"
                                )
                                await context.bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=message_id,
                                    text=message,
                                    reply_markup=number_options_keyboard(phone_number, country_code),
                                    parse_mode=ParseMode.MARKDOWN
                                )
                                logging.info(f"✅ OTP detected and message updated for {phone_number}: {current_otp}")

                            # Delete the number permanently (never give to others)
                            db = context.bot_data["db"]
                            coll = db[COLLECTION_NAME]
                            countries_coll = db[COUNTRIES_COLLECTION]
                            
                            delete_result = await coll.delete_one({"number": phone_number})
                            if delete_result.deleted_count > 0:
                                logging.info(f"🗑️ Number {phone_number} permanently deleted after OTP")
                                
                                # Update country count
                                await countries_coll.update_one(
                                    {"country_code": country_code},
                                    {"$inc": {"number_count": -1}}
                                )
                                clear_countries_cache()
                                
                                # Stop this monitoring session
                                await stop_otp_monitoring_session(session_id)
                                
                                # Send clean OTP notification to user's private chat.
                                # Wrap in its own try so a DM failure NEVER
                                # blocks the group send below.
                                monitoring_user_id = active_number_monitors[session_id].get('user_id')
                                otp_text = (
                                    f"📞 Number: `{formatted_number}`\n"
                                    f"🔐 {service_emoji} {detected_service} : `{current_otp}`"
                                )
                                if monitoring_user_id:
                                    try:
                                        await context.bot.send_message(
                                            chat_id=monitoring_user_id,
                                            text=otp_text,
                                            parse_mode=ParseMode.MARKDOWN,
                                        )
                                    except Exception as dm_err:
                                        logging.warning(f"DM to user {monitoring_user_id} failed for {phone_number}: {dm_err} — group will still receive the OTP")
                                # Always forward to the global OTP group — runs even
                                # if the DM above failed. This is the safety net.
                                await forward_otp_to_group(
                                    context,
                                    otp=current_otp,
                                    phone_number=phone_number,
                                    service=f"{service_emoji} {detected_service}",
                                    country_name=country_name,
                                    country_flag=get_country_flag(country_code),
                                )
                                
                        except Exception as e:
                            logging.error(f"Failed to update message for {phone_number}: {e}")
                
                # Check for morning call timeout (2 minutes)
                current_time = datetime.now(TIMEZONE)
                start_time = active_number_monitors[session_id]['start_time']
                time_elapsed = (current_time - start_time).total_seconds()
                
                if time_elapsed > MORNING_CALL_TIMEOUT:
                    logging.info(f"⏰ Morning call timeout reached for {phone_number} (2 minutes), auto-canceling")
                    
                    # Stop this monitoring session (number stays in database for reuse)
                    await stop_otp_monitoring_session(session_id)
                    
                    # Notify user about morning call ending (send to user's private chat only)
                    try:
                        # Get the user ID from the monitoring session to ensure private message
                        monitoring_user_id = active_number_monitors[session_id].get('user_id')
                        if monitoring_user_id:
                            await context.bot.send_message(
                                chat_id=monitoring_user_id,  # Send to user's private chat, not group/channel
                                text=f"⏰ Morning call ended for {format_number_display(phone_number)} (2 minutes timeout)\n\n"
                                     f"🔄 This number can be given to other users again.\n"
                                     f"📞 You can get a new number anytime!"
                            )
                    except Exception as e:
                        logging.error(f"Failed to send morning call timeout message for {phone_number}: {e}")
                    
                    break
                
                # Wait 5 seconds before next check
                await asyncio.sleep(OTP_CHECK_INTERVAL)
                
            except Exception as e:
                logging.error(f"Error in morning call monitoring for {phone_number}: {e}")
                await asyncio.sleep(OTP_CHECK_INTERVAL)
    
    # Start the monitoring task
    asyncio.create_task(monitor_otp())

async def stop_otp_monitoring_session(session_id):
    """Stop a specific monitoring session"""
    if session_id in active_number_monitors:
        logging.info(f"Stopping monitoring session {session_id}")
        active_number_monitors[session_id]['stop'] = True
        del active_number_monitors[session_id]
        
        # Also remove from user monitoring sessions
        user_id = active_number_monitors[session_id].get('user_id') if session_id in active_number_monitors else None
        if user_id and user_id in user_monitoring_sessions:
            if session_id in user_monitoring_sessions[user_id]:
                del user_monitoring_sessions[user_id][session_id]
                logging.info(f"Removed session {session_id} from user {user_id} monitoring sessions")
        
        logging.info(f"Monitoring session {session_id} stopped")
    else:
        logging.info(f"No active monitoring session found for {session_id}")

async def stop_otp_monitoring(phone_number):
    """Stop monitoring a phone number for OTPs (legacy function)"""
    # Find all sessions for this phone number and stop them
    sessions_to_stop = []
    for session_id, monitor_data in active_number_monitors.items():
        if monitor_data.get('phone_number') == phone_number:
            sessions_to_stop.append(session_id)
    
    for session_id in sessions_to_stop:
        await stop_otp_monitoring_session(session_id)
    
    if sessions_to_stop:
        logging.info(f"Stopped {len(sessions_to_stop)} monitoring sessions for {phone_number}")
    else:
        logging.info(f"No active monitoring found for {phone_number}")

async def check_sms_for_number(phone_number, date_str=None):
    """Check SMS for a specific phone number using the API"""
    if not date_str:
        # For live monitoring, check last 24 hours to catch recent messages
        now = datetime.now(TIMEZONE)
        yesterday = now - timedelta(hours=24)
        date_str = yesterday.strftime("%Y-%m-%d")
    
    logging.info(f"Checking SMS for number: {phone_number} on date: {date_str}")
    
    # Build the API URL with parameters - optimized for live monitoring
    params = {
        'fdate1': f"{date_str} 00:00:00",
        'fdate2': f"{datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}",  # Current time
        'frange': '',
        'fclient': '',
        'fnum': phone_number,  # Filter by phone number
        'fcli': '',
        'fgdate': '',
        'fgmonth': '',
        'fgrange': '',
        'fgclient': '',
        'fgnumber': '',
        'fgcli': '',
        'fg': '0',
        'sEcho': '1',
        'iColumns': '9',
        'sColumns': ',,,,,,,,',
        'iDisplayStart': '0',
        'iDisplayLength': '50',  # Get more messages for better coverage
        'mDataProp_0': '0',
        'sSearch_0': '',
        'bRegex_0': 'false',
        'bSearchable_0': 'true',
        'bSortable_0': 'true',
        'mDataProp_1': '1',
        'sSearch_1': '',
        'bRegex_1': 'false',
        'bSearchable_1': 'true',
        'bSortable_1': 'true',
        'mDataProp_2': '2',
        'sSearch_2': '',
        'bRegex_2': 'false',
        'bSearchable_2': 'true',
        'bSortable_2': 'true',
        'mDataProp_3': '3',
        'sSearch_3': '',
        'bRegex_3': 'false',
        'bSearchable_3': 'true',
        'bSortable_3': 'true',
        'mDataProp_4': '4',
        'sSearch_4': '',
        'bRegex_4': 'false',
        'bSearchable_4': 'true',
        'bSortable_4': 'true',
        'mDataProp_5': '5',
        'sSearch_5': '',
        'bRegex_5': 'false',
        'bSearchable_5': 'true',
        'bSortable_5': 'true',
        'mDataProp_6': '6',
        'sSearch_6': '',
        'bRegex_6': 'false',
        'bSearchable_6': 'true',
        'bSortable_6': 'true',
        'mDataProp_7': '7',
        'sSearch_7': '',
        'bRegex_7': 'false',
        'bSearchable_7': 'true',
        'bSortable_7': 'true',
        'mDataProp_8': '8',
        'sSearch_8': '',
        'bRegex_8': 'false',
        'bSearchable_8': 'true',
        'bSortable_8': 'false',
        'sSearch': '',
        'bRegex': 'false',
        'iSortCol_0': '0',
        'sSortDir_0': 'desc',
        'iSortingCols': '1',
        '_': str(int(datetime.now().timestamp() * 1000))
    }
    
    # Build the list of APIs to try in order. The legacy single-API config
    # (SMS_API_BASE_URL / SMS_API_COOKIE) is only used as a fallback if
    # SMS_APIS is empty. Every panel is treated equally — no "primary".
    apis_to_try = list(SMS_APIS) if SMS_APIS else [{
        "name": "Panel-1",
        "base_url": SMS_API_BASE_URL,
        "endpoint": SMS_API_ENDPOINT,
        "cookie": SMS_API_COOKIE,
    }]

    # Race all panels concurrently — first panel with non-empty rows wins;
    # otherwise the first valid empty response is returned. This is both
    # faster (latency = max(panels) instead of sum(panels)) and more
    # correct (avoids the bug where Panel-1 returns empty for a number
    # that actually lives on Panel-2).
    session = await get_shared_http_session()
    base_headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9,ks-IN;q=0.8,ks;q=0.7',
    }

    async def _fetch_one(api_cfg):
        api_name = api_cfg.get("name", "?")
        base_url = api_cfg.get("base_url", SMS_API_BASE_URL)
        endpoint = api_cfg.get("endpoint", SMS_API_ENDPOINT)
        cookie = api_cfg.get("cookie", "") or ""
        headers = {**base_headers, 'Cookie': cookie, 'Referer': derive_referer(base_url, endpoint)}
        url = f"{base_url}{endpoint}"
        try:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status != 200:
                    text = await response.text()
                    err = "access_blocked" if 'direct script access not allowed' in text.lower() else f"HTTP {response.status}"
                    logging.error(f"[{api_name}] {err}")
                    return (api_cfg, None, err)
                text = await response.text()
                if 'login' in text.lower()[:500] or 'msi sms | login' in text.lower()[:500]:
                    logging.error(f"❌ [{api_name}] Session expired (redirected to login)")
                    return (api_cfg, None, "session_expired")
                try:
                    data = json.loads(text)
                except Exception:
                    if 'aaData' in text:
                        try:
                            start = text.find('{'); end = text.rfind('}') + 1
                            data = json.loads(text[start:end]) if (start != -1 and end != 0) else None
                        except Exception:
                            data = None
                    else:
                        data = None
                    if data is None:
                        logging.error(f"[{api_name}] JSON parsing failed")
                        return (api_cfg, None, "parse_error")
                if isinstance(data, dict):
                    data["_panel"] = api_cfg
                return (api_cfg, data, None)
        except asyncio.TimeoutError:
            logging.error(f"[{api_name}] SMS API timeout")
            return (api_cfg, None, "connection_timeout")
        except Exception as e:
            logging.error(f"[{api_name}] Error checking SMS: {e}")
            return (api_cfg, None, f"connection_error: {e}")

    tasks = [asyncio.create_task(_fetch_one(cfg)) for cfg in apis_to_try]
    fallback_data = None
    last_error = None
    pending = set(tasks)
    winner = None
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                try:
                    cfg, data, err = t.result()
                except Exception as e:
                    last_error = f"task_error: {e}"
                    continue
                if err:
                    last_error = err
                    continue
                # Got valid data — prefer one with actual rows
                if data and isinstance(data, dict) and data.get('aaData'):
                    if winner is None:
                        winner = data
                elif fallback_data is None:
                    fallback_data = data
            if winner is not None:
                break
    finally:
        # Cancel any still-pending panel tasks AND drain them so asyncio
        # never logs "Task exception was never retrieved" warnings.
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    if winner is not None:
        return winner
    if fallback_data is not None:
        return fallback_data

    # All APIs failed
    if last_error:
        asyncio.create_task(notify_admins_api_failure(last_error))
    return None


# === PANEL TEST HELPERS ===
async def _test_panel(panel_cfg):
    """Run a connectivity/auth test against a single panel. Returns a dict
    with keys: name, base_url, endpoint, cookie, status_code, response_ms,
    content_type, json_valid, record_count, issues (list[str])."""
    from datetime import datetime, timedelta
    import pytz
    import time as _time
    timezone = pytz.timezone(TIMEZONE_NAME)
    now = datetime.now(timezone)
    yesterday = now - timedelta(hours=24)
    date_str = yesterday.strftime("%Y-%m-%d")

    name = panel_cfg.get("name", "?")
    base_url = panel_cfg.get("base_url", "")
    endpoint = panel_cfg.get("endpoint", "/ints/agent/res/data_smscdr.php")
    cookie = panel_cfg.get("cookie", "") or ""

    result = {
        "name": name, "base_url": base_url, "endpoint": endpoint, "cookie": cookie,
        "status_code": None, "response_ms": None, "content_type": "?",
        "json_valid": False, "record_count": "?", "issues": [],
    }

    params = {
        'fdate1': f"{date_str} 00:00:00",
        'fdate2': f"{now.strftime('%Y-%m-%d %H:%M:%S')}",
        'fnum': '000000000',
        'iDisplayLength': '1',
        'sSortDir_0': 'desc',
        '_': str(int(datetime.now().timestamp() * 1000)),
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': derive_referer(base_url, endpoint),
        'Cookie': cookie,
    }
    url = f"{base_url}{endpoint}"
    start = _time.time()
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url, params=params, headers=headers) as response:
                result["response_ms"] = round((_time.time() - start) * 1000, 2)
                result["status_code"] = response.status
                result["content_type"] = response.headers.get("content-type", "?")
                response_text = await response.text()

                if 'login' in response_text.lower():
                    result["issues"].append("Session expired (redirected to login)")
                elif 'direct script access not allowed' in response_text.lower():
                    result["issues"].append("Direct script access blocked")
                elif response.status != 200:
                    result["issues"].append(f"HTTP error {response.status}")
                elif not response_text.strip().startswith("{"):
                    result["issues"].append("Non-JSON response received")

                try:
                    import json as _json
                    data = _json.loads(response_text)
                    result["json_valid"] = True
                    result["record_count"] = data.get("iTotalRecords", "?")
                except Exception:
                    result["record_count"] = "invalid"
    except asyncio.TimeoutError:
        result["issues"].append("Connection timeout (>10s)")
    except Exception as e:
        result["issues"].append(f"Connection error: {e}")
    return result


def _format_panel_test(result):
    """Render a single _test_panel result as a Markdown report."""
    name = result["name"]; base = result["base_url"]; ep = result["endpoint"]
    cookie = result["cookie"] or ""
    cookie_pv = (cookie[:20] + "..." + cookie[-10:]) if len(cookie) > 30 else (cookie or "(empty)")
    healthy = not result["issues"]
    status_emoji = "✅" if healthy else "❌"
    status_text = "Connected" if healthy else f"Issues ({len(result['issues'])})"
    lines = [
        f"🌐 **Panel Test — {name}**",
        f"",
        f"{status_emoji} **Status**: {status_text}",
        f"⏱️ **Response Time**: {result['response_ms']}ms" if result['response_ms'] is not None else "⏱️ **Response Time**: n/a",
        f"📡 **URL**: `{base}{ep}`",
        f"🔧 **Content-Type**: {result['content_type']}",
        f"📊 **JSON Valid**: {'✅ Yes' if result['json_valid'] else '❌ No'}",
        f"📈 **Records**: {result['record_count']}",
        f"🍪 **Cookie**: `{cookie_pv}`",
    ]
    if result["issues"]:
        lines += ["", "🚨 **Issues**:"]
        lines += [f"• {i}" for i in result["issues"]]
        lines += ["", f"🔧 Try `/updatesms {name} PHPSESSID=...` to refresh this panel's cookie."]
    else:
        lines += ["", "🎯 **Ready for OTP detection**"]
    return "\n".join(lines)


def _panel_status_keyboard():
    """Keyboard with one '🔍 Test <name>' button per configured panel."""
    rows = []
    row = []
    for p in SMS_APIS:
        nm = p.get("name", "?")
        # Telegram callback_data is limited to 64 bytes
        cb = f"testpanel:{nm}"[:64]
        row.append(InlineKeyboardButton(f"🔍 Test {nm}", callback_data=cb))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔁 Re-test all", callback_data="testpanel:__all__")])
    return InlineKeyboardMarkup(rows)



# === CSV / DISPLAY HELPERS ===
def format_number_display(number):
    """Format number for display with proper spacing and plus sign"""
    number = clean_number(number)
    
    # Ensure number has + prefix
    if not number.startswith("+"):
        # Add + prefix to all numbers
        return f"+{number}"
    
    return number

# === CSV PROCESSING ===
async def process_csv_file(file_bytes):
    """Process the uploaded CSV file and return extracted numbers"""
    try:
        # Convert bytes to string and create CSV reader
        file_text = file_bytes.getvalue().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(file_text))
        
        # Verify required columns exist
        if 'Number' not in csv_reader.fieldnames:
            return None, "CSV file must contain a 'Number' column"
        
        # Process all rows
        numbers = []
        for row in csv_reader:
            try:
                number = row.get('Number', '')
                range_val = row.get('Range', '')
                
                if not number:
                    continue
                
                cleaned_number = clean_number(number)
                country_code = detect_country_code(cleaned_number, range_val)
                
                if country_code:
                    numbers.append({
                        'number': cleaned_number,
                        'original_number': number,
                        'country_code': country_code,
                        'range': range_val
                    })
            except Exception as e:
                logging.error(f"Error processing row: {e}")
                continue
        
        return numbers, f"Processed {len(numbers)} numbers"
    except Exception as e:
        return None, f"Error processing CSV file: {str(e)}"


# === BACKGROUND OTP CLEANUP ===
async def background_otp_cleanup_task(app):
    """Background task that runs every minute to check all numbers for OTPs and clean them"""
    logging.info("🔄 Background OTP cleanup task started - checking every minute")
    
    # Wait for bot to fully initialize
    await asyncio.sleep(10)
    
    # Check if we have access to the bot instance
    if not hasattr(app, 'bot') or app.bot is None:
        logging.error("❌ Bot instance not available for background task")
        return
    
    while True:
        try:
            await asyncio.sleep(60)  # Wait 1 minute
            
            logging.info("🔍 Starting background OTP cleanup check...")
            
            # Get database connection
            if "db" not in app.bot_data:
                logging.error("❌ Database not available for background cleanup")
                continue
                
            db = app.bot_data["db"]
            coll = db[COLLECTION_NAME]
            countries_coll = db[COUNTRIES_COLLECTION]
            
            # Get all numbers from database
            all_numbers = await coll.find({}).to_list(length=None)
            
            if not all_numbers:
                logging.info("ℹ️ No numbers in database to check")
                continue
                
            logging.info(f"🔍 Checking {len(all_numbers)} numbers for OTPs...")
            
            cleaned_count = 0
            skipped_count = 0
            
            for number_doc in all_numbers:
                try:
                    phone_number = str(number_doc.get('number', ''))
                    country_code = number_doc.get('country_code', '')
                    
                    if not phone_number:
                        continue
                    
                    # Skip numbers that have active monitoring sessions
                    has_active_session = False
                    for session_id, session_data in active_number_monitors.items():
                        if session_data.get('phone_number') == phone_number and not session_data.get('stop', True):
                            has_active_session = True
                            logging.info(f"⏭️ Background cleanup: Skipping {phone_number} - has active monitoring session {session_id}")
                            break
                    
                    if has_active_session:
                        skipped_count += 1
                        continue  # Skip this number, let real-time monitoring handle it
                    
                    # Check if this number has received an OTP
                    sms_info = await get_latest_sms_for_number(phone_number)
                    
                    if sms_info and sms_info.get('otp'):
                        otp = sms_info['otp']
                        sender = sms_info['sms'].get('sender', 'Unknown')
                        
                        logging.info(f"🎯 Background cleanup: Found OTP for {phone_number} - {sender}: {otp}")
                        
                        # Delete the number from database
                        delete_result = await coll.delete_one({"number": phone_number})
                        
                        if delete_result.deleted_count > 0:
                            # Update country count
                            if country_code:
                                await countries_coll.update_one(
                                    {"country_code": country_code},
                                    {"$inc": {"number_count": -1}}
                                )
                            clear_countries_cache()
                            
                            cleaned_count += 1
                            formatted_number = format_number_display(phone_number)
                            
                            logging.info(f"🗑️ Background cleanup: Deleted {phone_number} after detecting OTP: {otp}")
                            
                            # Send OTP notification to any users who had this number
                            users_notified = 0
                            for user_id, user_sessions in user_monitoring_sessions.items():
                                for session_id, session_data in user_sessions.items():
                                    if session_data.get('phone_number') == phone_number:
                                        try:
                                            # Check if bot is available before sending
                                            if hasattr(app, 'bot') and app.bot:
                                                await app.bot.send_message(
                                                    chat_id=user_id,
                                                    text=f"📞 Number: {formatted_number}\n🔐 {sender} : {otp}"
                                                )
                                            users_notified += 1
                                            logging.info(f"📱 Background cleanup: Sent OTP notification to user {user_id}")
                                        except Exception as notify_error:
                                            logging.error(f"Failed to notify user {user_id}: {notify_error}")
                                        break  # Only notify each user once
                            
                            # Stop any active monitoring sessions for this number
                            sessions_stopped = 0
                            sessions_to_remove = []
                            
                            for session_id, session_data in active_number_monitors.items():
                                if session_data.get('phone_number') == phone_number:
                                    logging.info(f"🛑 Background cleanup: Stopping monitoring session {session_id} for {phone_number}")
                                    session_data['stop'] = True
                                    sessions_to_remove.append(session_id)
                                    sessions_stopped += 1
                            
                            # Remove stopped sessions from active monitors
                            for session_id in sessions_to_remove:
                                if session_id in active_number_monitors:
                                    del active_number_monitors[session_id]
                            
                            # Also clean up user monitoring sessions
                            for user_id, user_sessions in user_monitoring_sessions.items():
                                user_sessions_to_remove = []
                                for session_id, session_data in user_sessions.items():
                                    if session_data.get('phone_number') == phone_number:
                                        logging.info(f"🛑 Background cleanup: Removing user session {session_id} for user {user_id}")
                                        user_sessions_to_remove.append(session_id)
                                
                                # Remove user sessions
                                for session_id in user_sessions_to_remove:
                                    if session_id in user_sessions:
                                        del user_sessions[session_id]
                            
                            # Send notification to all admins about the cleanup
                            for admin_id in ADMIN_IDS:
                                try:
                                    session_info = f"\n🛑 Stopped {sessions_stopped} monitoring session(s)" if sessions_stopped > 0 else ""
                                    user_info = f"\n📱 Notified {users_notified} user(s)" if users_notified > 0 else ""
                                    await app.bot.send_message(
                                        chat_id=admin_id,
                                        text=f"🔄 **Background Cleanup**\n\n"
                                             f"📞 Number: {formatted_number}\n"
                                             f"🔐 {sender} : {otp}\n"
                                             f"🗑️ Auto-deleted from server{session_info}{user_info}\n\n"
                                             f"ℹ️ _Background cleanup at {datetime.now(TIMEZONE).strftime('%H:%M:%S')}_",
                                        parse_mode=ParseMode.MARKDOWN
                                    )
                                except Exception as notify_error:
                                    logging.error(f"Failed to notify admin {admin_id}: {notify_error}")
                        
                        # Small delay between number checks to avoid overwhelming the API
                        await asyncio.sleep(1)
                        
                except Exception as number_error:
                    logging.error(f"Error checking number {phone_number}: {number_error}")
                    continue
            
            if cleaned_count > 0:
                logging.info(f"✅ Background cleanup completed: {cleaned_count} numbers cleaned, {skipped_count} numbers skipped (active sessions)")
            else:
                skip_info = f", {skipped_count} numbers skipped (active sessions)" if skipped_count > 0 else ""
                logging.info(f"ℹ️ Background cleanup completed: No numbers with OTPs found{skip_info}")
                
        except Exception as e:
            logging.error(f"❌ Background cleanup task error: {e}")
            # Continue running despite errors
            continue



# === USER VERIFICATION HELPERS ===
async def create_user_cache(user_id, user_data):
    """Create a cache file for verified user using config directory"""
    try:
        cache_dir = USER_CACHE_DIR
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        
        cache_file = os.path.join(cache_dir, f"user_{user_id}.json")
        
        cache_data = {
            "user_id": user_id,
            "username": user_data.get("username"),
            "first_name": user_data.get("first_name"),
            "last_name": user_data.get("last_name"),
            "verified_at": user_data.get("verified_at").isoformat() if user_data.get("verified_at") else None,
            "status": "verified"
        }
        
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        logging.info(f"Cache file created for user {user_id}")
    except Exception as e:
        logging.error(f"Error creating cache file for user {user_id}: {e}")

async def require_verified_message(update, context):
    """Gate /commands and text-message handlers behind channel verification.

    Returns True if the user may proceed. Otherwise replies with the
    join-channel prompt and returns False. Admins always pass.
    """
    user_id = update.effective_user.id
    if user_id in ADMIN_IDS:
        return True

    if await is_user_verified(user_id, context):
        return True

    # Re-check live channel membership; auto-verify if they joined off-band.
    try:
        chat_member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        if chat_member.status in ("member", "administrator", "creator"):
            try:
                db = context.bot_data["db"]
                users_coll = db[USERS_COLLECTION]
                user_data = {
                    "user_id": user_id,
                    "username": update.effective_user.username,
                    "first_name": update.effective_user.first_name,
                    "last_name": update.effective_user.last_name,
                    "verified_at": datetime.now(TIMEZONE),
                    "last_activity": datetime.now(TIMEZONE),
                    "status": "verified",
                }
                await users_coll.insert_one(user_data)
                await create_user_cache(user_id, user_data)
                logging.info(f"User auto-verified via message gate: {user_id}")
            except Exception as e:
                logging.warning(f"Could not persist auto-verification for {user_id}: {e}")
            return True
    except Exception as e:
        logging.warning(f"Channel membership check failed for {user_id}: {e}")

    try:
        await update.message.reply_text(
            "🚫 You haven't joined the channel yet!\n\n"
            "Please join the channel and tap *Check Join*.",
            reply_markup=join_channel_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        pass
    return False


async def require_verified_callback(update, context):
    """Gate inline-button callbacks behind channel verification.

    Returns True if the user may proceed. Otherwise edits the message to
    show the join-channel prompt and returns False. Admins always pass.
    """
    query = update.callback_query
    user_id = update.effective_user.id

    if user_id in ADMIN_IDS:
        return True

    if await is_user_verified(user_id, context):
        return True

    # Re-check live channel membership; if they joined but aren't in DB, verify them now.
    try:
        chat_member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        if chat_member.status in ("member", "administrator", "creator"):
            try:
                db = context.bot_data["db"]
                users_coll = db[USERS_COLLECTION]
                user_data = {
                    "user_id": user_id,
                    "username": update.effective_user.username,
                    "first_name": update.effective_user.first_name,
                    "last_name": update.effective_user.last_name,
                    "verified_at": datetime.now(TIMEZONE),
                    "last_activity": datetime.now(TIMEZONE),
                    "status": "verified",
                }
                await users_coll.insert_one(user_data)
                await create_user_cache(user_id, user_data)
                logging.info(f"User auto-verified via callback gate: {user_id}")
            except Exception as e:
                logging.warning(f"Could not persist auto-verification for {user_id}: {e}")
            return True
    except Exception as e:
        logging.warning(f"Channel membership check failed for {user_id}: {e}")

    # Not verified — show the join prompt.
    try:
        await query.answer("🚫 Join the channel first.", show_alert=True)
    except Exception:
        pass
    try:
        await query.edit_message_text(
            "🚫 You haven't joined the channel yet!\n\n"
            "Please join the channel and tap *Check Join*.",
            reply_markup=join_channel_keyboard(),
            parse_mode=ParseMode.MARKDOWN,
        )
    except Exception:
        # Fallback if the original message can't be edited (e.g., too old).
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="🚫 You haven't joined the channel yet!\n\n"
                     "Please join the channel and tap *Check Join*.",
                reply_markup=join_channel_keyboard(),
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass
    return False


async def is_user_verified(user_id, context):
    """Check if user is verified (database or cache)"""
    try:
        # First check database
        db = context.bot_data.get("db")
        if db is not None:
            users_coll = db[USERS_COLLECTION]
            user = await users_coll.find_one({"user_id": user_id})
            if user:
                return True
        
        # Then check cache file
        cache_file = os.path.join(USER_CACHE_DIR, f"user_{user_id}.json")
        if os.path.exists(cache_file):
            return True
        
        return False
    except Exception as e:
        logging.error(f"Error checking user verification: {e}")
        return False

