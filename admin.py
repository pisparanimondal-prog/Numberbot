# === ADMIN: command handlers reserved for ADMIN_IDS ===
import logging
import os
import asyncio
import re
import json
import time
from datetime import datetime, timedelta
from io import BytesIO

import pytz
import pycountry
import aiohttp

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from config import *
import engine
from engine import (
    reset_add_flow,
    clear_countries_cache,
    get_shared_http_session,
    reload_config_session,
    get_current_sms_cookie,
    update_runtime_session,
    update_config_file_session,
    derive_referer,
    create_user_cache,
    require_verified_message,
    require_verified_callback,
    is_user_verified,
    add_sms_api_to_config,
    remove_sms_api_from_config,
    update_panel_cookie_in_config,
    set_otp_group_in_config,
    forward_otp_to_group,
    notify_admins_api_failure,
    notify_admins_api_recovery,
    send_lol_message,
    extract_otp_from_message,
    get_country_flag,
    clean_number,
    extract_country_from_range,
    detect_country_code,
    detect_service_from_message,
    shorten_country_name,
    resolve_country_display,
    join_channel_keyboard,
    service_keyboard,
    countries_keyboard,
    number_options_keyboard,
    get_latest_sms_for_number,
    start_otp_monitoring,
    stop_otp_monitoring_session,
    stop_otp_monitoring,
    check_sms_for_number,
    _test_panel,
    _format_panel_test,
    _panel_status_keyboard,
    format_number_display,
    process_csv_file,
    background_otp_cleanup_task,
    uploaded_csv,
    user_states,
    manual_numbers,
    add_service,
    current_user_numbers,
    user_monitoring_sessions,
    active_number_monitors,
    change_number_last_press,
    TIMEZONE,
)


# === COUNTRY / DATA ADMIN ===
async def delete_country(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logging.info(f"Delete country command called by user {user_id}")
    
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    args = context.args
    logging.info(f"Delete country args: {args}")
    
    if not args:
        # Show available countries to delete
        db = context.bot_data["db"]
        countries_coll = db[COUNTRIES_COLLECTION]
        countries = await countries_coll.find({}).to_list(length=50)
        
        if not countries:
            await update.message.reply_text("📭 No countries found in database.")
            return
        
        message_lines = ["🗑️ Available countries to delete:"]
        for country in countries:
            flag = get_country_flag(country.get("detected_country", country["country_code"]))
            display_name = country.get("display_name", country["country_code"])
            count = country.get("number_count", 0)
            message_lines.append(f"{flag} {display_name} ({country['country_code']}) - {count} numbers")
        
        message_lines.append("\nUsage: /delete <country_code>")
        message_lines.append("Example: /delete india_ws")
        
        await update.message.reply_text("\n".join(message_lines))
        return

    country_code = args[0].lower()
    
    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]

    # Check if country exists
    country_info = await countries_coll.find_one({"country_code": country_code})
    if not country_info:
        await update.message.reply_text(f"❌ Country code '{country_code}' not found in database.")
        return

    # Get country display name
    display_name = country_info.get("display_name", country_code)
    
    # Delete numbers
    result = await coll.delete_many({"country_code": country_code})
    
    # Delete country from countries collection
    await countries_coll.delete_one({"country_code": country_code})
    
    flag = get_country_flag(country_info.get("detected_country", country_code))
    
    await update.message.reply_text(
        f"✅ Deleted {result.deleted_count} numbers for {flag} {display_name} (`{country_code}`)."
    )

async def remove_country_by_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ADMIN ONLY: Remove a country (and all its numbers) by name.

    Usage: /remove <country name>
    Examples:
      /remove Mozambique
      /remove Sri Lanka Ws
    """
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "🗑️ *Remove a country*\n\n"
            "Usage: `/remove <country name>`\n"
            "Example: `/remove Mozambique`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    name = " ".join(args).strip()
    name_code = name.lower().replace(" ", "_")

    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]

    # Match by country_code OR by display_name (case-insensitive)
    country_info = await countries_coll.find_one({
        "$or": [
            {"country_code": name_code},
            {"display_name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}},
        ]
    })

    if not country_info:
        await update.message.reply_text(
            f"❌ No country named `{name}` found.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    country_code = country_info["country_code"]
    display_name = country_info.get("display_name", country_code)

    result = await coll.delete_many({"country_code": country_code})
    await countries_coll.delete_one({"country_code": country_code})

    # Invalidate the cached country list so the next user query is fresh
    engine.clear_countries_cache()

    flag = get_country_flag(country_info.get("detected_country", country_code))
    await update.message.reply_text(
        f"✅ Removed {flag} *{display_name}* — {result.deleted_count} number(s) deleted.",
        parse_mode=ParseMode.MARKDOWN,
    )



# === API / PANEL CHECKS ===
async def check_api_connection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check status of every configured SMS API panel and show per-panel test buttons."""
    user_id = update.effective_user.id
    logging.info(f"Check API connection command called by user {user_id}")

    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    if not engine.SMS_APIS:
        await update.message.reply_text("⚠️ No SMS APIs configured.")
        return

    await update.message.reply_text(f"🔍 Checking {len(engine.SMS_APIS)} panel(s)...")

    # Run all panel tests in parallel
    tasks = [_test_panel(p) for p in engine.SMS_APIS]
    results = await asyncio.gather(*tasks)

    # Build summary
    healthy_count = sum(1 for r in results if not r["issues"])
    summary = [f"🌐 **SMS API Panels — {healthy_count}/{len(results)} healthy**\n"]
    for r in results:
        emoji = "✅" if not r["issues"] else "❌"
        rt = f"{r['response_ms']}ms" if r['response_ms'] is not None else "n/a"
        first_issue = f" — {r['issues'][0]}" if r["issues"] else ""
        summary.append(f"{emoji} **{r['name']}** · {rt} · `{r['base_url']}`{first_issue}")
    summary.append("\n_Tap a button below to re-test a specific panel._")

    await update.message.reply_text(
        "\n".join(summary),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_panel_status_keyboard(),
    )


async def test_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles inline 🔍 Test <name> buttons from /checkapi."""
    query = update.callback_query
    user_id = query.from_user.id
    if user_id not in ADMIN_IDS:
        await query.answer("Not allowed.", show_alert=True)
        return
    await query.answer("Testing...")

    data = query.data or ""
    if not data.startswith("testpanel:"):
        return
    target = data.split(":", 1)[1]

    if target == "__all__":
        # Re-run the full check
        if not engine.SMS_APIS:
            await query.message.reply_text("⚠️ No SMS APIs configured.")
            return
        tasks = [_test_panel(p) for p in engine.SMS_APIS]
        results = await asyncio.gather(*tasks)
        healthy_count = sum(1 for r in results if not r["issues"])
        summary = [f"🌐 **SMS API Panels — {healthy_count}/{len(results)} healthy**\n"]
        for r in results:
            emoji = "✅" if not r["issues"] else "❌"
            rt = f"{r['response_ms']}ms" if r['response_ms'] is not None else "n/a"
            first_issue = f" — {r['issues'][0]}" if r["issues"] else ""
            summary.append(f"{emoji} **{r['name']}** · {rt} · `{r['base_url']}`{first_issue}")
        summary.append("\n_Tap a button below to re-test a specific panel._")
        await query.message.reply_text(
            "\n".join(summary),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=_panel_status_keyboard(),
        )
        return

    # Find the named panel
    panel = None
    for p in engine.SMS_APIS:
        if p.get("name", "") == target:
            panel = p; break
    if not panel:
        await query.message.reply_text(f"❌ Panel '{target}' not found.")
        return

    result = await _test_panel(panel)
    await query.message.reply_text(_format_panel_test(result), parse_mode=ParseMode.MARKDOWN)


# === BULK ADMIN OPS ===
async def delete_all_numbers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete all numbers from database"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    # Ask for confirmation
    if not context.args or context.args[0] != "confirm":
        await update.message.reply_text(
            "⚠️ This will delete ALL numbers from the database!\n"
            "To confirm, use: /deleteall confirm"
        )
        return

    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]

    # Get count before deletion
    total_numbers = await coll.count_documents({})
    
    # Delete all numbers
    result = await coll.delete_many({})
    
    # Delete all countries
    await countries_coll.delete_many({})
    
    await update.message.reply_text(
        f"🗑️ Deleted all {result.deleted_count} numbers from database."
    )

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show database statistics"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]

    # Get total numbers
    total_numbers = await coll.count_documents({})
    
    # Get countries with counts
    countries = await countries_coll.find({}).to_list(length=50)
    
    message_lines = [
        "📊 Database Statistics:",
        f"📱 Total Numbers: {total_numbers}",
        f"🌍 Total Countries: {len(countries)}",
        "",
        "📋 Countries:"
    ]
    
    for country in countries:
        flag = get_country_flag(country.get("detected_country", country["country_code"]))
        display_name = country.get("display_name", country["country_code"])
        count = country.get("number_count", 0)
        message_lines.append(f"{flag} {display_name}: {count} numbers")
    
    await update.message.reply_text("\n".join(message_lines))

SERVICE_OPTIONS = {"facebook", "whatsapp", "telegram", "other"}


async def addservice_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tag the next /add upload with a specific service.

    Usage: /addservice <Facebook|WhatsApp|Telegram|Other>

    After running this, paste numbers (or upload a CSV) and finish with
    'done'. The bot will then ask for a country name and store every
    number tagged with both the country AND the chosen service. Users
    who tap that service in /start will only see those numbers."""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "📱 *Add Numbers for a Service*\n\n"
            "Usage: `/addservice <service> [country]`\n\n"
            "Examples:\n"
            "• `/addservice WhatsApp`\n"
            "• `/addservice Facebook Pakistan`\n\n"
            f"Allowed services: {', '.join(s.title() for s in SERVICE_OPTIONS)}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    service = args[0].strip().lower()
    if service not in SERVICE_OPTIONS:
        await update.message.reply_text(
            f"❌ Unknown service `{args[0]}`. Allowed: "
            f"{', '.join(s.title() for s in SERVICE_OPTIONS)}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # Always reset stale state from a previous add/addservice flow first
    reset_add_flow(user_id)

    add_service[user_id] = service
    user_states[user_id] = "waiting_for_manual_numbers"
    manual_numbers[user_id] = []

    pre_country = " ".join(args[1:]).strip() if len(args) > 1 else ""
    if pre_country:
        add_service[f"{user_id}_country"] = pre_country

    pre_country_msg = f"\n🌍 Country pre-set to *{pre_country}*\n" if pre_country else ""
    await update.message.reply_text(
        f"📱 *Adding numbers for service: {service.title()}*{pre_country_msg}\n"
        "Send the phone numbers (one per line) or upload a CSV.\n"
        "When you're done, send `done`.\n"
        "Send `cancel` to abort.",
        parse_mode=ParseMode.MARKDOWN,
    )


async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test command for debugging"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return
    
    args = context.args
    if args and args[0].isdigit():
        # Test specific phone number
        phone_number = args[0]
        await update.message.reply_text(f"🔍 Testing OTP for number: {phone_number}")
        
        # Check SMS for this number
        sms_info = await get_latest_sms_for_number(phone_number)
        
        if sms_info:
            await update.message.reply_text(
                f"📱 SMS Info for {phone_number}:\n"
                f"Sender: {sms_info['sms']['sender']}\n"
                f"Message: {sms_info['sms']['message']}\n"
                f"OTP: {sms_info['otp']}\n"
                f"Total Messages: {sms_info['total_messages']}"
            )
        else:
            await update.message.reply_text(f"❌ No SMS found for {phone_number}")
    else:
        # Test OTP extraction
        test_message = "# Snapchat 157737 is your one time passcode for phone enrollment"
        otp = extract_otp_from_message(test_message)
        
        await update.message.reply_text(
            f"🧪 Test Results:\n"
            f"Test Message: {test_message}\n"
            f"Extracted OTP: {otp}\n"
            f"Active Monitors: {list(active_number_monitors.keys())}"
        )

async def cleanup_used_numbers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clean up numbers that have received OTPs"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return
    
    await update.message.reply_text("🧹 Starting cleanup of numbers with OTPs...")
    
    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]
    
    # Get all numbers from database
    all_numbers = await coll.find({}).to_list(length=None)
    deleted_count = 0
    kept_count = 0
    
    for num_data in all_numbers:
        phone_number = num_data["number"]
        country_code = num_data["country_code"]
        
        # Check if this number has received any OTPs
        sms_info = await get_latest_sms_for_number(phone_number)
        
        if sms_info and sms_info['otp']:
            # This number has received an OTP, delete it
            await coll.delete_one({"number": phone_number})
            
            # Update country count
            await countries_coll.update_one(
                {"country_code": country_code},
                {"$inc": {"number_count": -1}}
            )
            clear_countries_cache()
            
            deleted_count += 1
            logging.info(f"Cleaned up number {phone_number} with OTP: {sms_info['otp']}")
        else:
            kept_count += 1
    
    await update.message.reply_text(
        f"✅ Cleanup completed!\n\n"
        f"🗑️ Deleted {deleted_count} numbers with OTPs\n"
        f"✅ Kept {kept_count} numbers without OTPs\n"
        f"📊 Total processed: {deleted_count + kept_count}"
    )

async def force_otp_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force OTP check for a specific number"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return
    
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /forceotp <phone_number>")
        return
    
    phone_number = args[0]
    await update.message.reply_text(f"🔍 Force checking OTP for {phone_number}")
    
    # Get latest SMS and OTP
    sms_info = await get_latest_sms_for_number(phone_number)
    
    if sms_info and sms_info['otp']:
        await update.message.reply_text(
            f"✅ OTP Found!\n"
            f"Number: {phone_number}\n"
            f"OTP: {sms_info['otp']}\n"
            f"Sender: {sms_info['sms']['sender']}\n"
            f"Time: {sms_info['sms']['datetime']}"
        )
    else:
        await update.message.reply_text(f"❌ No OTP found for {phone_number}")

async def check_monitoring_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check current OTP monitoring status"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return
    
    if active_number_monitors:
        status_text = "📊 Active OTP Monitoring:\n\n"
        for phone_number, monitor_data in active_number_monitors.items():
            status_text += f"📞 {phone_number}\n"
            status_text += f"   Status: {'Running' if not monitor_data['stop'] else 'Stopping'}\n"
            status_text += f"   Last OTP: {monitor_data['last_otp'] or 'None'}\n"
            status_text += f"   Start Time: {monitor_data['start_time']}\n\n"
    else:
        status_text = "📊 No active OTP monitoring"
    
    await update.message.reply_text(status_text)

async def check_country_numbers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check how many numbers are available for each country"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return
    
    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]
    
    # Get all countries
    countries = await countries_coll.find({}).to_list(length=None)
    
    status_text = "📊 Numbers Available by Country:\n\n"
    
    for country in countries:
        country_code = country["country_code"]
        country_name = country["display_name"]
        
        # Count numbers for this country
        count = await coll.count_documents({"country_code": country_code})
        
        status_text += f"🌍 {country_name} ({country_code})\n"
        status_text += f"   📱 Available: {count} numbers\n\n"
    
    await update.message.reply_text(status_text)

async def show_my_morning_calls(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all active morning calls for the user"""
    if not await require_verified_message(update, context):
        return
    user_id = update.effective_user.id
    
    if user_id not in user_monitoring_sessions or not user_monitoring_sessions[user_id]:
        await update.message.reply_text("📞 You have no active morning calls.")
        return
    
    status_text = "📞 Your Active Morning Calls:\n\n"
    
    for session_id, session_data in user_monitoring_sessions[user_id].items():
        phone_number = session_data['phone_number']
        country_name = session_data['country_name']
        start_time = session_data['start_time']
        
        # Calculate remaining time (2 minutes = 120 seconds)
        current_time = datetime.now(TIMEZONE)
        elapsed = (current_time - start_time).total_seconds()
        remaining = max(0, 120 - elapsed)
        
        status_text += f"📱 {format_number_display(phone_number)}\n"
        status_text += f"   🌍 {country_name}\n"
        status_text += f"   ⏰ Remaining: {int(remaining)} seconds\n"
        status_text += f"   🕐 Started: {start_time.strftime('%H:%M:%S')}\n\n"
    
    await update.message.reply_text(status_text)

# === SESSION / API MGMT ===
async def update_sms_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Update an SMS API panel's session cookie.
    Usage: /updatesms <PanelName> PHPSESSID=...
    """
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    args = context.args
    panel_list = "\n".join([f"  • `{p.get('name', '?')}`" for p in engine.SMS_APIS]) if engine.SMS_APIS else "  • (none configured)"

    if len(args) < 2:
        await update.message.reply_text(
            "🔑 **SMS API Session Update**\n\n"
            "**Usage:** `/updatesms <PanelName> <new_cookie>`\n\n"
            "**Example:** `/updatesms Panel-1 PHPSESSID=abc123def456`\n\n"
            "**How to get a new session:**\n"
            "1. Login to the SMS panel in your browser\n"
            "2. Open Developer Tools (F12) → Network tab\n"
            "3. Refresh the page\n"
            "4. Find a request to data_smscdr.php\n"
            "5. Copy the `Cookie:` header value\n\n"
            f"**Configured panels:**\n{panel_list}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    target_panel = args[0]
    new_cookie = " ".join(args[1:])

    if not new_cookie.startswith("PHPSESSID="):
        await update.message.reply_text("❌ Invalid session cookie format. Must start with 'PHPSESSID='")
        return

    # Resolve the named panel
    api_cfg = None
    for p in engine.SMS_APIS:
        if p.get("name", "").lower() == target_panel.lower():
            api_cfg = p
            break
    if not api_cfg:
        names = ", ".join([p.get("name", "?") for p in engine.SMS_APIS]) or "(none)"
        await update.message.reply_text(f"❌ Panel '{target_panel}' not found.\nConfigured: {names}")
        return

    test_base_url = api_cfg.get("base_url", SMS_API_BASE_URL)
    test_endpoint = api_cfg.get("endpoint", SMS_API_ENDPOINT)
    await update.message.reply_text(f"🔄 Testing new session for panel '{target_panel}'...")
    
    # Test the new session before applying
    try:
        url = f"{test_base_url}{test_endpoint}"
        
        from datetime import datetime, timedelta
        import pytz
        timezone = pytz.timezone(TIMEZONE_NAME)
        now = datetime.now(timezone)
        yesterday = now - timedelta(hours=24)
        date_str = yesterday.strftime("%Y-%m-%d")
        
        params = {
            'fdate1': f"{date_str} 00:00:00",
            'fdate2': f"{now.strftime('%Y-%m-%d %H:%M:%S')}",
            'fnum': '000000000',
            'iDisplayLength': '1',
            'sSortDir_0': 'desc',
            '_': str(int(datetime.now().timestamp() * 1000))
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Mobile Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': derive_referer(test_base_url, test_endpoint),
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.9,ks-IN;q=0.8,ks;q=0.7',
            'Cookie': new_cookie  # Test with new cookie
        }
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url, params=params, headers=headers) as response:
                response_text = await response.text()
                
                # Check if new session works
                if response.status == 200 and response_text.strip().startswith('{'):
                    try:
                        import json
                        json.loads(response_text)  # Validate JSON

                        # Session test passed — apply update to the named panel.
                        old_display = api_cfg.get("cookie", "") or "(none)"
                        ok = update_panel_cookie_in_config(target_panel, new_cookie)
                        config_updated = "✅ Panel cookie updated in config.py" if ok else "⚠️ Failed to update config.py"
                        scope = f"Panel '{target_panel}'"

                        await update.message.reply_text(
                            f"✅ **SMS API Session Updated Successfully!**\n\n"
                            f"🎯 **Scope:** {scope}\n"
                            f"🔑 **New session:** `{new_cookie[:20]}...{new_cookie[-10:]}`\n"
                            f"🔑 **Old session:** `{(old_display[:20] + '...' + old_display[-10:]) if old_display.startswith('PHPSESSID=') else old_display}`\n\n"
                            f"🔄 **Status:** Active immediately (no restart needed)\n"
                            f"📁 **Config:** {config_updated}\n"
                            f"🎯 **API:** Ready for OTP detection\n\n"
                            f"_Session updated at {now.strftime('%Y-%m-%d %H:%M:%S')}_",
                            parse_mode=ParseMode.MARKDOWN
                        )

                        
                    except:
                        await update.message.reply_text("❌ New session returns invalid JSON response")
                        
                elif 'login' in response_text.lower():
                    await update.message.reply_text("❌ New session is invalid - redirected to login")
                elif 'direct script access not allowed' in response_text.lower():
                    await update.message.reply_text("❌ New session blocked - direct script access not allowed")
                else:
                    await update.message.reply_text(f"❌ New session test failed - HTTP {response.status}")
                    
    except Exception as e:
        await update.message.reply_text(f"❌ Session test failed: {str(e)}")

async def add_api(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new SMS API panel from Telegram (no file edit needed).
    Usage:
      /addapi <Name> <base_url> PHPSESSID=...
    Example:
      /addapi Panel-2 http://1.2.3.4 PHPSESSID=abc123def456
    """
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "🆕 **Add a new SMS API panel**\n\n"
            "**Usage:**\n"
            "• `/addapi <Name> <base_url> PHPSESSID=...`\n"
            "• `/addapi <Name> <base_url> <endpoint> PHPSESSID=...`\n\n"
            "**Examples:**\n"
            "• `/addapi Panel-2 http://1.2.3.4 PHPSESSID=abc123def456`\n"
            "• `/addapi Panel-2 https://pscall.net /agent/res/data_smscdr.php PHPSESSID=abc123`\n\n"
            "• `Name` — short label, no spaces (e.g. `Panel-2`)\n"
            "• `base_url` — panel host (e.g. `http://1.2.3.4`)\n"
            "• `endpoint` *(optional)* — only if your panel uses a non-standard path. "
            "Default is `/ints/agent/res/data_smscdr.php`.\n"
            "• `PHPSESSID=...` — cookie from your logged-in browser session\n\n"
            "The bot tests the cookie first and only saves the panel if it works.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    name = args[0]
    base_url = args[1]
    # Optional 3rd arg is endpoint if it starts with '/' (otherwise it's the cookie)
    if len(args) >= 4 and args[2].startswith("/"):
        endpoint = args[2]
        cookie = " ".join(args[3:])
    else:
        endpoint = "/ints/agent/res/data_smscdr.php"
        cookie = " ".join(args[2:])

    if not cookie.startswith("PHPSESSID="):
        await update.message.reply_text("❌ The cookie must start with `PHPSESSID=`.", parse_mode=ParseMode.MARKDOWN)
        return
    if not (base_url.startswith("http://") or base_url.startswith("https://")):
        await update.message.reply_text("❌ `base_url` must start with `http://` or `https://`.", parse_mode=ParseMode.MARKDOWN)
        return
    if any(p.get("name", "").lower() == name.lower() for p in engine.SMS_APIS):
        await update.message.reply_text(f"❌ A panel named '{name}' already exists. Use `/listapis` to see them.", parse_mode=ParseMode.MARKDOWN)
        return

    await update.message.reply_text(f"🔄 Testing new panel '{name}' at {base_url}{endpoint}...")

    # Test the cookie against the new panel before saving
    try:
        from datetime import datetime, timedelta
        import pytz
        timezone = pytz.timezone(TIMEZONE_NAME)
        now = datetime.now(timezone)
        yesterday = now - timedelta(hours=24)
        date_str = yesterday.strftime("%Y-%m-%d")
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
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url, params=params, headers=headers) as response:
                response_text = await response.text()
                if response.status == 200 and response_text.strip().startswith('{'):
                    try:
                        import json as _json
                        _json.loads(response_text)
                    except Exception:
                        await update.message.reply_text("❌ Test failed: panel returned invalid JSON.")
                        return
                elif 'login' in response_text.lower():
                    await update.message.reply_text("❌ Test failed: cookie is invalid (redirected to login).")
                    return
                else:
                    await update.message.reply_text(f"❌ Test failed: HTTP {response.status}.")
                    return
    except Exception as e:
        await update.message.reply_text(f"❌ Test failed: {e}")
        return

    # Save it
    ok, msg = add_sms_api_to_config(name, base_url, cookie, endpoint)
    if not ok:
        await update.message.reply_text(f"❌ Could not save: {msg}")
        return

    await update.message.reply_text(
        f"✅ **Panel added!**\n\n"
        f"🏷️ **Name:** `{name}`\n"
        f"📡 **URL:** `{base_url}`\n"
        f"🍪 **Cookie:** `{cookie[:20]}...{cookie[-10:]}`\n\n"
        f"🎯 Active immediately — no restart needed.\n"
        f"_Use `/listapis` to see all panels._",
        parse_mode=ParseMode.MARKDOWN,
    )


async def remove_api(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a panel from SMS_APIS by name.
    Usage: /removeapi <Name>
    """
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    args = context.args
    if not args:
        names = ", ".join([p.get("name", "?") for p in engine.SMS_APIS]) or "(none)"
        await update.message.reply_text(
            "🗑️ **Remove a panel**\n\n"
            "**Usage:** `/removeapi <Name>`\n\n"
            f"**Configured:** {names}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    name = args[0]
    ok, msg = remove_sms_api_from_config(name)
    if ok:
        await update.message.reply_text(f"✅ {msg}")
    else:
        await update.message.reply_text(f"❌ {msg}")


async def set_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set the SINGLE Telegram group/channel that receives every OTP from
    every panel for every user. Also acts as the safety net when a user
    can't be DMed.

    Usage:
      /setgroup <chat_id>

    Example:
      /setgroup -1003140522913
    """
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    args = context.args
    if len(args) < 1:
        current = engine.OTP_GROUP_CHAT_ID or "(not set)"
        await update.message.reply_text(
            "👥 <b>Set the global OTP group</b>\n\n"
            "<b>Usage:</b> <code>/setgroup &lt;chat_id&gt;</code>\n\n"
            f"<b>Current group:</b> <code>{current}</code>\n\n"
            "<i>Tip: forward any message from your group to @userinfobot to get its chat ID.</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        chat_id = int(args[0])
    except ValueError:
        await update.message.reply_text(
            "❌ Chat ID must be an integer (e.g. <code>-1003140522913</code>).",
            parse_mode=ParseMode.HTML,
        )
        return

    # Quick sanity check: try sending a test message to the group
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text="✅ This group is now the global OTP group. "
                 "Every captured OTP from every panel will be posted here.",
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Could not send to chat <code>{chat_id}</code>: {e}\n\n"
            "Make sure the bot is added to that group and has permission to send messages.",
            parse_mode=ParseMode.HTML,
        )
        return

    ok, msg = set_otp_group_in_config(chat_id)
    if ok:
        await update.message.reply_text(f"✅ {msg}", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(f"❌ {msg}")


async def list_apis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all configured SMS API panels and their cookie status."""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    if not engine.SMS_APIS:
        await update.message.reply_text("⚠️ No SMS APIs configured. Edit config.py -> SMS_APIS.")
        return

    import html as _html
    lines = ["🌐 <b>Configured SMS API Panels</b>\n"]
    for i, p in enumerate(engine.SMS_APIS, start=1):
        name = _html.escape(str(p.get("name", f"#{i}")))
        base_url = _html.escape(str(p.get("base_url", "?")))
        cookie = p.get("cookie", "") or ""
        cookie_preview = (cookie[:20] + "..." + cookie[-10:]) if len(cookie) > 30 else (cookie or "(empty)")
        cookie_preview = _html.escape(cookie_preview)
        lines.append(
            f"{i}. <b>{name}</b>\n"
            f"   📡 {base_url}\n"
            f"   🍪 <code>{cookie_preview}</code>\n"
        )
    group_display = f"<code>{engine.OTP_GROUP_CHAT_ID}</code>" if engine.OTP_GROUP_CHAT_ID else "<i>(not set)</i>"
    lines.append(f"\n👥 <b>Global OTP group:</b> {group_display}")
    lines.append("<i>Update cookie:</i> <code>/updatesms &lt;PanelName&gt; PHPSESSID=...</code>")
    lines.append("<i>Change OTP group:</i> <code>/setgroup &lt;chat_id&gt;</code>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all admin commands with examples"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return
    
    admin_commands = f"""
🔧 *ADMIN COMMAND CENTER*
━━━━━━━━━━━━━━━━━━━━━━━━━━━

*📊 DATABASE MANAGEMENT:*
1️⃣ `/stats` — View database statistics
2️⃣ `/list` — List all numbers grouped by country
3️⃣ `/list Pakistan` — List numbers for a specific country
4️⃣ `/countrynumbers` — Show available number count per country
5️⃣ `/remove Mozambique` — Remove a country (and all its numbers) by name
6️⃣ `/delete <country_code>` — Remove a country by internal code (e.g. `/delete pakistan`)
7️⃣ `/deleteall` — Delete every number in the database (with confirmation)

*📱 NUMBER MANAGEMENT:*
8️⃣ `/addservice <Service> [Country]` — Add numbers tagged for a specific service
       Services: Facebook, WhatsApp, Telegram, Other
       Example: `/addservice Facebook Pakistan`
9️⃣ `/addlist` — Process an already-uploaded CSV by country name
🔟 `/cleanup` — Sweep numbers that have already received OTPs

*🔍 MONITORING & TESTING:*
1️⃣1️⃣ `/monitoring` — Check active OTP monitoring sessions
1️⃣2️⃣ `/morningcalls` — Show your active number-watch sessions
1️⃣3️⃣ `/resetnumber` — Clear your own current-number tracking
1️⃣4️⃣ `/test` — Debug helper for testing OTP extraction
1️⃣5️⃣ `/forceotp 923066082919` — Force an OTP check for a specific number

*🌐 API & SESSION MANAGEMENT:*
1️⃣6️⃣ `/checkapi` — Test every SMS panel's connectivity and auth
1️⃣7️⃣ `/listapis` — List configured SMS panels
1️⃣8️⃣ `/addapi` — Add a new SMS panel
1️⃣9️⃣ `/removeapi <name>` — Remove an SMS panel by name
2️⃣0️⃣ `/setgroup <chat_id>` — Set the global group that receives every OTP
2️⃣1️⃣ `/updatesms <Panel> PHPSESSID=abc123def456` — Update a panel's session cookie
2️⃣2️⃣ `/reloadsession` — Reload SMS sessions from `config.py`
2️⃣3️⃣ `/clearcache` — Clear the countries cache (force fresh DB read)
2️⃣4️⃣ `/resetuser <user_id | @username>` — Reset a user's channel verification

━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 *QUICK EXAMPLES:*

• Add Facebook-only Pakistan numbers: `/addservice Facebook Pakistan`
• Remove an entire country: `/remove Mozambique`
• Health check before fixing: `/checkapi` → `/updatesms PHPSESSID=…`
• See where things stand: `/stats`

━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚡ *POWER USER TIPS:*

🔄 Session: run `/checkapi` first; if a panel fails, refresh with `/updatesms <Panel> PHPSESSID=…` or `/reloadsession`.
📊 Health: `/stats` + `/countrynumbers` give a full snapshot of pools.
🧹 Maintenance: `/cleanup` weekly; `/remove <Country>` for stale pools.
🎯 Service routing: `/addservice` so users only see numbers for the service they pick.
🔍 Debug: `/test` + `/forceotp <number>` to investigate missing OTPs.

🎯 Admin ID: `{user_id}`
📍 Status: Full administrative access granted
"""

    await update.message.reply_text(admin_commands, parse_mode=ParseMode.MARKDOWN)


# === USER / CACHE / NUMBER ADMIN ===
async def clear_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear countries cache to force refresh"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return
    
    clear_countries_cache()
    await update.message.reply_text("✅ Countries cache cleared. Next country list will be refreshed from database.")

async def reset_user_verification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ADMIN ONLY: Reset a user's channel verification.

    Usage: /resetuser <user_id | @username>
    Removes the user from the verified-users database and deletes their cache file,
    forcing them to re-verify channel membership next time they /start.
    """
    admin_id = update.effective_user.id
    if admin_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: `/resetuser <user_id>` or `/resetuser @username`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    target = args[0].strip()
    db = context.bot_data["db"]
    users_coll = db[USERS_COLLECTION]

    target_user = None
    target_id = None

    if target.startswith("@"):
        username = target[1:].lower()
        target_user = await users_coll.find_one({
            "username": {"$regex": f"^{re.escape(username)}$", "$options": "i"}
        })
        if not target_user:
            await update.message.reply_text(
                f"❌ No verified user found with username `@{username}`.\n"
                "Tip: usernames only match users who verified at least once. "
                "Try the numeric user ID instead.",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
        raw_id = target_user.get("user_id")
        if not isinstance(raw_id, int):
            await update.message.reply_text(
                f"❌ Found a record for `@{username}` but it has no valid user ID. "
                "Please use the numeric user ID instead.",
                parse_mode=ParseMode.MARKDOWN,
            )
            return
        target_id = raw_id
    else:
        try:
            target_id = int(target)
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid input. Provide a numeric user ID or `@username`."
            )
            return
        target_user = await users_coll.find_one({"user_id": target_id})

    db_removed = False
    cache_removed = False
    db_error = None
    cache_error = None

    try:
        result = await users_coll.delete_one({"user_id": target_id})
        db_removed = result.deleted_count > 0
    except Exception as e:
        db_error = str(e)
        logging.error(f"Error removing user {target_id} from DB: {e}")

    cache_file = os.path.join(USER_CACHE_DIR, f"user_{target_id}.json")
    cache_existed = os.path.exists(cache_file)
    if cache_existed:
        try:
            os.remove(cache_file)
            cache_removed = True
        except Exception as e:
            cache_error = str(e)
            logging.error(f"Error removing cache file for user {target_id}: {e}")

    name_hint = ""
    if target_user:
        uname = target_user.get("username")
        fname = target_user.get("first_name") or ""
        lname = target_user.get("last_name") or ""
        full = (fname + " " + lname).strip()
        if uname:
            name_hint = f" (@{uname})"
        elif full:
            name_hint = f" ({full})"

    # Build a per-step status line so the admin sees exactly what happened.
    if db_error:
        db_line = f"❌ Database: error — {db_error}"
    elif db_removed:
        db_line = "✅ Database record removed"
    else:
        db_line = "ℹ️ Database: no record found"

    if cache_error:
        cache_line = f"❌ Cache: error — {cache_error}"
    elif cache_removed:
        cache_line = "✅ Cache file removed"
    elif cache_existed:
        cache_line = "❌ Cache: file existed but was not removed"
    else:
        cache_line = "ℹ️ Cache: no file found"

    # Decide overall headline.
    had_error = bool(db_error or cache_error)
    did_anything = db_removed or cache_removed

    if had_error:
        headline = f"⚠️ Reset partially failed for user `{target_id}`{name_hint}."
        footer = "Fix the underlying error and re-run, or remove the remaining artifact manually."
    elif did_anything:
        headline = f"✅ Reset done for user `{target_id}`{name_hint}."
        footer = "They will need to /start the bot and rejoin-verify the channel."
    else:
        headline = f"ℹ️ User `{target_id}`{name_hint} was not verified — nothing to reset."
        footer = ""

    logging.info(
        f"Admin {admin_id} ran /resetuser on {target_id}{name_hint}: "
        f"db_removed={db_removed} cache_removed={cache_removed} "
        f"db_error={db_error} cache_error={cache_error}"
    )

    msg = f"{headline}\n• {db_line}\n• {cache_line}"
    if footer:
        msg += f"\n\n{footer}"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def reload_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reload SMS API session from config file"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return
    
    await update.message.reply_text("🔄 Reloading session from config file...")
    
    old_session = get_current_sms_cookie()
    session_changed = reload_config_session()
    new_session = get_current_sms_cookie()
    
    if session_changed:
        await update.message.reply_text(
            f"✅ **Session Reloaded from Config File**\n\n"
            f"🔑 **Old session:** `{old_session[:20]}...{old_session[-10:]}`\n"
            f"🔑 **New session:** `{new_session[:20]}...{new_session[-10:]}`\n\n"
            f"🔄 **Status:** Active immediately\n"
            f"📁 **Source:** config.py file\n\n"
            f"💡 **Tip:** Use `/checkapi` to verify connection",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await update.message.reply_text(
            f"ℹ️ **No Session Change**\n\n"
            f"🔑 **Current session:** `{new_session[:20]}...{new_session[-10:]}`\n\n"
            f"✅ Session is already up to date with config file",
            parse_mode=ParseMode.MARKDOWN
        )

async def reset_current_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset current number tracking for debugging"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        return
    
    if user_id in current_user_numbers:
        old_number = current_user_numbers[user_id]
        del current_user_numbers[user_id]
        await update.message.reply_text(f"✅ Reset current number tracking for user {user_id}\nOld number: {old_number}")
    else:
        await update.message.reply_text(f"ℹ️ No current number tracking found for user {user_id}")

async def list_numbers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all numbers in database"""
    user_id = update.effective_user.id
    logging.info(f"List numbers command called by user {user_id}")
    
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    args = context.args
    country_filter = None
    if args:
        country_filter = args[0].lower()

    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]

    # Build query
    query = {}
    if country_filter:
        query["country_code"] = country_filter

    # Get numbers
    numbers = await coll.find(query).limit(20).to_list(length=20)
    
    if not numbers:
        if country_filter:
            await update.message.reply_text(f"📭 No numbers found for country '{country_filter}'.")
        else:
            await update.message.reply_text("📭 No numbers found in database.")
        return

    message_lines = [f"📱 Numbers in database{f' for {country_filter}' if country_filter else ''}:"]
    
    for num_data in numbers:
        flag = get_country_flag(num_data.get("detected_country", num_data["country_code"]))
        formatted_num = format_number_display(num_data["number"])
        country_code = num_data["country_code"]
        message_lines.append(f"{flag} {formatted_num} ({country_code})")
    
    if len(numbers) == 20:
        message_lines.append("\n... (showing first 20 numbers)")
    
    message_lines.append(f"\nTotal: {len(numbers)} numbers shown")
    
    await update.message.reply_text("\n".join(message_lines))


# === CSV / NUMBER UPLOAD FLOW ===
async def upload_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    if not update.message.document:
        await update.message.reply_text("❌ Please upload a CSV file.")
        return

    file = update.message.document
    if not file.file_name.lower().endswith('.csv'):
        await update.message.reply_text("❌ Only CSV files are supported.")
        return

    await update.message.reply_text("📥 CSV file received!")

    file_obj = await file.get_file()
    file_bytes = BytesIO()
    await file_obj.download_to_memory(out=file_bytes)
    file_bytes.seek(0)
    uploaded_csv[user_id] = file_bytes

    # Check if user is in add command flow (either waiting for manual numbers or CSV)
    if user_id in user_states and user_states[user_id] in ["waiting_for_csv", "waiting_for_manual_numbers"]:
        pre_country = add_service.get(f"{user_id}_country")
        if pre_country:
            user_states[user_id] = "waiting_for_name"
            await process_all_numbers_with_country(update, context, pre_country)
            return
        user_states[user_id] = "waiting_for_name"
        await update.message.reply_text(
            "🌍 Please enter the name for all the numbers (manual + CSV):\n"
            "Examples: Sri Lanka Ws, Sri Lanka Tg, etc.\n"
            "This name will be used for all numbers (manual and CSV)."
        )
    else:
        # Regular CSV upload flow
        user_states[user_id] = "waiting_for_country"
        await update.message.reply_text(
            "🌍 Please enter the country name for the numbers in this CSV file:\n"
            "Examples: India Ws, India Tg, Saudi Arabia, USA, etc.\n"
            "You can use custom names like 'India Ws' for WhatsApp numbers or 'India Tg' for Telegram numbers."
        )

async def addlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process CSV file by asking for country name directly"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return

    if not uploaded_csv.get(user_id):
        await update.message.reply_text("❌ No CSV file found. Please upload the file first.")
        return

    # Set user state to ask for country name directly
    user_states[user_id] = "waiting_for_country"
    await update.message.reply_text(
        "🌍 Please enter the country name for the numbers in the CSV file:\n"
        "Examples: Sri Lanka Ws, Sri Lanka Tg, India, Saudi Arabia, USA, etc.\n"
        "You can use custom names like 'India Ws' for WhatsApp numbers or 'India Tg' for Telegram numbers."
    )

async def process_all_numbers_with_country(update: Update, context: ContextTypes.DEFAULT_TYPE, country_name):
    """Process both manual numbers and CSV file with the provided country name"""
    user_id = update.effective_user.id

    await update.message.reply_text("🔍 Analyzing and processing all numbers...")

    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]

    # Get manual numbers
    manual_nums = manual_numbers.get(user_id, [])

    # Process CSV file if available
    csv_numbers = []
    csv_buf = uploaded_csv.get(user_id)
    if csv_buf:
        csv_numbers, process_msg = await process_csv_file(csv_buf)
        if not csv_numbers:
            csv_numbers = []

    # Combine all numbers
    all_numbers = []
    
    # Add manual numbers
    for number in manual_nums:
        all_numbers.append({
            'number': number,
            'original_number': number,
            'country_code': None,
            'range': '',
            'source': 'manual'
        })
    
    # Add CSV numbers
    for num_data in csv_numbers:
        all_numbers.append({
            'number': num_data['number'],
            'original_number': num_data['original_number'],
            'country_code': None,
            'range': num_data.get('range', ''),
            'source': 'csv'
        })

    if not all_numbers:
        await update.message.reply_text("❌ No numbers found to process.")
        reset_add_flow(user_id)
        return

    # Detect the most common country from all numbers
    detected_countries = {}
    for num_data in all_numbers:
        detected_country = detect_country_code(num_data['number'], num_data.get('range', ''))
        if detected_country:
            detected_countries[detected_country] = detected_countries.get(detected_country, 0) + 1
    
    # Get the most common detected country
    most_common_country = None
    if detected_countries:
        most_common_country = max(detected_countries, key=detected_countries.get)
    
    # Use the provided country name as the country code (custom naming)
    country_code = country_name.lower().replace(" ", "_")
    country_display_name = country_name
    
    # Store the detected country for flag purposes
    detected_country_code = most_common_country if most_common_country else "unknown"

    # Set country code for all numbers
    for num_data in all_numbers:
        num_data['country_code'] = country_code

    # Upload to database
    inserted_count = 0
    number_details = []
    manual_count = 0
    csv_count = 0

    service_tag = add_service.get(user_id)
    for num_data in all_numbers:
        try:
            doc = {
                "country_code": num_data['country_code'],
                "number": num_data['number'],
                "original_number": num_data['original_number'],
                "range": num_data['range'],
                "detected_country": detected_country_code,
                "added_at": datetime.now(TIMEZONE),
            }
            if service_tag:
                doc["service"] = service_tag
            await coll.insert_one(doc)

            inserted_count += 1
            if num_data['source'] == 'manual':
                manual_count += 1
            else:
                csv_count += 1
            
            # Get country flag from detected country, but display custom name
            flag = get_country_flag(detected_country_code)
            number_details.append(f"{flag} {num_data['number']} - {country_display_name}")
        except Exception as e:
            logging.error(f"Error inserting number: {e}")
            continue

    # Update countries collection
    set_doc = {
        "country_code": country_code,
        "display_name": country_display_name,
        "detected_country": detected_country_code,
        "last_updated": datetime.now(TIMEZONE),
    }
    update_doc = {"$set": set_doc, "$inc": {"number_count": inserted_count}}
    if service_tag:
        update_doc["$addToSet"] = {"services": service_tag}
    await countries_coll.update_one(
        {"country_code": country_code}, update_doc, upsert=True
    )
    clear_countries_cache()

    # Clear all per-admin add flow state
    reset_add_flow(user_id)

    # Prepare report
    report_lines = [
        "📊 Combined Upload Report:",
        f"✅ Successfully uploaded {inserted_count} numbers",
        f"📱 Manual numbers: {manual_count}",
        f"📄 CSV numbers: {csv_count}",
        f"🌍 Custom Name: {country_display_name}",
    ]
    
    if most_common_country:
        detected_country_name = "Unknown"
        try:
            country = pycountry.countries.get(alpha_2=most_common_country.upper())
            if country:
                detected_country_name = country.name
        except:
            pass
        report_lines.append(f"🏳️ Detected Country: {detected_country_name} ({most_common_country.upper()})")
    
    report_lines.extend([
        "",
        "📋 Sample numbers:",
        *number_details[:10]
    ])

    if len(number_details) > 10:
        report_lines.append(f"\n... and {len(number_details) - 10} more numbers")

    # Send report
    await update.message.reply_text("\n".join(report_lines))

    # Send complete list as file if many numbers
    if len(number_details) > 10:
        report_file = BytesIO()
        report_file.write("\n".join([
            "Number,Custom Country,Detected Country,Source",
            *[f"{num.split(' - ')[0]},{country_display_name},{detected_country_code.upper()},{'manual' if i < manual_count else 'csv'}" 
              for i, num in enumerate(number_details)]
        ]).encode('utf-8'))
        report_file.seek(0)
        await update.message.reply_document(
            document=report_file,
            filename="combined_number_upload_report.csv",
            caption="📄 Complete combined number upload report"
        )

async def process_csv_with_country(update: Update, context: ContextTypes.DEFAULT_TYPE, country_name):
    """Process CSV file with the provided country name"""
    user_id = update.effective_user.id
    csv_buf = uploaded_csv.get(user_id)

    if not csv_buf:
        await update.message.reply_text("❌ No CSV file found. Please upload the file first.")
        reset_add_flow(user_id)
        return

    await update.message.reply_text("🔍 Analyzing and processing numbers...")

    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]

    # Process CSV file first to detect country from numbers
    numbers, process_msg = await process_csv_file(csv_buf)
    if not numbers:
        await update.message.reply_text(f"❌ {process_msg}")
        reset_add_flow(user_id)
        return

    # Detect the most common country from the numbers
    detected_countries = {}
    for num_data in numbers:
        detected_country = detect_country_code(num_data['number'], num_data.get('range', ''))
        if detected_country:
            detected_countries[detected_country] = detected_countries.get(detected_country, 0) + 1
    
    # Get the most common detected country
    most_common_country = None
    if detected_countries:
        most_common_country = max(detected_countries, key=detected_countries.get)
    
    # Use the provided country name as the country code (custom naming)
    country_code = country_name.lower().replace(" ", "_")
    country_display_name = country_name
    
    # Store the detected country for flag purposes
    detected_country_code = most_common_country if most_common_country else "unknown"

    # Override country codes with the provided country
    for num_data in numbers:
        num_data['country_code'] = country_code

    # Upload to database
    inserted_count = 0
    number_details = []

    service_tag = add_service.get(user_id)
    for num_data in numbers:
        try:
            doc = {
                "country_code": num_data['country_code'],
                "number": num_data['number'],
                "original_number": num_data['original_number'],
                "range": num_data['range'],
                "detected_country": detected_country_code,
                "added_at": datetime.now(TIMEZONE),
            }
            if service_tag:
                doc["service"] = service_tag
            await coll.insert_one(doc)

            inserted_count += 1
            flag = get_country_flag(detected_country_code)
            number_details.append(f"{flag} {num_data['number']} - {country_display_name}")
        except Exception as e:
            logging.error(f"Error inserting number: {e}")
            continue

    # Update countries collection
    set_doc = {
        "country_code": country_code,
        "display_name": country_display_name,
        "detected_country": detected_country_code,
        "last_updated": datetime.now(TIMEZONE),
    }
    update_doc = {"$set": set_doc, "$inc": {"number_count": inserted_count}}
    if service_tag:
        update_doc["$addToSet"] = {"services": service_tag}
    await countries_coll.update_one(
        {"country_code": country_code}, update_doc, upsert=True
    )
    clear_countries_cache()

    # Clear all per-admin add flow state
    reset_add_flow(user_id)

    # Prepare report
    report_lines = [
        "📊 Upload Report:",
        f"✅ Successfully uploaded {inserted_count} numbers",
        f"🌍 Custom Name: {country_display_name}",
    ]
    
    if most_common_country:
        detected_country_name = "Unknown"
        try:
            country = pycountry.countries.get(alpha_2=most_common_country.upper())
            if country:
                detected_country_name = country.name
        except:
            pass
        report_lines.append(f"🏳️ Detected Country: {detected_country_name} ({most_common_country.upper()})")
    
    report_lines.extend([
        "",
        "📋 Sample numbers:",
        *number_details[:10]
    ])

    if len(number_details) > 10:
        report_lines.append(f"\n... and {len(number_details) - 10} more numbers")

    # Send report
    await update.message.reply_text("\n".join(report_lines))

    # Send complete list as file if many numbers
    if len(number_details) > 10:
        report_file = BytesIO()
        report_file.write("\n".join([
            "Number,Custom Country,Detected Country",
            *[f"{num.split(' - ')[0]},{country_display_name},{detected_country_code.upper()}" 
              for num in number_details]
        ]).encode('utf-8'))
        report_file.seek(0)
        await update.message.reply_document(
            document=report_file,
            filename="number_upload_report.csv",
            caption="📄 Complete number upload report"
        )

