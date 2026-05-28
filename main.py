import logging
import asyncio
import json
import re
import time
from datetime import datetime, timedelta

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

from config import *
import engine
from engine import *
from admin import *


# === USER COMMAND HANDLERS ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username
    first_name = update.effective_user.first_name
    last_name = update.effective_user.last_name
    
    try:
        # Check if user is already verified
        is_verified = await is_user_verified(user_id, context)
        
        if is_verified:
            # User already verified, proceed directly
            await update.message.reply_text(
                "👋 Welcome!",
                reply_markup=service_keyboard(),
            )
            return
        
        # Check channel membership for new user
        chat_member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        
        if chat_member.status in ("member", "administrator", "creator"):
            # Store user data in database
            db = context.bot_data["db"]
            users_coll = db[USERS_COLLECTION]
            
            user_data = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "verified_at": datetime.now(TIMEZONE),
                "last_activity": datetime.now(TIMEZONE),
                "status": "verified"
            }
            
            await users_coll.insert_one(user_data)
            
            # Create cache file for user
            await create_user_cache(user_id, user_data)
            
            logging.info(f"New user verified and stored via /start: {user_id} ({username})")
            
            await update.message.reply_text(
                "✅ You have successfully joined the channel!\n\n"
                "👋 Welcome!",
                reply_markup=service_keyboard(),
            )
        else:
            await update.message.reply_text("🚫 You haven't joined the channel yet!")
            await update.message.reply_text(
                "Please join the channel and check again.",
                reply_markup=join_channel_keyboard()
            )
    except Exception as e:
        logging.error(f"Error in start command: {e}")
        await update.message.reply_text("🚫 You haven't joined the channel yet!")
        await update.message.reply_text(
            "Please join the channel and check again.",
            reply_markup=join_channel_keyboard()
        )

async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        user_id = query.from_user.id
        username = query.from_user.username
        first_name = query.from_user.first_name
        last_name = query.from_user.last_name
        
        # Check if user is already verified in database
        db = context.bot_data["db"]
        users_coll = db[USERS_COLLECTION]
        
        existing_user = await users_coll.find_one({"user_id": user_id})
        
        if existing_user:
            # User already verified, proceed directly
            await query.edit_message_text("✅ Welcome back! You are already verified.")
            await context.bot.send_message(
                chat_id=user_id,
                text="👋 Welcome!",
                reply_markup=service_keyboard(),
            )
            return
        
        # Check channel membership for new user
        chat_member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        
        if chat_member.status in ['member', 'administrator', 'creator']:
            # Store user data in database
            user_data = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "verified_at": datetime.now(TIMEZONE),
                "last_activity": datetime.now(TIMEZONE),
                "status": "verified"
            }
            
            await users_coll.insert_one(user_data)
            
            # Create cache file for user
            await create_user_cache(user_id, user_data)
            
            logging.info(f"New user verified and stored: {user_id} ({username})")
            
            await query.edit_message_text(
                "✅ You have successfully joined the channel!\n"
                "📱 Your account has been verified."
            )
            await context.bot.send_message(
                chat_id=user_id,
                text="👋 Welcome!",
                reply_markup=service_keyboard(),
            )
        else:
            await query.answer("❌ You need to join the channel first!", show_alert=True)
    except Exception as e:
        logging.error(f"Error checking channel membership: {e}")
        await query.answer("❌ Error checking channel membership. Please try again.", show_alert=True)


# === NUMBER FLOW (callbacks) ===
async def send_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await require_verified_callback(update, context):
        return
    country_code = query.data.split('_', 1)[1]
    
    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]

    # Fetch up to 3 random numbers for this country (filtered by service if chosen)
    service = context.user_data.get("selected_service")
    base_match = {"country_code": country_code}
    svc_filter = engine._service_query_filter(service)
    if svc_filter:
        base_match = {"$and": [base_match, svc_filter]}

    country_name = country_code  # Default fallback
    results = []
    try:
        simple_pipeline = [
            {"$match": base_match},
            {"$sample": {"size": 3}}
        ]
        results = await coll.aggregate(simple_pipeline).to_list(length=3)

        if results:
            if engine.countries_cache:
                for country_info in engine.countries_cache:
                    if country_info.get("country_code") == country_code:
                        country_name = country_info.get("display_name", country_code)
                        break
            else:
                country_info = await countries_coll.find_one({"country_code": country_code}, {"display_name": 1})
                if country_info:
                    country_name = country_info.get("display_name", country_code)

    except Exception as e:
        logging.warning(f"Fast path failed, using full pipeline: {e}")
        pipeline = [
            {"$match": base_match},
            {"$sample": {"size": 3}},
            {"$lookup": {
                "from": COUNTRIES_COLLECTION,
                "localField": "country_code",
                "foreignField": "country_code",
                "as": "country_info"
            }},
            {"$addFields": {
                "country_name": {"$ifNull": [{"$arrayElemAt": ["$country_info.display_name", 0]}, country_code]}
            }}
        ]
        results = await coll.aggregate(pipeline).to_list(length=3)
        if results:
            country_name = results[0].get("country_name", country_code)

    if results:
        numbers = [r["number"] for r in results if "number" in r]
        formatted_numbers = [format_number_display(n) for n in numbers]

        # Track most recent number for this user (used by change_number)
        user_id = query.from_user.id
        current_user_numbers[user_id] = numbers[0]
        logging.info(f"Assigned {len(numbers)} numbers to user {user_id}: {numbers}")

        detected_country = results[0].get("detected_country", country_code)
        flag = get_country_flag(detected_country)

        numbers_block = "\n".join(f"<code>{fn}</code>" for fn in formatted_numbers)
        message = (
            f"✅ Numbers Assigned!\n\n"
            f"🌍 Country: {shorten_country_name(country_name)} {flag}\n"
            f"📱 Numbers:\n"
            f"{numbers_block}\n\n"
            f"⏳ OTP Status: Waiting..."
        )

        # Use the first number's country_code for the Change Number button
        sent_message = await query.edit_message_text(
            message,
            reply_markup=number_options_keyboard(numbers[0], country_code),
            parse_mode=ParseMode.HTML
        )

        # Start OTP monitoring for each number; pass message_id=None so the
        # multi-number summary message isn't overwritten when a single OTP arrives.
        for number in numbers:
            await start_otp_monitoring(
                number,
                None,
                query.message.chat_id,
                country_code,
                country_name,
                context,
                query.from_user.id
            )
    else:
        # Get country name for error message
        country_info = await countries_coll.find_one({"country_code": country_code})
        country_name = country_info["display_name"] if country_info else country_code

        keyboard = await countries_keyboard(db, service=service)
        suffix = f" for *{service}*" if service else ""
        if not keyboard.inline_keyboard:
            await query.edit_message_text(
                f"⚠️ No numbers available for {country_name}{suffix} right now, "
                f"and no other countries have stock. Please try again later or contact an admin.",
                parse_mode=ParseMode.MARKDOWN if service else None,
            )
        else:
            await query.edit_message_text(
                f"⚠️ No numbers available for {country_name}{suffix} right now. Please try another country.",
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN if service else None,
            )

async def change_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # TEMPORARILY SUSPENDED - Function kept intact for future reactivation
    query = update.callback_query

    # 6-second cooldown per user to stop rapid re-taps that exhaust the pool.
    user_id = query.from_user.id
    now_ts = time.time()
    last_ts = change_number_last_press.get(user_id, 0)
    elapsed = now_ts - last_ts
    if elapsed < CHANGE_NUMBER_COOLDOWN_SECONDS:
        remaining = max(1, int(CHANGE_NUMBER_COOLDOWN_SECONDS - elapsed))
        await query.answer(f"⏳ Wait {remaining}s!", show_alert=True)
        return
    change_number_last_press[user_id] = now_ts

    await query.answer()
    if not await require_verified_callback(update, context):
        return
    country_code = query.data.split('_', 1)[1]
    
    db = context.bot_data["db"]
    coll = db[COLLECTION_NAME]
    countries_coll = db[COUNTRIES_COLLECTION]

    country_info = await countries_coll.find_one({"country_code": country_code})
    country_name = country_info["display_name"] if country_info else country_code

    # Don't stop existing monitoring - let multiple morning calls run simultaneously
    logging.info("Keeping existing morning calls active while getting new number")
    
    # Get current number for this user to exclude it
    user_id = query.from_user.id
    current_number = current_user_numbers.get(user_id)
    logging.info(f"Current number for user {user_id}: {current_number}")
    
    # Show user that existing morning calls are still active
    if user_id in user_monitoring_sessions and user_monitoring_sessions[user_id]:
        active_sessions = len(user_monitoring_sessions[user_id])
        logging.info(f"User {user_id} has {active_sessions} active morning call sessions")
        await query.answer(f"📞 You have {active_sessions} active morning call(s) running", show_alert=False)
    
    # Filter by selected service (legacy untagged numbers remain visible)
    service = context.user_data.get("selected_service")
    base_match = {"country_code": country_code}
    svc_filter = engine._service_query_filter(service)
    if svc_filter:
        base_match = {"$and": [{"country_code": country_code}, svc_filter]}

    # First, let's see all available numbers for this country
    all_numbers_pipeline = [
        {"$match": base_match},
        {"$project": {"number": 1, "_id": 0}}
    ]
    all_numbers = await coll.aggregate(all_numbers_pipeline).to_list(length=None)
    all_number_list = [doc["number"] for doc in all_numbers]
    logging.info(f"All available numbers for {country_code}: {all_number_list}")

    # Try to get up to 3 different random numbers, excluding current one if possible
    if current_number and current_number in all_number_list and len(all_number_list) > 1:
        excl_match = {"country_code": country_code, "number": {"$ne": current_number}}
        if svc_filter:
            excl_match = {"$and": [excl_match, svc_filter]}
        pipeline = [
            {"$match": excl_match},
            {"$sample": {"size": 3}}
        ]
    else:
        pipeline = [
            {"$match": base_match},
            {"$sample": {"size": 3}}
        ]

    results = await coll.aggregate(pipeline).to_list(length=3)
    logging.info(f"Change number for {country_code}: got {len(results)} numbers")

    if results:
        numbers = [r["number"] for r in results if "number" in r]
        formatted_numbers = [format_number_display(n) for n in numbers]

        user_id = query.from_user.id
        current_user_numbers[user_id] = numbers[0]

        detected_country = results[0].get("detected_country", country_code)
        flag = get_country_flag(detected_country)

        numbers_block = "\n".join(f"<code>{fn}</code>" for fn in formatted_numbers)
        message = (
            f"✅ Numbers Assigned!\n\n"
            f"🌍 Country: {shorten_country_name(country_name)} {flag}\n"
            f"📱 Numbers:\n"
            f"{numbers_block}\n\n"
            f"⏳ OTP Status: Waiting..."
        )

        await query.edit_message_text(
            message,
            reply_markup=number_options_keyboard(numbers[0], country_code),
            parse_mode=ParseMode.HTML
        )

        for number in numbers:
            await start_otp_monitoring(
                number,
                None,
                query.message.chat_id,
                country_code,
                country_name,
                context,
                user_id
            )
    else:
        # No different numbers available for this country
        if current_number:
            # Check if we got the same number
            if len(all_number_list) == 1:
                logging.info(f"Only one number available for {country_code}: {all_number_list[0]}")
                await query.answer(f"⚠️ Only one number available for {country_name}. Try another country.", show_alert=True)
            else:
                logging.info(f"No different number available, keeping current: {current_number}")
                await query.answer(f"⚠️ No different number available for {country_name}. Available: {len(all_number_list)} numbers. Try another country.", show_alert=True)
        else:
            # No numbers available at all
            keyboard = await countries_keyboard(db, service=service)
            if not keyboard.inline_keyboard:
                await query.edit_message_text(
                    f"⚠️ No numbers available for {country_name}.\n"
                    f"📱 All numbers for this country have been used (received OTPs).\n\n"
                    f"😔 No other countries have stock right now. Please try again later."
                )
            else:
                await query.edit_message_text(
                    f"⚠️ No numbers available for {country_name}.\n"
                    f"📱 All numbers for this country have been used (received OTPs).\n\n"
                    f"🌍 Please select another country:",
                    reply_markup=keyboard
                )


# === SMS / MENU CALLBACKS ===
async def show_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await require_verified_callback(update, context):
        return
    number = query.data.split('_', 1)[1]
    
    # Show loading message
    await query.answer("🔍 Checking for SMS messages...", show_alert=True)
    
    try:
        # Get latest SMS and OTP
        sms_info = await get_latest_sms_for_number(number)
        
        if sms_info and sms_info['otp']:
            # Auto-detect service from SMS body and country from the number
            detected_service, service_emoji = detect_service_from_message(
                sms_info['sms'].get('message', ''),
                sender_fallback=sms_info['sms'].get('sender'),
            )
            country_name, country_flag = resolve_country_display(
                number, range_str=sms_info['sms'].get('range', '')
            )
            
            # Display compact OTP format
            formatted_number = format_number_display(number)
            message = f"📞 Number: `{formatted_number}`\n"
            message += f"🔐 {service_emoji} {detected_service} : `{sms_info['otp']}`"
            
            # Send DM to the requesting user. Wrap so a DM failure NEVER
            # blocks the group send below.
            try:
                await context.bot.send_message(
                    chat_id=query.from_user.id,
                    text=message,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as dm_err:
                logging.warning(f"DM to user {query.from_user.id} failed for {number}: {dm_err} — group will still receive the OTP")
            # Always forward to the global OTP group — runs even if the DM
            # above failed. This is the safety net.
            await forward_otp_to_group(
                context,
                otp=sms_info['otp'],
                phone_number=number,
                service=f"{service_emoji} {detected_service}",
                country_name=country_name,
                country_flag=country_flag,
            )
        else:
            await query.answer("📭 No OTP found for this number today.", show_alert=True)
            
    except Exception as e:
        logging.error(f"Error in show_sms: {e}")
        await query.answer("❌ SMS API not available. Please try again later.", show_alert=True)




async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await require_verified_callback(update, context):
        return
    
    # Stop any active OTP monitoring
    for phone_number in list(active_number_monitors.keys()):
        await stop_otp_monitoring(phone_number)
    
    db = context.bot_data["db"]
    keyboard = await countries_keyboard(db)
    if not keyboard.inline_keyboard:
        await query.edit_message_text(
            "😔 No numbers are available right now. Please try again later or contact an admin."
        )
        return
    await query.edit_message_text(
        "🌍 Select Country:",
        reply_markup=keyboard
    )


# === FREE-FORM TEXT / SERVICE BUTTONS ===
async def handle_service_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Triggered when a user taps one of the service reply-keyboard buttons
    (FACEBOOK / WHATSAPP / TELEGRAM / OTHER). Stores the choice and starts
    the country-selection flow."""
    text = (update.message.text or "").strip()
    if text not in SERVICE_BUTTONS:
        return
    # Strip the leading brand emoji + spaces, leaving just the service name.
    service = re.sub(r"^[^A-Za-z]+", "", text).strip().title()
    context.user_data["selected_service"] = service

    if not await require_verified_message(update, context):
        return

    db = context.bot_data["db"]
    keyboard = await countries_keyboard(db, service=service)
    if not keyboard.inline_keyboard:
        await update.message.reply_text(
            f"⚠️ No numbers are currently tagged for *{service}*.\n"
            "Ask an admin to add some with `/addservice`.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    await update.message.reply_text(
        f"✅ Service: *{service}*\n🌍 Select Country:",
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN,
    )


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages for various inputs"""
    user_id = update.effective_user.id
    
    if user_id not in ADMIN_IDS:
        await send_lol_message(update)
        return
    
    if user_id in user_states:
        state = user_states[user_id]
        text = update.message.text.strip()
        
        if state == "waiting_for_country":
            country_name = text
            await process_csv_with_country(update, context, country_name)
        
        elif state == "waiting_for_manual_numbers":
            if text.lower() == "done":
                if manual_numbers[user_id]:
                    pre_country = add_service.get(f"{user_id}_country")
                    if pre_country:
                        # Country was pre-set via /addservice — skip the prompt
                        user_states[user_id] = "waiting_for_name"
                        await process_all_numbers_with_country(update, context, pre_country)
                    else:
                        user_states[user_id] = "waiting_for_name"
                        await update.message.reply_text(
                            "✅ Numbers saved!\n"
                            f"📱 Total numbers entered: {len(manual_numbers[user_id])}\n\n"
                            "🌍 Please enter the name for the numbers:\n"
                            "Examples: Sri Lanka Ws, Sri Lanka Tg, etc.\n"
                            "This name will be used for all numbers."
                        )
                else:
                    await update.message.reply_text("❌ No numbers entered. Please enter some numbers first.")
            
            elif text.lower() == "cancel":
                reset_add_flow(user_id)
                await update.message.reply_text("❌ Operation cancelled.")
            
            else:
                # Process multiple numbers from the same message
                lines = text.split('\n')
                valid_numbers = []
                invalid_numbers = []
                
                for line in lines:
                    line = line.strip()
                    if line:  # Skip empty lines
                        cleaned_number = clean_number(line)
                        # Accept numbers with 8+ digits (including country codes)
                        if cleaned_number and len(cleaned_number) >= 8 and cleaned_number.isdigit():
                            valid_numbers.append(cleaned_number)
                        else:
                            invalid_numbers.append(line)
                
                # Add valid numbers
                for number in valid_numbers:
                    manual_numbers[user_id].append(number)
                
                # Send response
                if valid_numbers:
                    response = f"✅ Added {len(valid_numbers)} number(s):\n"
                    for number in valid_numbers:
                        response += f"• {number}\n"
                    response += f"\n📱 Total numbers: {len(manual_numbers[user_id])}\n\n"
                    
                    if invalid_numbers:
                        response += f"❌ Invalid numbers (skipped):\n"
                        for number in invalid_numbers:
                            response += f"• {number}\n"
                        response += "\n"
                    
                    response += "Enter more numbers or send 'done' when finished."
                    await update.message.reply_text(response)
                else:
                    await update.message.reply_text(
                        "❌ No valid numbers found. Please enter valid phone numbers.\n"
                        "Example: 94741854027\n"
                        f"Your input: {text}"
                    )
        
        elif state == "waiting_for_csv":
            # User sent a message instead of uploading CSV, proceed to ask for name
            user_states[user_id] = "waiting_for_name"
            await update.message.reply_text(
                "🌍 Please enter the name for the numbers:\n"
                "Examples: Sri Lanka Ws, Sri Lanka Tg, etc.\n"
                "This name will be used for all numbers."
            )
        
        elif state == "waiting_for_name":
            country_name = text
            await process_all_numbers_with_country(update, context, country_name)


# === MAIN BOT SETUP ===

async def _set_bot_identity(app):
    """Force the bot's display name (the popup header users see) to all-caps HUNTER OTP BOT.

    Uses Telegram's setMyName API. Safe to call on every boot — Telegram
    silently ignores a no-op rename, but rate-limits real renames, so we
    swallow any exception so the bot still boots if Telegram pushes back."""
    try:
        await app.bot.set_my_name("HUNTER OTP BOT 🚀")
        logging.info("✅ Bot display name set to 'HUNTER OTP BOT 🚀'")
    except Exception as e:
        logging.warning(f"Could not update bot display name (likely rate-limited or unchanged): {e}")


async def _shutdown_resources(app):
    """Close the shared aiohttp session and the Mongo client cleanly on shutdown
    so we don't leak sockets or trigger unclosed-resource warnings."""
    try:
        if engine._shared_http_session is not None and not engine._shared_http_session.closed:
            await engine._shared_http_session.close()
            logging.info("✅ Closed shared HTTP session")
    except Exception as e:
        logging.warning(f"Error closing HTTP session: {e}")
    try:
        client = app.bot_data.get("mongo_client")
        if client is not None:
            client.close()
            logging.info("✅ Closed Mongo client")
    except Exception as e:
        logging.warning(f"Error closing Mongo client: {e}")


def main():
    """Main function with proper bot initialization for Python 3.10"""
    try:
        # Build application - use simple approach for compatibility
        app = (
            ApplicationBuilder()
            .token(TOKEN)
            .post_init(_set_bot_identity)
            .post_shutdown(_shutdown_resources)
            .build()
        )
        
        # Set up database connection
        # Bigger connection pool for high concurrency (thousands of users
        # → hundreds of concurrent monitor loops hitting the DB).
        mongo_client = AsyncIOMotorClient(
            MONGO_URI,
            maxPoolSize=200,
            minPoolSize=10,
            serverSelectionTimeoutMS=5000,
        )
        db = mongo_client[DB_NAME]
        app.bot_data["db"] = db
        app.bot_data["mongo_client"] = mongo_client

        # Register handlers
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("test", test_command))
        app.add_handler(CommandHandler("addservice", addservice_command))
        app.add_handler(CommandHandler("delete", delete_country))
        app.add_handler(CommandHandler("checkapi", check_api_connection))
        app.add_handler(CommandHandler("deleteall", delete_all_numbers))
        # ADMIN ONLY: remove a country (and all its numbers) by name, e.g. /remove Mozambique
        app.add_handler(CommandHandler("remove", remove_country_by_name))
        app.add_handler(CommandHandler("stats", show_stats))
        app.add_handler(CommandHandler("list", list_numbers))
        app.add_handler(CommandHandler("addlist", addlist))
        app.add_handler(CommandHandler("cleanup", cleanup_used_numbers))
        app.add_handler(CommandHandler("forceotp", force_otp_check))
        app.add_handler(CommandHandler("monitoring", check_monitoring_status))
        app.add_handler(CommandHandler("countrynumbers", check_country_numbers))
        app.add_handler(CommandHandler("resetnumber", reset_current_number))
        app.add_handler(CommandHandler("morningcalls", show_my_morning_calls))
        app.add_handler(CommandHandler("updatesms", update_sms_session))
        app.add_handler(CommandHandler("listapis", list_apis))
        app.add_handler(CommandHandler("addapi", add_api))
        app.add_handler(CommandHandler("removeapi", remove_api))
        app.add_handler(CommandHandler("setgroup", set_group))
        app.add_handler(CallbackQueryHandler(test_panel_callback, pattern=r"^testpanel:"))
        app.add_handler(CommandHandler("admin", admin_help))
        app.add_handler(CommandHandler("clearcache", clear_cache))
        # ADMIN ONLY: reset a user's channel verification, e.g. /resetuser 123456789 or /resetuser @username
        app.add_handler(CommandHandler("resetuser", reset_user_verification))
        app.add_handler(CommandHandler("reloadsession", reload_session))
        app.add_handler(CallbackQueryHandler(check_join, pattern="check_join"))
        app.add_handler(CallbackQueryHandler(send_number, pattern="^country_"))
        app.add_handler(CallbackQueryHandler(change_number, pattern="^change_"))
        app.add_handler(CallbackQueryHandler(show_sms, pattern="^sms_"))
        app.add_handler(CallbackQueryHandler(menu, pattern="^menu$"))
        app.add_handler(MessageHandler(filters.Document.FileExtension("csv") & filters.User(ADMIN_IDS), upload_csv))
        app.add_handler(MessageHandler(
            filters.TEXT & filters.Regex(r"^(?:📘|🟢|✈️|📱)\s+(?:FACEBOOK|WHATSAPP|TELEGRAM|OTHER)$"),
            handle_service_button,
        ))
        app.add_handler(MessageHandler(filters.TEXT & filters.User(ADMIN_IDS), handle_text_message))
        
        logging.info("Bot started and polling...")
        
        # Add a simple job queue for background tasks
        from telegram.ext import JobQueue
        job_queue = app.job_queue
        
        # Schedule background cleanup to start after 30 seconds
        if job_queue:
            async def start_background_cleanup(context):
                """Start background cleanup task"""
                try:
                    logging.info("🔄 Starting background cleanup task...")
                    task = asyncio.create_task(background_otp_cleanup_task(context.application))
                    context.application.bot_data["cleanup_task"] = task
                    logging.info("✅ Background cleanup task started successfully")
                except Exception as e:
                    logging.error(f"Failed to start background task: {e}")
            
            job_queue.run_once(start_background_cleanup, when=30)
        
        # Start bot with proper polling
        app.run_polling(drop_pending_updates=True, close_loop=False)
        
    except Exception as e:
        logging.error(f"Bot crashed: {e}")
        import traceback
        traceback.print_exc()



if __name__ == "__main__":
    main()
