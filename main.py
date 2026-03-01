"""
WeddingLeadIntel AI - Telegram Bot
India weddings only. Personal use.
"""

import logging
import asyncio
import os
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes
)
from keep_alive import keep_alive
from lead_engine import discover_leads, is_indian_city, normalize_city

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── Rate limiting ────────────────────────────────────────────
last_search_time = {}
SEARCH_COOLDOWN = 600  # 10 minutes


def can_search(user_id: int) -> tuple:
    now = time.time()
    last = last_search_time.get(user_id, 0)
    elapsed = now - last
    if elapsed < SEARCH_COOLDOWN:
        wait = int(SEARCH_COOLDOWN - elapsed)
        return False, wait
    return True, 0


def mark_searched(user_id: int):
    last_search_time[user_id] = time.time()


# ── Format profile card ──────────────────────────────────────
def format_profile_card(profile: dict) -> tuple:
    username = profile.get('username', 'unknown')
    confidence = profile.get('confidence', 75)
    is_private = profile.get('is_private')
    city = profile.get('detected_city', '').title()
    state = profile.get('state', '')
    wedding_month = profile.get('wedding_month', '')
    days_est = profile.get('days_estimate') or profile.get('days_until_wedding')
    caption = profile.get('post_caption', '')
    source = profile.get('source_type', 'hashtag')
    followers = profile.get('follower_count')
    likes = profile.get('likes_count', 0) or 0
    comments = profile.get('comments_count', 0) or 0
    multi = profile.get('multi_source', False)

    tier_emoji = "🟢" if confidence >= 85 else "🟡"

    if is_private is True:
        privacy = "🔒 Private"
    elif is_private is False:
        privacy = "🌐 Public"
    else:
        privacy = "👤 Unknown"

    location_str = f"{city}, {state}" if state and state != "India" else city

    timing_str = ""
    if days_est:
        timing_str = f"⏰ ~{days_est} days away"
    if wedding_month:
        timing_str += f"\n📅 {wedding_month}"

    er_str = ""
    if followers and followers > 0 and (likes + comments) > 0:
        er = round((likes + comments) / followers * 100, 1)
        er_str = f"\n📊 ER: {er}% | 👥 {followers:,}"
    elif followers:
        er_str = f"\n👥 {followers:,} followers"

    caption_preview = ""
    if caption:
        preview = caption[:80].replace('\n', ' ')
        caption_preview = f'\n💬 "{preview}..."'

    source_map = {
        'vendor_tag': '🏪 Vendor tag',
        'hashtag': '#️⃣ Hashtag',
        'mention': '📢 Mention',
    }
    source_str = source_map.get(source, '🔍 Search')
    multi_str = " ⭐" if multi else ""

    card = (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{tier_emoji} @{username}{multi_str}\n"
        f"🔗 instagram.com/{username}\n"
        f"{privacy}\n"
        f"📍 {location_str}\n"
        f"{timing_str}\n"
        f"{er_str}"
        f"{caption_preview}\n"
        f"👥 Via: {source_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "📱 Open Instagram",
            url=f"https://instagram.com/{username}"
        )
    ]])

    return card, keyboard


# ── Commands ─────────────────────────────────────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Type any Indian city to find wedding leads! 🇮🇳\n"
        "Example: Kochi, Mumbai, Delhi, Jaipur..."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 How to use:\n\n"
        "1️⃣ Type any Indian city\n"
        "2️⃣ Wait 30-60 seconds\n"
        "3️⃣ Get 50 real bride/groom profiles\n"
        "4️⃣ Follow them on Instagram\n"
        "5️⃣ They see your portfolio\n"
        "6️⃣ They book you! 📸\n\n"
        "🟢 High confidence\n"
        "🟡 Medium confidence\n"
        "⏱ 1 search per 10 minutes\n"
        "🇮🇳 India weddings only"
    )


async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong! 🏓")


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Example: /search Kochi")
        return
    city = ' '.join(args)
    await process_search(update, context, city)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text.startswith('/'):
        return
    await process_search(update, context, text)


async def process_search(update: Update, context: ContextTypes.DEFAULT_TYPE, location: str):
    user_id = update.effective_user.id

    # Rate limit check
    can, wait_secs = can_search(user_id)
    if not can:
        mins = wait_secs // 60
        secs = wait_secs % 60
        await update.message.reply_text(
            f"⏳ Please wait {mins}m {secs}s before next search."
        )
        return

    # India validation
    if not is_indian_city(location):
        await update.message.reply_text(
            "⚠️ India Only!\n\n"
            "Try: Kochi, Mumbai, Delhi,\n"
            "Goa, Chennai, Jaipur... 🇮🇳"
        )
        return

    mark_searched(user_id)

    city_data = normalize_city(location)
    display = city_data.get('display', location.title())

    searching_msg = await update.message.reply_text(
        f"🔍 Searching brides and grooms\n"
        f"📍 {display}\n\n"
        f"⏳ Please wait 30-60 seconds...\n"
        f"✅ Confidence filter: 75%+\n"
        f"✅ Follower cap: 15,000\n"
        f"✅ India weddings only 🇮🇳"
    )

    try:
        profiles = await discover_leads(location)

        if not profiles:
            await searching_msg.edit_text(
                f"😔 No profiles found in {display}.\n\n"
                f"Try:\n"
                f"• Nearby bigger city\n"
                f"• State name: Kerala\n"
                f"• Different spelling"
            )
            return

        public_count = sum(1 for p in profiles if p.get('is_private') is False)
        private_count = sum(1 for p in profiles if p.get('is_private') is True)

        await searching_msg.edit_text(
            f"✅ Found {len(profiles)} profiles in {display}\n\n"
            f"🌐 Public: {public_count}\n"
            f"🔒 Private: {private_count}\n"
            f"📅 Window: 10-180 days\n\n"
            f"Sending profiles... 👇"
        )

        for batch_start in range(0, len(profiles), 10):
            batch = profiles[batch_start:batch_start + 10]
            batch_text = ""
            keyboards = []

            for profile in batch:
                card, keyboard = format_profile_card(profile)
                batch_text += card + "\n\n"
                keyboards.append(keyboard.inline_keyboard[0])

            combined_keyboard = InlineKeyboardMarkup(keyboards[:10])
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=batch_text[:4000],
                reply_markup=combined_keyboard
            )
            await asyncio.sleep(1)

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                f"✅ Done! {len(profiles)} profiles sent.\n\n"
                f"🌐 Public → Follow directly\n"
                f"🔒 Private → Send follow request\n"
                f"→ They see your portfolio\n"
                f"→ They book you! 📸\n\n"
                f"🔄 Search another Indian city!"
            )
        )

    except Exception as e:
        logger.error(f"Search error: {e}")
        await searching_msg.edit_text(
            f"❌ Search failed for {display}.\n\n"
            f"Please try again in a few minutes."
        )


# ── Error handler ────────────────────────────────────────────
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Bot error: {context.error}")


# ── Main ─────────────────────────────────────────────────────
def main():
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN not set!")

    keep_alive()
    logger.info("🟢 Keep-alive started on port 8080")

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CommandHandler("search", search_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(error_handler)

    logger.info("🚀 WeddingLeadIntel AI is LIVE!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
