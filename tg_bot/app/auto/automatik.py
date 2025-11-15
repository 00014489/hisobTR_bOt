import asyncio
from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from app.data.dbContext import get_users_by_time, get_todays_dengies, insert_daily_category_reports, insert_monthly_category_reports, insert_yearly_category_reports
from apscheduler.triggers.cron import CronTrigger
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import os, io, hashlib, math
from playwright.async_api import async_playwright


from app.cmn.transtalor import get_text, smart_sleep
import logging
from aiogram.types import FSInputFile
from typing import List, Tuple
# import app.keyboards.inLine as inKb
# import app.data.connection as postgresql
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import plotly.graph_objects as go




async def schedule_hourly_task(bot: Bot):
    """
    Schedule the `automatik`
    """
    scheduler = AsyncIOScheduler()
    # def next_minute():
    #     now = datetime.now()
    #     next_min = (now.minute + 1) % 60
    #     # logging.info(f"the fun returns: {next_min}")
    #     return next_min
    # next_min_ = next_minute()
    trigger = CronTrigger(minute=0)
    # trigger = IntervalTrigger(minutes=0)
    
    # Define a wrapper to ensure sequential execution
    async def sequential_task():
        # First part sending reminders
        await sending_reminder(bot)
        
        users = await get_users_by_time(0, 1)

        if not users:
            # No users to process
            only_user_ids = []
            logging.info("No users to process")
        else:
            only_user_ids = [user_id for user_id, lang in users]
            # Send ONLY user_ids to update_daily_report
            await update_daily_report(only_user_ids)

            # Send full list (user_id + language) to statistik
            await sending_statistik_daily(bot, users)
        
    
    # Schedule the sequential task
    scheduler.add_job(sequential_task, trigger=trigger)
    
    scheduler.start()
    logging.info("Scheduler started for daily tasks: Reminder and updating the database.")
    
    # Keep the scheduler running
    await asyncio.Event().wait()


async def sending_reminder(bot: Bot):
    reminder_user_ids = await get_users_by_time(21, 0)
    
    if not reminder_user_ids:
        logging.info("No users found at this time.")
        return

    for user_id, lng_code in reminder_user_ids:
        rows = await get_todays_dengies(user_id)

        if not rows:
            # No expenses today — send just reminder text
            await smart_sleep(
                bot.send_message,
                chat_id=user_id,
                text=await get_text(lng_code, "reminder"),
                parse_mode='HTML'
            )
            continue

        # Build formatted message
        message_lines = []
        for amount, category_name, comment_text, created_time in rows:
            # Format like "HH:MM - Category (Amount - Comment)"
            # Assuming created_date is not returned — if needed, we can add it to the query
            # For now, we just show category + amount (+ comment if exists)
            if comment_text:
                line = f"{created_time} - {category_name} ({amount:.2f} - {comment_text})"
            else:
                line = f"{created_time} - {category_name} ({amount:.2f})"
            message_lines.append(line)

        # Join all lines
        

        # Build your message as before
        message_text = "\n".join(message_lines)

        # Combine with the reminder header
        full_message = f"{await get_text(lng_code, 'reminder')}\n\n{message_text}"

        # Split into chunks if too long
        for chunk in chunk_message(full_message):
            await smart_sleep(
                bot.send_message,
                chat_id=user_id,
                text=chunk,
                parse_mode='HTML'
            )


def chunk_message(text: str, max_length: int = 4096):
            """Split a long message into chunks under max_length, preserving line breaks."""
            lines = text.split("\n")
            chunks = []
            current = ""
            for line in lines:
                # +1 for newline
                if len(current) + len(line) + 1 > max_length:
                    chunks.append(current)
                    current = line
                else:
                    if current:
                        current += "\n" + line
                    else:
                        current = line
            if current:
                chunks.append(current)
            return chunks

async def update_daily_report(user_ids: list[int]):

    if not user_ids:
        logging.info("No users found for data updating.")
        return
    
    # For every day
    await insert_daily_category_reports(user_ids)

    # For every month
    await insert_monthly_category_reports(user_ids)

    await insert_yearly_category_reports(user_ids)

async def sending_statistik_daily(bot: Bot, user_ids: list[int]):
    for user_id, lang_code in user_ids:

        # Header text based on user's language
        header = await get_text(lang_code, "todaysDate")

        rows = await get_todays_dengies(user_id)

        if not rows:
            logging.info(f"{user_id} does not have expenses for today")
            continue

        # -------------------------------------------------------------------
        # Build individual lines
        # -------------------------------------------------------------------
        lines = []
        total_amount = 0
        category_totals = {}  # {category_name: sum}

        for amount, category_name, comment_text, created_time, currency_is in rows:

            # Sum total
            total_amount += amount

            # Sum by category
            if category_name not in category_totals:
                category_totals[category_name] = 0
            category_totals[category_name] += amount

            # Format line
            if comment_text:
                line = f"⏰ {created_time} — {currency_is} {amount} — {category_name} ({comment_text})"
            else:
                line = f"⏰ {created_time} — {currency_is} {amount} — {category_name}"

            lines.append(line)

        # -------------------------------------------------------------------
        # Build category totals section
        # -------------------------------------------------------------------
        cat_lines = []
        for category, amt in category_totals.items():
            cat_lines.append(f"• {category}: {currency_is} {amt}")

        category_summary = "\n".join(cat_lines)

        # -------------------------------------------------------------------
        # Final message text
        # -------------------------------------------------------------------
        message = (
            f"{header}\n\n"
            + "\n".join(lines)
            + f"\n\n<b>{await get_text(lang_code, 'totalWord')}</b> {currency_is} {total_amount}\n"
            + f"<b>{await get_text(lang_code, 'totalCat')}</b>\n{category_summary}"
        )

        for chunk in chunk_message(message):
            await smart_sleep(
                bot.send_message,
                chat_id=user_id,
                text=chunk,
                parse_mode='HTML'
            )




# PIE_COLORS = [
#     "#6366F1", "#8B5CF6", "#EC4899", "#F59E0B", "#10B981",
#     "#3B82F6", "#EF4444", "#14B8A6", "#F97316", "#8B5CF6",
#     "#EC4899", "#F59E0B", "#10B981", "#3B82F6", "#EF4444",
#     "#14B8A6", "#F97316", "#6366F1", "#8B5CF6", "#EC4899",
# ]

# # Currency symbols
# CURRENCY_SYMBOLS = {
#     "USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "INR": "₹", "CAD": "$", "AUD": "$"
# }


# ------------------------------------------------------------------
# Async Function: Generate Daily Report
# ------------------------------------------------------------------
# async def generate_daily_statistics_image_from_rows(
#     user_id: int,
#     rows: List[Tuple[float, str, str, str]],
#     currency: str = "UZS"  # e.g., "EUR", "GBP", "INR"
# ) -> str:
#     """
#     Generates 2351×1200 daily expense summary:
#     - Left: Categories (Name + Amount + %)
#     - Right: Donut chart
#     - Top of chart: Total spent
#     - Top-left: Date
#     Returns path to saved PNG.
#     """
#     # --- Paths ---
#     folder_path = Path("images") / str(user_id)
#     folder_path.mkdir(parents=True, exist_ok=True)
#     timestamp = int(datetime.utcnow().timestamp())
#     output_path = folder_path / f"daily_report_{timestamp}.png"

#     # --- Colors ---
#     COLORS = {
#         'bg': (250, 251, 255),
#         'text': (31, 41, 55),
#         'text_muted': (107, 114, 128),
#         'primary': (79, 70, 229),
#         'border': (229, 231, 235),
#     }

#     # --- Canvas ---
#     WIDTH, HEIGHT = 2351, 1200
#     img = Image.new("RGBA", (WIDTH, HEIGHT), COLORS['bg'] + (255,))
#     draw = ImageDraw.Draw(img)

#     # --- Font Loader ---
#     def load_font(size: int, bold: bool = False):
#         base = "fonts"
#         candidates = []
#         if bold:
#             candidates = [
#                 f"{base}/Inter-Bold.ttf",
#                 "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
#                 "C:/Windows/Fonts/arialbd.ttf",
#             ]
#         else:
#             candidates = [
#                 f"{base}/Inter-Regular.ttf",
#                 "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
#                 "C:/Windows/Fonts/arial.ttf",
#             ]
#         for path in candidates:
#             if Path(path).exists():
#                 try:
#                     return ImageFont.truetype(path, size)
#                 except:
#                     continue
#         return ImageFont.load_default()

#     font_date = load_font(48, bold=True)
#     font_title = load_font(64, bold=True)
#     font_cat = load_font(38)
#     font_amount = load_font(36, bold=True)
#     font_percent = load_font(32)
#     font_total = load_font(52, bold=True)

#     # --- No Data ---
#     if not rows:
#         msg = "No expenses today"
#         w = draw.textlength(msg, font=font_title)
#         draw.text(((WIDTH - w) // 2, HEIGHT // 2 - 50), msg, fill=COLORS['text_muted'], font=font_title)
#         img.convert("RGB").save(output_path)
#         return str(output_path)

#     # --- Aggregate ---
#     category_totals = {}
#     for amount, cat, _, _ in rows:
#         category_totals[cat] = category_totals.get(cat, 0) + amount

#     categories = list(category_totals.keys())
#     amounts = list(category_totals.values())
#     total = sum(amounts)
#     percentages = [amt / total * 100 for amt in amounts]

#     # Currency
#     symbol = CURRENCY_SYMBOLS.get(currency.upper(), "$")

#     # --- Donut Chart (Right Side) ---
#     chart_size = 700
#     hole_size = 260
#     chart_x = WIDTH - chart_size - 200
#     chart_y = HEIGHT // 2
#     center_x = chart_x + chart_size // 2
#     center_y = chart_y

#     outer_bbox = [
#         center_x - chart_size // 2,
#         center_y - chart_size // 2,
#         center_x + chart_size // 2,
#         center_y + chart_size // 2
#     ]
#     inner_bbox = [
#         center_x - hole_size // 2,
#         center_y - hole_size // 2,
#         center_x + hole_size // 2,
#         center_y + hole_size // 2
#     ]

#     # Draw pie slices
#     start = 0.0
#     for i, pct in enumerate(percentages):
#         angle = pct * 3.6
#         end = start + angle
#         draw.pieslice(
#             outer_bbox,
#             start=start,
#             end=end,
#             fill=PIE_COLORS[i % len(PIE_COLORS)],
#             outline="white",
#             width=10
#         )
#         start = end

#     # Cut hole
#     hole = Image.new("RGBA", (hole_size, hole_size), (0, 0, 0, 0))
#     hdraw = ImageDraw.Draw(hole)
#     hdraw.ellipse([0, 0, hole_size - 1, hole_size - 1], fill=COLORS['bg'] + (255,))
#     img.paste(hole, (center_x - hole_size // 2, center_y - hole_size // 2), hole)

#     # --- Total Spent ON TOP of Chart ---
#     total_text = f"{symbol}{total:,.2f}"
#     total_w = draw.textlength(total_text, font=font_total)
#     draw.text(
#         (center_x - total_w // 2, center_y - chart_size // 2 - 80),
#         total_text,
#         fill=COLORS['text'],
#         font=font_total
#     )
#     subtitle = "Total Spent"
#     sub_w = draw.textlength(subtitle, font=font_percent)
#     draw.text(
#         (center_x - sub_w // 2, center_y - chart_size // 2 - 30),
#         subtitle,
#         fill=COLORS['text_muted'],
#         font=font_percent
#     )

#     # --- Date (Top-Left) ---
#     date_str = "12.11.2025"
#     draw.text((100, 100), date_str, fill=COLORS['text'], font=font_date)

#     # --- Category List (Left Side) ---
#     list_x = 150
#     list_y_start = 280
#     row_height = 90

#     for i, (cat, amt, pct) in enumerate(zip(categories, amounts, percentages)):
#         y = list_y_start + i * row_height

#         # Color box
#         box_x = list_x
#         draw.rectangle([box_x, y + 10, box_x + 50, y + 60], fill=PIE_COLORS[i % len(PIE_COLORS)])

#         # Category name
#         draw.text((box_x + 70, y + 10), cat, fill=COLORS['text'], font=font_cat)

#         # Amount
#         amt_text = f"{symbol}{amt:,.2f}"
#         amt_w = draw.textlength(amt_text, font=font_amount)
#         draw.text((list_x + 500 - amt_w, y + 5), amt_text, fill=COLORS['primary'], font=font_amount)

#         # Percentage
#         pct_text = f"{pct:.1f}%"
#         draw.text((list_x + 500, y + 45), pct_text, fill=COLORS['text_muted'], font=font_percent)

#     # --- Save ---
#     img.convert("RGB").save(output_path, dpi=(300, 300))
#     print(f"Generated: {output_path}")
#     return str(output_path)

# async def send_image(bot: Bot, chat_id: int, file_path: str, caption: str = ""):
#     """
#     Send a local image to a Telegram chat as compressed photo.
    
#     :param bot: Aiogram Bot instance
#     :param chat_id: Telegram user or chat id
#     :param file_path: Local path to the image file
#     :param caption: Optional caption text
#     """
#     # Wrap local file path into InputFile
#     photo = FSInputFile(file_path)

#     message =await bot.send_photo(
#         chat_id=chat_id,
#         photo=photo,
#         caption=caption
#     )
#     # Telegram file_id of the uploaded photo (the largest size variant)
#     # Photos are sent as an array of sizes, we take the last one (largest)
#     file_id = message.photo[-1].file_id

#     return file_id, file_path