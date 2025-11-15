import psycopg
from psycopg import sql, Error, AsyncConnection
import logging
import app.cmn.transtalor as translator
from typing import Optional, Tuple, List, Dict
from datetime import datetime, timedelta, timezone
from psycopg.rows import dict_row
import os



THISUSER = os.getenv('NAME')
THISPASSWORD = os.getenv('PASSWORD')
THISHOST = os.getenv('HOST')
THISPORT = os.getenv('PORT')
THISDBNAME = os.getenv('DB_NAME')

async def get_db_connection():
    # logging.info(f"Connected to PostgreSQL database at {THISHOST}:{THISPORT}/{THISDBNAME} as user {THISUSER}")

    conn = await psycopg.AsyncConnection.connect(
        user=THISUSER,
        password=THISPASSWORD,
        host=THISHOST,
        port=THISPORT,
        dbname=THISDBNAME
    )

    
    # Set client encoding to UTF-8
    async with conn.cursor() as cursor:
        await cursor.execute("SET client_encoding TO 'UTF8'")
    
    return conn



async def get_todays_dengies(user_id: int):
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                    SELECT 
                        d.amount,
                        c.title AS category_name,
                        d.comment_text,
                        to_char(d.created_date, 'HH24:MI') AS created_time,
                        u.currency_is               -- ⬅ added currency
                    FROM dengies d
                    JOIN categories c ON d.category_id = c.id
                    JOIN users u ON d.user_id = u.id
                    WHERE 
                        u.tg_user_id = %s
                        AND c.is_ex = TRUE
                        AND date_trunc('day', d.created_date) = 
                            date_trunc('day', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + u.time_utc)
                    ORDER BY d.created_date DESC;
                """,
                (user_id,)
            )

            rows = await cursor.fetchall()
            logging.info(f"Fetched {len(rows)} records for user_id={user_id}")
            return rows  # now each row has 5 values

    except (Exception, Error) as error:
        logging.error("Error while fetching today's dengies: %s", error)
        return []

    finally:
        if connection is not None:
            await connection.close()


async def insert_daily_category_reports(tg_user_ids: list[int]):
    """
    Aggregate today's expenses per category for each user (using tg_user_id)
    and insert into daily_category_reports.
    month_id is NULL.
    created_date is set to (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + u.time_utc
    """
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            
            for tg_user_id in tg_user_ids:
                # Aggregate total amount per category for today for this tg_user_id
                await cursor.execute(
                    """
                    SELECT 
                        d.category_id,
                        SUM(d.amount) AS total_amount,
                        u.id AS user_id,
                        u.time_utc
                    FROM dengies d
                    JOIN users u ON d.user_id = u.id
                    WHERE 
                        u.tg_user_id = %s
                        AND date_trunc('day', d.created_date) = 
                            date_trunc('day', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + u.time_utc)
                    GROUP BY d.category_id, u.id, u.time_utc;
                    """,
                    (tg_user_id,)
                )
                
                aggregated_rows = await cursor.fetchall()
                
                # Insert aggregated totals into daily_category_reports
                for category_id, total_amount, user_id, time_utc in aggregated_rows:
                    await cursor.execute(
                        """
                        INSERT INTO daily_category_reports (
                            user_id, category_id, month_id, total_amount, created_date
                        )
                        VALUES (%s, %s, NULL, %s, date_trunc('second', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + %s);
                        """,
                        (user_id, category_id, total_amount, time_utc)
                    )
            
            await connection.commit()
            logging.info("Daily category reports inserted successfully for tg_user_ids.")

    except (Exception, Error) as e:
        logging.error("Error inserting daily category reports: %s", e)
        if connection:
            await connection.rollback()
    
    finally:
        if connection is not None:
            await connection.close()



async def insert_daily_reports(tg_user_ids: list[int]):
    """
    Aggregate today's expenses and incomes for each user and insert
    into the daily_reports table.
    
    created_date is set to: date_trunc('second', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + u.time_utc
    """
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:

            for tg_user_id in tg_user_ids:

                # Aggregate today's totals grouped by expense/income type
                await cursor.execute(
                    """
                    SELECT
                        u.id AS user_id,
                        u.time_utc,
                        SUM(d.amount) AS total_amount,
                        CASE 
                            WHEN c.is_ex THEN TRUE
                            ELSE FALSE
                        END AS is_ex
                    FROM dengies d
                    JOIN users u ON d.user_id = u.id
                    JOIN categories c ON c.id = d.category_id
                    WHERE u.tg_user_id = %s
                        AND date_trunc('day', d.created_date) =
                            date_trunc('day', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + u.time_utc)
                    GROUP BY u.id, u.time_utc, is_ex;
                    """,
                    (tg_user_id,)
                )

                aggregated_rows = await cursor.fetchall()

                # Insert aggregated totals into daily_reports
                for user_id, time_utc, total_amount, is_ex in aggregated_rows:
                    await cursor.execute(
                        """
                        INSERT INTO daily_reports (
                            user_id, total_amount, is_ex, created_date
                        )
                        VALUES (%s, %s, %s, date_trunc('second', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + %s));
                        """,
                        (user_id, total_amount, is_ex, time_utc)
                    )

            await connection.commit()
            logging.info("Daily reports inserted successfully for tg_user_ids.")

    except Exception as e:
        logging.error(f"Error inserting daily reports: {e}")
        if connection:
            await connection.rollback()

    finally:
        if connection is not None:
            await connection.close()



async def minus_user_balance(user_id: int, amount: float) -> float | None:
    """
    Atomically subtracts the given amount from the user's balance if sufficient funds exist.
    Returns the new balance, or None if insufficient funds or user not found.
    """
    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cur:
            # Perform subtraction only if enough balance exists
            await cur.execute(
                """
                UPDATE users
                SET balans = balans - %s
                WHERE tg_user_id = %s AND balans >= %s
                RETURNING balans;
                """,
                (amount, user_id, amount)
            )

            result = await cur.fetchone()
            await conn.commit()

            if result:
                # Successfully updated and returned new balance
                return result[0]
            else:
                # Either user not found or insufficient funds
                logging.warning(
                    f"Insufficient funds or user not found: user_id={user_id}, amount={amount}"
                )
                return None

    except Exception as e:
        logging.error(f"Failed to minus balance for user_id={user_id}: {e}")
        return None

    finally:
        if conn:
            await conn.close()



async def get_category_name(cat_id: int) -> str | None:
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT title FROM categories WHERE id = %s LIMIT 1;", (cat_id,)
            )
            result = await cursor.fetchone()
            if result:
                return result[0]  # language_is
            return None

    except (Exception, Error) as error:
        logging.error("Error while fetching user language: %s", error)
        return None

    finally:
        if connection is not None:
            await connection.close()


async def get_todays_expense_count(tg_user_id: int):
    """
    Returns how many expenses the user made today according to their local time (created_date is already in local time).
    """
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                    SELECT COUNT(*)
                    FROM dengies d
                    JOIN users u ON d.user_id = u.id
                    WHERE u.tg_user_id = %s
                    AND d.created_date::date = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + u.time_utc::interval)::date;
                """,
                (tg_user_id,)
            )
            result = await cursor.fetchone()
            return result[0] if result else 0

    except Exception as e:
        logging.error(f"Error fetching today's expense count for {tg_user_id}: {e}")
        return 0

    finally:
        if connection is not None:
            await connection.close()


async def infos_get_user(tg_user_id: int):
    """
    Returns user information including:
    - balance
    - currency
    - lang_code
    - is_premium
    - premium_date
    - monthly_expenses
    - monthly_income (using user's local time via time_utc)
    """
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:

            await cursor.execute(
                """
                    SELECT 
                        u.balans,
                        u.currency_is,
                        u.language_is AS lang_code,
                        u.is_premium,
                        TO_CHAR(u.premium_date, 'YYYY-MM-DD') AS premium_date,

                        -- Monthly expenses (local month)
                        COALESCE((
                            SELECT SUM(dr.total_amount)
                            FROM daily_reports dr
                            WHERE dr.user_id = u.id
                              AND dr.is_ex = TRUE
                              AND DATE_TRUNC(
                                    'month',
                                    dr.created_date
                                  ) = DATE_TRUNC(
                                    'month',
                                    (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + u.time_utc::interval
                                  )
                        ), 0) AS monthly_expenses,

                        -- Monthly income (local month)
                        COALESCE((
                            SELECT SUM(dr.total_amount)
                            FROM daily_reports dr
                            WHERE dr.user_id = u.id
                              AND dr.is_ex = FALSE
                              AND DATE_TRUNC(
                                    'month',
                                    dr.created_date
                                  ) = DATE_TRUNC(
                                    'month',
                                    (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + u.time_utc::interval
                                  )
                        ), 0) AS monthly_income

                    FROM users u
                    WHERE u.tg_user_id = %s
                    LIMIT 1;
                """,
                (tg_user_id,)
            )

            row = await cursor.fetchone()
            if not row:
                return None

            return {
                "balance": float(row[0]),
                "currency": row[1],
                "lang_code": row[2],
                "is_premium": row[3],
                "premium_date": row[4],
                "monthly_expenses": float(row[5]),
                "monthly_income": float(row[6]),
            }

    except Exception as e:
        logging.error(f"Error fetching user data for tg_user_id {tg_user_id}: {e}")
        return None

    finally:
        if connection is not None:
            await connection.close()

async def get_users_by_time(target_hour: int, target_minute: int) -> list[tuple[int, str]] | None:
    """
    Return a list of (tg_user_id, language_is) of users whose local time
    matches the target hour and minute by calculating the required time_utc.
    """
    connection = None
    try:
        now_utc = datetime.utcnow()

        # Build target datetime today
        target_time = now_utc.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)

        # Shift to next day if target time is earlier than now
        if target_time < now_utc:
            target_time += timedelta(days=1)

        # Compute required offset
        offset: timedelta = target_time - now_utc

        # Round to nearest minute to avoid second mismatches
        total_seconds = int(round(offset.total_seconds() / 60) * 60)

        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Format as PostgreSQL INTERVAL string
        interval_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"  # 'HH:MM:SS'

        logging.info(f"Computed time_utc interval: {interval_str}")

        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            await cursor.execute("""
                SELECT tg_user_id, language_is
                FROM users
                WHERE time_utc = CAST(%s AS INTERVAL);
            """, (interval_str,))

            rows = await cursor.fetchall()
            if not rows:
                return None

            # Return list of tuples: (tg_user_id, language_is)
            return [(row[0], row[1]) for row in rows]

    except Exception as error:
        logging.error("Error fetching users by time: %s", error)
        return None

    finally:
        if connection is not None:
            await connection.close()



async def get_last_amounts(category_id: int, user_id: int) -> list[int]:
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT amount, id
                FROM (
                    SELECT DISTINCT ON (amount) amount, id
                    FROM dengies
                    WHERE category_id = %s
                    AND user_id = (SELECT id FROM users WHERE tg_user_id = %s)
                    ORDER BY amount, id
                ) AS distinct_amounts
                ORDER BY id DESC
                LIMIT 5;
                """,
                (category_id, user_id)
            )
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    except (Exception, Error) as error:
        logging.error("Error while fetching last amounts: %s", error)
        return None

    finally:
        if connection:
            await connection.close()

async def insert_dengies(amount: float, category_id: int, user_id: int) -> int | None:
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO dengies (amount, created_date, category_id, user_id)
                SELECT
                    %s AS amount,
                    date_trunc('second', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + u.time_utc AS created_date,
                    %s AS category_id,
                    u.id AS user_id
                FROM users u
                WHERE u.tg_user_id = %s
                RETURNING id;
                """,
                (amount, category_id, user_id)
            )
            inserted_id_row = await cursor.fetchone()
            await connection.commit()
            logging.info(f"Amount: {amount} is inserted to category: {category_id}")
            return inserted_id_row[0] if inserted_id_row else None

    except (Exception, Error) as error:
        logging.error("Error while inserting expense: %s", error)
        return None

    finally:
        if connection is not None:
            await connection.close()


async def get_active_categories_by_type(
    tg_user_id: int, is_ex: bool
) -> tuple[list[tuple[int, str]], int] | None:
    """
    Returns active categories (id, title) for a user based on expense/income type.
    Also returns the max allowed categories based on the user's premium status:
    - 20 for premium users
    - 8 for non-premium users
    """
    conn: AsyncConnection | None = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cur:
            # 1️⃣ Check if user is premium
            await cur.execute(
                "SELECT is_premium, id FROM users WHERE tg_user_id = %s LIMIT 1;",
                (tg_user_id,)
            )
            user_row = await cur.fetchone()
            if not user_row:
                return [], 8  # default to 8 if user not found

            is_premium, user_id = user_row
            max_categories = 20 if is_premium else 8

            # 2️⃣ Fetch categories for this user
            await cur.execute(
                """
                SELECT c.id, c.title
                FROM categories AS c
                WHERE c.is_active = TRUE
                  AND c.is_ex = %s
                  AND c.user_id = %s;
                """,
                (is_ex, user_id)
            )
            rows = await cur.fetchall()
            rows = rows or []

            return rows, max_categories

    except Exception as e:
        logging.error("Failed to fetch categories for user %s: %s", tg_user_id, e)
        return None

    finally:
        if conn:
            await conn.close()



async def get_is_premium(user_id: int) -> bool | None:
    """
    Fetches the is_premium status for a given Telegram user ID.

    :param user_id: Telegram user ID
    :return: True/False if user exists, None if error or user not found
    """
    conn: AsyncConnection | None = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT is_premium FROM users WHERE tg_user_id = %s LIMIT 1;",
                (user_id,)
            )
            result = await cursor.fetchone()
            if result:
                return result[0]  # bool
            return None

    except Exception as error:
        logging.error("Error while fetching is_premium for user %s: %s", user_id, error)
        return None

    finally:
        if conn:
            await conn.close()


async def is_exist_title(tg_user_id: int, title: str, is_ex: bool) -> bool | None:
    """
    Returns:
        True  -> category exists and active
        False -> category exists but was inactive (reactivated)
        None  -> category does not exist
    """
    conn: AsyncConnection | None = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, is_active
                FROM categories
                WHERE user_id = (SELECT id FROM users WHERE tg_user_id = %s)
                  AND LOWER(title) = LOWER(%s)
                  AND is_ex = %s
                LIMIT 1;
                """,
                (tg_user_id, title, is_ex)
            )
            row = await cur.fetchone()
            if row:
                category_id, is_active = row
                if is_active:
                    return True   # Already active
                else:
                    # Reactivate the category
                    await cur.execute(
                        "UPDATE categories SET is_active = TRUE WHERE id = %s",
                        (category_id,)
                    )
                    await conn.commit()
                    return False  # Reactivated
            else:
                return None  # Does not exist

    finally:
        if conn:
            await conn.close()



async def create_category(
    tg_user_id: int,
    title: str,
    is_ex: bool = True
) -> bool:
    """
    Creates a new category for a user.

    :param tg_user_id: Telegram user ID
    :param title: Category title
    :param is_ex: True for expense category, False for income category
    :return: True if created successfully, False otherwise
    """
    title = title.strip()
    conn: AsyncConnection | None = None

    try:
        conn = await get_db_connection()
        async with conn.cursor() as cur:
            # Get user's internal ID
            await cur.execute(
                "SELECT id FROM users WHERE tg_user_id = %s LIMIT 1;",
                (tg_user_id,)
            )
            user_row = await cur.fetchone()
            user_id = user_row[0]

            # Insert new category
            await cur.execute(
                """
                INSERT INTO categories (title, is_ex, user_id)
                VALUES (%s, %s, %s);
                """,
                (title, is_ex, user_id)
            )
            await conn.commit()
            logging.info(f"Category '{title}' created for user {tg_user_id}")
            return True

    except Exception as e:
        logging.error(f"Failed to create category '{title}' for user {tg_user_id}: {e}")
        return False

    finally:
        if conn:
            await conn.close()



async def deactivate_category(category_id: int) -> Optional[str]:
    """
    Deactivates a category by setting is_active to FALSE.
    
    :param category_id: ID of the category to deactivate
    :return: 'deactivated' if changed from TRUE to FALSE,
             'already_inactive' if it was already FALSE,
             None if an error occurred.
    """
    conn: Optional[AsyncConnection] = None
    try:
        conn = await get_db_connection()  # Your async DB connection function
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE categories
                SET is_active = FALSE
                WHERE id = %s
                  AND is_active = TRUE;
                """,
                (category_id,)
            )
            await conn.commit()

            if cur.rowcount == 1:
                logging.info(f"Category {category_id} was active and now deactivated.")
                return "deactivated"
            else:
                logging.info(f"Category {category_id} was already inactive.")
                return "already_inactive"

    except Exception as e:
        logging.error(f"Failed to deactivate category {category_id}: {e}")
        return None

    finally:
        if conn:
            await conn.close()


async def get_user_language(user_id: int) -> str | None:
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT language_is FROM users WHERE tg_user_id = %s LIMIT 1;", (user_id,)
            )
            result = await cursor.fetchone()
            if result:
                return result[0]  # language_is
            return None

    except (Exception, Error) as error:
        logging.error("Error while fetching user language: %s", error)
        return None

    finally:
        if connection is not None:
            await connection.close()


async def user_exist(user_id: int) -> bool:
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1 FROM users WHERE tg_user_id = %s LIMIT 1;", (user_id,))
            result = await cursor.fetchone()
            return result is not None
    except Exception as e:
        logging(f"Error user_exist: {e}")
        return False
    finally:
        if conn:
            await conn.close()

async def insert_or_update_user(
    user_id: int,
    first_name: str,
    user_name: Optional[str],
    language_is: str
) -> Optional[str]:
    """
    Inserts or updates a user.
    - Creates default categories for new users.
    - Updates default category names if user language changes.
    Returns: 'inserted', 'updated', or None on error.
    """
    conn: Optional[AsyncConnection] = None
    try:
        conn = await get_db_connection()
        async with conn.cursor(row_factory=dict_row) as cur:
            # Insert or update the user
            await cur.execute(
                """
                INSERT INTO users (
                    tg_user_id, first_name, user_name, language_is
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tg_user_id) DO UPDATE
                SET
                    first_name = EXCLUDED.first_name,
                    user_name = EXCLUDED.user_name,
                    language_is = EXCLUDED.language_is
                RETURNING id, (xmax = 0) AS inserted;
                """,
                (user_id, first_name, user_name, language_is)
            )
            result = await cur.fetchone()
            if not result:
                return None

            user_db_id = result["id"]
            inserted = result["inserted"]


            #Get translated category names
            food = await translator.get_text(language_is, "cat_food")
            # food_names = translator.get_all_values_by_key(translator.translations, "cat_food")

            salary = await translator.get_text(language_is, "cat_salary")
            # sal_names = translator.get_all_values_by_key(translator.translations, "cat_salary")

            transport = await translator.get_text(language_is, "cat_transport")
            # transport_names = translator.get_all_values_by_key(translator.translations, "cat_transport")

            gifts = await translator.get_text(language_is, "cat_gift")
            # gifts_names = translator.get_all_values_by_key(translator.translations, "cat_gift")
            
            other = await translator.get_text(language_is, "other")
            # other_names = translator.get_all_values_by_key(translator.translations, "other")

            if inserted:
                # Create default categories if user is new
                await cur.executemany(
                    """
                    INSERT INTO categories (title, is_ex, user_id)
                    VALUES (%s, TRUE, %s);
                    """,
                    [(food, user_db_id), (transport, user_db_id), (other, user_db_id)]
                )
                await cur.executemany(
                    """
                    INSERT INTO categories (title, is_ex, user_id)
                    VALUES (%s, FALSE, %s);
                    """,
                    [(salary, user_db_id), (gifts, user_db_id), (other, user_db_id)]
                )
                logging.info(f"✅ Created default categories for new user {user_id}")
            # else:
            #     # Update existing default categories to match new language
            #     await cur.execute(
            #         """
            #         UPDATE categories
            #         SET title = CASE
            #             WHEN LOWER(title) = ANY(%s) THEN %s
            #             WHEN LOWER(title) = ANY(%s) THEN %s
            #             WHEN LOWER(title) = ANY(%s) THEN %s
            #             ELSE title
            #         END
            #         WHERE user_id = %s AND is_ex = TRUE;
            #         """,
            #         (food_names, food, transport_names, transport, other_names, other, user_db_id)
            #     )

            #     await cur.execute(
            #         """
            #         UPDATE categories
            #         SET title = CASE
            #             WHEN LOWER(title) = ANY(%s) THEN %s
            #             WHEN LOWER(title) = ANY(%s) THEN %s
            #             WHEN LOWER(title) = ANY(%s) THEN %s
            #             ELSE title
            #         END
            #         WHERE user_id = %s AND is_ex = FALSE;
            #         """,
            #         (sal_names, salary, gifts_names, gifts, other_names, other, user_db_id)
            #     )
            #     logging.info(f"🔁 Updated default categories for existing user {user_id} language {language_is}")

            await conn.commit()
            return "inserted" if inserted else "updated"

    except Exception as e:
        logging.error("❌ Failed to insert/update user %s: %s", user_id, e)
        return None

    finally:
        if conn:
            await conn.close()



async def get_last_times() -> list[str] | None:
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT time_utc, id
                FROM (
                    SELECT DISTINCT ON (time_utc) time_utc, id
                    FROM users
                    ORDER BY time_utc, id
                ) AS distinct_times
                ORDER BY id DESC
                LIMIT 5;
                """
            )
            rows = await cursor.fetchall()

            # Get current UTC time (not local time)
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

            result_times = []
            for row in rows:
                time_offset: timedelta = row[0]
                local_time = now_utc + time_offset
                result_times.append(local_time.strftime("%Y-%m-%d %H:%M"))

            # logging.info(f"Local times based on UTC now: {result_times}")
            return result_times

    except (Exception, Error) as error:
        logging.error("Error while calculating local times: %s", error)
        return None

    finally:
        if connection:
            await connection.close()


async def get_last_currencies() -> list[str] | None:
    connection: Optional[AsyncConnection] = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:
            await cursor.execute(
                """
                SELECT currency_is
                FROM (
                    SELECT DISTINCT ON (currency_is) currency_is, id
                    FROM users
                    ORDER BY currency_is, id DESC
                ) AS distinct_currencies
                ORDER BY id DESC
                LIMIT 5;
                """
            )
            rows = await cursor.fetchall()

            # Extract currency codes as a list of strings
            result_currencies = [row[0] for row in rows]
            # logging.info(f"Returning keyboard with {result_currencies} values")
            return result_currencies

    except Exception as error:
        logging.error("Error while fetching last currencies: %s", error)
        return None

    finally:
        if connection:
            await connection.close()


async def update_user_info(
    user_id: int,
    rounded_offset,
    currency: str,
    balans: float | None = None
) -> bool:
    """
    Updates time_utc, currency, and optionally balans for the given user.
    If balans is None, it will not be updated.
    """
    conn: AsyncConnection | None = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cur:
            # Build the SQL dynamically
            sql = "UPDATE users SET time_utc = %s, currency_is = %s"
            params = [rounded_offset, currency]

            if balans is not None:
                sql += ", balans = %s"
                params.append(balans)

            sql += " WHERE tg_user_id = %s;"
            params.append(user_id)

            await cur.execute(sql, params)
            await conn.commit()
            
            logging.info(
                f"Updated user {user_id} with values utc {rounded_offset}, currency {currency}"
                + (f", balans {balans}" if balans is not None else "")
            )
            return True

    except Exception as e:
        logging.error("Failed to update user %s: %s", user_id, e)
        return False

    finally:
        if conn:
            await conn.close()

async def update_comment_text(dengies_id: int, comment_text: str) -> bool:
    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE dengies
                SET comment_text = %s
                WHERE id = %s;
                """,
                (comment_text, dengies_id)
            )
            await conn.commit()
            logging.info(f"Succesfuly saved comment to {dengies_id}")
            return cur.rowcount > 0  # True if a row was updated

    except Exception as e:
        logging.error(f"Failed to update comment for amount_id={dengies_id}: {e}")
        return False

    finally:
        if conn:
            await conn.close()


async def add_user_balance(user_id: int, amount: float, MAX_NUMERIC_12_2: float) -> float | None:
    """
    Atomically adds the given amount to the user's balance if it doesn't exceed NUMERIC(12,2) max.
    Returns the new balance, or None if addition would overflow or user not found.
    """
    conn = None
    try:
        conn = await get_db_connection()
        async with conn.cursor() as cur:
            # Perform addition only if it does not exceed MAX_NUMERIC_12_2
            await cur.execute(
                """
                UPDATE users
                SET balans = balans + %s
                WHERE tg_user_id = %s AND balans + %s <= %s
                RETURNING balans;
                """,
                (amount, user_id, amount, MAX_NUMERIC_12_2)
            )

            result = await cur.fetchone()
            await conn.commit()

            if result:
                # Successfully updated and returned new balance
                return float(result[0])
            else:
                # Either user not found or addition would exceed NUMERIC(12,2) max
                logging.warning(
                    f"Cannot add amount: user_id={user_id}, amount={amount} would exceed NUMERIC(12,2) limit"
                )
                return None

    except Exception as e:
        logging.error(f"Failed to add balance for user_id={user_id}: {e}")
        return None

    finally:
        if conn:
            await conn.close()


async def insert_monthly_category_reports(tg_user_ids: list[int]):
    """
    Aggregate the previous month's daily category reports for each user,
    insert into monthly_category_reports with created_date in user's local time,
    and update daily_category_reports.month_id.
    """
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:

            # Determine previous month and year
            today = datetime.utcnow()
            first_day_this_month = today.replace(day=1)
            last_day_prev_month = first_day_this_month - timedelta(days=1)
            prev_month = last_day_prev_month.month
            prev_year = last_day_prev_month.year

            # # --- Manual override for testing ---
            # prev_month = 11   # May
            # prev_year = 2025 # Year 2024

            for tg_user_id in tg_user_ids:
                # Get the user's internal id and time_utc
                await cursor.execute(
                    "SELECT id, time_utc FROM users WHERE tg_user_id = %s",
                    (tg_user_id,)
                )
                result = await cursor.fetchone()
                if not result:
                    continue
                user_id, time_utc = result

                # Aggregate total_amount per category for previous month from daily_category_reports
                await cursor.execute(
                    """
                    SELECT 
                        category_id,
                        SUM(total_amount) AS total_amount
                    FROM daily_category_reports
                    WHERE user_id = %s
                        AND EXTRACT(MONTH FROM created_date) = %s
                        AND EXTRACT(YEAR FROM created_date) = %s
                        AND month_id IS NULL
                    GROUP BY category_id
                    """,
                    (user_id, prev_month, prev_year)
                )
                aggregated_rows = await cursor.fetchall()

                # Insert into monthly_category_reports and update daily_category_reports
                for category_id, total_amount in aggregated_rows:
                    await cursor.execute(
                        """
                        INSERT INTO monthly_category_reports (
                            user_id, category_id, year_id, total_amount, created_date
                        )
                        VALUES (%s, %s, NULL, %s, date_trunc('second', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + %s)
                        RETURNING id
                        """,
                        (user_id, category_id, total_amount, time_utc)
                    )
                    monthly_id_row = await cursor.fetchone()
                    monthly_id = monthly_id_row[0]

                    # Update daily_category_reports.month_id for this user and category
                    await cursor.execute(
                        """
                        UPDATE daily_category_reports
                        SET month_id = %s
                        WHERE user_id = %s
                            AND category_id = %s
                            AND month_id IS NULL
                            AND EXTRACT(MONTH FROM created_date) = %s
                            AND EXTRACT(YEAR FROM created_date) = %s
                        """,
                        (monthly_id, user_id, category_id, prev_month, prev_year)
                    )

            await connection.commit()
            logging.info("Monthly category reports inserted and daily reports updated successfully.")

    except (Exception, Error) as e:
        logging.error("Error inserting monthly category reports: %s", e)
        if connection:
            await connection.rollback()

    finally:
        if connection is not None:
            await connection.close()


async def insert_yearly_category_reports(tg_user_ids: list[int]):
    """
    Aggregate the previous year's monthly totals per category for each user,
    insert into yearly_category_reports with created_date in user's local time.
    Then update monthly_category_reports.year_id for the inserted yearly report.
    """
    connection = None
    try:
        connection = await get_db_connection()
        async with connection.cursor() as cursor:

            for tg_user_id in tg_user_ids:
                # Get user's internal id and time_utc
                await cursor.execute(
                    "SELECT id, time_utc FROM users WHERE tg_user_id = %s",
                    (tg_user_id,)
                )
                result = await cursor.fetchone()
                if not result:
                    continue
                user_id, time_utc = result

                # Calculate user's local date
                now_utc = datetime.utcnow()
                local_time = now_utc + time_utc  # time_utc is INTERVAL in Postgres
                # Uncomment if you want to restrict to Jan 1
                if local_time.month != 1 or local_time.day != 1:
                    logging.info(f"Today is not Jan 1 for user {tg_user_id}. Skipping.")
                    continue

                prev_year = local_time.year - 1  # last completed year
                # prev_year = 2025  # for testing manually

                # Aggregate total_amount per category for the previous year
                await cursor.execute(
                    """
                    SELECT 
                        category_id,
                        SUM(total_amount) AS total_amount
                    FROM monthly_category_reports
                    WHERE user_id = %s
                        AND EXTRACT(YEAR FROM created_date) = %s
                    GROUP BY category_id
                    """,
                    (user_id, prev_year)
                )
                aggregated_rows = await cursor.fetchall()

                for category_id, total_amount in aggregated_rows:
                    # Insert into yearly_category_reports
                    await cursor.execute(
                        """
                        INSERT INTO yearly_category_reports (
                            user_id, category_id, total_amount, created_date
                        )
                        VALUES (%s, %s, %s, date_trunc('second', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + %s)
                        RETURNING id
                        """,
                        (user_id, category_id, total_amount, time_utc)
                    )
                    yearly_id_row = await cursor.fetchone()
                    yearly_id = yearly_id_row[0]

                    # Update monthly_category_reports.year_id where it is null for this user/category
                    await cursor.execute(
                        """
                        UPDATE monthly_category_reports
                        SET year_id = %s
                        WHERE user_id = %s
                            AND category_id = %s
                            AND year_id IS NULL
                            AND EXTRACT(YEAR FROM created_date) = %s
                        """,
                        (yearly_id, user_id, category_id, prev_year)
                    )

            await connection.commit()
            logging.info("Yearly category reports inserted and monthly reports updated successfully.")

    except (Exception, Error) as e:
        logging.error("Error inserting yearly category reports: %s", e)
        if connection:
            await connection.rollback()

    finally:
        if connection is not None:
            await connection.close()