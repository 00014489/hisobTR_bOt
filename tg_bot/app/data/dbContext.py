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

import logging

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
                INSERT INTO dengies (amount, category_id, user_id)
                VALUES (%s, %s, (SELECT id from users WHERE tg_user_id = %s))
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
            food_names = translator.get_all_values_by_key(translator.translations, "cat_food")

            salary = await translator.get_text(language_is, "cat_salary")
            sal_names = translator.get_all_values_by_key(translator.translations, "cat_salary")

            transport = await translator.get_text(language_is, "cat_transport")
            transport_names = translator.get_all_values_by_key(translator.translations, "cat_transport")

            gifts = await translator.get_text(language_is, "cat_gift")
            gifts_names = translator.get_all_values_by_key(translator.translations, "cat_gift")
            
            other = await translator.get_text(language_is, "other")
            other_names = translator.get_all_values_by_key(translator.translations, "other")

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
            else:
                # Update existing default categories to match new language
                await cur.execute(
                    """
                    UPDATE categories
                    SET title = CASE
                        WHEN LOWER(title) = ANY(%s) THEN %s
                        WHEN LOWER(title) = ANY(%s) THEN %s
                        WHEN LOWER(title) = ANY(%s) THEN %s
                        ELSE title
                    END
                    WHERE user_id = %s AND is_ex = TRUE;
                    """,
                    (food_names, food, transport_names, transport, other_names, other, user_db_id)
                )

                await cur.execute(
                    """
                    UPDATE categories
                    SET title = CASE
                        WHEN LOWER(title) = ANY(%s) THEN %s
                        WHEN LOWER(title) = ANY(%s) THEN %s
                        WHEN LOWER(title) = ANY(%s) THEN %s
                        ELSE title
                    END
                    WHERE user_id = %s AND is_ex = FALSE;
                    """,
                    (sal_names, salary, gifts_names, gifts, other_names, other, user_db_id)
                )
                logging.info(f"🔁 Updated default categories for existing user {user_id} language {language_is}")

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
