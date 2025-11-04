import psycopg
from psycopg import sql, Error, AsyncConnection
import logging
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
    Returns 'inserted' if a new record was created,
            'updated' if an existing record was modified,
            None if an error occurred.
    """
    conn: Optional[AsyncConnection] = None
    try:
        conn = await get_db_connection()
        async with conn.cursor(row_factory=dict_row) as cur:
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
                RETURNING
                    (xmax = 0) AS inserted;
                """,
                (user_id, first_name, user_name, language_is)
            )
            result = await cur.fetchone()
            await conn.commit()

            if result and result["inserted"]:
                logging.info(f"user {user_id} is inserted")
                return "inserted"
            else:
                logging.info(f"user {user_id} is updated")
                return "updated"

    except Exception as e:
        logging.error("Failed to insert/update user %s: %s", user_id, e)
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

