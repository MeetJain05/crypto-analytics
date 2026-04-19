import asyncio
import asyncpg

async def main():
    dsn = "postgresql://vibestream:vibestream@localhost:55432/vibestream"
    try:
        conn = await asyncpg.connect(dsn)
        user = await conn.fetchval("select current_user")
        print("OK", user)
        await conn.close()
    except Exception as exc:
        print("ERR", type(exc).__name__, exc)

asyncio.run(main())
