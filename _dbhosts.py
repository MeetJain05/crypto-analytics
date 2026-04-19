import asyncio
import asyncpg

async def test(host):
    dsn = f"postgresql://vibestream:vibestream@{host}:55432/vibestream"
    try:
        conn = await asyncpg.connect(dsn)
        v = await conn.fetchval("select version()")
        print(host, "OK", v.split()[0], v.split()[1])
        await conn.close()
    except Exception as exc:
        print(host, "ERR", type(exc).__name__, exc)

async def main():
    for h in ["localhost", "127.0.0.1", "::1"]:
        await test(h)

asyncio.run(main())
