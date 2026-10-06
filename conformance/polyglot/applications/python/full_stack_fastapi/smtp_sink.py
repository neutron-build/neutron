"""Minimal loopback SMTP sink so email-sending routes behave the same on both sides.

Accepts any message and discards it. Standard library only; bind address is loopback.
"""
import asyncio


async def _session(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    async def reply(line: str) -> None:
        writer.write((line + "\r\n").encode())
        await writer.drain()

    try:
        await reply("220 localhost ESMTP sink")
        while True:
            raw = await reader.readline()
            if not raw:
                return
            command = raw.decode(errors="replace").strip().upper()
            if command.startswith("DATA"):
                await reply("354 End data with <CR><LF>.<CR><LF>")
                while True:
                    line = await reader.readline()
                    if not line or line.strip() == b".":
                        break
                await reply("250 OK")
            elif command.startswith("QUIT"):
                await reply("221 Bye")
                return
            else:
                await reply("250 OK")
    except (ConnectionError, asyncio.IncompleteReadError):
        return
    finally:
        writer.close()


async def start(port: int = 0) -> asyncio.Server:
    return await asyncio.start_server(_session, "127.0.0.1", port)
