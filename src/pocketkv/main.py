import asyncio
from typing import Any, Optional
import sys

from loguru import logger

HOST = "localhost"
PORT = 6379

connections = 0


def _patcher_fn(record):
    if record["extra"].get("connection_id") is not None:
        record.update(
            message=f"[{record['extra']['connection_id']}] {record['message']}"
        )


logger.configure(
    extra={"connection_id": None},
    patcher=_patcher_fn,
    handlers=[
        {
            "sink": sys.stderr,
            "format": "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | {level:<8} | {message}",
        }
    ],
)


def fmt_addr(addr: Optional[Any]) -> Optional[str]:
    if addr is None:
        return None
    if isinstance(addr, tuple):
        # IPv4: (host, port)
        if len(addr) == 2:
            host, port = addr
            return f"{host}:{port}"
        # IPv6: (host, port, flowinfo, scopeid)
        if len(addr) == 4:
            host, port = addr[0], addr[1]
            return f"[{host}]:{port}"
    # Unix domain sockets: string path
    return str(addr)


def get_connection_info(writer: asyncio.StreamWriter) -> tuple[str, str]:
    gi = writer.get_extra_info
    server = fmt_addr(gi("sockname")) or "unknown-local"
    client = fmt_addr(gi("peername")) or "unknown-peer"

    # TODO: add tls info
    # tls = ""
    # if gi("ssl_object"):
    #     cipher = gi("cipher")
    #     if cipher:
    #         name, proto, bits = cipher
    #         tls = f" [TLS {proto} {name}/{bits}b]"
    #     else:
    #         tls = " [TLS]"

    return server, client


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    global connections

    server, client = get_connection_info(writer)
    conn_logger = logger.bind(connection_id=connections)
    connections += 1
    conn_logger.info(f"connection accepted {client} -> {server}")
    try:
        while True:
            line = await reader.readline()  # waits for '\n' or EOF
            if not line:  # EOF => client closed
                break

            msg = line.rstrip(b"\r\n")
            if msg == b"PING":
                writer.write(b"+PONG\r\n")
                await writer.drain()
            # else:
            #     break
    except asyncio.CancelledError:
        raise
    except Exception:
        conn_logger.exception("error while handling client")
    finally:
        conn_logger.info("client closed connection")
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionResetError:
            pass


async def create_server():
    try:
        server = await asyncio.start_server(handle_client, HOST, PORT)
        for s in server.sockets:
            logger.info(
                f"creating server TCP listening socket {fmt_addr(s.getsockname())}"
            )
        async with server:
            await server.serve_forever()
    except OSError:
        logger.exception("error while creating server")


def main():
    try:
        asyncio.run(create_server())
    except KeyboardInterrupt:  # graceful Ctrl+C
        print("")  # add a new line after ^C
        logger.info("shutting down server")
        raise SystemExit(130)
