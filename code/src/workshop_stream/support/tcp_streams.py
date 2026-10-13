"""TCP source and server lifecycle plumbing for the network example."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from types import TracebackType

from workshop_stream.support.stream import Stream


class Client:
    __slots__ = ("_reader", "_writer", "_on_close", "_reads", "_closed")

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        on_close: Callable[[Client], None],
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._on_close = on_close
        self._closed = False
        self._reads = Stream._from_factory(self._lines)

    @property
    def reads(self) -> Stream[str]:
        return self._reads

    async def _lines(self) -> AsyncIterator[str]:
        try:
            while line := await self._reader.readline():
                try:
                    yield line.decode("utf-8").rstrip("\r\n")
                except UnicodeDecodeError:
                    continue
        finally:
            await self._close()

    async def _close(self) -> None:
        self._start_close()
        try:
            await self._writer.wait_closed()
        except OSError:
            pass

    def _start_close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._on_close(self)
        self._writer.close()


class TCPReportServer:
    def __init__(self, listener: socket.socket) -> None:
        self._listener = listener
        self._clients: set[Client] = set()
        self._accept_lock = asyncio.Lock()
        self._accept_task: asyncio.Task[object] | None = None
        self._stopped = False

    @property
    def port(self) -> int:
        address = self._listener.getsockname()
        return int(address[1])

    @property
    def clients(self) -> Stream[Client]:
        return Stream._from_factory(self._client_streams)

    async def _client_streams(self) -> AsyncIterator[Client]:
        while True:
            try:
                client = await self._accept_client()
            except asyncio.CancelledError:
                if self._stopped:
                    return
                raise
            if client is None:
                return
            yield client

    async def _accept_client(self) -> Client | None:
        async with self._accept_lock:
            if self._stopped:
                return None
            task = asyncio.current_task()
            self._accept_task = task
            try:
                accepted, _address = await asyncio.get_running_loop().sock_accept(
                    self._listener
                )
                try:
                    reader, writer = await asyncio.open_connection(sock=accepted)
                except BaseException:
                    accepted.close()
                    raise
            finally:
                self._accept_task = None
        client = Client(reader, writer, self._clients.discard)
        self._clients.add(client)
        return client

    async def _stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._listener.close()
        accept_task = self._accept_task
        if accept_task is not None:
            accept_task.cancel()
        clients = tuple(self._clients)
        for client in clients:
            client._start_close()
        if accept_task is not None:
            await asyncio.gather(accept_task, return_exceptions=True)
        await asyncio.gather(
            *(client._close() for client in clients),
            return_exceptions=True,
        )


def tcp_report_server(
    host: str = "127.0.0.1", port: int = 0
) -> AbstractAsyncContextManager[TCPReportServer]:
    return _TCPReportServerContext(host, port)


class _TCPReportServerContext:
    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._server: TCPReportServer | None = None

    async def __aenter__(self) -> TCPReportServer:
        listener = socket.create_server((self._host, self._port))
        listener.setblocking(False)
        self._server = TCPReportServer(listener)
        return self._server

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if self._server is not None:
            await self._server._stop()
            self._server = None
        return None


def tcp_clients(host: str, port: int) -> Stream[Client]:
    return Stream._from_factory(lambda: _tcp_clients(host, port))


async def _tcp_clients(host: str, port: int) -> AsyncIterator[Client]:
    async with tcp_report_server(host=host, port=port) as server:
        async with server.clients.stream() as clients:
            async for client in clients:
                yield client
