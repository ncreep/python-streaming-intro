"""Integration tests for the newline-delimited TCP report source."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager

import pytest

import workshop_stream.from_network as network
import workshop_stream.support.tcp_streams as tcp_streams
from workshop_stream.support.stream import Stream
from workshop_stream.support.tcp_streams import (
    Client,
    TCPReportServer,
    tcp_clients,
    tcp_report_server,
)
from workshop_stream.vacation_report import VacationReport


async def next_report(
    reports: AsyncIterator[VacationReport],
) -> VacationReport:
    return await asyncio.wait_for(anext(reports), timeout=2.0)


async def pull[T](iterator: AsyncIterator[T]) -> T:
    return await anext(iterator)


class FakeTCPReportServer(TCPReportServer):
    def __init__(self, clients: Stream[Client]) -> None:
        self._test_clients = clients

    @property
    def clients(self) -> Stream[Client]:
        return self._test_clients


class FakeClient(Client):
    __slots__ = ("_test_reads",)

    def __init__(self, reads: Stream[str]) -> None:
        self._test_reads = reads

    @property
    def reads(self) -> Stream[str]:
        return self._test_reads


def _reports_from(
    clients: Stream[Client], *, capacity: int = 100
) -> Stream[VacationReport]:
    return clients.map(
        lambda client: (
            client.reads.map(VacationReport.parse_csv)
            .drop_none()
            .handle_error(lambda _error: Stream.empty())
        )
    ).flatten(task_limit=capacity)


@pytest.mark.asyncio
async def test_server_exposes_the_os_selected_port_and_merges_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with tcp_report_server(port=0) as server:
        assert server.port > 0
        monkeypatch.setattr(network, "from_network", _reports_from(server.clients))

        async with network.from_network.stream() as reports:
            first_report = asyncio.create_task(pull(reports))
            first_reader, first_writer = await asyncio.open_connection(
                "127.0.0.1", server.port
            )
            second_reader, second_writer = await asyncio.open_connection(
                "127.0.0.1", server.port
            )
            _ = first_reader, second_reader

            first_writer.write(b"bad row\nAlice,2,true\nnot-an-int,nope,false\n")
            second_writer.write(b"Bob,4,false\n")
            await asyncio.gather(first_writer.drain(), second_writer.drain())

            received = {
                await asyncio.wait_for(first_report, timeout=2.0),
                await next_report(reports),
            }

        assert received == {
            VacationReport("Alice", 2, True),
            VacationReport("Bob", 4, False),
        }

        first_writer.close()
        second_writer.close()
        await asyncio.gather(first_writer.wait_closed(), second_writer.wait_closed())


@pytest.mark.asyncio
async def test_server_clients_wrap_per_connection_reads_stream() -> None:
    async with tcp_report_server() as server:
        async with Stream._from_factory(lambda: server.clients).stream() as clients:
            client_demand = asyncio.create_task(pull(clients))
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            client = await asyncio.wait_for(client_demand, timeout=2.0)

            writer.write(b"Alice,2,true\n")
            await writer.drain()

        assert type(client) is Client
        async with Stream._from_factory(lambda: client.reads).stream() as lines:
            assert await asyncio.wait_for(anext(lines), timeout=2.0) == "Alice,2,true"

        writer.close()
        await writer.wait_closed()
        assert await asyncio.wait_for(reader.read(), timeout=2.0) == b""


@pytest.mark.asyncio
async def test_failing_client_stream_is_isolated_from_healthy_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failed_reads() -> AsyncIterator[str]:
        yield "BeforeFailure,1,false"
        raise OSError("client disconnected unexpectedly")

    async def fake_clients() -> AsyncIterator[Client]:
        yield FakeClient(Stream._from_factory(failed_reads))
        yield FakeClient(Stream.values("Healthy,2,true"))

    clients = Stream._from_factory(fake_clients)
    monkeypatch.setattr(network, "from_network", _reports_from(clients))
    reports = network.from_network

    async with reports.stream() as items:
        received = {report async for report in items}

    assert received == {
        VacationReport("BeforeFailure", 1, False),
        VacationReport("Healthy", 2, True),
    }


@pytest.mark.asyncio
async def test_clients_are_consumed_concurrently_and_keep_per_client_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with tcp_report_server() as server:
        monkeypatch.setattr(network, "from_network", _reports_from(server.clients))
        async with network.from_network.stream() as reports:
            first_report = asyncio.create_task(pull(reports))
            peers = [
                await asyncio.open_connection("127.0.0.1", server.port)
                for _ in range(3)
            ]
            for index, (_, writer) in enumerate(peers):
                writer.write(f"Client{index},1,false\nClient{index},2,true\n".encode())
            await asyncio.gather(*(writer.drain() for _, writer in peers))

            received = [await asyncio.wait_for(first_report, timeout=2.0)]
            for _ in range(5):
                received.append(await next_report(reports))

        positions: dict[str, list[int]] = {}
        for report in received:
            positions.setdefault(report.name, []).append(report.days)
        assert positions == {
            "Client0": [1, 2],
            "Client1": [1, 2],
            "Client2": [1, 2],
        }

        for _, writer in peers:
            writer.close()
        await asyncio.gather(*(writer.wait_closed() for _, writer in peers))


@pytest.mark.asyncio
async def test_invalid_utf8_is_dropped_and_client_disconnect_is_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with tcp_report_server() as server:
        monkeypatch.setattr(network, "from_network", _reports_from(server.clients))
        async with network.from_network.stream() as reports:
            first_report = asyncio.create_task(pull(reports))
            broken_reader, broken_writer = await asyncio.open_connection(
                "127.0.0.1", server.port
            )
            healthy_reader, healthy_writer = await asyncio.open_connection(
                "127.0.0.1", server.port
            )
            _ = broken_reader, healthy_reader

            broken_writer.write(b"\xff,1,true\n")
            broken_writer.close()
            await broken_writer.wait_closed()
            healthy_writer.write(b"StillServing,3,false\n")
            await healthy_writer.drain()

            assert await asyncio.wait_for(first_report, timeout=2.0) == VacationReport(
                "StillServing", 3, False
            )

        healthy_writer.close()
        await healthy_writer.wait_closed()


@pytest.mark.asyncio
async def test_early_downstream_close_closes_the_client_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with tcp_report_server() as server:
        monkeypatch.setattr(network, "from_network", _reports_from(server.clients))
        async with network.from_network.stream() as reports:
            first_report = asyncio.create_task(pull(reports))
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            writer.write(b"Early,1,false\n")
            await writer.drain()

            assert await asyncio.wait_for(first_report, timeout=2.0) == VacationReport(
                "Early", 1, False
            )

        assert await asyncio.wait_for(reader.read(), timeout=2.0) == b""
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_context_exit_unblocks_outer_stream_and_closes_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = asyncio.Event()
    client_received = asyncio.Event()

    async def consume(server: TCPReportServer) -> None:
        monkeypatch.setattr(network, "from_network", _reports_from(server.clients))
        async with network.from_network.stream() as reports:
            ready.set()
            await next_report(reports)
            client_received.set()
            with pytest.raises(StopAsyncIteration):
                async with asyncio.timeout(2.0):
                    await anext(reports)

    async with asyncio.TaskGroup() as tasks:
        async with tcp_report_server() as server:
            tasks.create_task(consume(server))
            await asyncio.wait_for(ready.wait(), timeout=2.0)
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            writer.write(b"Connected,1,false\n")
            await writer.drain()
            await asyncio.wait_for(client_received.wait(), timeout=2.0)
        assert await asyncio.wait_for(reader.read(), timeout=2.0) == b""
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_idle_connection_waits_for_a_later_client_pull() -> None:
    async with tcp_report_server() as server:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                await reader.read(1)

        async with server.clients.stream() as clients:
            client = await asyncio.wait_for(pull(clients), timeout=2.0)
            writer.write(b"Backlogged,1,true\n")
            await writer.drain()
            async with client.reads.stream() as lines:
                assert await asyncio.wait_for(anext(lines), timeout=2.0) == (
                    "Backlogged,1,true"
                )

        writer.close()
        await writer.wait_closed()
        assert await asyncio.wait_for(reader.read(), timeout=2.0) == b""


@pytest.mark.asyncio
async def test_connection_with_pending_client_demand_is_delivered() -> None:
    async with tcp_report_server() as server:
        async with server.clients.stream() as clients:
            pending_client = asyncio.create_task(pull(clients))
            await asyncio.sleep(0)

            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            client = await asyncio.wait_for(pending_client, timeout=2.0)

            assert type(client) is Client

            writer.write(b"Demanded,1,true\n")
            await writer.drain()
            async with client.reads.stream() as lines:
                assert await asyncio.wait_for(anext(lines), timeout=2.0) == (
                    "Demanded,1,true"
                )

            writer.close()
            await writer.wait_closed()
            assert await asyncio.wait_for(reader.read(), timeout=2.0) == b""


@pytest.mark.asyncio
async def test_concurrent_subscriptions_receive_connections_in_fifo_order() -> None:
    async with tcp_report_server() as server:
        async with server.clients.stream() as first_subscription:
            async with server.clients.stream() as second_subscription:
                first_pull = asyncio.create_task(pull(first_subscription))
                second_pull = asyncio.create_task(pull(second_subscription))
                await asyncio.sleep(0)

                first_reader, first_writer = await asyncio.open_connection(
                    "127.0.0.1", server.port
                )
                second_reader, second_writer = await asyncio.open_connection(
                    "127.0.0.1", server.port
                )
                first_client, second_client = await asyncio.gather(
                    asyncio.wait_for(first_pull, timeout=2.0),
                    asyncio.wait_for(second_pull, timeout=2.0),
                )

                first_writer.write(b"First,1,true\n")
                second_writer.write(b"Second,2,false\n")
                await asyncio.gather(first_writer.drain(), second_writer.drain())

                async with first_client.reads.stream() as first_lines:
                    assert await asyncio.wait_for(anext(first_lines), timeout=2.0) == (
                        "First,1,true"
                    )
                async with second_client.reads.stream() as second_lines:
                    assert await asyncio.wait_for(anext(second_lines), timeout=2.0) == (
                        "Second,2,false"
                    )

        assert await asyncio.wait_for(first_reader.read(), timeout=2.0) == b""
        assert await asyncio.wait_for(second_reader.read(), timeout=2.0) == b""
        first_writer.close()
        second_writer.close()
        await asyncio.gather(first_writer.wait_closed(), second_writer.wait_closed())


@pytest.mark.asyncio
async def test_cancelled_pull_does_not_consume_the_next_connection() -> None:
    async with tcp_report_server() as server:
        async with server.clients.stream() as cancelled_subscription:
            async with server.clients.stream() as live_subscription:
                cancelled_pull = asyncio.create_task(pull(cancelled_subscription))
                live_pull = asyncio.create_task(pull(live_subscription))
                await asyncio.sleep(0)

                cancelled_pull.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await cancelled_pull

                reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
                client = await asyncio.wait_for(live_pull, timeout=2.0)
                writer.write(b"StillDemanded,3,true\n")
                await writer.drain()

                async with client.reads.stream() as lines:
                    assert await asyncio.wait_for(anext(lines), timeout=2.0) == (
                        "StillDemanded,3,true"
                    )

        assert await asyncio.wait_for(reader.read(), timeout=2.0) == b""
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_pending_connection_is_accepted_when_flatten_capacity_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capacity = 2
    async with tcp_report_server() as server:
        monkeypatch.setattr(
            network,
            "from_network",
            _reports_from(server.clients, capacity=capacity),
        )

        async with network.from_network.stream() as reports:
            first_report = asyncio.create_task(pull(reports))
            active_peers = [await asyncio.open_connection("127.0.0.1", server.port)]
            active_peers[0][1].write(b"Active0,1,true\n")
            await active_peers[0][1].drain()
            received = [await asyncio.wait_for(first_report, timeout=2.0)]

            if capacity > 1:
                second_report = asyncio.create_task(pull(reports))
                active_peers.append(
                    await asyncio.open_connection("127.0.0.1", server.port)
                )
                active_peers[1][1].write(b"Active1,1,true\n")
                await active_peers[1][1].drain()
                received.append(await asyncio.wait_for(second_report, timeout=2.0))

            pending_reader, pending_writer = await asyncio.open_connection(
                "127.0.0.1", server.port
            )
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.05):
                    await pending_reader.read(1)

            active_peers[0][1].close()
            await active_peers[0][1].wait_closed()

            pending_writer.write(b"Replacement,2,false\n")
            await pending_writer.drain()

            assert await asyncio.wait_for(pull(reports), timeout=2.0) == VacationReport(
                "Replacement", 2, False
            )

        for _, writer in active_peers:
            writer.close()
        pending_writer.close()
        await asyncio.gather(
            *(writer.wait_closed() for _, writer in active_peers),
            pending_writer.wait_closed(),
        )
        assert await asyncio.wait_for(pending_reader.read(), timeout=2.0) == b""

        assert received == [
            VacationReport("Active0", 1, True),
            VacationReport("Active1", 1, True),
        ]


@pytest.mark.asyncio
async def test_tcp_clients_closes_server_when_subscription_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    exited = asyncio.Event()

    async def waiting_clients() -> AsyncIterator[Client]:
        await asyncio.Event().wait()
        yield FakeClient(Stream.empty())

    @asynccontextmanager
    async def fake_server(*, host: str, port: int) -> AsyncGenerator[TCPReportServer]:
        assert host == "example.test"
        assert port == 4321
        entered.set()
        try:
            yield FakeTCPReportServer(Stream._from_factory(waiting_clients))
        finally:
            exited.set()

    monkeypatch.setattr(tcp_streams, "tcp_report_server", fake_server)

    async def consume() -> None:
        async with tcp_clients("example.test", 4321).stream() as clients:
            await anext(clients)

    task = asyncio.create_task(consume())
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    assert entered.is_set() is True
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert exited.is_set() is True
