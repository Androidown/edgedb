#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

import asyncio
import os
import socket
import struct
import typing


_uint64_unpacker = struct.Struct('!Q').unpack
_uint64_packer = struct.Struct('!Q').pack


class MessageStream:
    """Data stream that yields messages."""

    def __init__(self):
        self._buffer = b''
        self._curmsg_len = -1

    def feed_data(self, data):
        # TODO: rewrite to avoid buffer copies.
        self._buffer += data
        while self._buffer:
            if self._curmsg_len == -1:
                if len(self._buffer) >= 8:
                    self._curmsg_len = _uint64_unpacker(self._buffer[:8])[0]
                    self._buffer = self._buffer[8:]
                else:
                    return

            if 0 < self._curmsg_len <= len(self._buffer):
                msg = self._buffer[:self._curmsg_len]
                self._buffer = self._buffer[self._curmsg_len:]
                self._curmsg_len = -1
                yield msg
            else:
                return


class HubProtocol(asyncio.BufferedProtocol):
    """The Protocol used on the hub side connecting to workers."""

    def __init__(self, *, loop, on_pid, on_connection_lost):
        self._loop = loop
        self._transport = None
        self._closed = False
        self._resp_waiters = {}
        self._on_pid = on_pid
        self._on_connection_lost = on_connection_lost
        self._pid = None

        self._new_buffer(16)

    def connection_made(self, tr):
        self._transport = tr

    def send(self, req_id: int, waiter: asyncio.Future, payload: bytes):
        if req_id in self._resp_waiters:
            raise RuntimeError('FramedProtocol: duplicate request ID')
        self._resp_waiters[req_id] = waiter
        self._transport.writelines(
            (_uint64_packer(len(payload) + 8), _uint64_packer(req_id), payload)
        )

    def process_message(self, msgview):
        req_id = _uint64_unpacker(msgview[:8])[0]
        waiter = self._resp_waiters.pop(req_id, None)
        if waiter is None:
            # This could have happened if the previous request got cancelled.
            return
        if not waiter.done():
            waiter.set_result(msgview[8:])

    def _new_buffer(
        self,
        size: int,
        is_payload: bool = False,
    ):
        self._buf_idx = 0
        self._buf = memoryview(bytearray(size))
        self._is_payload = is_payload
        self._expect_len = size

    def get_buffer(self, sizehint: int):
        return self._buf[self._buf_idx:]

    def buffer_updated(self, nbytes: int) -> None:
        self._buf_idx += nbytes

        if self._buf_idx != self._expect_len:
            return

        if self._pid is None:
            self._pid = _uint64_unpacker(self._buf[:8])[0]
            version = _uint64_unpacker(self._buf[8:16])[0]
            self._on_pid(self, self._transport, self._pid, version)
            self._new_buffer(8)
        elif self._is_payload:
            self.process_message(self._buf)
            self._new_buffer(8)
        else:
            self._new_buffer(
                _uint64_unpacker(self._buf)[0],
                is_payload=True
            )

    def connection_lost(self, exc):
        self._closed = True

        if self._resp_waiters:
            if exc is not None:
                for waiter in self._resp_waiters.values():
                    waiter.set_exception(exc)
            else:
                for waiter in self._resp_waiters.values():
                    waiter.set_exception(ConnectionError(
                        'lost connection to the worker during a call'))
            self._resp_waiters = {}

        self._on_connection_lost(self._pid)


class HubConnection:
    """An abstraction of the hub connections to the workers."""

    def __init__(self, transport, protocol, loop, version):
        self._transport = transport
        self._protocol = protocol
        self._loop = loop
        self._req_id_cnt = 0
        self._version = version
        self._aborted = False

    def is_closed(self):
        return self._protocol._closed

    async def request(self, data: bytes) -> bytes:
        self._req_id_cnt += 1
        req_id = self._req_id_cnt

        waiter = self._loop.create_future()
        self._protocol.send(req_id, waiter, data)
        return await waiter

    def abort(self):
        self._aborted = True
        self._transport.abort()


class WorkerProtocol(asyncio.BufferedProtocol):
    def __init__(self, on_message):
        self._transport = None
        self._new_buffer(8)
        self._on_msg = on_message
        self._closed = False

    def connection_made(self, tr):
        self._transport = tr

    def _new_buffer(
        self,
        size: int,
        is_payload: bool = False,
    ):
        self._buf_idx = 0
        self._buf = memoryview(bytearray(size))
        self._is_payload = is_payload
        self._expect_len = size

    def get_buffer(self, sizehint: int):
        return self._buf[self._buf_idx:]

    def buffer_updated(self, nbytes: int) -> None:
        self._buf_idx += nbytes

        if self._buf_idx != self._expect_len:
            return

        if self._is_payload:
            self._on_msg(self._buf)
            self._new_buffer(8)
        else:
            self._new_buffer(
                _uint64_unpacker(self._buf)[0],
                is_payload=True
            )

    def connection_lost(self, error):
        self._closed = True


class WorkerConnection:
    def __init__(self, sockname, version, *, loop):
        self.sockname = sockname
        self.version = version

        self._loop = loop
        self.transport = None
        self.proto = None

        self._pending_msg = asyncio.Queue()

    def on_message(self, msgview: memoryview):
        req_id = _uint64_unpacker(msgview[:8])[0]
        self._pending_msg.put_nowait((req_id, msgview[8:]))

    async def connect(self):
        self.transport, self.proto = await \
            self._loop.create_unix_connection(
                lambda: WorkerProtocol(self.on_message),
                self.sockname
            )

        self.transport.writelines((
            _uint64_packer(os.getpid()),
            _uint64_packer(self.version)
        ))

    async def iter_request(self):
        while True:
            yield await self._pending_msg.get()

    def abort(self):
        if self.transport is not None:
            self.transport.abort()
        self.transport = None
        self.proto = None

    def reply(self, req_id, payload):
        self.transport.writelines((
            _uint64_packer(len(payload) + 8),
            _uint64_packer(req_id),
            payload,
        ))


class ServerProtocol:
    def worker_connected(self, pid, version):
        pass

    def worker_disconnected(self, pid):
        pass


class Server:

    _proto: ServerProtocol
    _pids: typing.Dict[int, HubConnection]

    def __init__(self, sockname, loop, server_protocol):
        self._sockname = sockname
        self._loop = loop
        self._srv = None
        self._pids = {}
        self._proto = server_protocol

    def _on_pid_connected(self, proto, tr, pid, version):
        assert pid not in self._pids
        self._pids[pid] = HubConnection(tr, proto, self._loop, version)
        self._proto.worker_connected(pid, version)

    def _on_pid_disconnected(self, pid: typing.Optional[int]):
        if not pid:
            return
        if pid in self._pids:
            self._pids.pop(pid)
            self._proto.worker_disconnected(pid)

    def _proto_factory(self):
        return HubProtocol(
            loop=self._loop,
            on_pid=self._on_pid_connected,
            on_connection_lost=self._on_pid_disconnected,
        )

    def get_by_pid(self, pid):
        return self._pids[pid]

    async def start(self):
        self._srv = await self._loop.create_unix_server(
            self._proto_factory,
            path=self._sockname)

    async def stop(self):
        self._srv.close()
        await self._srv.wait_closed()
        for con in self._pids.values():
            con.abort()

    def kill_outdated_worker(self, current_version):
        for conn in self._pids.values():
            if conn._version < current_version and not conn._aborted:
                conn.abort()
                break
