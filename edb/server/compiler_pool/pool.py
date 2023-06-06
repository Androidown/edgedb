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

import uuid
from typing import *  # NoQA

import asyncio
import functools
import hmac
import logging
import os
import os.path
import pickle
import signal
import subprocess
import sys
import time

import immutables

from edb.common import debug
from edb.common import taskgroup
from edb.common import util
from edb import errors

from edb.pgsql import params as pgparams

from edb.server import args as srvargs
from edb.server import defines
from edb.server import metrics
from edb.schema import schema as s_schema

from . import amsg
from . import queue
from . import state


PROCESS_INITIAL_RESPONSE_TIMEOUT: float = 60.0
KILL_TIMEOUT: float = 10.0
ADAPTIVE_SCALE_UP_WAIT_TIME: float = 3.0
ADAPTIVE_SCALE_DOWN_WAIT_TIME: float = 60.0
WORKER_PKG: str = __name__.rpartition('.')[0] + '.'
UNKNOW_VER_ID = uuid.UUID('ffffffff-ffff-ffff-eeee-eeeeeeeeeeee')

logger = logging.getLogger("edb.server")
log_metrics = logging.getLogger("edb.server.metrics")


# Inherit sys.path so that import system can find worker class
# in unittests.
_ENV = os.environ.copy()
_ENV['PYTHONPATH'] = ':'.join(sys.path)


@util.simple_lru(weakref_key='schema', weakref_pos=0)
def _pickle_memoized(schema):
    with util.disable_gc():
        return pickle.dumps(schema, -1)


def _trim_uuid(uid: uuid.UUID):
    return hex(uid.int & 0xFFFFFFFF)[2:]


class _SchemaMutation(NamedTuple):
    base: uuid.UUID
    target: uuid.UUID
    bytes: bytes
    obj: s_schema.SchemaMutationLogger

    def __repr__(self):
        return f"<MUT {_trim_uuid(self.base)} -> {_trim_uuid(self.target)}>"


class MutationHistory:
    def __init__(self, dbname: str):
        self._history: List[_SchemaMutation] = []
        self._index: Dict[uuid.UUID, int] = {}
        self._cursor: Dict[uuid.UUID, int] = {}
        self._db = dbname

    @property
    def latest_ver(self):
        if not self._history:
            return
        return self._history[-1].base

    def clear(self):
        self._history.clear()
        self._index.clear()
        self._cursor.clear()

    def get_pickled_mutation(self, worker: BaseWorker) -> Optional[bytes]:
        start = self._cursor.get(worker.get_user_schema_id(self._db))
        if start is None:
            return

        if start == len(self._history) - 1:
            mut_bytes = self._history[start].bytes
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"::CPOOL:: WOKER<{worker.identifier}> | DB<{self._db}> - "
                    f"Using stored {self._history[start]} to update."
                )
        else:
            mut = s_schema.SchemaMutationLogger.merge([m.obj for m in self._history[start:]])
            logger.info(
                f"::CPOOL:: WOKER<{worker.identifier}> | DB<{self._db}> - "
                f"Using merged <MUT {_trim_uuid(mut.id)} -> {_trim_uuid(mut.target)}> to update."
            )
            mut_bytes = pickle.dumps(mut)
        return mut_bytes

    def append(self, mut: _SchemaMutation):
        self._history.append(mut)
        self._index[mut.target] = len(self._history)
        self._cursor[mut.base] = len(self._history) - 1

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Mutation history appended to {self}")

    def try_trim_history(self, schema_ids: Iterable[uuid.UUID]):
        start = min(self._index.get(usid, 0) for usid in schema_ids)
        for _ in range(start):
            mut = self._history.pop(0)
            self._index.pop(mut.target, None)
            self._cursor.pop(mut.base, None)

        if start > 0:
            for k in self._index:
                self._index[k] -= start

            for k in self._cursor:
                self._cursor[k] -= start

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Mutation history trimmed to {self}")

    def __repr__(self):
        return f"<cursor: {self._cursor} | index: {self._index} | history: {self._history}>"

    def __len__(self):
        return len(self._history)


class BaseWorker:

    _dbs: state.DatabasesState

    def __init__(
        self,
        dbs: state.DatabasesState,
        backend_runtime_params: pgparams.BackendRuntimeParams,
        std_schema,
        refl_schema,
        schema_class_layout,
        global_schema,
        system_config,
    ):
        self._dbs = dbs
        self._backend_runtime_params = backend_runtime_params
        self._std_schema = std_schema
        self._refl_schema = refl_schema
        self._schema_class_layout = schema_class_layout
        self._global_schema = global_schema
        self._system_config = system_config
        self._last_pickled_state = None

        self._con = None
        self._last_used = time.monotonic()
        self._closed = False

    def get_user_schema_id(self, dbname: str) -> uuid.UUID:
        if dbname not in self._dbs:
            return UNKNOW_VER_ID

        return self._dbs[dbname].user_schema_version

    @functools.cached_property
    def identifier(self):
        return id(self)

    async def call(self, method_name, *args, sync_state=None):
        assert not self._closed

        if self._con.is_closed():
            raise RuntimeError(
                'the connection to the compiler worker process is '
                'unexpectedly closed')

        data = await self._request(method_name, args)

        status, *data = pickle.loads(data)

        self._last_used = time.monotonic()

        if status == 0:
            if sync_state is not None:
                sync_state()
            return data[0]
        elif status == 1:
            exc, tb = data
            if (sync_state is not None and
                    not isinstance(exc, state.FailedStateSync)):
                sync_state()
            exc.__formatted_error__ = tb
            raise exc
        else:
            exc = RuntimeError(
                'could not serialize result in worker subprocess')
            exc.__formatted_error__ = data[0]
            raise exc

    async def _request(self, method_name, args):
        msg = pickle.dumps((method_name, args))
        return await self._con.request(msg)


class Worker(BaseWorker):
    def __init__(self, manager, server, pid, *args):
        super().__init__(*args)

        self._pid = pid
        self._last_pickled_state = None
        self._manager = manager
        self._server = server

    async def _attach(self, init_args_pickled: bytes):
        self._manager._stats_spawned += 1

        self._con = self._server.get_by_pid(self._pid)

        await self.call(
            '__init_worker__',
            init_args_pickled,
        )

    @functools.cached_property
    def identifier(self):
        return self._pid

    def get_pid(self):
        return self._pid

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._manager._stats_killed += 1
        self._manager._workers.pop(self._pid, None)
        self._manager._report_worker(self, action="kill")
        try:
            os.kill(self._pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


class AbstractPool:
    def __init__(
        self,
        *,
        loop,
        dbindex,
        backend_runtime_params: pgparams.BackendRuntimeParams,
        std_schema,
        refl_schema,
        schema_class_layout,
        **kwargs,
    ):
        self._loop = loop
        self._dbindex = dbindex

        self._backend_runtime_params = backend_runtime_params
        self._std_schema = std_schema
        self._refl_schema = refl_schema
        self._schema_class_layout = schema_class_layout
        self._mut_history: Dict[str, MutationHistory] = {}

    @functools.lru_cache(maxsize=None)
    def _get_init_args(self):
        init_args = self._get_init_args_uncached()
        pickled_args = self._get_pickled_init_args(init_args)
        return init_args, pickled_args

    def _get_init_args_uncached(self):
        dbs: state.DatabasesState = immutables.Map()
        for db in self._dbindex.iter_dbs():
            db_user_schema = db.user_schema
            version_id = UNKNOW_VER_ID if db_user_schema is None else db_user_schema.version_id
            dbs = dbs.set(
                db.name,
                state.DatabaseState(
                    name=db.name,
                    user_schema=db_user_schema,
                    user_schema_version=version_id,
                    reflection_cache=db.reflection_cache,
                    database_config=db.db_config,
                )
            )

        init_args = (
            dbs,
            self._backend_runtime_params,
            self._std_schema,
            self._refl_schema,
            self._schema_class_layout,
            self._dbindex.get_global_schema(),
            self._dbindex.get_compilation_system_config(),
        )
        return init_args

    def _get_pickled_init_args(self, init_args):
        pickled_args = pickle.dumps(init_args, -1)
        return pickled_args

    async def start(self):
        raise NotImplementedError

    async def stop(self):
        raise NotImplementedError

    def collect_worker_schema_ids(self, dbname) -> List[uuid.UUID]:
        raise NotImplementedError

    def get_template_pid(self):
        return None

    async def sync_user_schema(
        self,
        dbname,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    ):
        worker = await self._acquire_worker()
        await asyncio.sleep(0)

        try:
            preargs, sync_state = await self._compute_compile_preargs(
                worker,
                dbname,
                user_schema,
                global_schema,
                reflection_cache,
                database_config,
                system_config,
            )

            if preargs[2] is not None:
                logger.debug(f"[W::{worker.identifier}] Sync user schema.")
            else:
                if worker.get_user_schema_id(dbname) is not UNKNOW_VER_ID:
                    logger.warning(f"[W::{worker.identifier}] Attempt to sync user schema failed.")
                logger.info(f"[W::{worker.identifier}] Initialize user schema.")

            await worker.call('__sync__', *preargs, False, sync_state=sync_state)

        finally:
            self._release_worker(worker)

    async def _compute_compile_preargs(
        self,
        worker,
        dbname,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
    ):

        def sync_worker_state_cb(
            *,
            worker,
            dbname,
            user_schema=None,
            global_schema=None,
            reflection_cache=None,
            database_config=None,
            system_config=None,
        ):
            worker_db = worker._dbs.get(dbname)
            if worker_db is None:
                assert user_schema is not None
                assert reflection_cache is not None
                assert global_schema is not None
                assert database_config is not None
                assert system_config is not None

                worker._dbs = worker._dbs.set(dbname, state.DatabaseState(
                    name=dbname,
                    user_schema=user_schema,
                    user_schema_version=user_schema.version_id,
                    reflection_cache=reflection_cache,
                    database_config=database_config,
                ))
                worker._global_schema = global_schema
                worker._system_config = system_config
            else:
                if (
                    user_schema is not None
                    or reflection_cache is not None
                    or database_config is not None
                ):
                    new_user_schema = user_schema or worker_db.user_schema
                    worker._dbs = worker._dbs.set(dbname, state.DatabaseState(
                        name=dbname,
                        user_schema=new_user_schema,
                        user_schema_version=new_user_schema.version_id,
                        reflection_cache=(
                            reflection_cache or worker_db.reflection_cache),
                        database_config=(
                            database_config if database_config is not None
                            else worker_db.database_config),
                    ))

                if global_schema is not None:
                    worker._global_schema = global_schema
                if system_config is not None:
                    worker._system_config = system_config

        worker_db: state.DatabaseState = worker._dbs.get(dbname)
        preargs = (dbname,)
        to_update = {}

        if worker_db is None:
            preargs += (
                _pickle_memoized(user_schema),
                None,
                _pickle_memoized(reflection_cache),
                _pickle_memoized(global_schema),
                _pickle_memoized(database_config),
                _pickle_memoized(system_config),
            )
            to_update = {
                'user_schema': user_schema,
                'reflection_cache': reflection_cache,
                'global_schema': global_schema,
                'database_config': database_config,
                'system_config': system_config,
            }
        else:
            if worker_db.user_schema_version != user_schema.version_id:
                if worker_db.user_schema_version is UNKNOW_VER_ID:
                    preargs += (_pickle_memoized(user_schema), None)
                    logger.info(
                        f"::CPOOL:: WOKER<{worker.identifier}> | DB<{dbname}> - "
                        f"Initialize db <{dbname}> schema version to: [{user_schema.version_id}]"
                    )
                else:
                    if dbname not in self._mut_history:
                        # 当前实例初始化后未执行任何ddl，此时在其他实例发生DDL，
                        # 触发当前实例的introspect_db，导致worker的schema版本失效，
                        # 这种情况下，当前实例_mut_history可能不包含dbname
                        mutation_pickled = None
                    else:
                        mutation_pickled = self._mut_history[dbname].get_pickled_mutation(worker)
                    if mutation_pickled is None:
                        logger.warning(
                            f"::CPOOL:: WOKER<{worker.identifier}> | DB<{dbname}> - "
                            f"No schema mutation available. "
                            f"Schema <{worker_db.user_schema_version}> is outdated, will issue a full update."
                        )
                        preargs += (_pickle_memoized(user_schema), None)
                    else:
                        preargs += (None, mutation_pickled)
                to_update['user_schema'] = user_schema
            else:
                preargs += (None, None)

            if worker_db.reflection_cache is not reflection_cache:
                preargs += (
                    _pickle_memoized(reflection_cache),
                )
                to_update['reflection_cache'] = reflection_cache
            else:
                preargs += (None,)

            if worker._global_schema is not global_schema:
                preargs += (
                    _pickle_memoized(global_schema),
                )
                to_update['global_schema'] = global_schema
            else:
                preargs += (None,)

            if worker_db.database_config is not database_config:
                preargs += (
                    _pickle_memoized(database_config),
                )
                to_update['database_config'] = database_config
            else:
                preargs += (None,)

            if worker._system_config is not system_config:
                preargs += (
                    _pickle_memoized(system_config),
                )
                to_update['system_config'] = system_config
            else:
                preargs += (None,)

        if to_update:
            callback = functools.partial(
                sync_worker_state_cb,
                worker=worker,
                dbname=dbname,
                **to_update
            )
        else:
            callback = None

        return preargs, callback

    async def _acquire_worker(self, *, condition=None, weighter=None):
        raise NotImplementedError

    def _release_worker(self, worker, *, put_in_front: bool = True):
        raise NotImplementedError

    def append_schema_mutation(
        self,
        dbname,
        mut_bytes,
        mutation: s_schema.SchemaMutationLogger,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
    ):
        if is_fresh := (dbname not in self._mut_history):
            self._mut_history[dbname] = MutationHistory(dbname)

        hist = self._mut_history[dbname]
        hist.append(_SchemaMutation(
            base=mutation.id,
            target=user_schema.version_id,
            bytes=mut_bytes,
            obj=mutation
        ))

        if not is_fresh:
            usids = self.collect_worker_schema_ids(dbname)
            hist.try_trim_history(usids)

            if (
                len(hist) > defines.MAX_RESERVED_MUTATION_HISTORY
                and (n := len(usids)) > 0
            ):
                logger.debug(f"Schedule {n} tasks to sync worker's user schema.")
                for _ in range(n):
                    asyncio.create_task(self.sync_user_schema(
                        dbname,
                        user_schema,
                        reflection_cache,
                        global_schema,
                        database_config,
                        system_config,
                    ))

    async def compile(
        self,
        dbname,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
        *compile_args
    ):
        worker = await self._acquire_worker()
        try:
            preargs, sync_state = await self._compute_compile_preargs(
                worker,
                dbname,
                user_schema,
                global_schema,
                reflection_cache,
                database_config,
                system_config,
            )

            result = await worker.call(
                'compile',
                *preargs,
                *compile_args,
                sync_state=sync_state
            )
            worker._last_pickled_state = result[1]
            if len(result) == 2:
                return *result, 0
            else:
                return result

        finally:
            self._release_worker(worker)

    async def compile_in_tx(
        self,
        dbname,
        txid,
        pickled_state,
        state_id,
        base_user_schema: s_schema.FlatSchema,
        *compile_args
    ):
        # When we compile a query, the compiler returns a tuple:
        # a QueryUnit and the state the compiler is in if it's in a
        # transaction.  The state contains the information about all savepoints
        # and transient schema changes, so the next time we need to
        # compile a new query in this transaction the state is needed
        # to be passed to the next compiler compiling it.
        #
        # The compile state can be quite heavy and contain multiple versions
        # of schema, configs, and other session-related data. So the compiler
        # worker pickles it before sending it to the IO process, and the
        # IO process doesn't need to ever unpickle it.
        #
        # There's one crucial optimization we do here though. We try to
        # find the compiler process that we used before, that already has
        # this state unpickled. If we can find it, it means that the
        # compiler process won't have to waste time unpickling the state.
        #
        # We use "is" in `w._last_pickled_state is pickled_state` deliberately,
        # because `pickled_state` is saved on the Worker instance and
        # stored in edgecon; we never modify it, so `is` is sufficient and
        # is faster than `==`.
        worker = await self._acquire_worker(
            condition=lambda w: (w._last_pickled_state is pickled_state)
        )

        if worker._last_pickled_state is pickled_state:
            # Since we know that this particular worker already has the
            # state, we don't want to waste resources transferring the
            # state over the network. So we replace the state with a marker,
            # that the compiler process will recognize.
            pickled_state = state.REUSE_LAST_STATE_MARKER
            user_schema = None
        else:
            usid = worker.get_user_schema_id(dbname)
            if state_id == 0:
                if base_user_schema.version_id != usid:
                    user_schema = _pickle_memoized(base_user_schema)
                else:
                    user_schema = None
            else:
                if base_user_schema.version_id != state_id:
                    raise errors.TransactionError(
                        'Transaction aborted. '
                        f'Base schema version: {base_user_schema.version_id} '
                        f'is not consistent with current version: {state_id}, '
                        f'which indicates another DDL might have been '
                        f'executed outside this transaction.')
                elif state_id != usid:
                    user_schema = _pickle_memoized(base_user_schema)
                else:
                    user_schema = None

        try:
            units, new_pickled_state, new_state_id = await worker.call(
                'compile_in_tx',
                pickled_state,
                dbname,
                user_schema,
                txid,
                *compile_args
            )
            worker._last_pickled_state = new_pickled_state
            return units, new_pickled_state, new_state_id

        finally:
            # Put the worker at the end of the queue so that the chance
            # of reusing it later (and maximising the chance of
            # the w._last_pickled_state is pickled_state` check returning
            # `True` is higher.
            self._release_worker(worker, put_in_front=False)

    async def compile_notebook(
        self,
        dbname,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
        *compile_args
    ):
        worker = await self._acquire_worker()
        try:
            preargs, sync_state = await self._compute_compile_preargs(
                worker,
                dbname,
                user_schema,
                global_schema,
                reflection_cache,
                database_config,
                system_config,
            )

            return await worker.call(
                'compile_notebook',
                *preargs,
                *compile_args,
                sync_state=sync_state
            )

        finally:
            self._release_worker(worker)

    async def try_compile_rollback(
        self,
        *compile_args,
        **compile_kwargs,
    ):
        worker = await self._acquire_worker()
        try:
            return await worker.call(
                'try_compile_rollback',
                *compile_args,
                **compile_kwargs,
            )
        finally:
            self._release_worker(worker)

    async def compile_graphql(
        self,
        dbname,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
        *compile_args
    ):
        worker = await self._acquire_worker()
        try:
            preargs, sync_state = await self._compute_compile_preargs(
                worker,
                dbname,
                user_schema,
                global_schema,
                reflection_cache,
                database_config,
                system_config,
            )

            return await worker.call(
                'compile_graphql',
                *preargs,
                *compile_args,
                sync_state=sync_state
            )

        finally:
            self._release_worker(worker)

    async def infer_expr(
        self,
        dbname,
        user_schema,
        global_schema,
        reflection_cache,
        database_config,
        system_config,
        *compile_args
    ):
        worker = await self._acquire_worker()
        try:
            preargs, sync_state = await self._compute_compile_preargs(
                worker,
                dbname,
                user_schema,
                global_schema,
                reflection_cache,
                database_config,
                system_config,
            )

            return await worker.call(
                'infer_expr',
                *preargs,
                *compile_args,
                sync_state=sync_state
            )

        finally:
            self._release_worker(worker)

    async def describe_database_dump(
        self,
        *args,
        **kwargs
    ):
        worker = await self._acquire_worker()
        try:
            return await worker.call(
                'describe_database_dump',
                *args,
                **kwargs
            )

        finally:
            self._release_worker(worker)

    async def describe_database_restore(
        self,
        *args,
        **kwargs
    ):
        worker = await self._acquire_worker()
        try:
            return await worker.call(
                'describe_database_restore',
                *args,
                **kwargs
            )

        finally:
            self._release_worker(worker)


class BaseLocalPool(
    AbstractPool, amsg.ServerProtocol, asyncio.SubprocessProtocol
):

    _worker_class = Worker
    _worker_mod = "worker"
    _workers_queue: queue.WorkerQueue[Worker]
    _workers: Dict[int, Worker]

    def __init__(
        self,
        *,
        runstate_dir,
        pool_size,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._runstate_dir = runstate_dir

        self._poolsock_name = os.path.join(self._runstate_dir, 'ipc')
        assert len(self._poolsock_name) <= (
            defines.MAX_RUNSTATE_DIR_PATH
            + defines.MAX_UNIX_SOCKET_PATH_LENGTH
            + 1
        ), "pool IPC socket length exceeds maximum allowed"

        assert pool_size >= 1
        self._pool_size = pool_size
        self._workers = {}

        self._server = amsg.Server(self._poolsock_name, self._loop, self)
        self._ready_evt = asyncio.Event()

        self._running = None

        self._stats_spawned = 0
        self._stats_killed = 0
        self._worker_locks = {}

    def is_running(self):
        return bool(self._running)

    async def _attach_worker(self, pid: int):
        if not self._running:
            return
        logger.debug("Sending init args to worker with PID %s.", pid)
        init_args, init_args_pickled = self._get_init_args()
        worker = self._worker_class(  # type: ignore
            self,
            self._server,
            pid,
            *init_args,
        )
        await worker._attach(init_args_pickled)
        self._report_worker(worker)

        self._workers[pid] = worker
        self._worker_locks[pid] = asyncio.Lock()
        self._workers_queue.release(worker)
        self._worker_attached()

        logger.debug("started compiler worker process (PID %s)", pid)
        if (
            not self._ready_evt.is_set()
            and len(self._workers) == self._pool_size
        ):
            logger.info(
                f"started {self._pool_size} compiler worker "
                f"process{'es' if self._pool_size > 1 else ''}",
            )
            self._ready_evt.set()

        return worker

    def _worker_attached(self):
        pass

    def worker_connected(self, pid, version):
        logger.debug("Worker with PID %s connected.", pid)
        self._loop.create_task(self._attach_worker(pid))
        metrics.compiler_process_spawns.inc()
        metrics.current_compiler_processes.inc()

    def worker_disconnected(self, pid):
        logger.debug("Worker with PID %s disconnected.", pid)
        self._workers.pop(pid, None)
        self._worker_locks.pop(pid, None)
        metrics.current_compiler_processes.dec()

    async def start(self):
        if self._running is not None:
            raise RuntimeError(
                'the compiler pool has already been started once')

        self._workers_queue = queue.WorkerQueue(self._loop)

        await self._server.start()
        self._running = True

        await self._start()

        await self._wait_ready()

    async def _wait_ready(self):
        await asyncio.wait_for(
            self._ready_evt.wait(),
            PROCESS_INITIAL_RESPONSE_TIMEOUT
        )

    async def _create_compiler_process(self, numproc=None, version=0):
        # Create a new compiler process. When numproc is None, a single
        # standalone compiler worker process is started; if numproc is an int,
        # a compiler template process will be created, which will then fork
        # itself into `numproc` actual worker processes and run as a supervisor

        env = _ENV
        if debug.flags.server:
            env = {'EDGEDB_DEBUG_SERVER': '1', **_ENV}

        cmdline = [sys.executable]
        if sys.flags.isolated:
            cmdline.append('-I')

        cmdline.extend([
            '-m', WORKER_PKG + self._worker_mod,
            '--sockname', self._poolsock_name,
            '--version-serial', str(version),
        ])
        if numproc:
            cmdline.extend([
                '--numproc', str(numproc),
            ])

        transport, _ = await self._loop.subprocess_exec(
            lambda: self,
            *cmdline,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=None,
            stderr=None,
        )
        return transport

    async def _start(self):
        raise NotImplementedError

    async def stop(self):
        if not self._running:
            return
        self._running = False

        await self._server.stop()
        self._server = None

        self._workers_queue = queue.WorkerQueue(self._loop)
        self._workers.clear()

        await self._stop()

    async def _stop(self):
        raise NotImplementedError

    def _report_worker(self, worker: Worker, *, action: str = "spawn"):
        action = action.capitalize()
        if not action.endswith("e"):
            action += "e"
        action += "d"
        log_metrics.info(
            "%s a compiler worker with PID %d; pool=%d;"
            + " spawned=%d; killed=%d",
            action,
            worker.get_pid(),
            len(self._workers),
            self._stats_spawned,
            self._stats_killed,
        )

    async def _acquire_worker(self, *, condition=None, weighter=None):
        while (
            worker := await self._workers_queue.acquire(
                condition=condition, weighter=weighter
            )
        ).get_pid() not in self._workers:
            # The worker was disconnected; skip to the next one.
            pass
        return worker

    def _release_worker(self, worker, *, put_in_front: bool = True):
        # Skip disconnected workers
        if worker.get_pid() in self._workers:
            self._workers_queue.release(worker, put_in_front=put_in_front)

    def collect_worker_schema_ids(self, dbname) -> Iterable[uuid.UUID]:
        return [w.get_user_schema_id(dbname) for w in self._workers.values()]


@srvargs.CompilerPoolMode.Fixed.assign_implementation
class FixedPool(BaseLocalPool):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._template_transport = None
        self._template_proc_scheduled = False
        self._template_proc_version = 0

    def _worker_attached(self):
        if len(self._workers) > self._pool_size:
            self._server.kill_outdated_worker(self._template_proc_version)

    def worker_connected(self, pid, version):
        if version < self._template_proc_version:
            logger.debug(
                "Outdated worker with PID %s connected; discard now.", pid
            )
            self._server.get_by_pid(pid).abort()
            metrics.compiler_process_spawns.inc()
        else:
            super().worker_connected(pid, version)

    def process_exited(self):
        # Template process exited
        self._template_transport = None
        if self._running:
            logger.error("Template compiler process exited; recreating now.")
            self._schedule_template_proc(0)

    def get_template_pid(self):
        if self._template_transport is None:
            return None
        else:
            return self._template_transport.get_pid()

    async def _start(self):
        await self._create_template_proc(retry=False)

    async def _create_template_proc(self, retry=True):
        self._template_proc_scheduled = False
        if not self._running:
            return
        self._template_proc_version += 1
        try:
            # Create the template process, which will then fork() into numproc
            # child processes and manage them, so that we don't have to manage
            # the actual compiler worker processes in the main process.
            self._template_transport = await self._create_compiler_process(
                numproc=self._pool_size,
                version=self._template_proc_version,
            )
        except Exception:
            if retry:
                if self._running:
                    t = defines.BACKEND_COMPILER_TEMPLATE_PROC_RESTART_INTERVAL
                    logger.exception(
                        f"Unexpected error occurred creating template compiler"
                        f" process; retry in {t} second{'s' if t > 1 else ''}."
                    )
                    self._schedule_template_proc(t)
            else:
                raise

    def _schedule_template_proc(self, sleep):
        if self._template_proc_scheduled:
            return
        self._template_proc_scheduled = True
        self._loop.call_later(
            sleep, self._loop.create_task, self._create_template_proc()
        )

    async def _stop(self):
        trans, self._template_transport = self._template_transport, None
        if trans is not None:
            trans.terminate()
            await trans._wait()


class DebugWorker:
    _dbs: state.DatabasesState = None
    _backend_runtime_params: pgparams.BackendRuntimeParams = None
    _std_schema: s_schema.FlatSchema = None
    _global_schema: s_schema.FlatSchema = None
    _refl_schema: s_schema.FlatSchema = None
    _system_config = None
    _schema_class_layout = None
    _last_pickled_state = None
    connected = False

    def get_user_schema_id(self, dbname):
        return BaseWorker.get_user_schema_id(self, dbname)  # noqa

    async def call(self, method_name, *args, sync_state=None):
        from . import worker

        method = getattr(worker, method_name)
        r = method(*args)
        if sync_state is not None:
            sync_state()
        return r

    @functools.cached_property
    def identifier(self):
        return os.getpid()


@srvargs.CompilerPoolMode.Solo.assign_implementation
class SoloPool(BaseLocalPool):
    _worker = DebugWorker()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        from . import worker
        self.cworker = worker

    @functools.lru_cache(maxsize=None)
    def _get_init_args(self):
        dbs: state.DatabasesState = immutables.Map()
        for db in self._dbindex.iter_dbs():
            db_user_schema = db.user_schema
            version_id = UNKNOW_VER_ID if db_user_schema is None else db_user_schema.version_id
            dbs = dbs.set(
                db.name,
                state.DatabaseState(
                    name=db.name,
                    user_schema=db_user_schema,
                    user_schema_version=version_id,
                    reflection_cache=db.reflection_cache,
                    database_config=db.db_config,
                )
            )
        self._worker._dbs = dbs
        self._worker._backend_runtime_params = self._backend_runtime_params
        self._worker._std_schema = self._std_schema
        self._worker._global_schema = self._dbindex.get_global_schema()
        self._worker._refl_schema = self._refl_schema
        self._worker._schema_class_layout = self._schema_class_layout
        self._worker._system_config = self._dbindex.get_compilation_system_config()
        init_args = (
            dbs,
            self._backend_runtime_params,
            self._std_schema,
            self._refl_schema,
            self._schema_class_layout,
            self._dbindex.get_global_schema(),
            self._dbindex.get_compilation_system_config(),
        )
        return pickle.dumps(init_args, -1)

    def worker_connected(self, pid, version):
        self.cworker.__init_worker__(self._get_init_args())
        self._worker.connected = True
        metrics.compiler_process_spawns.inc()
        metrics.current_compiler_processes.inc()
        self._workers[pid] = self._worker
        self._worker_locks[pid] = asyncio.Lock()

    def worker_disconnected(self, pid):
        self._worker.connected = False
        metrics.current_compiler_processes.dec()

    def get_template_pid(self):
        return None

    async def _acquire_worker(self, *, condition=None, **kwargs):
        return self._worker

    def _release_worker(self, worker, **kwargs):
        return

    async def start(self):
        if self._running is not None:
            raise RuntimeError(
                'the compiler pool has already been started once')
        self._running = True
        if not self._worker.connected:
            self.worker_connected(os.getpid(), 'debug')

    async def _start(self):
        pass

    async def _stop(self):
        pass

    async def stop(self):
        if not self._running:
            return
        self._running = False
        if self._worker.connected:
            self.worker_disconnected(os.getpid())

    def collect_worker_schema_ids(self, dbname) -> Iterable[uuid.UUID]:
        return [self._worker.get_user_schema_id(dbname)]


@srvargs.CompilerPoolMode.OnDemand.assign_implementation
class SimpleAdaptivePool(BaseLocalPool):
    def __init__(self, *, pool_size, **kwargs):
        super().__init__(pool_size=1, **kwargs)
        self._worker_transports = {}
        self._expected_num_workers = 0
        self._scale_up_handle = None
        self._scale_down_handle = None
        self._max_num_workers = pool_size

    async def _start(self):
        async with taskgroup.TaskGroup() as g:
            for _i in range(self._pool_size):
                g.create_task(self._create_worker())

    async def _stop(self):
        self._expected_num_workers = 0
        transports, self._worker_transports = self._worker_transports, {}
        for transport in transports.values():
            await transport._wait()

    async def _acquire_worker(self, *, condition=None, weighter=None):
        if (
            self._running and
            self._scale_up_handle is None
            and self._workers_queue.qsize() == 0
            and (
                len(self._workers)
                == self._expected_num_workers
                < self._max_num_workers
            )
        ):
            self._scale_up_handle = self._loop.call_later(
                ADAPTIVE_SCALE_UP_WAIT_TIME,
                self._maybe_scale_up,
                self._workers_queue.count_waiters() + 1,
            )
        if self._scale_down_handle is not None:
            self._scale_down_handle.cancel()
            self._scale_down_handle = None
        return await super()._acquire_worker(
            condition=condition, weighter=weighter
        )

    def _release_worker(self, worker, *, put_in_front: bool = True):
        if self._scale_down_handle is not None:
            self._scale_down_handle.cancel()
            self._scale_down_handle = None
        super()._release_worker(worker, put_in_front=put_in_front)
        if (
            self._running and
            self._workers_queue.count_waiters() == 0 and
            len(self._workers) > self._pool_size
        ):
            self._scale_down_handle = self._loop.call_later(
                ADAPTIVE_SCALE_DOWN_WAIT_TIME,
                self._scale_down,
            )

    def worker_disconnected(self, pid):
        num_workers_before = len(self._workers)
        super().worker_disconnected(pid)
        self._worker_transports.pop(pid, None)
        if not self._running:
            return
        if len(self._workers) < self._pool_size:
            # The auto-scaler will not scale down below the pool_size, so we
            # should restart the unexpectedly-exited worker process.
            logger.warning(
                "Compiler worker process[%d] exited unexpectedly; "
                "start a new one now.", pid
            )
            self._loop.create_task(self._create_worker())
            self._expected_num_workers = len(self._workers)
        elif num_workers_before == self._expected_num_workers:
            # This is likely the case when a worker died unexpectedly, and we
            # don't want to restart the worker because the auto-scaler will
            # start a new one again if necessary.
            self._expected_num_workers = len(self._workers)

    def process_exited(self):
        if self._running:
            for pid, transport in list(self._worker_transports.items()):
                if transport.is_closing():
                    self._worker_transports.pop(pid, None)

    async def _create_worker(self):
        try:
            # Creates a single compiler worker process.
            transport = await self._create_compiler_process()
            self._worker_transports[transport.get_pid()] = transport
            self._expected_num_workers += 1
        finally:
            self._scale_up_handle = None

    def _maybe_scale_up(self, starting_num_waiters):
        if not self._running:
            return
        if self._workers_queue.count_waiters() > starting_num_waiters:
            logger.info(
                "Compile requests are queuing up in the past %d seconds, "
                "spawn a new compiler worker process now.",
                ADAPTIVE_SCALE_UP_WAIT_TIME,
            )
            self._loop.create_task(self._create_worker())
        else:
            self._scale_up_handle = None

    def _scale_down(self):
        self._scale_down_handle = None
        if not self._running or len(self._workers) <= self._pool_size:
            return
        logger.info(
            "The compiler pool is not used in %d seconds, scaling down to %d.",
            ADAPTIVE_SCALE_DOWN_WAIT_TIME, self._pool_size,
        )
        self._expected_num_workers = self._pool_size
        for worker in sorted(
            self._workers.values(), key=lambda w: w._last_used
        )[:-self._pool_size]:
            worker.close()


class RemoteWorker(BaseWorker):
    def __init__(self, con, secret, *args):
        super().__init__(*args)
        self._con = con
        self._secret = secret

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._con.abort()

    async def _request(self, method_name, args):
        msg = pickle.dumps((method_name, args))
        digest = hmac.digest(self._secret, msg, "sha256")
        return await self._con.request(digest + msg)


@srvargs.CompilerPoolMode.Remote.assign_implementation
class RemotePool(AbstractPool):
    def __init__(self, *, address, pool_size, **kwargs):
        super().__init__(**kwargs)
        self._pool_addr = address
        self._worker = None
        self._sync_lock = asyncio.Lock()
        self._semaphore = asyncio.BoundedSemaphore(pool_size)
        secret = os.environ.get("_EDGEDB_SERVER_COMPILER_POOL_SECRET")
        if not secret:
            raise AssertionError(
                "_EDGEDB_SERVER_COMPILER_POOL_SECRET environment variable "
                "is not set"
            )
        self._secret = secret.encode()

    async def start(self, retry=False):
        if self._worker is None:
            self._worker = self._loop.create_future()
        try:
            await self._loop.create_connection(
                lambda: amsg.HubProtocol(
                    loop=self._loop,
                    on_pid=lambda *args: self._loop.create_task(
                        self._connection_made(retry, *args)
                    ),
                    on_connection_lost=self._connection_lost,
                ),
                *self._pool_addr,
            )
        except Exception:
            if not retry:
                raise
            if self._worker is not None:
                self._loop.call_later(1, lambda: self._loop.create_task(
                    self.start(retry=True)
                ))
        else:
            if not retry:
                await self._worker

    async def stop(self):
        if self._worker is not None:
            worker, self._worker = self._worker, None
            if worker.done():
                (await worker).close()

    def _get_pickled_init_args(self, init_args):
        (
            dbs,
            backend_runtime_params,
            std_schema,
            refl_schema,
            schema_class_layout,
            global_schema,
            system_config,
        ) = init_args
        std_args = (std_schema, refl_schema, schema_class_layout)
        client_args = (dbs, backend_runtime_params)
        return (
            pickle.dumps(std_args, -1),
            pickle.dumps(client_args, -1),
            pickle.dumps(global_schema, -1),
            pickle.dumps(system_config, -1),
        )

    async def _connection_made(
        self, retry, protocol, transport, _pid, version
    ):
        if self._worker is None:
            return
        try:
            init_args, init_args_pickled = self._get_init_args()
            worker = RemoteWorker(
                amsg.HubConnection(transport, protocol, self._loop, version),
                self._secret,
                *init_args,
            )
            await worker.call(
                '__init_server__',
                defines.EDGEDB_CATALOG_VERSION,
                init_args_pickled,
            )
        except state.IncompatibleClient as ex:
            transport.abort()
            if self._worker is not None:
                self._worker.set_exception(ex)
                self._worker = None
        except BaseException as ex:
            transport.abort()
            if self._worker is not None:
                if retry:
                    await self.start(retry=True)
                else:
                    self._worker.set_exception(ex)
                    self._worker = None
        else:
            if self._worker is not None:
                self._worker.set_result(worker)

    def _connection_lost(self, _pid):
        if self._worker is not None:
            self._worker = self._loop.create_future()
            self._loop.create_task(self.start(retry=True))

    async def _acquire_worker(self, *, condition=None, cmp=None):
        await self._semaphore.acquire()
        return await self._worker

    def _release_worker(self, worker, *, put_in_front: bool = True):
        if self._sync_lock.locked():
            self._sync_lock.release()
        self._semaphore.release()

    async def compile_in_tx(
        self, txid, pickled_state, state_id, *compile_args
    ):
        worker = await self._acquire_worker()
        try:
            return await worker.call(
                'compile_in_tx',
                state.REUSE_LAST_STATE_MARKER,
                state_id,
                txid,
                *compile_args
            )
        except state.StateNotFound:
            return await worker.call(
                'compile_in_tx',
                pickled_state,
                0,
                txid,
                *compile_args
            )
        finally:
            self._release_worker(worker)

    async def _compute_compile_preargs(self, *args):
        preargs, callback = await super()._compute_compile_preargs(*args)
        if callback:
            del preargs, callback
            await self._sync_lock.acquire()
            preargs, callback = await super()._compute_compile_preargs(*args)
            if not callback:
                self._sync_lock.release()
        return preargs, callback

    def collect_worker_schema_ids(self, dbname) -> Iterable[uuid.UUID]:
        return []


async def create_compiler_pool(
    *,
    runstate_dir: str,
    pool_size: int,
    dbindex,
    backend_runtime_params: pgparams.BackendRuntimeParams,
    std_schema,
    refl_schema,
    schema_class_layout,
    pool_class=FixedPool,
    **kwargs,
) -> AbstractPool:
    assert issubclass(pool_class, AbstractPool)
    loop = asyncio.get_running_loop()
    pool = pool_class(
        loop=loop,
        pool_size=pool_size,
        runstate_dir=runstate_dir,
        backend_runtime_params=backend_runtime_params,
        std_schema=std_schema,
        refl_schema=refl_schema,
        schema_class_layout=schema_class_layout,
        dbindex=dbindex,
        **kwargs,
    )

    await pool.start()
    return pool
