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
from typing import *  # NoQA

import pickle
import os
import sys

import immutables
from loguru import logger

from edb import edgeql, errors
from edb import graphql
from edb.pgsql import params as pgparams
from edb.schema import schema as s_schema
from edb.server import compiler
from edb.server import config
from edb.server import defines
from edb.common import util

from . import state
from . import worker_proc


INITED: bool = False
DBS: state.DatabasesState = immutables.Map()
BACKEND_RUNTIME_PARAMS: pgparams.BackendRuntimeParams = \
    pgparams.get_default_runtime_params()
COMPILER: compiler.Compiler
LAST_STATE: Optional[compiler.dbstate.CompilerConnectionState] = None
STD_SCHEMA: s_schema.FlatSchema
GLOBAL_SCHEMA: s_schema.FlatSchema
INSTANCE_CONFIG: immutables.Map[str, config.SettingValue]
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
LOG_FILE = os.path.join(PROJECT_ROOT, 'edb_compiler.log')

sys.setrecursionlimit(100000)


def __init_worker__(
    init_args_pickled: bytes,
) -> None:
    global INITED
    global DBS
    global BACKEND_RUNTIME_PARAMS
    global COMPILER
    global STD_SCHEMA
    global GLOBAL_SCHEMA
    global INSTANCE_CONFIG

    (
        dbs,
        backend_runtime_params,
        std_schema,
        refl_schema,
        schema_class_layout,
        global_schema,
        system_config,
    ) = pickle.loads(init_args_pickled)

    INITED = True
    DBS = dbs
    BACKEND_RUNTIME_PARAMS = backend_runtime_params
    COMPILER = compiler.Compiler(
        backend_runtime_params=BACKEND_RUNTIME_PARAMS,
    )
    STD_SCHEMA = std_schema
    GLOBAL_SCHEMA = global_schema
    INSTANCE_CONFIG = system_config

    COMPILER.initialize(
        std_schema, refl_schema, schema_class_layout,
    )

    # -----------------------------------------------------------------------------
    # setup loguru logger
    logger.remove()
    logger.configure(
        handlers=[{
            "level": 'INFO',
            "sink": LOG_FILE,
            "format": "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                      "<yellow>{process}</yellow> | "
                      "<level>{level: <8}</level> | "
                      "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>"
                      " - "
                      "<level>{message}</level>",
        }],
    )


def __sync__(
    dbname: str,
    namespace: str,
    user_schema: Optional[bytes],
    user_schema_mutation: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    need_return: bool = True
) -> state.DatabaseState:
    global DBS
    global GLOBAL_SCHEMA
    global INSTANCE_CONFIG

    try:
        ns_db = DBS.get(dbname, {}).get(namespace)
        if ns_db is None:
            assert user_schema is not None
            assert reflection_cache is not None
            assert database_config is not None
            user_schema_unpacked = pickle.loads(user_schema)
            reflection_cache_unpacked = pickle.loads(reflection_cache)
            database_config_unpacked = pickle.loads(database_config)
            ns = DBS.get(dbname, immutables.Map())
            ns_db = state.DatabaseState(
                dbname,
                namespace,
                user_schema_unpacked,
                user_schema_unpacked.version_id,
                reflection_cache_unpacked,
                database_config_unpacked,
            )
            ns.set(namespace, ns_db)
            DBS = DBS.set(dbname, ns)
        else:
            updates = {}

            if user_schema is not None:
                updates['user_schema'] = user_schema_unpacked = pickle.loads(user_schema)
                updates['user_schema_version'] = user_schema_unpacked.version_id

            if user_schema_mutation is not None:
                updates['user_schema'] = user_schema_unpacked = \
                    pickle.loads(user_schema_mutation).apply(db.user_schema)
                updates['user_schema_version'] = user_schema_unpacked.version_id

            if reflection_cache is not None:
                updates['reflection_cache'] = pickle.loads(reflection_cache)
            if database_config is not None:
                updates['database_config'] = pickle.loads(database_config)

            if updates:
                ns_db = ns_db._replace(**updates)
                ns = DBS[dbname]
                ns.set(namespace, ns_db)
                DBS = DBS.set(dbname, ns)

        if global_schema is not None:
            GLOBAL_SCHEMA = pickle.loads(global_schema)

        if system_config is not None:
            INSTANCE_CONFIG = pickle.loads(system_config)

    except Exception as ex:
        raise state.FailedStateSync(
            f'failed to sync worker state: {type(ex).__name__}({ex})') from ex

    if need_return:
        return ns_db


def compile(
    dbname: str,
    namespace: str,
    user_schema: Optional[bytes],
    user_schema_mutation: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    with util.disable_gc():
        db = __sync__(
            dbname,
            namespace,
            user_schema,
            user_schema_mutation,
            reflection_cache,
            global_schema,
            database_config,
            system_config,
        )

        units, cstate = COMPILER.compile(
            namespace,
            db.user_schema,
            GLOBAL_SCHEMA,
            db.reflection_cache,
            db.database_config,
            INSTANCE_CONFIG,
            *compile_args,
            **compile_kwargs
        )

        global LAST_STATE
        LAST_STATE = cstate
        pickled_state = None
        if cstate is not None:
            pickled_state = pickle.dumps(cstate.compress(), -1)

        return units, pickled_state


def compile_in_tx(cstate, dbname, namespace, user_schema_pickled, *args, **kwargs):
    global LAST_STATE
    global DBS

    with util.disable_gc():
        if cstate == state.REUSE_LAST_STATE_MARKER:
            cstate = LAST_STATE
        else:
            cstate: compiler.CompilerConnectionState = pickle.loads(cstate)

            if user_schema_pickled is not None:
                user_schema: s_schema.FlatSchema = pickle.loads(user_schema_pickled)
            else:
                user_schema = DBS.get(dbname).get(namespace).user_schema

            cstate = cstate.restore(user_schema)

        units, cstate = COMPILER.compile_in_tx(cstate, namespace, *args, **kwargs)
        LAST_STATE = cstate
        return units, pickle.dumps(cstate.compress(), -1), cstate.base_user_schema_id


def compile_notebook(
    dbname: str,
    namespace: str,
    user_schema: Optional[bytes],
    user_schema_mutation: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        namespace,
        user_schema,
        user_schema_mutation,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    return COMPILER.compile_notebook(
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )


def infer_expr(
    dbname: str,
    namespace: str,
    user_schema: Optional[bytes],
    user_schema_mutation: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        namespace,
        user_schema,
        user_schema_mutation,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    return COMPILER.infer_expr(
        STD_SCHEMA,
        db.user_schema,
        GLOBAL_SCHEMA,
        *compile_args,
        **compile_kwargs
    )


def try_compile_rollback(
    *compile_args: Any,
    **compile_kwargs: Any,
):
    return COMPILER.try_compile_rollback(*compile_args, **compile_kwargs)


def describe_database_dump(
    *compile_args: Any,
    **compile_kwargs: Any,
):
    return COMPILER.describe_database_dump(*compile_args, **compile_kwargs)


def describe_database_restore(
    *compile_args: Any,
    **compile_kwargs: Any,
):
    return COMPILER.describe_database_restore(*compile_args, **compile_kwargs)


def compile_graphql(
    dbname: str,
    namespace: str,
    user_schema: Optional[bytes],
    user_schema_mutation: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
) -> tuple[compiler.QueryUnitGroup, graphql.TranspiledOperation]:
    *_, query_only, module, limit = compile_args

    try:
        if int(limit) < 0:
            raise errors.QueryError("LIMIT must not be negative")
    except ValueError:
        raise errors.QueryError("LIMIT must be an integer.")

    db = __sync__(
        dbname,
        namespace,
        user_schema,
        user_schema_mutation,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    gql_op = graphql.compile_graphql(
        dbname,
        namespace,
        STD_SCHEMA,
        db.user_schema,
        GLOBAL_SCHEMA,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )

    source = edgeql.Source.from_string(
        edgeql.generate_source(gql_op.edgeql_ast, pretty=True),
    )

    unit_group, _ = COMPILER.compile(
        namespace=namespace,
        user_schema=db.user_schema,
        global_schema=GLOBAL_SCHEMA,
        reflection_cache=db.reflection_cache,
        database_config=db.database_config,
        system_config=INSTANCE_CONFIG,
        source=source,
        sess_modaliases=None,
        sess_config=None,
        output_format=compiler.OutputFormat.JSON,
        expect_one=True,
        implicit_limit=int(limit),
        inline_typeids=False,
        inline_typenames=False,
        inline_objectids=False,
        json_parameters=True,
        skip_first=False,
        protocol_version=defines.CURRENT_PROTOCOL,
    )

    if (unit_group.capabilities & compiler.Capability.MODIFICATIONS) and query_only:
        raise errors.QueryError("仅可执行查询操作")

    return unit_group, gql_op


def get_handler(methname):
    if methname == "__init_worker__":
        meth = __init_worker__
    else:
        if not INITED:
            raise RuntimeError(
                "call on uninitialized compiler worker"
            )
        if methname == "compile":
            meth = compile
        elif methname == "compile_in_tx":
            meth = compile_in_tx
        elif methname == "compile_notebook":
            meth = compile_notebook
        elif methname == "compile_graphql":
            meth = compile_graphql
        elif methname == "infer_expr":
            meth = infer_expr
        elif methname == "try_compile_rollback":
            meth = try_compile_rollback
        elif methname == '__sync__':
            meth = __sync__
        else:
            meth = getattr(COMPILER, methname)
    return meth


if __name__ == "__main__":
    try:
        worker_proc.main(get_handler)
    except KeyboardInterrupt:
        pass
