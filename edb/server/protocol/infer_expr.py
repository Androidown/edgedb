#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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


import http
import json
from typing import List
from edb import errors
from edb.common import debug
from edb.common import markup


async def handle_request(
    request,
    response,
    db,
    args: List,
    server,
):
    if len(args) > 0:
        response.body = b'Unknown path'
        response.status = http.HTTPStatus.NOT_FOUND
        response.close_connection = True
        return

    if request.method != b'POST':
        response.status = http.HTTPStatus.METHOD_NOT_ALLOWED
        response.close_connection = True
        response.body = b'Expect POST method.'
        return

    try:
        if request.content_type and b'json' in request.content_type:
            body = json.loads(request.body)
            if not isinstance(body, dict):
                raise TypeError(
                    'the body of the request must be a JSON object'
                )
            module = body.get('module')
            objname = body.get('object')
            expr = body.get('expression')
        else:
            raise TypeError(
                'the body of the request must be a JSON object'
            )

        if module is None:
            raise ValueError("Field 'module' is required.")
        if objname is None:
            raise ValueError("Field 'object' is required.")
        if expr is None:
            raise ValueError("Field 'expression' is required.")

        if not isinstance(module, str):
            raise TypeError("Field 'module' must be a string.")
        if not isinstance(objname, str):
            raise TypeError("Field 'object' must be a string.")
        if not isinstance(expr, str):
            raise TypeError("Field 'expression' must be a string.")

    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        response.body = str(ex).encode()
        response.status = http.HTTPStatus.BAD_REQUEST
        response.close_connection = True
        return

    response.status = http.HTTPStatus.OK
    response.content_type = b'application/json'

    await db.introspection()

    try:
        result = await execute(db, server, module, objname, expr)
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        ex_type = type(ex)
        if not issubclass(ex_type, errors.EdgeDBError):
            ex_type = errors.InternalServerError

        err_dct = {
            'message': str(ex),
            'type': str(ex_type.__name__),
            'code': ex_type.get_code(),
        }

        response.body = json.dumps({'error': err_dct}).encode()
    else:
        response.body = json.dumps(result).encode()


async def execute(db, server, module: str, objname: str, expression: str):
    dbver = db.dbver
    query_cache = server._http_query_cache

    name_str = f"{module}::{objname}"

    cache_key = ('infer_expr', name_str, expression, dbver, module)

    entry = query_cache.get(cache_key, None)

    if entry is not None:
        return entry

    compiler_pool = server.get_compiler_pool()
    result = await compiler_pool.infer_expr(
        db.name,
        db.user_schema,
        server.get_global_schema(),
        db.reflection_cache,
        db.db_config,
        server.get_compilation_system_config(),
        name_str,
        expression
    )

    query_cache[cache_key] = result
    server.remove_on_ddl.add(cache_key)
    # Clean if should
    while query_cache.needs_cleanup():
        query_cache.cleanup_one()
    return result
