#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


import cython
import http
import json
import logging
import urllib.parse
from typing import Any, Dict, Tuple, List, Optional, Union

from graphql.language import lexer as gql_lexer

from edb import _graphql_rewrite
from edb import errors
from edb.graphql import errors as gql_errors
from edb.server.pgcon import errors as pgerrors

from edb.common import debug
from edb.common import markup

from . import explore
from . import compiler


logger = logging.getLogger(__name__)
_USER_ERRORS = (
    _graphql_rewrite.LexingError,
    _graphql_rewrite.SyntaxError,
    _graphql_rewrite.NotFoundError,
)

@cython.final
cdef class CacheRedirect:
    cdef public list key_vars  # List[str],  must be sorted

    def __init__(self, key_vars: List[str]):
        self.key_vars = key_vars


CacheEntry = Union[CacheRedirect, compiler.CompiledOperation]


async def handle_request(
    object request,
    object response,
    object db,
    list args,
    object server,
):
    query_only = False

    if args == ['explore'] and request.method == b'GET':
        response.body = explore.EXPLORE_HTML
        response.content_type = b'text/html'
        return
    if args == ['query']:
        query_only = True
    elif args != []:
        response.body = b'Unknown path'
        response.status = http.HTTPStatus.NOT_FOUND
        response.close_connection = True
        return

    operation_name = None
    variables = None
    query = None
    module = None
    limit = 0

    try:
        if request.method == b'POST':
            if request.content_type and b'json' in request.content_type:
                body = json.loads(request.body)
                if not isinstance(body, dict):
                    raise TypeError(
                        'the body of the request must be a JSON object')
                query = body.get('query')
                operation_name = body.get('operationName')
                variables = body.get('variables')
                module = body.get('module')
                limit = body.get('limit', 0)
            elif request.content_type == 'application/graphql':
                query = request.body.decode('utf-8')
            else:
                raise TypeError(
                    'unable to interpret GraphQL POST request')

        elif request.method == b'GET':
            if request.url.query:
                url_query = request.url.query.decode('ascii')
                qs = urllib.parse.parse_qs(url_query)

                query = qs.get('query')
                if query is not None:
                    query = query[0]

                operation_name = qs.get('operationName')
                if operation_name is not None:
                    operation_name = operation_name[0]

                variables = qs.get('variables')
                if variables is not None:
                    try:
                        variables = json.loads(variables[0])
                    except Exception:
                        raise TypeError(
                            '"variables" must be a JSON object')

                module = qs.get('module')
                if module is not None:
                    module = module[0]

                limit = qs.get('limit')
                if limit is not None:
                    limit = limit[0]

        else:
            raise TypeError('expected a GET or a POST request')

        if not query:
            raise TypeError('invalid GraphQL request: query is missing')

        if (operation_name is not None and
                not isinstance(operation_name, str)):
            raise TypeError('operationName must be a string')

        if variables is not None and not isinstance(variables, dict):
            raise TypeError('"variables" must be a JSON object')

    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        response.body = str(ex).encode()
        response.status = http.HTTPStatus.BAD_REQUEST
        response.close_connection = True
        return

    response.status = http.HTTPStatus.OK
    response.content_type = b'application/json'
    try:
        result = await execute(db, server, query, operation_name, variables, query_only, module or None, limit)
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        ex_type = type(ex)
        if issubclass(ex_type, (gql_errors.GraphQLError,
                                pgerrors.BackendError)):
            # XXX Fix this when LSP "location" objects are implemented
            ex_type = errors.QueryError

        err_dct = {
            'message': f'{ex_type.__name__}: {ex}',
        }

        if (isinstance(ex, errors.EdgeDBError) and
                hasattr(ex, 'line') and
                hasattr(ex, 'col')):
            err_dct['locations'] = [{'line': ex.line, 'column': ex.col}]

        response.body = json.dumps({'errors': [err_dct]}).encode()
    else:
        response.body = b'{"data":' + result + b'}'


async def compile(
    db,
    server,
    query: str,
    tokens: Optional[List[Tuple[int, int, int, str]]],
    substitutions: Optional[Dict[str, Tuple[str, int, int]]],
    operation_name: Optional[str],
    variables: Dict[str, Any],
    query_only: bool,
    module: Optional[str],
    limit: int,
):
    compiler_pool = server.get_compiler_pool()
    return await compiler_pool.compile_graphql(
        db.name,
        db.user_schema,
        server.get_global_schema(),
        db.reflection_cache,
        db.db_config,
        server.get_compilation_system_config(),
        query,
        tokens,
        substitutions,
        operation_name,
        variables,
        query_only,
        module,
        limit
    )


async def execute(db, server, query, operation_name, variables, query_only, module, limit):
    dbver = db.dbver
    query_cache = server._http_query_cache

    if variables:
        for var_name in variables:
            if var_name.startswith('_edb_arg__'):
                raise errors.QueryError(
                    f"Variables starting with '_edb_arg__' are prohibited")

    if debug.flags.graphql_compile:
        debug.header('Input graphql')
        print(query)
        print(f'variables: {variables}')

    try:
        rewritten = _graphql_rewrite.rewrite(operation_name, query)

        vars = rewritten.variables().copy()
        if variables:
            vars.update(variables)
        key_var_names = rewritten.key_vars()
        # on bad queries the following line can trigger KeyError
        key_vars = tuple(vars[k] for k in key_var_names)
    except _graphql_rewrite.QueryError as e:
        raise errors.QueryError(e.args[0])
    except Exception as e:
        if isinstance(e, _USER_ERRORS):
            logger.info("Error rewriting graphql query: %r", e)
        else:
            logger.warning("Error rewriting graphql query: %r", e)
        rewritten = None
        rewrite_error = e
        prepared_query = query
        vars = variables.copy() if variables else {}
        key_var_names = []
        key_vars = ()
    else:
        prepared_query = rewritten.key()

        if debug.flags.graphql_compile:
            debug.header('GraphQL optimized query')
            print(rewritten.key())
            print(f'key_vars: {key_var_names}')
            print(f'variables: {vars}')

    cache_key = ('graphql', prepared_query, key_vars, operation_name, dbver, query_only, module, limit)
    use_prep_stmt = False

    entry: CacheEntry = query_cache.get(cache_key, None)

    if isinstance(entry, CacheRedirect):
        key_vars2 = tuple(vars[k] for k in entry.key_vars)
        cache_key2 = (prepared_query, key_vars2, operation_name, dbver, query_only, module, limit)
        entry = query_cache.get(cache_key2, None)

    if entry is None:
        if rewritten is not None:
            op = await compile(
                db,
                server,
                query,
                rewritten.tokens(gql_lexer.TokenKind),
                rewritten.substitutions(),
                operation_name,
                vars,
                query_only,
                module,
                limit
            )
        else:
            op = await compile(
                db,
                server,
                query,
                None,
                None,
                operation_name,
                vars,
                query_only,
                module,
                limit
            )

        key_var_set = set(key_var_names)
        if op.cache_deps_vars and op.cache_deps_vars != key_var_set:
            key_var_set.update(op.cache_deps_vars)
            key_var_names = sorted(key_var_set)
            redir = CacheRedirect(key_vars=key_var_names)
            query_cache[cache_key] = redir
            key_vars2 = tuple(vars[k] for k in key_var_names)
            cache_key2 = (
                'graphql', prepared_query, key_vars2, operation_name, dbver, query_only, module, limit
            )
            query_cache[cache_key2] = op
        else:
            query_cache[cache_key] = op
    else:
        op = entry
        # This is at least the second time this query is used
        # and it's safe to cache.
        use_prep_stmt = True

    args = []
    if op.sql_args:
        for name in op.sql_args:
            if name not in vars:
                default = op.variables.get(name)
                if default is None:
                    raise errors.QueryError(
                        f'no value for the {name!r} variable')
                args.append(default)
            else:
                args.append(vars[name])

    pgcon = await server.acquire_pgcon(db.name)
    try:
        data = await pgcon.parse_execute_json(
            op.sql, op.sql_hash, dbver,
            use_prep_stmt, args)
    finally:
        server.release_pgcon(db.name, pgcon)

    if data is None:
        raise errors.InternalServerError(
            f'no data received for a JSON query {op.sql!r}')

    return data
