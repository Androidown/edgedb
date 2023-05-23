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


import decimal
import http
import json
import urllib.parse

import immutables

from edb import errors
from edb import edgeql
from edb.server import defines as edbdef
from edb.server.protocol import execute

from edb.common import debug
from edb.common import markup

from edb.edgeql import qltypes

from edb.server import compiler
from edb.server import config
from edb.server.compiler import enums
from edb.server.dbview cimport dbview
from edb.server.pgproto.pgproto cimport WriteBuffer


async def handle_request(
    object request,
    object response,
    object db,
    list args,
    object server,
):
    read_only = False

    if args == ['query']:
        read_only = True

    elif args != []:
        response.body = b'Unknown path'
        response.status = http.HTTPStatus.NOT_FOUND
        response.close_connection = True
        return

    variables = None
    globals_ = None
    query = None
    module = None
    limit = 0
    namespace = edbdef.DEFAULT_NS

    try:
        if request.method == b'POST':
            if request.content_type and b'json' in request.content_type:
                body = json.loads(request.body)
                if not isinstance(body, dict):
                    raise TypeError(
                        'the body of the request must be a JSON object')
                query = body.get('query')
                variables = body.get('variables')
                globals_ = body.get('globals')
                module = body.get('module')
                namespace = body.get('namespace', edbdef.DEFAULT_NS)
                limit = body.get('limit', 0)
            else:
                raise TypeError(
                    'unable to interpret EdgeQL POST request')

        elif request.method == b'GET':
            if request.url.query:
                url_query = request.url.query.decode('ascii')
                qs = urllib.parse.parse_qs(url_query)

                query = qs.get('query')
                if query is not None:
                    query = query[0]

                variables = qs.get('variables')
                if variables is not None:
                    try:
                        variables = json.loads(variables[0])
                    except Exception:
                        raise TypeError(
                            '"variables" must be a JSON object')

                globals_ = qs.get('globals')
                if globals_ is not None:
                    try:
                        globals_ = json.loads(globals_[0])
                    except Exception:
                        raise TypeError(
                            '"globals" must be a JSON object')

                module = qs.get('module')
                if module is not None:
                    module = module[0]

                namespace = qs.get('namespace')
                if namespace is not None:
                    namespace = namespace[0]
                else:
                    namespace = edbdef.DEFAULT_NS

                limit = qs.get('limit')
                if limit is not None:
                    limit = int(limit[0])
                else:
                    limit = 0
        else:
            raise TypeError('expected a GET or a POST request')

        if not query:
            raise TypeError('invalid EdgeQL request: query is missing')

        if variables is not None and not isinstance(variables, dict):
            raise TypeError('"variables" must be a JSON object')

        if globals_ is not None and not isinstance(globals_, dict):
            raise TypeError('"globals" must be a JSON object')

        if module is not None and not isinstance(module, str):
            raise TypeError('"module" must be a str object')

        if namespace is not None and not isinstance(namespace, str):
            raise TypeError('"namespace" must be a str object')

        if limit is not None and not isinstance(limit, int):
            raise TypeError('"limit" must be an integer object')

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
        result = await execute.parse_execute_json(
            db,
            namespace,
            query,
            variables=variables or {},
            globals_=globals_ or {},
            read_only=read_only,
            module=module,
            limit=limit
        )
    except Exception as ex:
        if debug.flags.server:
            markup.dump(ex)

        ex_type = type(ex)
        if not issubclass(ex_type, errors.EdgeDBError):
            # XXX Fix this when LSP "location" objects are implemented
            ex_type = errors.InternalServerError

        err_dct = {
            'message': str(ex),
            'type': str(ex_type.__name__),
            'code': ex_type.get_code(),
        }

        response.body = json.dumps({'error': err_dct}).encode()
    else:
        response.body = b'{"data":' + result + b'}'
