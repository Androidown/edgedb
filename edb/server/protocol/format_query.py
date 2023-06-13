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
import asyncio
import http
import json
from edb import errors, edgeql
from edb.edgeql.parser import qlparser
from edb.common import debug
from edb.common import markup


async def handle_request(request, response):
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
            query = body.get('query')
        else:
            raise TypeError(
                'the body of the request must be a JSON object'
            )

        if query is None:
            raise ValueError("Field 'query' is required.")

        if not isinstance(query, str):
            raise TypeError("Field 'query' must be a string.")

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
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, format_source, query)

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
        response.body = json.dumps({'result': result}).encode()


def format_source(query):
    source = edgeql.Source.from_string(query)
    parser = qlparser.EdgeQLBlockParser()
    result = edgeql.codegen.generate_source(parser.parse(source))
    return result
