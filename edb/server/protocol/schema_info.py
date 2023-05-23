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
import urllib.parse
import uuid
from typing import List
from edb import errors
from edb.common import debug
from edb.common import markup
from edb.server import defines


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

    query_uuid = None
    namespace = defines.DEFAULT_NS

    try:
        if request.method == b'POST':
            if request.content_type and b'json' in request.content_type:
                body = json.loads(request.body)
                if not isinstance(body, dict):
                    raise TypeError(
                        'the body of the request must be a JSON object')
                query_uuid = body.get('uuid')
                namespace = body.get('namespace', defines.DEFAULT_NS)
            else:
                raise TypeError(
                    'unable to interpret SchemaInfo POST request')

        elif request.method == b'GET':
            if request.url.query:
                url_query = request.url.query.decode('ascii')
                qs = urllib.parse.parse_qs(url_query)
                query_uuid = qs.get('uuid')
                if query_uuid is not None:
                    query_uuid = query_uuid[0]
                namespace = qs.get('namespace')
                if namespace is not None:
                    namespace = namespace[0]
                else:
                    namespace = defines.DEFAULT_NS
        else:
            raise TypeError('expected a GET or a POST request')

        if query_uuid is None:
            raise TypeError('invalid SchemaInfo request: "uuid" is missing')

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
        result = await execute(db, server, namespace, query_uuid)
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


async def execute(db, server, namespace: str, query_uuid: str):
    if namespace not in db.ns_map:
        raise errors.InternalServerError(
            f'NameSpace: [{namespace}] not in current db [{db.name}](ver:{db.dbver})'
        )
    user_schema = db.ns_map[namespace].user_schema
    global_schema = server.get_global_schema()

    obj_id = uuid.UUID(query_uuid)
    actual_schema = None

    info = {}

    if obj := global_schema.get_by_id(obj_id, default=None):
        actual_schema = global_schema
    else:
        obj = user_schema.get_by_id(obj_id, default=None)
        if obj is not None:
            actual_schema = user_schema

    if actual_schema is None:
        raise errors.InvalidReferenceError(
            f'Can\'t find Object with uuid: <{obj_id}>'
            f' among schema: {["global", db.dbname]}.'
        )

    all_fields = type(obj).get_schema_fields()
    data = list(actual_schema._id_to_data[obj_id])

    for field_name, field in all_fields.items():
        findex = field.index
        info[field_name] = data[findex]

    return json.dumps(info, default=str).encode('utf-8')
