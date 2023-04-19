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

import functools
import http
import textwrap
import itertools
from typing import *
import json
import enum


from edb import errors
from edb.server.protocol import execute
from edb.pgsql.types import base_type_name_map_r

from edb.common import debug
from edb.common import markup
from edb.common import struct
from edb.common import checked

PG_TYPE_TO_EDB_TYPE = {
    **{k: str(v) for k, v in base_type_name_map_r.items()},
    'varchar': 'std::str',
    'str': 'std::str',
}


class Cardinality(str, enum.Enum):
    single = 'SINGLE'
    multi = 'MULTI'

    def is_multi(self):
        return self.value == self.multi

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            uv = value.upper()
            for member in cls:
                if member.value == uv:
                    return member


class ViewDef(NamedTuple):
    relation: str
    columns: Dict[str, Dict[str, str]]


class _Request(struct.RTStruct, use_slots=False):
    def __init__(self, **kwargs):
        super().__init__(**self.check_kwargs(kwargs))
        self.validate_fields()

    def check_kwargs(self, kwargs):
        return kwargs

    def validate_fields(self):
        pass


class _Pointer(_Request):
    #: 外部表的列名
    name: str = struct.Field(type_=str)
    #: 属性在edge中的类型
    type: str = struct.Field(type_=str, default=None)
    #: 作为edge对象的属性名，可选，为空时使用name
    alias: str = struct.Field(type_=str, default=None)
    #: 属性基数，single/multi 大小写不敏感
    cardinality: Cardinality = struct.Field(type_=Cardinality, default=Cardinality.single, coerce=True)
    #: 是否必须
    required: bool = struct.Field(type_=bool, default=False)

    @functools.cached_property
    def column_def(self) -> Dict[str, str]:
        return {self.realname: self.name}

    @functools.cached_property
    def realname(self) -> str:
        return self.alias or self.name


class CreateProperty(_Pointer):
    #: 计算属性的表达式，表达式中的字段必须使用edge属性名，不可使用外部表列名
    expr: str = struct.Field(type_=str, default=None)
    #: 是否排他（edge不会给外部表增加排他约束，需要依靠外部表自身保证排他性）
    exclusive: bool = struct.Field(type_=bool, default=False)

    @functools.cached_property
    def edb_type(self) -> str:
        return PG_TYPE_TO_EDB_TYPE[self.type]

    def validate_fields(self):
        if self.type is None and self.expr is None:
            raise ValueError(
                f'Either type or expr must be specified '
                f'for property {self.realname!r}.')

        if self.type is not None and self.type not in PG_TYPE_TO_EDB_TYPE:
            raise ValueError(
                f'Property {self.realname!r} has unknown pg type {self.type!r}.')

    def to_ddl(self, pretty=False):
        if self.cardinality.is_multi():
            raise ValueError('Multi property is not yet suppported.')

        if self.expr is None:
            req = ' required ' if self.required else ' '
            stmt = f"CREATE{req}PROPERTY {self.realname} -> {self.edb_type}"

            if self.exclusive:
                stmt += "{ create constraint exclusive }"

        else:
            stmt = f"CREATE PROPERTY {self.realname} := {self.expr}"
        return stmt


class CreateLink(_Pointer):
    #: link目标表的关联字段
    to = struct.Field(type_=str)
    #: link源表的关联字段，如果link没有独立表，本字段值将被忽略
    from_ = struct.Field(type_=str, default='id')

    relation = struct.Field(type_=str, default=None)
    source = struct.Field(type_=str, default=None)
    target = struct.Field(type_=str, default=None)
    properties = struct.Field(type_=checked.CheckedList[CreateProperty], default=None)

    def check_kwargs(self, kwargs):
        if props := kwargs.get('properties'):
            kwargs['properties'] = checked.CheckedList[CreateProperty](
                CreateProperty(**p) for p in props)
        else:
            kwargs['properties'] = checked.CheckedList[CreateProperty]()

        if (from_ := kwargs.pop('from', None)) is not None:
            kwargs['from_'] = from_

        return kwargs

    def validate_fields(self):
        if self.type is None:
            raise ValueError(f"Missing type for link '{self.realname}'.")

        if self.has_table:
            missing = []

            if not self.relation:
                missing.append('relation')
            if not self.source:
                missing.append('source')
            if not self.target:
                missing.append('target')

            if missing:
                raise ValueError(
                    f"Link '{self.realname}' has a table but {missing} is missing.")

            if self.from_ == 'id':
                raise ValueError(
                    f"Cannot link '{self.realname}' from 'id' because it has a table. "
                    f"Hint: You might have to specify the value of field 'from'.")

    @functools.cached_property
    def lprops(self) -> List[str]:
        return [p.to_ddl() for p in self.properties]

    def to_ddl(self, pretty=False):
        create_link = f"CREATE {self.cardinality.value} LINK {self.realname} -> {self.type}"
        body = ";\n".join([f"ON {self.from_} TO {self.to}"] + self.lprops)

        if pretty:
            stmt = f"{create_link} {{\n{textwrap.indent(body, '  ')}\n}}"
        else:
            stmt = f"{create_link} {{{body}}}"

        return stmt

    @functools.cached_property
    def has_table(self):
        return (
            self.cardinality.is_multi()
            or self.properties
        )

    @functools.cached_property
    def view_def(self):
        assert self.relation and self.source and self.target
        columns = {
            'source': self.source,
            'target': self.target,
            **{k: v for p in self.properties for k, v in p.column_def.items()}
        }

        return ViewDef(relation=self.relation, columns=columns)


class BaseObjectType(_Request):
    module = struct.Field(type_=str, default='default')
    name = struct.Field(type_=str)

    @functools.cached_property
    def qualname(self):
        return f"{self.module}::{self.name}"

    @classmethod
    def from_dict(cls, query: Dict):
        return cls(**query)

    def resolve_view(self):
        return {}


class CreateObjectType(BaseObjectType):
    relation = struct.Field(type_=str)
    properties = struct.Field(type_=checked.CheckedList[CreateProperty], default=None)
    links = struct.Field(type_=checked.CheckedList[CreateLink], default=None)

    @classmethod
    def from_dict(cls, query: Dict):
        props = checked.CheckedList[CreateProperty](
            CreateProperty(**p) for p in
            query.get('properties', [])
        )

        links = checked.CheckedList[CreateLink](
            CreateLink(**lnk) for lnk in
            query.get('links', [])
        )

        upd_query = {**query, "properties": props, "links": links}
        return cls(**upd_query)

    def pointers(self):
        yield from itertools.chain(self.properties, self.links)

    @functools.cached_property
    def view_def(self):
        columns = {k: v for ptr in self.pointers() for k, v in ptr.column_def.items()}
        return ViewDef(relation=self.relation, columns=columns)

    def to_ddl(self, pretty=False):
        sorted_props = sorted(self.properties, key=lambda p: p.expr is not None)
        body = ';\n'.join(ptr.to_ddl(pretty=pretty) for ptr in self.pointers())
        if pretty:
            stmt = f"CREATE TYPE {self.qualname} {{\n{textwrap.indent(body, '  ')}\n}}"
        else:
            stmt = f"CREATE TYPE {self.qualname} {{{body}}}"
        return stmt

    def resolve_view(self):
        view = {self.qualname: self.view_def}

        for lnk in self.links:
            if lnk.has_table:
                view[(self.qualname, lnk.realname)] = lnk.view_def

        return view


class DeleteObjectType(BaseObjectType):
    def to_ddl(self):
        return f"DROP TYPE {self.qualname}"



async def handle_request(
    request,
    response,
    db,
    args,
    server,
):
    def _unknown_path():
        response.body = b'Unknown path'
        response.status = http.HTTPStatus.NOT_FOUND
        response.close_connection = True

    if request.method != b'POST':
        response.status = http.HTTPStatus.METHOD_NOT_ALLOWED
        response.close_connection = True
        response.body = b'Expect POST method.'
        return

    if len(args) != 1:
        _unknown_path()
        return

    endpoint = args[0]

    try:
        if request.content_type and b'json' in request.content_type:
            body = json.loads(request.body)
            if not isinstance(body, dict):
                raise TypeError(
                    'the body of the request must be a JSON object')

            if endpoint == 'create-type':
                req = CreateObjectType.from_dict(body)
            elif endpoint == 'delete-type':
                req = DeleteObjectType.from_dict(body)
            else:
                _unknown_path()
                return
        else:
            raise TypeError(
                'unable to interpret EdgeQL extern request')

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
        await execute.parse_execute(
            db,
            req.to_ddl(),
            external_view=req.resolve_view()
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
        response.body = b'{"data": "ok"}'


