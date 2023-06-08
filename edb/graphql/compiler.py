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


from __future__ import annotations

import uuid
from typing import *

from edb import graphql

from edb.schema import schema as s_schema

from graphql.language import lexer as gql_lexer


GQLCoreCache: Dict[
    Tuple[str, str],
    Dict[
        (s_schema.FlatSchema, uuid.UUID, s_schema.FlatSchema, str),
        graphql.GQLCoreSchema
    ]
] = {}


def _get_gqlcore(
    dbname: str,
    namespace: str,
    std_schema: s_schema.FlatSchema,
    user_schema: s_schema.FlatSchema,
    global_schema: s_schema.FlatSchema,
    module: str = None
) -> graphql.GQLCoreSchema:
    key = (std_schema, user_schema.version_id, global_schema, module)
    if cache := GQLCoreCache.get((dbname, namespace)):
        if key in cache:
            return cache[key]
        else:
            cache.clear()
    else:
        cache = GQLCoreCache.setdefault((dbname, namespace), {})

    core = graphql.GQLCoreSchema(
        s_schema.ChainedSchema(
            std_schema,
            user_schema,
            global_schema
        ),
        module
    )
    cache[key] = core
    return core


def compile_graphql(
    dbname: str,
    namespace: str,
    std_schema: s_schema.FlatSchema,
    user_schema: s_schema.FlatSchema,
    global_schema: s_schema.FlatSchema,
    database_config: Mapping[str, Any],
    system_config: Mapping[str, Any],
    gql: str,
    tokens: Optional[
        List[Tuple[gql_lexer.TokenKind, int, int, int, int, str]]],
    substitutions: Optional[Dict[str, Tuple[str, int, int]]],
    operation_name: str = None,
    variables: Optional[Mapping[str, object]] = None,
    query_only: bool = False,
    module: str = None,
    limit: int = 0
) -> graphql.TranspiledOperation:
    if tokens is None:
        ast = graphql.parse_text(gql)
    else:
        ast = graphql.parse_tokens(gql, tokens)

    gqlcore = _get_gqlcore(dbname, namespace, std_schema, user_schema, global_schema, module)

    return graphql.translate_ast(
        gqlcore,
        ast,
        variables=variables,
        substitutions=substitutions,
        operation_name=operation_name,
        module=module,
        limit=limit
    )
