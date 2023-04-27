#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

from . import base
from . import ddl
from ..common import quote_ident as qi


class NameSpace:
    def __init__(
        self,
        name: str,
    ):
        self.name = name

    def get_type(self):
        return 'NAMESPACE'

    def get_id(self):
        return qi(self.name)

    def is_shared(self) -> bool:
        return False


class DropNameSpace(
    ddl.SchemaObjectOperation,
    ddl.NonTransactionalDDLOperation
):

    def code(self, block: base.PLBlock) -> str:
        schemas = ",".join(
            [
                qi(self.name.name + "_" + schema)
                for schema in ['edgedb', 'edgedbss', 'edgedbpub', 'edgedbstd', 'edgedbinstdata', 'edgedbext']
            ]
        )
        return f'DROP SCHEMA IF EXISTS {schemas} CASCADE;'
