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

from edb import errors
from edb.edgeql import ast as qlast
from edb.edgeql import qltypes
from . import abc as s_abc
from . import annos as s_anno
from . import delta as sd
from . import objects as so
from . import schema as s_schema


class NameSpace(
    so.ExternalObject,
    s_anno.AnnotationSubject,
    s_abc.NameSpace,
    qlkind=qltypes.SchemaObjectClass.NAMESPACE,
    data_safe=False,
):
    pass


class NameSpaceCommandContext(sd.ObjectCommandContext[NameSpace]):
    pass


class NameSpaceCommand(
    sd.ExternalObjectCommand[NameSpace],
    context_class=NameSpaceCommandContext,
):
    def _validate_name(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        name = self.get_attribute_value('name')
        if str(name).startswith('pg_'):
            source_context = self.get_attribute_source_context('name')
            raise errors.SchemaDefinitionError(
                f'NameSpace names can not be started with \'pg_\', '
                f'as such names are reserved for system schemas',
                context=source_context,
            )


class CreateNameSpace(NameSpaceCommand, sd.CreateExternalObject[NameSpace]):
    astnode = qlast.CreateNameSpace

    def validate_create(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        super().validate_create(schema, context)
        self._validate_name(schema, context)


class DeleteNameSpace(NameSpaceCommand, sd.DeleteExternalObject[NameSpace]):
    astnode = qlast.DropNameSpace
