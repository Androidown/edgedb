# mypy: ignore-errors

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

import enum
from typing import *

import collections.abc
import itertools
import textwrap

from edb import errors

from edb.edgeql import ast as ql_ast
from edb.edgeql import qltypes as ql_ft
from edb.edgeql import compiler as qlcompiler

from edb.schema import annos as s_anno
from edb.schema import casts as s_casts
from edb.schema import scalars as s_scalars
from edb.schema import objtypes as s_objtypes
from edb.schema import constraints as s_constr
from edb.schema import database as s_db
from edb.schema import delta as sd
from edb.schema import expr as s_expr
from edb.schema import expraliases as s_aliases
from edb.schema import extensions as s_exts
from edb.schema import functions as s_funcs
from edb.schema import globals as s_globals
from edb.schema import indexes as s_indexes
from edb.schema import links as s_links
from edb.schema import policies as s_policies
from edb.schema import properties as s_props
from edb.schema import migrations as s_migrations
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as so
from edb.schema import operators as s_opers
from edb.schema import pointers as s_pointers
from edb.schema import pseudo as s_pseudo
from edb.schema import roles as s_roles
from edb.schema import sources as s_sources
from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import version as s_ver
from edb.schema import utils as s_utils

from edb.common import markup
from edb.common import ordered
from edb.common import uuidgen

from edb.ir import pathid as irpathid
from edb.ir import typeutils as irtyputils
from edb.ir import utils as irutils

from edb.pgsql import common
from edb.pgsql import dbops
from edb.pgsql import params

from edb.server import defines as edbdef
from edb.server.config import ops as config_ops

from . import ast as pg_ast
from .common import qname as q
from .common import quote_literal as ql
from .common import quote_ident as qi
from .common import quote_type as qt
from . import compiler
from . import codegen
from . import schemamech
from . import types


def has_table(obj, schema):
    if isinstance(obj, s_objtypes.ObjectType):
        return not (
            obj.is_compound_type(schema) or
            obj.get_is_derived(schema) or
            obj.is_view(schema)
        )
    elif obj.is_pure_computable(schema) or obj.get_is_derived(schema):
        return False
    elif obj.generic(schema):
        return (
            not isinstance(obj, s_props.Property)
            and str(obj.get_name(schema)) != 'std::link'
        )
    elif obj.is_link_property(schema):
        return not obj.singular(schema)
    elif not has_table(obj.get_source(schema), schema):
        return False
    else:
        ptr_stor_info = types.get_pointer_storage_info(
            obj, resolve_type=False, schema=schema, link_bias=True)

        return (
            ptr_stor_info is not None
            and ptr_stor_info.table_type == 'link'
        )


class GB(enum.Flag):
    SOURCE_T = enum.auto()
    TARGET_T = enum.auto()
    SOURCE_F = enum.auto()
    TARGET_F = enum.auto()

    @classmethod
    def get_flag(
        cls,
        disposition: Literal['source', 'target'],
        aspect: Literal['type', 'field'] = 'field'
    ):
        return {
            'source': {'field': cls.SOURCE_F, 'type': cls.SOURCE_T},
            'target': {'field': cls.TARGET_F, 'type': cls.TARGET_T},
        }[disposition][aspect]


class CommandMeta(sd.CommandMeta):
    pass


class _LinkAlterInfo(NamedTuple):
    new_target: s_props.Property
    old_target: s_props.Property
    new_source: s_props.Property
    old_source: s_props.Property
    type_altered: bool

    @property
    def target_altered(self):
        return self.old_target != self.new_target

    @property
    def source_altered(self):
        return self.old_source != self.new_source


class MetaCommand(sd.Command, metaclass=CommandMeta):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pgops = ordered.OrderedSet()

    def apply_prerequisites(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply_prerequisites(schema, context)
        for op in self.get_prerequisites():
            if not isinstance(op, sd.AlterObjectProperty):
                self.pgops.add(op)
        return schema

    def apply_subcommands(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply_subcommands(schema, context)
        for op in self.get_subcommands(
            include_prerequisites=False,
            include_caused=False,
        ):
            if not isinstance(op, sd.AlterObjectProperty):
                self.pgops.add(op)
        return schema

    def apply_caused(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply_caused(schema, context)
        for op in self.get_caused():
            if not isinstance(op, sd.AlterObjectProperty):
                self.pgops.add(op)
        return schema

    def generate(self, block: dbops.PLBlock) -> None:
        for op in self.pgops:
            op.generate(block)

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=str(self))

        for dd in self.pgops:
            if isinstance(dd, AlterObjectProperty):
                diff = markup.elements.doc.ValueDiff(
                    before=repr(dd.old_value), after=repr(dd.new_value))

                if dd.new_inherited:
                    diff.comment = 'inherited'
                elif dd.new_computed:
                    diff.comment = 'computed'

                node.add_child(label=dd.property, node=diff)
            else:
                node.add_child(node=markup.serialize(dd, ctx=ctx))

        return node

    def _get_backend_params(
        self,
        context: sd.CommandContext,
    ) -> params.BackendRuntimeParams:

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        return backend_params

    def _get_instance_params(
        self,
        context: sd.CommandContext,
    ) -> params.BackendInstanceParams:
        return self._get_backend_params(context).instance_params

    def _get_tenant_id(self, context: sd.CommandContext) -> str:
        return self._get_instance_params(context).tenant_id

    def schedule_inhview_update(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        source: s_sources.Source,
        ctxcls: Type[sd.CommandContextToken[sd.Command]],
    ) -> None:
        ctx = context.get_topmost_ancestor(ctxcls)
        if ctx is None:
            raise AssertionError(f"there is no {ctxcls} in context stack")
        ctx.op.inhview_updates.add((source, True))
        for anc in source.get_ancestors(schema).objects(schema):
            ctx.op.inhview_updates.add((anc, False))

    def schedule_inhview_source_update(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        ptr: s_pointers.Pointer,
        ctxcls: Type[sd.CommandContextToken[sd.Command]],
    ) -> None:
        ctx = context.get_topmost_ancestor(ctxcls)
        if ctx is None:
            raise AssertionError(f"there is no {ctxcls} in context stack")

        ctx.op.inhview_updates.add((ptr.get_source(schema), True))
        for anc in ptr.get_ancestors(schema).objects(schema):
            if src := anc.get_source(schema):
                ctx.op.inhview_updates.add((src, False))

    def schedule_post_inhview_update_command(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        cmd: sd.Command,
        ctxcls: Type[sd.CommandContextToken[sd.Command]],
    ) -> None:
        ctx = context.get_topmost_ancestor(ctxcls)
        if ctx is None:
            raise AssertionError(f"there is no {ctxcls} in context stack")
        ctx.op.post_inhview_update_commands.append(cmd)


class CommandGroupAdapted(MetaCommand, adapts=sd.CommandGroup):
    pass


class Nop(MetaCommand, adapts=sd.Nop):
    pass


class Query(MetaCommand, adapts=sd.Query):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        sql_tree = compiler.compile_ir_to_sql_tree(
            self.expr.irast,
            output_format=compiler.OutputFormat.NATIVE_INTERNAL,
            explicit_top_cast=irtyputils.type_to_typeref(
                schema,
                schema.get('std::str'),
            ),
            backend_runtime_params=context.backend_runtime_params,
        )

        sql_text = codegen.generate_source(sql_tree)

        # The INTO _dummy_text bit is needed because PL/pgSQL _really_
        # wants the result of a returning query to be stored in a variable,
        # and the PERFORM hack does not work if the query has DML CTEs.
        self.pgops.add(dbops.Query(
            text=f'{sql_text} INTO _dummy_text',
        ))

        return schema


class AlterObjectProperty(MetaCommand, adapts=sd.AlterObjectProperty):
    pass


class SchemaVersionCommand(MetaCommand):
    pass


class CreateSchemaVersion(
    SchemaVersionCommand,
    adapts=s_ver.CreateSchemaVersion,
):
    pass


class AlterSchemaVersion(
    SchemaVersionCommand,
    adapts=s_ver.AlterSchemaVersion,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        expected_ver = self.get_orig_attribute_value('version')
        check = dbops.Query(
            f'''
                SELECT
                    edgedb.raise_on_not_null(
                        (SELECT NULLIF(
                            (SELECT
                                version::text
                            FROM
                                edgedb."_SchemaSchemaVersion"
                            FOR UPDATE),
                            {ql(str(expected_ver))}
                        )),
                        'serialization_failure',
                        msg => (
                            'Cannot serialize DDL: '
                            || (SELECT version::text FROM
                                edgedb."_SchemaSchemaVersion")
                        )
                    )
                INTO _dummy_text
            '''
        )
        self.pgops.add(check)
        return schema


class GlobalSchemaVersionCommand(MetaCommand):
    pass


class CreateGlobalSchemaVersion(
    MetaCommand,
    adapts=s_ver.CreateGlobalSchemaVersion,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        ver_id = str(self.scls.id)
        ver_name = str(self.scls.get_name(schema))
        tenant_id = self._get_tenant_id(context)

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        metadata = {
            ver_id: {
                'id': ver_id,
                'name': ver_name,
                'version': str(self.scls.get_version(schema)),
                'builtin': self.scls.get_builtin(schema),
                'internal': self.scls.get_internal(schema),
            }
        }
        if backend_params.has_create_database:
            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.Database(name=common.get_database_backend_name(
                        edbdef.EDGEDB_TEMPLATE_DB, tenant_id=tenant_id)),
                    section='GlobalSchemaVersion',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='GlobalSchemaVersion',
                    metadata=metadata
                )
            )

        return schema


class AlterGlobalSchemaVersion(
    MetaCommand,
    adapts=s_ver.AlterGlobalSchemaVersion,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        ver_id = str(self.scls.id)
        ver_name = str(self.scls.get_name(schema))

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        tpl_db_name = common.get_database_backend_name(
            edbdef.EDGEDB_TEMPLATE_DB, tenant_id=backend_params.tenant_id)

        if not backend_params.has_create_database:
            key = f'{edbdef.EDGEDB_TEMPLATE_DB}metadata'
            lock = dbops.Query(
                f'''
                SELECT
                    json
                FROM
                    edgedbinstdata.instdata
                WHERE
                    key = {ql(key)}
                FOR UPDATE
                INTO _dummy_text
            '''
            )
        elif backend_params.has_superuser_access:
            # Only superusers are generally allowed to make an UPDATE
            # lock on shared catalogs.
            lock = dbops.Query(
                f'''
                    SELECT
                        description
                    FROM
                        pg_catalog.pg_shdescription
                    WHERE
                        objoid = (
                            SELECT oid
                            FROM pg_database
                            WHERE datname = {ql(tpl_db_name)}
                        )
                        AND classoid = 'pg_database'::regclass::oid
                    FOR UPDATE
                    INTO _dummy_text
                '''
            )
        else:
            # Without superuser access we have to resort to lock polling.
            # This is racy, but is unfortunately the best we can do.
            lock = dbops.Query(f'''
                SELECT
                    edgedb.raise_on_not_null(
                        (
                            SELECT 'locked'
                            FROM pg_catalog.pg_locks
                            WHERE
                                locktype = 'object'
                                AND classid = 'pg_database'::regclass::oid
                                AND objid = (
                                    SELECT oid
                                    FROM pg_database
                                    WHERE
                                        datname = {ql(tpl_db_name)}
                                )
                                AND mode = 'ShareUpdateExclusiveLock'
                                AND granted
                                AND pid != pg_backend_pid()
                        ),
                        'serialization_failure',
                        msg => (
                            'Cannot serialize global DDL: '
                            || (SELECT version::text FROM
                                edgedb."_SysGlobalSchemaVersion")
                        )
                    )
                INTO _dummy_text
            ''')

        self.pgops.add(lock)

        expected_ver = self.get_orig_attribute_value('version')
        check = dbops.Query(
            f'''
                SELECT
                    edgedb.raise_on_not_null(
                        (SELECT NULLIF(
                            (SELECT
                                version::text
                            FROM
                                edgedb."_SysGlobalSchemaVersion"
                            ),
                            {ql(str(expected_ver))}
                        )),
                        'serialization_failure',
                        msg => (
                            'Cannot serialize global DDL: '
                            || (SELECT version::text FROM
                                edgedb."_SysGlobalSchemaVersion")
                        )
                    )
                INTO _dummy_text
            '''
        )
        self.pgops.add(check)

        metadata = {
            ver_id: {
                'id': ver_id,
                'name': ver_name,
                'version': str(self.scls.get_version(schema)),
                'builtin': self.scls.get_builtin(schema),
                'internal': self.scls.get_internal(schema),
            }
        }
        if backend_params.has_create_database:
            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.Database(name=tpl_db_name),
                    section='GlobalSchemaVersion',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='GlobalSchemaVersion',
                    metadata=metadata
                )
            )

        return schema


class PseudoTypeCommand(MetaCommand):
    pass


class CreatePseudoType(
    PseudoTypeCommand,
    adapts=s_pseudo.CreatePseudoType,
):
    pass


class TupleCommand(MetaCommand):

    pass


class CreateTuple(TupleCommand, adapts=s_types.CreateTuple):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        if self.scls.is_polymorphic(schema):
            return schema

        elements = self.scls.get_element_types(schema).items(schema)

        ctype = dbops.CompositeType(
            name=common.get_backend_name(schema, self.scls, catenate=False),
            columns=[
                dbops.Column(
                    name=n,
                    type=qt(types.pg_type_from_object(
                        schema, t, persistent_tuples=True)),
                )
                for n, t in elements
            ]
        )

        self.pgops.add(dbops.CreateCompositeType(type=ctype))

        return schema


class AlterTuple(TupleCommand, adapts=s_types.AlterTuple):
    pass


class RenameTuple(TupleCommand, adapts=s_types.RenameTuple):
    pass


class DeleteTuple(TupleCommand, adapts=s_types.DeleteTuple):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        tup = schema.get_global(s_types.Tuple, self.classname)

        if not tup.is_polymorphic(schema):
            self.pgops.add(dbops.DropCompositeType(
                name=common.get_backend_name(schema, tup, catenate=False),
            ))

        schema = super().apply(schema, context)

        return schema


class ExprAliasCommand(MetaCommand):
    pass


class CreateAlias(
    ExprAliasCommand,
    adapts=s_aliases.CreateAlias,
):
    pass


class RenameAlias(
    ExprAliasCommand,
    adapts=s_aliases.RenameAlias,
):
    pass


class AlterAlias(
    ExprAliasCommand,
    adapts=s_aliases.AlterAlias,
):
    pass


class DeleteAlias(
    ExprAliasCommand,
    adapts=s_aliases.DeleteAlias,
):
    pass


class GlobalCommand(MetaCommand):
    pass


class CreateGlobal(
    GlobalCommand,
    adapts=s_globals.CreateGlobal,
):
    pass


class RenameGlobal(
    GlobalCommand,
    adapts=s_globals.RenameGlobal,
):
    pass


class AlterGlobal(
    GlobalCommand,
    adapts=s_globals.AlterGlobal,
):
    pass


class SetGlobalType(
    GlobalCommand,  # ???
    adapts=s_globals.SetGlobalType,
):
    def register_config_op(self, op, context):
        ops = context.get(sd.DeltaRootContext).op.config_ops
        ops.append(op)

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:

        schema = super().apply(schema, context)
        if self.reset_value:
            op = config_ops.Operation(
                opcode=config_ops.OpCode.CONFIG_RESET,
                scope=ql_ft.ConfigScope.GLOBAL,
                setting_name=str(self.scls.get_name(schema)),
                value=None,
            )
            self.register_config_op(op, context)

        return schema


class DeleteGlobal(
    GlobalCommand,
    adapts=s_globals.DeleteGlobal,
):
    pass


class AccessPolicyCommand(MetaCommand):
    pass


class CreateAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.CreateAccessPolicy,
):
    pass


class RenameAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.RenameAccessPolicy,
):
    pass


class RebaseAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.RebaseAccessPolicy,
):
    pass


class AlterAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.AlterAccessPolicy,
):
    pass


class DeleteAccessPolicy(
    AccessPolicyCommand,
    adapts=s_policies.DeleteAccessPolicy,
):
    pass


class TupleExprAliasCommand(MetaCommand):
    pass


class CreateTupleExprAlias(
    TupleExprAliasCommand,
    adapts=s_types.CreateTupleExprAlias,
):
    pass


class RenameTupleExprAlias(
    TupleExprAliasCommand,
    adapts=s_types.RenameTupleExprAlias,
):
    pass


class AlterTupleExprAlias(
    TupleExprAliasCommand,
    adapts=s_types.AlterTupleExprAlias,
):
    pass


class DeleteTupleExprAlias(
    TupleExprAliasCommand,
    adapts=s_types.DeleteTupleExprAlias,
):
    pass


class ArrayCommand(MetaCommand):
    pass


class CreateArray(ArrayCommand, adapts=s_types.CreateArray):
    pass


class AlterArray(ArrayCommand, adapts=s_types.AlterArray):
    pass


class RenameArray(ArrayCommand, adapts=s_types.RenameArray):
    pass


class DeleteArray(ArrayCommand, adapts=s_types.DeleteArray):
    pass


class ArrayExprAliasCommand(MetaCommand):
    pass


class CreateArrayExprAlias(
    ArrayExprAliasCommand,
    adapts=s_types.CreateArrayExprAlias,
):
    pass


class RenameArrayExprAlias(
    ArrayExprAliasCommand,
    adapts=s_types.RenameArrayExprAlias,
):
    pass


class AlterArrayExprAlias(
    ArrayExprAliasCommand,
    adapts=s_types.AlterArrayExprAlias,
):
    pass


class DeleteArrayExprAlias(
    ArrayExprAliasCommand,
    adapts=s_types.DeleteArrayExprAlias,
):
    pass


class RangeCommand(MetaCommand):
    pass


class CreateRange(RangeCommand, adapts=s_types.CreateRange):
    pass


class AlterRange(RangeCommand, adapts=s_types.AlterRange):
    pass


class RenameRange(RangeCommand, adapts=s_types.RenameRange):
    pass


class DeleteRange(RangeCommand, adapts=s_types.DeleteRange):
    pass


class RangeExprAliasCommand(MetaCommand):
    pass


class CreateRangeExprAlias(
    RangeExprAliasCommand,
    adapts=s_types.CreateRangeExprAlias,
):
    pass


class RenameRangeExprAlias(
    RangeExprAliasCommand,
    adapts=s_types.RenameRangeExprAlias,
):
    pass


class AlterRangeExprAlias(
    RangeExprAliasCommand,
    adapts=s_types.AlterRangeExprAlias,
):
    pass


class DeleteRangeExprAlias(
    RangeExprAliasCommand,
    adapts=s_types.DeleteRangeExprAlias,
):
    pass


class ParameterCommand(MetaCommand):
    pass


class CreateParameter(
    ParameterCommand,
    adapts=s_funcs.CreateParameter,
):
    pass


class DeleteParameter(
    ParameterCommand,
    adapts=s_funcs.DeleteParameter,
):
    pass


class RenameParameter(
    ParameterCommand,
    adapts=s_funcs.RenameParameter,
):
    pass


class AlterParameter(
    ParameterCommand,
    adapts=s_funcs.AlterParameter,
):
    pass


class FunctionCommand(MetaCommand):
    def get_pgname(self, func: s_funcs.Function, schema):
        return common.get_backend_name(schema, func, catenate=False)

    def get_pgtype(self, func: s_funcs.Function, obj, schema):
        if obj.is_any(schema):
            return ('anyelement',)

        try:
            return types.pg_type_from_object(
                schema, obj, persistent_tuples=True)
        except ValueError:
            raise errors.QueryError(
                f'could not compile parameter type {obj!r} '
                f'of function {func.get_shortname(schema)}',
                context=self.source_context) from None

    def compile_default(self, func: s_funcs.Function,
                        default: s_expr.Expression, schema):
        try:
            comp = s_expr.Expression.compiled(
                default,
                schema=schema,
                as_fragment=True,
            )

            ir = comp.irast
            if not irutils.is_const(ir.expr):
                raise ValueError('expression not constant')

            sql_tree = compiler.compile_ir_to_sql_tree(
                ir.expr, singleton_mode=True)
            return codegen.SQLSourceGenerator.to_source(sql_tree)

        except Exception as ex:
            raise errors.QueryError(
                f'could not compile default expression {default!r} '
                f'of function {func.get_shortname(schema)}: {ex}',
                context=self.source_context) from ex

    def compile_args(self, func: s_funcs.Function, schema):
        func_params = func.get_params(schema)
        has_inlined_defaults = func.has_inlined_defaults(schema)

        args = []

        func_language = func.get_language(schema)
        if func_language is ql_ast.Language.EdgeQL:
            args.append(('__edb_json_globals__', ('jsonb'), None))

        if has_inlined_defaults:
            args.append(('__defaults_mask__', ('bytea',), None))

        compile_defaults = not (
            has_inlined_defaults or func_params.find_named_only(schema)
        )

        for param in func_params.get_in_canonical_order(schema):
            param_type = param.get_type(schema)
            param_default = param.get_default(schema)

            pg_at = self.get_pgtype(func, param_type, schema)

            default = None
            if compile_defaults and param_default is not None:
                default = self.compile_default(func, param_default, schema)

            pn = param.get_parameter_name(schema)
            args.append((pn, pg_at, default))

            if param_type.is_object_type():
                args.append((f'__{pn}__type', ('uuid',), None))

        return args

    def make_function(self, func: s_funcs.Function, code, schema):
        func_return_typemod = func.get_return_typemod(schema)
        func_params = func.get_params(schema)
        return dbops.Function(
            name=self.get_pgname(func, schema),
            args=self.compile_args(func, schema),
            has_variadic=func_params.find_variadic(schema) is not None,
            set_returning=func_return_typemod is ql_ft.TypeModifier.SetOfType,
            volatility=func.get_volatility(schema),
            strict=func.get_impl_is_strict(schema),
            returns=self.get_pgtype(
                func, func.get_return_type(schema), schema),
            text=code)

    def compile_sql_function(self, func: s_funcs.Function, schema):
        return self.make_function(func, func.get_code(schema), schema)

    def _compile_edgeql_function(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        func: s_funcs.Function,
        body: s_expr.Expression,
    ) -> s_expr.Expression:
        return s_funcs.compile_function(
            schema,
            context,
            body=body,
            params=func.get_params(schema),
            language=ql_ast.Language.EdgeQL,
            return_type=func.get_return_type(schema),
            return_typemod=func.get_return_typemod(schema),
        )

    def fix_return_type(
            self, func: s_funcs.Function, nativecode, schema, context):

        return_type = func.get_return_type(schema)
        ir = nativecode.irast

        if not (
            return_type.is_object_type()
            or s_types.is_type_compatible(return_type, ir.stype,
                                          schema=nativecode.schema)
        ):
            # Add a cast and recompile it
            qlexpr = qlcompiler.astutils.ensure_qlstmt(ql_ast.TypeCast(
                type=s_utils.typeref_to_ast(schema, return_type),
                expr=nativecode.qlast,
            ))
            nativecode = self._compile_edgeql_function(
                schema,
                context,
                func,
                type(nativecode).from_ast(qlexpr, schema),
            )

        return nativecode

    def compile_edgeql_function_body(
        self,
        func: s_funcs.Function,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> str:
        nativecode = func.get_nativecode(schema)
        if nativecode.irast is None:
            nativecode = self._compile_edgeql_function(
                schema,
                context,
                func,
                nativecode,
            )

        nativecode = self.fix_return_type(func, nativecode, schema, context)

        sql_text, _ = compiler.compile_ir_to_sql(
            nativecode.irast,
            ignore_shapes=True,
            explicit_top_cast=irtyputils.type_to_typeref(  # note: no cache
                schema, func.get_return_type(schema)),
            output_format=compiler.OutputFormat.NATIVE,
            use_named_params=True)

        return sql_text

    def compile_edgeql_overloaded_function_body(
        self,
        func: s_funcs.Function,
        overloads: List[s_funcs.Function],
        ov_param_idx: int,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> str:
        func_return_typemod = func.get_return_typemod(schema)
        set_returning = func_return_typemod is ql_ft.TypeModifier.SetOfType
        my_params = func.get_params(schema).objects(schema)
        param_name = my_params[ov_param_idx].get_parameter_name(schema)
        type_param_name = f'__{param_name}__type'
        cases = {}
        all_overloads = list(overloads)
        if not isinstance(self, DeleteFunction):
            all_overloads.append(func)
        for overload in all_overloads:
            ov_p = tuple(overload.get_params(schema).objects(schema))
            ov_p_t = ov_p[ov_param_idx].get_type(schema)
            ov_body = self.compile_edgeql_function_body(
                overload, schema, context)

            if set_returning:
                case = (
                    f"(SELECT * FROM ({ov_body}) AS q "
                    f"WHERE ancestor = {ql(str(ov_p_t.id))})"
                )
            else:
                case = (
                    f"WHEN ancestor = {ql(str(ov_p_t.id))} "
                    f"THEN \n({ov_body})"
                )

            cases[ov_p_t] = case

        impl_ids = ', '.join(f'{ql(str(t.id))}::uuid' for t in cases)
        branches = list(cases.values())

        # N.B: edgedb.raise and coalesce are used below instead of
        #      raise_on_null, because the latter somehow results in a
        #      significantly more complex query plan.
        matching_impl = f"""
            coalesce(
                (
                    SELECT
                        ancestor
                    FROM
                        (SELECT
                            {qi(type_param_name)} AS ancestor,
                            -1 AS index
                        UNION ALL
                        SELECT
                            target AS ancestor,
                            index
                        FROM
                            edgedb."_SchemaObjectType__ancestors"
                            WHERE source = {qi(type_param_name)}
                        ) a
                    WHERE ancestor IN ({impl_ids})
                    ORDER BY index
                    LIMIT 1
                ),

                edgedb.raise(
                    NULL::uuid,
                    'assert_failure',
                    msg => format(
                        'unhandled object type %s in overloaded function',
                        {qi(type_param_name)}
                    )
                )
            ) AS impl(ancestor)
        """

        if set_returning:
            arms = "\nUNION ALL\n".join(branches)
            return f"""
                SELECT
                    q.*
                FROM
                    {matching_impl},
                    LATERAL (
                        {arms}
                    ) AS q
            """
        else:
            arms = "\n".join(branches)
            return f"""
                SELECT
                    (CASE {arms} END)
                FROM
                    {matching_impl}
            """

    def compile_edgeql_function(self, func: s_funcs.Function, schema, context):
        nativecode = func.get_nativecode(schema)

        if nativecode.irast is None:
            nativecode = self._compile_edgeql_function(
                schema,
                context,
                func,
                nativecode,
            )

        nativecode = self.fix_return_type(func, nativecode, schema, context)

        replace = False

        obj_overload = func.find_object_param_overloads(schema)
        if obj_overload is not None:
            ov, ov_param_idx = obj_overload
            body = self.compile_edgeql_overloaded_function_body(
                func, ov, ov_param_idx, schema, context)
            replace = True
        else:
            body, _ = compiler.compile_ir_to_sql(
                nativecode.irast,
                ignore_shapes=True,
                explicit_top_cast=irtyputils.type_to_typeref(  # note: no cache
                    schema, func.get_return_type(schema)),
                output_format=compiler.OutputFormat.NATIVE,
                use_named_params=True)

        return self.make_function(func, body, schema), replace

    def sql_rval_consistency_check(
        self,
        cobj: s_funcs.CallableObject,
        expr: str,
        schema: s_schema.Schema,
    ) -> dbops.Command:
        fname = cobj.get_verbosename(schema)
        rtype = types.pg_type_from_object(
            schema,
            cobj.get_return_type(schema),
            persistent_tuples=True,
        )
        rtype_desc = '.'.join(rtype)

        # Determine the actual returned type of the SQL function.
        # We can't easily do this by looking in system catalogs because
        # of polymorphic dispatch, but, fortunately, there's pg_typeof().
        # We only need to be sure to actually NOT call the target function,
        # as we can't assume how it'll behave with dummy inputs. Hence, the
        # weird looking query below, where we rely in Postgres executor to
        # skip the call, because no rows satisfy the WHERE condition, but
        # we then still generate a NULL row via a LEFT JOIN.
        f_test = textwrap.dedent(f'''\
            (SELECT
                pg_typeof(f.i)
            FROM
                (SELECT NULL::text) AS spreader
                LEFT JOIN (SELECT {expr} WHERE False) AS f(i) ON (true))''')

        check = dbops.Query(text=f'''
            PERFORM
                edgedb.raise_on_not_null(
                    NULLIF(
                        pg_typeof(NULL::{qt(rtype)}),
                        {f_test}
                    ),
                    'invalid_function_definition',
                    msg => format(
                        '%s is declared to return SQL type "%s", but '
                        || 'the underlying SQL function returns "%s"',
                        {ql(fname)},
                        {ql(rtype_desc)},
                        {f_test}::text
                    ),
                    hint => (
                        'Declare the function with '
                        || '`force_return_cast := true`, '
                        || 'or add an explicit cast to its body.'
                    )
                );
        ''')

        return check

    def sql_strict_consistency_check(
        self,
        cobj: s_funcs.CallableObject,
        func: str,
        schema: s_schema.Schema,
    ) -> dbops.Command:
        fname = cobj.get_verbosename(schema)

        # impl_is_strict means that the function is strict in all
        # singleton arguments, so we don't need to do the check if
        # no such arguments exist.
        if (
            not cobj.get_impl_is_strict(schema)
            or not cobj.get_params(schema).has_type_mod(
                schema, ql_ft.TypeModifier.SingletonType
            )
        ):
            return dbops.CommandGroup()

        if '.' in func:
            ns, func = func.split('.')
        else:
            ns = 'pg_catalog'

        f_test = textwrap.dedent(f'''\
            COALESCE((
                SELECT bool_and(proisstrict) FROM pg_proc
                INNER JOIN pg_namespace ON pg_namespace.oid = pronamespace
                WHERE proname = {ql(func)} AND nspname = {ql(ns)}
            ), false)
        ''')

        check = dbops.Query(text=f'''
            PERFORM
                edgedb.raise_on_null(
                    NULLIF(
                        false,
                        {f_test}
                    ),
                    'invalid_function_definition',
                    msg => format(
                        '%s is declared to have a strict impl but does not',
                        {ql(fname)}
                    ),
                    hint => (
                        'Add `impl_is_strict := false` to the declaration.'
                    )
                );
        ''')

        return check

    def get_dummy_func_call(
        self,
        cobj: s_funcs.CallableObject,
        sql_func: str,
        schema: s_schema.Schema,
    ) -> str:
        args = []
        func_params = cobj.get_params(schema)
        for param in func_params.get_in_canonical_order(schema):
            param_type = param.get_type(schema)
            pg_at = self.get_pgtype(cobj, param_type, schema)
            args.append(f'NULL::{qt(pg_at)}')

        return f'{sql_func}({", ".join(args)})'

    def make_op(
        self,
        func: s_funcs.Function,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        or_replace: bool=False,
    ) -> Iterable[dbops.Command]:
        if func.get_from_expr(schema):
            # Intrinsic function, handled directly by the compiler.
            return ()
        elif sql_func := func.get_from_function(schema):
            func_params = func.get_params(schema)

            if (
                func.get_force_return_cast(schema)
                or func_params.has_polymorphic(schema)
                or func.get_sql_func_has_out_params(schema)
            ):
                return ()
            else:
                # Function backed directly by an SQL function.
                # Check the consistency of the return type.
                dexpr = self.get_dummy_func_call(func, sql_func, schema)
                return (
                    self.sql_rval_consistency_check(func, dexpr, schema),
                    self.sql_strict_consistency_check(func, sql_func, schema),
                )
        else:
            func_language = func.get_language(schema)

            if func_language is ql_ast.Language.SQL:
                dbf = self.compile_sql_function(func, schema)
            elif func_language is ql_ast.Language.EdgeQL:
                dbf, overload_replace = self.compile_edgeql_function(
                    func, schema, context)
                if overload_replace:
                    or_replace = True
            else:
                raise errors.QueryError(
                    f'cannot compile function {func.get_shortname(schema)}: '
                    f'unsupported language {func_language}',
                    context=self.source_context)

            op = dbops.CreateFunction(dbf, or_replace=or_replace)
            return (op,)


class CreateFunction(
    FunctionCommand,
    adapts=s_funcs.CreateFunction,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        self.pgops.update(self.make_op(self.scls, schema, context))
        return schema


class RenameFunction(FunctionCommand, adapts=s_funcs.RenameFunction):
    pass


class AlterFunction(FunctionCommand, adapts=s_funcs.AlterFunction):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        if self.metadata_only:
            return schema

        if (
            self.get_attribute_value('volatility') is not None or
            self.get_attribute_value('nativecode') is not None
        ):
            self.pgops.update(
                self.make_op(self.scls, schema, context, or_replace=True))

        return schema


class DeleteFunction(FunctionCommand, adapts=s_funcs.DeleteFunction):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        func = self.get_object(schema, context)
        nativecode = func.get_nativecode(schema)

        if func.get_code(schema) or nativecode:
            # An EdgeQL or a SQL function
            # (not just an alias to a SQL function).

            overload = False
            if nativecode and func.find_object_param_overloads(schema):
                dbf, overload_replace = self.compile_edgeql_function(
                    func, schema, context)
                if overload_replace:
                    self.pgops.add(dbops.CreateFunction(dbf, or_replace=True))
                    overload = True

            if not overload:
                variadic = func.get_params(schema).find_variadic(schema)
                self.pgops.add(
                    dbops.DropFunction(
                        name=self.get_pgname(func, schema),
                        args=self.compile_args(func, schema),
                        has_variadic=variadic is not None,
                    )
                )

        return super().apply(schema, context)


class OperatorCommand(FunctionCommand):

    def oper_name_to_pg_name(
        self,
        schema,
        name: sn.QualName,
    ) -> Tuple[str, str]:
        return common.get_operator_backend_name(
            name, catenate=False)

    def get_pg_operands(self, schema, oper: s_opers.Operator):
        left_type = None
        right_type = None
        oper_params = list(oper.get_params(schema).objects(schema))
        oper_kind = oper.get_operator_kind(schema)

        if oper_kind is ql_ft.OperatorKind.Infix:
            left_type = types.pg_type_from_object(
                schema, oper_params[0].get_type(schema))

            right_type = types.pg_type_from_object(
                schema, oper_params[1].get_type(schema))

        elif oper_kind is ql_ft.OperatorKind.Prefix:
            right_type = types.pg_type_from_object(
                schema, oper_params[0].get_type(schema))

        elif oper_kind is ql_ft.OperatorKind.Postfix:
            left_type = types.pg_type_from_object(
                schema, oper_params[0].get_type(schema))

        else:
            raise RuntimeError(
                f'unexpected operator type: {oper.get_type(schema)!r}')

        return left_type, right_type

    def compile_args(self, oper: s_opers.Operator, schema):
        args = []
        oper_params = oper.get_params(schema)

        for param in oper_params.get_in_canonical_order(schema):
            pg_at = self.get_pgtype(oper, param.get_type(schema), schema)
            args.append((param.get_parameter_name(schema), pg_at))

        return args

    def make_operator_function(self, oper: s_opers.Operator, schema):
        return dbops.Function(
            name=common.get_backend_name(
                schema, oper, catenate=False, aspect='function'),
            args=self.compile_args(oper, schema),
            volatility=oper.get_volatility(schema),
            returns=self.get_pgtype(
                oper, oper.get_return_type(schema), schema),
            text=oper.get_code(schema))

    def get_dummy_operator_call(
        self,
        oper: s_opers.Operator,
        pgop: str,
        from_args: Tuple[Tuple[str, ...], ...],
        schema: s_schema.Schema,
    ) -> str:
        # Need a proxy function with casts
        oper_kind = oper.get_operator_kind(schema)

        if oper_kind is ql_ft.OperatorKind.Infix:
            op = f'NULL::{qt(from_args[0])} {pgop} NULL::{qt(from_args[1])}'
        elif oper_kind is ql_ft.OperatorKind.Postfix:
            op = f'NULL::{qt(from_args[0])} {pgop}'
        elif oper_kind is ql_ft.OperatorKind.Prefix:
            op = f'{pgop} NULL::{qt(from_args[1])}'
        else:
            raise RuntimeError(f'unexpected operator kind: {oper_kind!r}')

        return op


class CreateOperator(OperatorCommand, adapts=s_opers.CreateOperator):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        oper = self.scls
        if oper.get_abstract(schema):
            return schema

        params = oper.get_params(schema)
        oper_language = oper.get_language(schema)
        oper_fromop = oper.get_from_operator(schema)
        oper_fromfunc = oper.get_from_function(schema)
        oper_code = oper.get_code(schema)
        oper_comm = oper.get_commutator(schema)
        if oper_comm:
            commutator = self.oper_name_to_pg_name(schema, oper_comm)
        else:
            commutator = None
        oper_neg = oper.get_negator(schema)
        if oper_neg:
            negator = self.oper_name_to_pg_name(schema, oper_neg)
        else:
            negator = None

        if oper_language is ql_ast.Language.SQL and oper_fromop:
            pg_oper_name = oper_fromop[0]
            args = self.get_pg_operands(schema, oper)
            if len(oper_fromop) > 1:
                # Explicit operand types given in FROM SQL OPERATOR.
                from_args = oper_fromop[1:]
            else:
                from_args = args

            if oper_code:
                oper_func = self.make_operator_function(oper, schema)
                self.pgops.add(dbops.CreateFunction(oper_func))
                oper_func_name = common.qname(*oper_func.name)

            elif oper_fromfunc:
                oper_func_name = oper_fromfunc[0]
                if len(oper_fromfunc) > 1:
                    from_args = oper_fromfunc[1:]

            elif from_args != args:
                # Need a proxy function with casts
                oper_kind = oper.get_operator_kind(schema)

                if oper_kind is ql_ft.OperatorKind.Infix:
                    op = (f'$1::{from_args[0]} {pg_oper_name} '
                          f'$2::{from_args[1]}')
                elif oper_kind is ql_ft.OperatorKind.Postfix:
                    op = f'$1::{from_args[0]} {pg_oper_name}'
                elif oper_kind is ql_ft.OperatorKind.Prefix:
                    op = f'{pg_oper_name} $1::{from_args[1]}'
                else:
                    raise RuntimeError(
                        f'unexpected operator kind: {oper_kind!r}')

                rtype = self.get_pgtype(
                    oper, oper.get_return_type(schema), schema)

                oper_func = dbops.Function(
                    name=common.get_backend_name(
                        schema, oper, catenate=False, aspect='function'),
                    args=[(None, a) for a in args if a],
                    volatility=oper.get_volatility(schema),
                    returns=rtype,
                    text=f'SELECT ({op})::{qt(rtype)}',
                )

                self.pgops.add(dbops.CreateFunction(oper_func))
                oper_func_name = common.qname(*oper_func.name)

            else:
                oper_func_name = None

            if (
                pg_oper_name is not None
                and not params.has_polymorphic(schema)
                or all(
                    p.get_type(schema).is_array()
                    for p in params.objects(schema)
                )
            ):
                self.pgops.add(dbops.CreateOperatorAlias(
                    name=common.get_backend_name(schema, oper, catenate=False),
                    args=args,
                    procedure=oper_func_name,
                    base_operator=('pg_catalog', pg_oper_name),
                    operator_args=from_args,
                    commutator=commutator,
                    negator=negator,
                ))

                if not params.has_polymorphic(schema):
                    if oper_func_name is not None:
                        cexpr = self.get_dummy_func_call(
                            oper, oper_func_name, schema)
                    else:
                        cexpr = self.get_dummy_operator_call(
                            oper, pg_oper_name, from_args, schema)

                    # We don't do a strictness consistency check for
                    # USING SQL OPERATOR because they are heavily
                    # overloaded, and so we'd need to take the types
                    # into account; this is doable, but doesn't seem
                    # worth doing since the only non-strict operator
                    # is || on arrays, and we use array_cat for that
                    # anyway!
                    check = self.sql_rval_consistency_check(
                        oper, cexpr, schema)
                    self.pgops.add(check)
            elif oper_func_name is not None:
                self.pgops.add(dbops.CreateOperator(
                    name=common.get_backend_name(schema, oper, catenate=False),
                    args=from_args,
                    procedure=oper_func_name,
                ))

        elif oper_language is ql_ast.Language.SQL and oper_code:
            args = self.get_pg_operands(schema, oper)
            oper_func = self.make_operator_function(oper, schema)
            self.pgops.add(dbops.CreateFunction(oper_func))
            oper_func_name = common.qname(*oper_func.name)

            self.pgops.add(dbops.CreateOperator(
                name=common.get_backend_name(schema, oper, catenate=False),
                args=args,
                procedure=oper_func_name,
            ))

            if not params.has_polymorphic(schema):
                cexpr = self.get_dummy_func_call(
                    oper, q(*oper_func.name), schema)
                check = self.sql_rval_consistency_check(oper, cexpr, schema)
                self.pgops.add(check)

        elif oper_language is ql_ast.Language.SQL and oper_fromfunc:
            args = self.get_pg_operands(schema, oper)
            oper_func_name = oper_fromfunc[0]
            if len(oper_fromfunc) > 1:
                args = oper_fromfunc[1:]

            cargs = []
            for t in args:
                if t is not None:
                    cargs.append(f'NULL::{qt(t)}')

            if not params.has_polymorphic(schema):
                cexpr = f"{qi(oper_func_name)}({', '.join(cargs)})"
                check = self.sql_rval_consistency_check(oper, cexpr, schema)
                self.pgops.add(check)
            check2 = self.sql_strict_consistency_check(
                oper, oper_func_name, schema)
            self.pgops.add(check2)

        elif oper.get_from_expr(schema):
            # This operator is handled by the compiler and does not
            # need explicit representation in the backend.
            pass

        else:
            raise errors.QueryError(
                f'cannot create operator {oper.get_shortname(schema)}: '
                f'only "FROM SQL" and "FROM SQL OPERATOR" operators '
                f'are currently supported',
                context=self.source_context)

        return schema


class RenameOperator(OperatorCommand, adapts=s_opers.RenameOperator):
    pass


class AlterOperator(OperatorCommand, adapts=s_opers.AlterOperator):
    pass


class DeleteOperator(OperatorCommand, adapts=s_opers.DeleteOperator):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        oper = schema.get(self.classname)

        if oper.get_abstract(schema):
            return super().apply(schema, context)

        name = common.get_backend_name(schema, oper, catenate=False)
        args = self.get_pg_operands(schema, oper)

        schema = super().apply(schema, context)
        if not oper.get_from_expr(orig_schema):
            self.pgops.add(dbops.DropOperator(name=name, args=args))
        return schema


class CastCommand(MetaCommand):
    def make_cast_function(self, cast: s_casts.Cast, schema):
        name = common.get_backend_name(
            schema, cast, catenate=False, aspect='function')

        args = [(
            'val',
            types.pg_type_from_object(schema, cast.get_from_type(schema))
        )]

        returns = types.pg_type_from_object(schema, cast.get_to_type(schema))

        # N.B: Semantically, strict *ought* to be true, since we want
        # all of our casts to have strict behavior. Unfortunately,
        # actually marking them as strict causes a huge performance
        # regression when bootstrapping (and probably anything else that
        # is heavy on json casts), so instead we just need to make sure
        # to write cast code that is naturally strict (this is enforced
        # by test_edgeql_casts_all_null).
        return dbops.Function(
            name=name,
            args=args,
            returns=returns,
            strict=False,
            text=cast.get_code(schema),
        )


class CreateCast(CastCommand, adapts=s_casts.CreateCast):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        cast = self.scls
        cast_language = cast.get_language(schema)
        cast_code = cast.get_code(schema)
        from_cast = cast.get_from_cast(schema)
        from_expr = cast.get_from_expr(schema)

        if cast_language is ql_ast.Language.SQL and cast_code:
            cast_func = self.make_cast_function(cast, schema)
            self.pgops.add(dbops.CreateFunction(cast_func))

        elif from_cast is not None or from_expr is not None:
            # This operator is handled by the compiler and does not
            # need explicit representation in the backend.
            pass

        else:
            raise errors.QueryError(
                f'cannot create cast: '
                f'only "FROM SQL" and "FROM SQL FUNCTION" casts '
                f'are currently supported',
                context=self.source_context)

        return schema


class RenameCast(CastCommand, adapts=s_casts.RenameCast):
    pass


class AlterCast(CastCommand, adapts=s_casts.AlterCast):
    pass


class DeleteCast(CastCommand, adapts=s_casts.DeleteCast):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        cast = schema.get(self.classname)
        cast_language = cast.get_language(schema)
        cast_code = cast.get_code(schema)

        schema = super().apply(schema, context)

        if cast_language is ql_ast.Language.SQL and cast_code:
            cast_func = self.make_cast_function(cast, schema)
            self.pgops.add(dbops.DropFunction(
                cast_func.name, cast_func.args))

        return schema


class AnnotationCommand(MetaCommand):
    pass


class CreateAnnotation(AnnotationCommand, adapts=s_anno.CreateAnnotation):
    pass


class RenameAnnotation(AnnotationCommand, adapts=s_anno.RenameAnnotation):
    pass


class AlterAnnotation(AnnotationCommand, adapts=s_anno.AlterAnnotation):
    pass


class DeleteAnnotation(AnnotationCommand, adapts=s_anno.DeleteAnnotation):
    pass


class AnnotationValueCommand(MetaCommand):
    pass


class CreateAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.CreateAnnotationValue,
):
    pass


class AlterAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.AlterAnnotationValue,
):
    pass


class AlterAnnotationValueOwned(
    AnnotationValueCommand,
    adapts=s_anno.AlterAnnotationValueOwned,
):
    pass


class RenameAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.RenameAnnotationValue,
):
    pass


class RebaseAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.RebaseAnnotationValue,
):
    pass


class DeleteAnnotationValue(
    AnnotationValueCommand,
    adapts=s_anno.DeleteAnnotationValue,
):
    pass


class ConstraintCommand(MetaCommand):
    @classmethod
    def constraint_is_effective(cls, schema, constraint):
        subject = constraint.get_subject(schema)
        if subject is None:
            return False

        ancestors = [
            a for a in constraint.get_ancestors(schema).objects(schema)
            if not a.generic(schema)
        ]

        if (
            constraint.get_delegated(schema)
            and all(ancestor.get_delegated(schema) for ancestor in ancestors)
        ):
            return False

        elif isinstance(subject, s_pointers.Pointer):
            if subject.generic(schema):
                return True
            else:
                return has_table(subject.get_source(schema), schema)
        elif isinstance(subject, s_objtypes.ObjectType):
            return has_table(subject, schema)
        else:
            return True

    @classmethod
    def fixup_base_constraint_triggers(
        cls, constraint, orig_schema, schema, context,
        source_context=None, *, is_delete
    ):
        base_schema = orig_schema if is_delete else schema

        # When a constraint is added or deleted, we need to check its
        # parents and potentially enable/disable their triggers
        # (since we want to disable triggers on types without
        # parents or children affected by the constraint)
        op = dbops.CommandGroup()
        for base in constraint.get_bases(base_schema).objects(base_schema):
            if (
                schema.has_object(base.id)
                and cls.constraint_is_effective(schema, base)
                and (base.is_independent(orig_schema)
                     != base.is_independent(schema))
                and not context.is_creating(base)
                and not context.is_deleting(base)
            ):
                subject = base.get_subject(schema)
                schemac_to_backendc = \
                    schemamech.ConstraintMech.\
                    schema_constraint_to_backend_constraint
                bconstr = schemac_to_backendc(
                    subject, base, schema, context, source_context)
                op.add_command(bconstr.alter_ops(
                    bconstr, only_modify_enabled=True))

        return op

    @classmethod
    def create_constraint(
            cls, constraint, schema, context, source_context=None):
        op = dbops.CommandGroup()
        if cls.constraint_is_effective(schema, constraint):
            subject = constraint.get_subject(schema)

            if subject is not None:
                schemac_to_backendc = \
                    schemamech.ConstraintMech.\
                    schema_constraint_to_backend_constraint
                bconstr = schemac_to_backendc(
                    subject, constraint, schema, context,
                    source_context)

                op.add_command(bconstr.create_ops())

        return op

    @classmethod
    def delete_constraint(
            cls, constraint, schema, context, source_context=None):
        op = dbops.CommandGroup()
        if cls.constraint_is_effective(schema, constraint):
            subject = constraint.get_subject(schema)

            if subject is not None:
                schemac_to_backendc = \
                    schemamech.ConstraintMech.\
                    schema_constraint_to_backend_constraint
                bconstr = schemac_to_backendc(
                    subject, constraint, schema, context,
                    source_context)

                op.add_command(bconstr.delete_ops())

        return op

    @classmethod
    def enforce_constraint(
            cls, constraint, schema, context, source_context=None):

        if cls.constraint_is_effective(schema, constraint):
            subject = constraint.get_subject(schema)

            if subject is not None:
                schemac_to_backendc = \
                    schemamech.ConstraintMech.\
                    schema_constraint_to_backend_constraint
                bconstr = schemac_to_backendc(
                    subject, constraint, schema, context,
                    source_context)

                return bconstr.enforce_ops()
        else:
            return dbops.CommandGroup()


class CreateConstraint(ConstraintCommand, adapts=s_constr.CreateConstraint):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)
        constraint = self.scls

        if constraint.get_subject(schema) in context.external_objs:
            return schema

        op = self.create_constraint(
            constraint, schema, context, self.source_context)
        self.pgops.add(op)
        self.pgops.add(self.fixup_base_constraint_triggers(
            constraint, orig_schema, schema, context, self.source_context,
            is_delete=False))

        # If the constraint is being added to existing data,
        # we need to enforce it on the existing data. (This only
        # matters when inheritance is in play and we use triggers
        # to enforce exclusivity across tables.)
        if (
            (subject := constraint.get_subject(schema))
            and isinstance(
                subject, (s_objtypes.ObjectType, s_pointers.Pointer))
            and not context.is_creating(subject)
        ):
            op = self.enforce_constraint(
                constraint, schema, context, self.source_context)
            self.schedule_post_inhview_update_command(
                schema, context, op, s_sources.SourceCommandContext)

        return schema


class RenameConstraint(ConstraintCommand, adapts=s_constr.RenameConstraint):
    pass


class AlterConstraintOwned(
    ConstraintCommand,
    adapts=s_constr.AlterConstraintOwned,
):
    pass


class AlterConstraint(
    ConstraintCommand,
    adapts=s_constr.AlterConstraint,
):
    def apply(self, schema, context):
        orig_schema = schema
        schema = super().apply(schema, context)
        constraint = self.scls
        if self.metadata_only:
            return schema
        if (
            not self.constraint_is_effective(schema, constraint)
            and not self.constraint_is_effective(orig_schema, constraint)
        ):
            return schema

        subject = constraint.get_subject(schema)

        subcommands = list(self.get_subcommands())
        if (not subcommands or
                isinstance(subcommands[0], s_constr.RenameConstraint)):
            # This is a pure rename, so everything had been handled by
            # RenameConstraint above.
            return schema

        if subject is not None:
            schemac_to_backendc = \
                schemamech.ConstraintMech.\
                schema_constraint_to_backend_constraint

            bconstr = schemac_to_backendc(
                subject, constraint, schema, context, self.source_context)

            orig_bconstr = schemac_to_backendc(
                constraint.get_subject(orig_schema),
                constraint,
                orig_schema,
                context,
                self.source_context,
            )

            op = dbops.CommandGroup()
            if not self.constraint_is_effective(orig_schema, constraint):
                op.add_command(bconstr.create_ops())

                # XXX: I don't think any of this logic is needed??
                for child in constraint.children(schema):
                    orig_cbconstr = schemac_to_backendc(
                        child.get_subject(orig_schema),
                        child,
                        orig_schema,
                        context,
                        self.source_context,
                    )
                    cbconstr = schemac_to_backendc(
                        child.get_subject(schema),
                        child,
                        schema,
                        context,
                        self.source_context,
                    )
                    op.add_command(cbconstr.alter_ops(orig_cbconstr))
            elif not self.constraint_is_effective(schema, constraint):
                op.add_command(bconstr.alter_ops(orig_bconstr))

                for child in constraint.children(schema):
                    orig_cbconstr = schemac_to_backendc(
                        child.get_subject(orig_schema),
                        child,
                        orig_schema,
                        context,
                        self.source_context,
                    )
                    cbconstr = schemac_to_backendc(
                        child.get_subject(schema),
                        child,
                        schema,
                        context,
                        self.source_context,
                    )
                    op.add_command(cbconstr.alter_ops(orig_cbconstr))
            else:
                op.add_command(bconstr.alter_ops(orig_bconstr))
            self.pgops.add(op)

            if (
                (subject := constraint.get_subject(schema))
                and isinstance(
                    subject, (s_objtypes.ObjectType, s_pointers.Pointer))
                and not context.is_creating(subject)
            ):
                op = self.enforce_constraint(
                    constraint, schema, context, self.source_context)
                self.pgops.add(op)

            self.pgops.add(self.fixup_base_constraint_triggers(
                constraint, orig_schema, schema, context, self.source_context,
                is_delete=False))

        return schema


class DeleteConstraint(ConstraintCommand, adapts=s_constr.DeleteConstraint):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        delta_root_ctx = context.top()
        orig_schema = delta_root_ctx.original_schema
        constraint = schema.get(self.classname)
        is_external = constraint.get_subject(schema).get_external(schema)

        schema = super().apply(schema, context)

        if is_external:
            return schema

        op = self.delete_constraint(
            constraint, orig_schema, context, self.source_context)
        self.pgops.add(op)

        self.pgops.add(self.fixup_base_constraint_triggers(
            constraint, orig_schema, schema, context, self.source_context,
            is_delete=True))

        return schema


class RebaseConstraint(ConstraintCommand, adapts=s_constr.RebaseConstraint):
    pass


class AliasCapableMetaCommand(MetaCommand):
    pass


class ScalarTypeMetaCommand(AliasCapableMetaCommand):

    @classmethod
    def is_sequence(cls, schema, scalar):
        seq = schema.get('std::sequence', default=None)
        return seq is not None and scalar.issubclass(schema, seq)


class CreateScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.CreateScalarType):

    @classmethod
    def create_scalar(cls, scalar, default, schema, context):
        ops = dbops.CommandGroup()

        new_domain_name = types.pg_type_from_scalar(schema, scalar)

        if scalar.is_concrete_enum(schema):
            enum_values = scalar.get_enum_values(schema)
            new_enum_name = common.get_backend_name(
                schema, scalar, catenate=False)
            ops.add_command(dbops.CreateEnum(
                dbops.Enum(name=new_enum_name, values=enum_values)))
            base = q(*new_enum_name)

        else:
            base = types.get_scalar_base(schema, scalar)

            if cls.is_sequence(schema, scalar):
                seq_name = common.get_backend_name(
                    schema, scalar, catenate=False, aspect='sequence')
                ops.add_command(dbops.CreateSequence(name=seq_name))

            domain = dbops.Domain(name=new_domain_name, base=base)
            ops.add_command(dbops.CreateDomain(domain=domain))

            if (default is not None
                    and not isinstance(default, s_expr.Expression)):
                # We only care to support literal defaults here. Supporting
                # defaults based on queries has no sense on the database
                # level since the database forbids queries for DEFAULT and
                # pre- calculating the value does not make sense either
                # since the whole point of query defaults is for them to be
                # dynamic.
                ops.add_command(
                    dbops.AlterDomainAlterDefault(
                        name=new_domain_name, default=default))

        return ops

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        scalar = self.scls

        if scalar.get_abstract(schema):
            return schema

        if types.is_builtin_scalar(schema, scalar):
            return schema

        default = self.get_resolved_attribute_value(
            'default',
            schema=schema,
            context=context,
        )
        self.pgops.add(self.create_scalar(scalar, default, schema, context))

        return schema


class RenameScalarType(
    ScalarTypeMetaCommand,
    adapts=s_scalars.RenameScalarType,
):
    pass


class RebaseScalarType(
    ScalarTypeMetaCommand,
    adapts=s_scalars.RebaseScalarType,
):
    # Actual rebase is taken care of in AlterScalarType
    pass


class AlterScalarType(ScalarTypeMetaCommand, adapts=s_scalars.AlterScalarType):

    problematic_refs: Optional[Tuple[
        Tuple[so.Object, ...],
        List[Tuple[s_props.Property, s_types.TypeShell]],
    ]]

    def _get_problematic_refs(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        composite_only: bool,
    ) -> Optional[Tuple[
        Tuple[so.Object, ...],
        Dict[s_props.Property, s_types.TypeShell],
    ]]:
        """Find problematic references to this scalar type that need handled.

        This is used to work around two irritating limitations of Postgres:
          1. That elements of enum types may not be removed or reordered
          2. That a constraint may not be added to a domain type if that
             domain type appears in a *composite* type that is used in a
             column somewhere.

        We don't want to have these limitations, and we need to do a decent
        amount of work to work around them.

        1. Find all of the affected properties. For case 2, this is any
           property whose type is a container type that contains this
           scalar. (Possibly transitively.) For case 1, the container type
           restriction is dropped.
        2. Change the type of all offending properties to an equivalent type
           that does not reference this scalar. This may require creating
           new types. (See _undo_everything.)
        3. Add the constraint.
        4. Restore the type of all offending properties. If existing data
           violates the new constraint, we will fail here. Delete any
           temporarily created types. (See _redo_everything.)

        Somewhat hackily, _undo_everything and _redo_everything
        operate by creating new schema delta command objects, and
        adapting and applying them. This is the most straightforward
        way to perform the high-level operations needed here.

        I've kept this code in pgsql/delta instead of trying to put in
        schema/delta because it is pretty aggressively an irritating
        pgsql implementation detail and because I didn't want it to
        have to interact with ordering ever.

        This function finds all of the relevant properties and returns
        a list of them along with the appropriate replacement type.

        In case 1, it also finds other referencing objects which need
        to be deleted and then recreated.
        """

        seen_props = set()
        seen_other = set()

        typ = self.scls
        typs = [typ]
        # Do a worklist driven search for properties that refer to this scalar
        # through a collection type. We search backwards starting from
        # referring collection types or from all refs, depending on
        # composite_only.
        scls_type = s_types.Collection if composite_only else None
        wl = list(schema.get_referrers(typ, scls_type=scls_type))
        while wl:
            obj = wl.pop()
            if isinstance(obj, s_props.Property):
                seen_props.add(obj)
            elif isinstance(obj, s_scalars.ScalarType) and not composite_only:
                wl.extend(schema.get_referrers(obj))
                seen_other.add(obj)
                typs.append(obj)
            elif isinstance(obj, s_types.Collection):
                wl.extend(schema.get_referrers(obj))
            elif isinstance(obj, s_funcs.Parameter) and not composite_only:
                wl.extend(schema.get_referrers(obj))
            elif isinstance(obj, s_funcs.Function) and not composite_only:
                wl.extend(schema.get_referrers(obj))
                seen_other.add(obj)
            elif isinstance(obj, s_constr.Constraint) and not composite_only:
                seen_other.add(obj)
            elif isinstance(obj, s_indexes.Index) and not composite_only:
                seen_other.add(obj)

        if not seen_props and not seen_other:
            return None

        props = {}
        if seen_props:
            type_substs = {}
            for typ in typs:
                # Find a concrete ancestor to substitute in.
                if typ.is_enum(schema):
                    ancestor = schema.get(sn.QualName('std', 'str'))
                else:
                    for ancestor in typ.get_ancestors(schema).objects(schema):
                        if not ancestor.get_abstract(schema):
                            break
                    else:
                        raise AssertionError(
                            "can't find concrete base for scalar")
                type_substs[typ.get_name(schema)] = ancestor.as_shell(schema)

            props = {
                prop:
                s_utils.type_shell_multi_substitute(
                    type_substs,
                    prop.get_target(schema).as_shell(schema))
                for prop in seen_props
            }

        objs = sd.sort_by_cross_refs(schema, seen_props | seen_other)

        return objs, props

    def _undo_everything(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        objs: Tuple[so.Object, ...],
        props: Dict[s_props.Property, s_types.TypeShell],
    ) -> s_schema.Schema:
        """Rewrite the type of everything that uses this scalar dangerously.

        See _get_problematic_refs above for details.
        """

        # First we need to strip out any default value that might reference
        # one of the functions we are going to delete.
        # We also create any new types, in this pass.
        cmd = sd.DeltaRoot()
        for prop, new_typ in props.items():
            try:
                cmd.add(new_typ.as_create_delta(schema))
            except errors.UnsupportedFeatureError:
                pass

            if prop.get_default(schema):
                delta_alter, cmd_alter, alter_context = prop.init_delta_branch(
                    schema, context, cmdtype=sd.AlterObject)
                cmd_alter.set_attribute_value('default', None)
                cmd.add(delta_alter)

        cmd.apply(schema, context)
        acmd = CommandMeta.adapt(cmd)
        schema = acmd.apply(schema, context)
        self.pgops.update(acmd.get_subcommands())

        # Now process all the objects in the appropriate order
        for obj in objs:
            if isinstance(obj, s_funcs.Function):
                # Force function deletions at the SQL level without ever
                # bothering to remove them from our schema.
                fc = FunctionCommand()
                variadic = obj.get_params(schema).find_variadic(schema)
                self.pgops.add(
                    dbops.DropFunction(
                        name=fc.get_pgname(obj, schema),
                        args=fc.compile_args(obj, schema),
                        has_variadic=variadic is not None,
                    )
                )
            elif isinstance(obj, s_constr.Constraint):
                self.pgops.add(
                    ConstraintCommand.delete_constraint(obj, schema, context))
            elif isinstance(obj, s_indexes.Index):
                self.pgops.add(DeleteIndex.delete_index(obj, schema, context))
            elif isinstance(obj, s_scalars.ScalarType):
                self.pgops.add(DeleteScalarType.delete_scalar(
                    obj, schema, context))
            elif isinstance(obj, s_props.Property):
                new_typ = props[obj]

                delta_alter, cmd_alter, alter_context = obj.init_delta_branch(
                    schema, context, cmdtype=sd.AlterObject)
                cmd_alter.set_attribute_value('target', new_typ)
                cmd_alter.set_attribute_value('default', None)

                delta_alter.apply(schema, context)
                acmd = CommandMeta.adapt(delta_alter)
                schema = acmd.apply(schema, context)
                self.pgops.add(acmd)

        return schema

    def _redo_everything(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        objs: Tuple[so.Object, ...],
        props: Dict[s_props.Property, s_types.TypeShell],
    ) -> s_schema.Schema:
        """Restore the type of everything that uses this scalar dangerously.

        See _get_problematic_refs above for details.
        """

        for obj in reversed(objs):
            if isinstance(obj, s_funcs.Function):
                # Super hackily recreate the functions
                fc = CreateFunction(classname=obj.get_name(schema))
                for f in ('language', 'params', 'return_type'):
                    fc.set_attribute_value(f, obj.get_field_value(schema, f))
                self.pgops.update(fc.make_op(obj, schema, context))
            elif isinstance(obj, s_constr.Constraint):
                self.pgops.add(
                    ConstraintCommand.create_constraint(obj, schema, context))
            elif isinstance(obj, s_indexes.Index):
                self.pgops.add(
                    CreateIndex.create_index(obj, orig_schema, context))
            elif isinstance(obj, s_scalars.ScalarType):
                self.pgops.add(
                    CreateScalarType.create_scalar(
                        obj, obj.get_default(schema), orig_schema, context))
            elif isinstance(obj, s_props.Property):
                new_typ = props[obj]

                delta_alter, cmd_alter, _ = obj.init_delta_branch(
                    schema, context, cmdtype=sd.AlterObject)
                cmd_alter.set_attribute_value(
                    'target', obj.get_target(orig_schema))

                delta_alter.apply(schema, context)
                acmd = CommandMeta.adapt(delta_alter)
                schema = acmd.apply(schema, context)
                self.pgops.add(acmd)

        # Restore defaults and prune newly created types
        cmd = sd.DeltaRoot()
        for prop, new_typ in props.items():
            rnew_typ = new_typ.resolve(schema)
            if delete := rnew_typ.as_type_delete_if_dead(schema):
                cmd.add_caused(delete)

            delta_alter, cmd_alter, _ = prop.init_delta_branch(
                schema, context, cmdtype=sd.AlterObject)
            cmd_alter.set_attribute_value(
                'default', prop.get_default(orig_schema))
            cmd.add(delta_alter)

        # do an apply of the schema-level command to force it to canonicalize,
        # which prunes out duplicate deletions
        cmd.apply(schema, context)

        for sub in cmd.get_subcommands():
            acmd = CommandMeta.adapt(sub)
            schema = acmd.apply(schema, context)
            self.pgops.add(acmd)

        return schema

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        new_scalar = self.scls

        has_create_constraint = bool(
            list(self.get_subcommands(type=s_constr.CreateConstraint)))
        has_rebase = bool(
            list(self.get_subcommands(type=s_scalars.RebaseScalarType)))

        old_enum_values = new_scalar.get_enum_values(orig_schema) or []

        if has_rebase and old_enum_values:
            # Ugly hack alert: we need to do this "lookahead" rebase
            # apply to get the list of new enum values to decide
            # whether special handling is needed _before_ the actual
            # _alter_innards() takes place, because we are also handling
            # domain constraints here.  TODO: a cleaner way to handle this
            # would be to move this logic into actual subcomands
            # (RebaseScalarType and CreateConstraint).
            rebased_schema = super()._alter_innards(schema, context)
            new_enum_values = new_scalar.get_enum_values(rebased_schema) or []
        else:
            new_enum_values = old_enum_values

        # If values were deleted or reordered, we need to drop the enum
        # and recreate it.
        needs_recreate = (
            old_enum_values != new_enum_values
            and old_enum_values != new_enum_values[:len(old_enum_values)])

        self.problematic_refs = None
        if needs_recreate or has_create_constraint:
            self.problematic_refs = self._get_problematic_refs(
                schema, context, composite_only=not needs_recreate)
            if self.problematic_refs:
                objs, props = self.problematic_refs
                schema = self._undo_everything(schema, context, objs, props)

        if new_enum_values:
            type_name = common.get_backend_name(
                schema, new_scalar, catenate=False)

            if needs_recreate:
                self.pgops.add(
                    dbops.DropEnum(name=type_name))
                self.pgops.add(dbops.CreateEnum(
                    dbops.Enum(name=type_name, values=new_enum_values)))

            elif old_enum_values != new_enum_values:
                old_idx = 0
                old_enum_values = list(old_enum_values)
                for v in new_enum_values:
                    if old_idx >= len(old_enum_values):
                        self.pgops.add(
                            dbops.AlterEnumAddValue(
                                type_name, v,
                            )
                        )
                    elif v != old_enum_values[old_idx]:
                        self.pgops.add(
                            dbops.AlterEnumAddValue(
                                type_name, v, before=old_enum_values[old_idx],
                            )
                        )
                        old_enum_values.insert(old_idx, v)
                    else:
                        old_idx += 1

        default_delta = self.get_resolved_attribute_value(
            'default',
            schema=schema,
            context=context,
        )
        if default_delta:
            if (default_delta is None or
                    isinstance(default_delta, s_expr.Expression)):
                new_default = None
            else:
                new_default = default_delta

            domain_name = common.get_backend_name(
                schema, new_scalar, catenate=False)
            adad = dbops.AlterDomainAlterDefault(
                name=domain_name, default=new_default)
            self.pgops.add(adad)

        return schema

    def _alter_finalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_finalize(schema, context)
        if self.problematic_refs:
            objs, props = self.problematic_refs
            schema = self._redo_everything(
                schema,
                context.current().original_schema,
                context,
                objs,
                props,
            )

        return schema


class DeleteScalarType(ScalarTypeMetaCommand,
                       adapts=s_scalars.DeleteScalarType):
    @classmethod
    def delete_scalar(cls, scalar, orig_schema, context):
        old_domain_name = common.get_backend_name(
            orig_schema, scalar, catenate=False)

        if scalar.is_concrete_enum(orig_schema):
            old_enum_name = common.get_backend_name(
                orig_schema, scalar, catenate=False)
            cond = dbops.EnumExists(old_enum_name)
            return dbops.DropEnum(name=old_enum_name, conditions=[cond])
        else:
            cond = dbops.DomainExists(old_domain_name)
            return dbops.DropDomain(name=old_domain_name, conditions=[cond])

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)
        scalar = self.scls

        link = None
        if context:
            link = context.get(s_links.LinkCommandContext)

        ops = link.op.pgops if link else self.pgops

        ops.add(self.delete_scalar(scalar, orig_schema, context))

        if self.is_sequence(orig_schema, scalar):
            seq_name = common.get_backend_name(
                orig_schema, scalar, catenate=False, aspect='sequence')
            self.pgops.add(dbops.DropSequence(name=seq_name))

        return schema


class CompositeMetaCommand(MetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table_name = None
        self._multicommands = {}
        self.update_search_indexes = None
        self.inhview_updates = set()
        self.post_inhview_update_commands = []

    def _get_multicommand(
            self, context, cmdtype, object_name, *,
            force_new=False, manual=False, cmdkwargs=None):
        if cmdkwargs is None:
            cmdkwargs = {}
        key = (object_name, frozenset(cmdkwargs.items()))

        try:
            typecommands = self._multicommands[cmdtype]
        except KeyError:
            typecommands = self._multicommands[cmdtype] = {}

        commands = typecommands.get(key)

        if commands is None or force_new or manual:
            command = cmdtype(object_name, **cmdkwargs)

            if not manual:
                try:
                    commands = typecommands[key]
                except KeyError:
                    commands = typecommands[key] = []

                commands.append(command)
        else:
            command = commands[-1]

        return command

    def _attach_multicommand(self, context, cmdtype):
        try:
            typecommands = self._multicommands[cmdtype]
        except KeyError:
            return
        else:
            commands = list(
                itertools.chain.from_iterable(typecommands.values()))

            if commands:
                self.pgops.update(commands)

    def get_alter_table(
            self, schema, context, force_new=False,
            contained=False, manual=False, table_name=None):

        tabname = table_name if table_name else self.table_name

        if not tabname:
            ctx = context.get(self.__class__)
            assert ctx
            tabname = common.get_backend_name(schema, ctx.scls, catenate=False)
            if table_name is None:
                self.table_name = tabname

        return self._get_multicommand(
            context, dbops.AlterTable, tabname,
            force_new=force_new, manual=manual,
            cmdkwargs={'contained': contained})

    def attach_alter_table(self, context):
        self._attach_multicommand(context, dbops.AlterTable)

    @classmethod
    def get_source_and_pointer_ctx(cls, schema, context):
        if context:
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)
            link = context.get(s_links.LinkCommandContext)
        else:
            objtype = link = None

        if objtype:
            source, pointer = objtype, link
        elif link:
            property = context.get(s_props.PropertyCommandContext)
            source, pointer = link, property
        else:
            source = pointer = None

        return source, pointer

    @classmethod
    def _get_select_from(
        cls,
        schema: s_schema.Schema,
        obj: s_sources.Source,
        ptrnames: Dict[sn.UnqualName, Tuple[str, Tuple[str, ...]]],
        pg_schema: Optional[str] = None,
    ) -> Optional[str]:
        if isinstance(obj, s_sources.Source):
            ptrs = dict(obj.get_pointers(schema).items(schema))

            cols = []

            for ptrname, (alias, pgtype) in ptrnames.items():
                ptr = ptrs.get(ptrname)
                if ptr is not None:
                    ptr_stor_info = types.get_pointer_storage_info(
                        ptr,
                        link_bias=isinstance(obj, s_links.Link),
                        schema=schema,
                    )
                    if ptr_stor_info.column_type != pgtype:
                        return None
                    cols.append((ptr_stor_info.column_name, alias, True))
                elif ptrname == sn.UnqualName('source'):
                    cols.append(('NULL::uuid', alias, False))
                else:
                    return None
        else:
            cols = [
                (ptrname, alias, True)
                for ptrname, (alias, _) in ptrnames.items()
            ]

        tabname = common.get_backend_name(
            schema,
            obj,
            catenate=False,
            aspect='table',
        )

        if pg_schema is not None:
            tabname = (pg_schema, tabname[1])

        talias = qi(tabname[1])

        coltext = ',\n'.join(
            f'{f"{talias}.{qi(col)}"} AS {qi(alias)}' if is_col else
            f'{col} AS {qi(alias)}'
            for col, alias, is_col in cols
        )

        return textwrap.dedent(f'''\
            (SELECT
               {coltext}
             FROM
               {q(*tabname)} AS {talias}
            )
        ''')

    @classmethod
    def get_inhview_boost(
        cls,
        inhview_name: str,
        schema: s_schema.Schema,
        obj: s_sources.Source,
        exclude_children: FrozenSet[s_sources.Source] = frozenset(),
        exclude_self: bool = False,
    ):
        descendants = (
            child for child in obj.descendants(schema)
            if has_table(child, schema) and child not in exclude_children
        )
        if not exclude_self:
            rels = itertools.chain([obj], descendants)
        else:
            rels = descendants

        get_schema_name = common.get_module_backend_name
        query = '\nUNION ALL\n'.join(
            '(SELECT "{rel}".id AS id, '
            '"{rel}".__type__ AS __type__ '
            'FROM {module}."{rel}" AS "{rel}")'.format(
                rel=rel.id,
                module=get_schema_name(rel.get_name(schema).get_module_name()),
            )
            for rel in rels
            if not rel.get_external(schema)
        )
        return dbops.View(
            name=inhview_name,
            query=query,
        )

    @classmethod
    def get_inhview(
        cls,
        schema: s_schema.Schema,
        obj: s_sources.Source,
        exclude_children: FrozenSet[s_sources.Source] = frozenset(),
        exclude_ptrs: FrozenSet[s_pointers.Pointer] = frozenset(),
        exclude_self: bool = False,
        pg_schema: Optional[str] = None,
        from_command: Optional[sd.Command] = None,
        context: Optional[sd.CommandContext] = None
    ) -> Optional[dbops.View]:
        inhview_name = common.get_backend_name(
            schema, obj, catenate=False, aspect='inhview')

        if pg_schema is not None:
            inhview_name = (pg_schema, inhview_name[1])

        if (
            context is not None
            and not context.stdmode
            and from_command is not None
            and obj.get_name(schema) in (
                sn.QualName('std', 'Object'),
                sn.QualName('std', 'BaseObject')
            )
        ):
            exclude = []
            if isinstance(from_command, s_links.DeleteLink):
                if from_command.get_displayname() != '__type__':
                    return
                exclude.append(from_command.scls.get_source(schema))

            elif isinstance(from_command, s_props.DeleteProperty):
                if from_command.get_displayname() != 'id':
                    return
                exclude.append(from_command.scls.get_source(schema))

            return cls.get_inhview_boost(
                inhview_name,
                schema=schema,
                obj=obj,
                exclude_children=frozenset(exclude),
                exclude_self=exclude_self
            )

        ptrs = {}

        if isinstance(obj, s_sources.Source):
            pointers = list(obj.get_pointers(schema).items(schema))
            # Sort by UUID timestamp for stable VIEW column order.
            pointers.sort(key=lambda p: p[1].id.time)

            for ptrname, ptr in pointers:
                if ptr in exclude_ptrs:
                    continue
                if ptr.is_pure_computable(schema):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    ptr,
                    link_bias=isinstance(obj, s_links.Link),
                    schema=schema,
                )
                if (
                    isinstance(obj, s_links.Link)
                    or ptr_stor_info.table_type == 'ObjectType'
                ):
                    ptrs[ptrname] = (
                        ptr_stor_info.column_name,
                        ptr_stor_info.column_type,
                    )

        else:
            # MULTI PROPERTY
            ptrs['source'] = ('source', 'uuid')
            lp_info = types.get_pointer_storage_info(
                obj,
                link_bias=True,
                schema=schema,
            )
            ptrs['target'] = ('target', lp_info.column_type)

        descendants = [
            child for child in obj.descendants(schema)
            if has_table(child, schema) and child not in exclude_children
        ]

        # Hackily force 'source' to appear in abstract links. We need
        # source present in the code we generate to enforce newly
        # created exclusive constraints across types.
        if (
            ptrs
            and isinstance(obj, s_links.Link)
            and sn.UnqualName('source') not in ptrs
            and obj.generic(schema)
        ):
            ptrs[sn.UnqualName('source')] = ('source', ('uuid',))

        components = []
        if not exclude_self:
            components.append(
                cls._get_select_from(schema, obj, ptrs, pg_schema))

        components.extend(
            cls._get_select_from(schema, child, ptrs, pg_schema)
            for child in descendants
        )

        query = '\nUNION ALL\n'.join(filter(None, components))

        return dbops.View(
            name=inhview_name,
            query=query,
        )

    def update_base_inhviews_on_rebase(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        obj: s_sources.Source,
    ) -> None:
        bases = set(obj.get_bases(schema).objects(schema))
        orig_bases = set(obj.get_bases(orig_schema).objects(orig_schema))

        for new_base in bases - orig_bases:
            if has_table(new_base, schema):
                self.alter_inhview(schema, context, new_base)

        for old_base in orig_bases - bases:
            if has_table(old_base, schema) and not context.is_deleting(
                old_base
            ):
                self.alter_inhview(
                    schema, context, old_base,
                    exclude_children=frozenset((obj,)))

    def alter_ancestor_inhviews(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        obj: s_sources.Source,
        *,
        exclude_children: FrozenSet[s_sources.Source] = frozenset(),
    ) -> None:
        for base in obj.get_ancestors(schema).objects(schema):
            if has_table(base, schema) and not context.is_deleting(base):
                self.alter_inhview(
                    schema,
                    context,
                    base,
                    exclude_children=exclude_children,
                    alter_ancestors=False,
                )

    def alter_ancestor_source_inhviews(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        obj: s_pointers.Pointer,
        *,
        exclude_children: FrozenSet[s_sources.Source] = frozenset(),
    ) -> None:
        for base in obj.get_ancestors(schema).objects(schema):
            src = base.get_source(schema)
            if src and has_table(src, schema) and not context.is_deleting(src):
                self.alter_inhview(
                    schema,
                    context,
                    src,
                    exclude_children=exclude_children,
                    alter_ancestors=False,
                )

    def create_inhview(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        obj: s_sources.Source,
        *,
        exclude_ptrs: FrozenSet[s_pointers.Pointer] = frozenset(),
        alter_ancestors: bool = True,
    ) -> None:
        assert has_table(obj, schema)
        inhview = self.get_inhview(
            schema, obj, exclude_ptrs=exclude_ptrs,
            from_command=self, context=context
        )
        if inhview is not None:
            self.pgops.add(dbops.CreateView(view=inhview))
            self.pgops.add(dbops.Comment(
                object=inhview,
                text=(
                    f"{obj.get_verbosename(schema, with_parent=True)} "
                    f"and descendants"
                )
            ))
        if alter_ancestors:
            self.alter_ancestor_inhviews(schema, context, obj)

    def alter_inhview(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        obj: s_sources.Source,
        *,
        exclude_children: FrozenSet[s_sources.Source] = frozenset(),
        alter_ancestors: bool = True,
    ) -> None:
        assert has_table(obj, schema)

        inhview = self.get_inhview(
            schema,
            obj,
            exclude_children=exclude_children,
            from_command=self,
            context=context
        )
        if inhview is not None:
            self.pgops.add(dbops.CreateView(view=inhview, or_replace=True))
            self.pgops.add(dbops.Comment(
                object=inhview,
                text=(
                    f"{obj.get_verbosename(schema, with_parent=True)} "
                    f"and descendants"
                )
            ))
        if alter_ancestors:
            self.alter_ancestor_inhviews(
                schema, context, obj, exclude_children=exclude_children)

    def recreate_inhview(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        obj: s_sources.Source,
        *,
        exclude_ptrs: FrozenSet[s_pointers.Pointer] = frozenset(),
        alter_ancestors: bool = True,
    ) -> None:
        # We cannot use the regular CREATE OR REPLACE VIEW flow
        # when removing or altering VIEW columns, because postgres
        # does not allow that.
        self.drop_inhview(schema, context, obj, conditional=True)
        self.create_inhview(
            schema,
            context,
            obj,
            exclude_ptrs=exclude_ptrs,
            alter_ancestors=alter_ancestors,
        )

    def drop_inhview(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        obj: s_sources.Source,
        conditional: bool = False,
    ) -> None:
        inhview_name = common.get_backend_name(
            schema, obj, catenate=False, aspect='inhview')
        conditions = []
        if conditional:
            conditions.append(dbops.ViewExists(inhview_name))
        self.pgops.add(dbops.DropView(inhview_name, conditions=conditions))

    def apply_scheduled_inhview_updates(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        if self.inhview_updates:
            to_recreate = {k for k, r in self.inhview_updates if r}
            to_alter = {
                k for k, _ in self.inhview_updates if k not in to_recreate}

            for s in to_recreate:
                self.recreate_inhview(
                    schema, context, s, alter_ancestors=False)

            for s in to_alter:
                if has_table(s, schema):
                    self.alter_inhview(
                        schema, context, s, alter_ancestors=False)

        self.pgops.update(self.post_inhview_update_commands)

    def schedule_external_type_inhview_create(self, obj, schema, context):
        create_extern_view_actions = context.get(
            sd.DeltaRootContext).op.create_extern_view_actions
        create_extern_view_actions.collect_external_objects(obj, schema, context)


class IndexCommand(MetaCommand):
    pass


class CreateIndex(IndexCommand, adapts=s_indexes.CreateIndex):
    @classmethod
    def create_index(cls, index, schema, context):
        subject = index.get_subject(schema)

        singletons = [subject]
        path_prefix_anchor = ql_ast.Subject().name

        options = qlcompiler.CompilerOptions(
            modaliases=context.modaliases,
            schema_object_context=cls.get_schema_metaclass(),
            anchors={ql_ast.Subject().name: subject},
            path_prefix_anchor=path_prefix_anchor,
            singletons=singletons,
            apply_query_rewrites=not context.stdmode,
        )

        index_expr = index.get_expr(schema)
        ir = index_expr.irast
        if ir is None:
            index_expr = type(index_expr).compiled(
                index_expr,
                schema=schema,
                options=options,
            )
            ir = index_expr.irast

        table_name = common.get_backend_name(
            schema, subject, catenate=False)

        sql_tree = compiler.compile_ir_to_sql_tree(
            ir.expr, singleton_mode=True)

        if isinstance(sql_tree, pg_ast.ImplicitRowExpr):
            sql_exprs = [
                codegen.SQLSourceGenerator.to_source(el)
                for el in sql_tree.args
            ]
        else:
            sql_exprs = [codegen.SQLSourceGenerator.to_source(sql_tree)]

        except_expr = index.get_except_expr(schema)
        if except_expr and not except_expr.irast:
            except_expr = type(except_expr).compiled(
                except_expr,
                schema=schema,
                options=options,
            )
        if except_expr:
            except_tree = compiler.compile_ir_to_sql_tree(
                except_expr.irast.expr, singleton_mode=True)
            except_src = codegen.SQLSourceGenerator.to_source(except_tree)
            except_src = f'({except_src}) is not true'
        else:
            except_src = None

        module_name = index.get_name(schema).module
        index_name = common.get_index_backend_name(
            index.id, module_name, catenate=False)
        pg_index = dbops.Index(
            name=index_name[1], table_name=table_name, exprs=sql_exprs,
            unique=False, inherit=True,
            predicate=except_src,
            metadata={'schemaname': str(index.get_name(schema))})
        return dbops.CreateIndex(pg_index)

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        index = self.scls
        self.pgops.add(self.create_index(index, schema, context))

        return schema


class RenameIndex(IndexCommand, adapts=s_indexes.RenameIndex):
    pass


class AlterIndexOwned(IndexCommand, adapts=s_indexes.AlterIndexOwned):
    pass


class AlterIndex(IndexCommand, adapts=s_indexes.AlterIndex):
    pass


class DeleteIndex(IndexCommand, adapts=s_indexes.DeleteIndex):
    @classmethod
    def delete_index(cls, index, schema, context):
        subject = index.get_subject(schema)
        table_name = common.get_backend_name(
            schema, subject, catenate=False)
        module_name = index.get_name(schema).module
        orig_idx_name = common.get_index_backend_name(
            index.id, module_name, catenate=False)
        index = dbops.Index(
            name=orig_idx_name[1], table_name=table_name, inherit=True)
        index_exists = dbops.IndexExists(
            (table_name[0], index.name_in_catalog))
        return dbops.DropIndex(index, conditions=(index_exists,))

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context)
        index = self.scls

        source = context.get(s_links.LinkCommandContext)
        if not source:
            source = context.get(s_objtypes.ObjectTypeCommandContext)

        if not isinstance(source.op, sd.DeleteObject):
            # We should not drop indexes when the host is being dropped since
            # the indexes are dropped automatically in this case.
            #
            self.pgops.add(self.delete_index(index, orig_schema, context))

        return schema


class RebaseIndex(IndexCommand, adapts=s_indexes.RebaseIndex):
    pass


class CreateUnionType(
    MetaCommand,
    adapts=s_types.CreateUnionType,
    metaclass=CommandMeta,
):
    pass


class ObjectTypeMetaCommand(AliasCapableMetaCommand,
                            CompositeMetaCommand):
    def schedule_endpoint_delete_action_update(self, obj, schema, context):
        endpoint_delete_actions = context.get(
            sd.DeltaRootContext).op.update_endpoint_delete_actions
        changed_targets = endpoint_delete_actions.changed_targets
        changed_targets.add((self, obj))


class CreateObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.CreateObjectType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        objtype = self.scls
        if (
            objtype.is_compound_type(schema)
            or objtype.get_is_derived(schema)
            or objtype in context.external_objs
        ):
            if objtype in context.external_objs:
                self.schedule_external_type_inhview_create(objtype, schema, context)
            return schema

        self.attach_alter_table(context)

        if self.update_search_indexes:
            schema = self.update_search_indexes.apply(schema, context)
            self.pgops.add(self.update_search_indexes)

        self.schedule_endpoint_delete_action_update(self.scls, schema, context)

        return schema

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._create_begin(schema, context)
        objtype = self.scls
        if (
            objtype.is_compound_type(schema)
            or objtype.get_is_derived(schema)
            or objtype in context.external_objs
        ):
            return schema

        new_table_name = common.get_backend_name(
            schema, self.scls, catenate=False)
        self.table_name = new_table_name
        columns = []

        objtype_table = dbops.Table(name=new_table_name, columns=columns)
        self.pgops.add(dbops.CreateTable(table=objtype_table))
        self.pgops.add(dbops.Comment(
            object=objtype_table,
            text=str(objtype.get_verbosename(schema)),
        ))
        self.create_inhview(schema, context, objtype, alter_ancestors=False)
        return schema

    def _create_finalize(self, schema, context):
        schema = super()._create_finalize(schema, context)
        self.apply_scheduled_inhview_updates(schema, context)
        return schema


class RenameObjectType(
    ObjectTypeMetaCommand,
    adapts=s_objtypes.RenameObjectType,
):
    pass


class RebaseObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.RebaseObjectType):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if has_table(self.scls, schema):
            self.update_base_inhviews_on_rebase(
                schema, context.current().original_schema, context, self.scls)

        schema = super()._alter_innards(schema, context)
        self.schedule_endpoint_delete_action_update(self.scls, schema, context)

        return schema


class AlterObjectType(ObjectTypeMetaCommand,
                      adapts=s_objtypes.AlterObjectType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super().apply(schema, context=context)
        objtype = self.scls

        self.apply_scheduled_inhview_updates(schema, context)

        self.table_name = common.get_backend_name(
            schema, objtype, catenate=False)

        self._maybe_do_abstract_test(orig_schema, schema, context)

        if has_table(objtype, schema):
            self.attach_alter_table(context)

            if self.update_search_indexes:
                schema = self.update_search_indexes.apply(schema, context)
                self.pgops.add(self.update_search_indexes)

        return schema

    def _maybe_do_abstract_test(
        self,
        orig_schema: s_schema.Schema,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_abstract = self.scls.get_abstract(orig_schema)
        new_abstract = self.scls.get_abstract(schema)
        if orig_abstract or not new_abstract:
            return

        table = q(*common.get_backend_name(
            schema,
            self.scls,
            catenate=False,
        ))

        vn = self.scls.get_verbosename(schema)
        check_qry = textwrap.dedent(f'''\
            SELECT
                edgedb.raise(
                    NULL::text,
                    'cardinality_violation',
                    msg => {common.quote_literal(
                            f"may not make non-empty {vn} abstract")},
                    "constraint" => 'set abstract'
                )
            FROM {table}
            INTO _dummy_text;
        ''')

        self.pgops.add(dbops.Query(check_qry))


class DeleteObjectType(ObjectTypeMetaCommand,
                       adapts=s_objtypes.DeleteObjectType):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        self.scls = objtype = schema.get(self.classname)
        self._sort_subcommand()
        is_external = objtype.get_external(schema)

        old_table_name = common.get_backend_name(
            schema, objtype, catenate=False)

        orig_schema = schema
        schema = super().apply(schema, context)

        self.apply_scheduled_inhview_updates(schema, context)

        if is_external:
            self.pgops.add(dbops.DropView(name=('edgedbpub', str(objtype.id))))
            self.pgops.add(dbops.DropView(name=('edgedbpub', str(objtype.id) + '_t')))
            return schema

        if has_table(objtype, orig_schema):
            self.attach_alter_table(context)
            self.drop_inhview(orig_schema, context, objtype)
            self.pgops.add(dbops.DropTable(name=old_table_name))

        return schema

    def _sort_subcommand(self):
        """
        Always delete link __type__ first. So that the first delete link will
        create a correct view for both std::Object and std::BaseObject.

        只要先删除 id或__type__，就可以使得Object/BaseObject的view里面不包含当前ObjectType。
        """
        target = None
        for cmd in self.get_subcommands(type=s_links.DeleteLink):
            if cmd.get_displayname() == '__type__':
                target = cmd
                break

        assert target is not None
        idx = self.ops.index(target)
        if idx == len(self.ops) - 1:
            self.ops = [target] + self.ops[: idx]
        else:
            self.ops = [target] + self.ops[: idx] + self.ops[idx + 1:]


class SchedulePointerCardinalityUpdate(MetaCommand):
    pass


class CancelPointerCardinalityUpdate(MetaCommand):
    pass


class PointerMetaCommand(MetaCommand):
    def get_host(self, schema, context):
        if context:
            link = context.get(s_links.LinkCommandContext)
            if link and isinstance(self, s_props.PropertyCommand):
                return link
            objtype = context.get(s_objtypes.ObjectTypeCommandContext)
            if objtype:
                return objtype

    def is_sequence_ptr(self, ptr, schema):
        return bool(
            (tgt := ptr.get_target(schema))
            and tgt.issubclass(schema, schema.get('std::sequence'))
        )

    def get_pointer_default(self, ptr, schema, context):
        if ptr.is_pure_computable(schema):
            return None

        default = ptr.get_default(schema)
        default_value = None

        if default is not None and ptr.is_link_property(schema):
            default_value = schemamech.ptr_default_to_col_default(
                schema, ptr, default)
        elif self.is_sequence_ptr(ptr, schema):
            # TODO: replace this with a generic scalar type default
            #       using std::nextval().
            seq_name = common.quote_literal(
                common.get_backend_name(
                    schema, ptr.get_target(schema), aspect='sequence'))
            default_value = f'nextval({seq_name}::regclass)'

        return default_value

    @classmethod
    def get_columns(cls, pointer, schema, default=None, sets_required=False):
        ptr_stor_info = types.get_pointer_storage_info(pointer, schema=schema)
        col_type = list(ptr_stor_info.column_type)
        if col_type[-1].endswith('[]'):
            # Array
            col_type[-1] = col_type[-1][:-2]
            col_type = common.qname(*col_type) + '[]'
        else:
            col_type = common.qname(*col_type)

        return [
            dbops.Column(
                name=ptr_stor_info.column_name,
                type=col_type,
                required=(
                    (
                        pointer.get_required(schema)
                        and not pointer.is_pure_computable(schema)
                        and not sets_required
                        and not (pointer.get_default(schema) and not default)
                    ) or (
                        ptr_stor_info.table_type == 'link'
                        and not pointer.is_link_property(schema)
                    )
                ),
                default=default,
                comment=str(pointer.get_shortname(schema)),
            ),
        ]

    def create_table(self, ptr, schema, context):
        if has_table(ptr, schema):
            c = self._create_table(ptr, schema, context, conditional=True)
            self.pgops.add(c)
            self.alter_inhview(schema, context, ptr)
            return True
        else:
            return False

    def _alter_pointer_cardinality(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        ptr = self.scls
        ptr_stor_info = types.get_pointer_storage_info(ptr, schema=schema)
        old_ptr_stor_info = types.get_pointer_storage_info(
            ptr, schema=orig_schema)
        ptr_table = ptr_stor_info.table_type == 'link'
        is_lprop = ptr.is_link_property(schema)
        is_multi = ptr_table and not is_lprop
        is_required = ptr.get_required(schema)
        is_scalar = ptr.is_property(schema)

        ref_ctx = self.get_referrer_context_or_die(context)
        ref_op = ref_ctx.op

        if is_multi:
            if isinstance(self, sd.AlterObjectFragment):
                source_op = self.get_parent_op(context)
            else:
                source_op = self
        else:
            source_op = ref_op

        # Ignore cardinality changes resulting from the creation of
        # an overloaded pointer as there is no data yet.
        if isinstance(source_op, sd.CreateObject):
            return

        if self.conv_expr is not None:
            _, conv_sql_expr, orig_rel_alias, _ = (
                self._compile_conversion_expr(
                    pointer=ptr,
                    conv_expr=self.conv_expr,
                    schema=schema,
                    orig_schema=orig_schema,
                    context=context,
                    orig_rel_is_always_source=True,
                    target_as_singleton=False,
                )
            )

            if is_lprop:
                obj_id_ref = f'{qi(orig_rel_alias)}.source'
            else:
                obj_id_ref = f'{qi(orig_rel_alias)}.id'

            if is_required and not is_multi:
                conv_sql_expr = textwrap.dedent(f'''\
                    edgedb.raise_on_null(
                        ({conv_sql_expr}),
                        'not_null_violation',
                        msg => 'missing value for required property',
                        detail => '{{"object_id": "' || {obj_id_ref} || '"}}',
                        "column" => {ql(str(ptr.id))}
                    )
                ''')
        else:
            orig_rel_alias = f'alias_{uuidgen.uuid1mc()}'

            if not is_multi:
                raise AssertionError(
                    'explicit conversion expression was expected'
                    ' for multi->single transition'
                )
            else:
                # single -> multi
                conv_sql_expr = (
                    f'SELECT '
                    f'{qi(orig_rel_alias)}.{qi(old_ptr_stor_info.column_name)}'
                )

        tab = q(*ptr_stor_info.table_name)
        target_col = ptr_stor_info.column_name

        if not is_multi:
            # Moving from pointer table to source table.
            cols = self.get_columns(ptr, schema)
            alter_table = source_op.get_alter_table(
                schema, context, manual=True)

            for col in cols:
                cond = dbops.ColumnExists(
                    ptr_stor_info.table_name,
                    column_name=col.name,
                )
                op = (dbops.AlterTableAddColumn(col), None, (cond, ))
                alter_table.add_operation(op)

            self.pgops.add(alter_table)

            update_qry = textwrap.dedent(f'''\
                UPDATE {tab} AS {qi(orig_rel_alias)}
                SET {qi(target_col)} = ({conv_sql_expr})
            ''')
            self.pgops.add(dbops.Query(update_qry))

            # A link might still own a table if it has properties.
            if not has_table(ptr, schema):
                self.drop_inhview(orig_schema, context, ptr)
                otabname = common.get_backend_name(
                    orig_schema, ptr, catenate=False)
                condition = dbops.TableExists(name=otabname)
                dt = dbops.DropTable(name=otabname, conditions=[condition])
                self.pgops.add(dt)

            self.schedule_inhview_source_update(
                schema, context, ptr,
                s_objtypes.ObjectTypeCommandContext,)
        else:
            # Moving from source table to pointer table.
            self.create_table(ptr, schema, context)
            source = ptr.get_source(orig_schema)
            src_tab = q(*common.get_backend_name(
                orig_schema,
                source,
                catenate=False,
            ))

            update_qry = textwrap.dedent(f'''\
                INSERT INTO {tab} (source, target)
                (
                    SELECT
                        {qi(orig_rel_alias)}.id,
                        q.val
                    FROM
                        {src_tab} AS {qi(orig_rel_alias)},
                        LATERAL (
                            {conv_sql_expr}
                        ) AS q(val)
                    WHERE
                        q.val IS NOT NULL
                )
            ''')

            if not is_scalar:
                update_qry += 'ON CONFLICT (source, target) DO NOTHING'

            self.pgops.add(dbops.Query(update_qry))

            self.recreate_inhview(
                schema, context, ref_op.scls, alter_ancestors=False)
            self.alter_ancestor_source_inhviews(
                schema, context, ptr)

            ref_op = self.get_referrer_context_or_die(context).op
            alter_table = ref_op.get_alter_table(
                schema, context, manual=True)
            col = dbops.Column(
                name=old_ptr_stor_info.column_name,
                type=common.qname(*old_ptr_stor_info.column_type),
            )
            alter_table.add_operation(dbops.AlterTableDropColumn(col))
            self.pgops.add(alter_table)

    def _alter_pointer_optionality(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        fill_expr: Optional[s_expr.Expression],
    ) -> None:
        new_required = self.scls.get_required(schema)

        ptr = self.scls
        ptr_stor_info = types.get_pointer_storage_info(ptr, schema=schema)
        ptr_table = ptr_stor_info.table_type == 'link'
        is_lprop = ptr.is_link_property(schema)
        is_multi = ptr_table and not is_lprop
        is_required = ptr.get_required(schema)

        source_ctx = self.get_referrer_context_or_die(context)
        source_op = source_ctx.op

        alter_table = None
        if not ptr_table or is_lprop:
            alter_table = source_op.get_alter_table(
                schema,
                context,
                manual=True,
            )
            alter_table.add_operation(
                dbops.AlterTableAlterColumnNull(
                    column_name=ptr_stor_info.column_name,
                    null=not new_required,
                )
            )

        # Ignore optionality changes resulting from the creation of
        # an overloaded pointer as there is no data yet.
        if isinstance(source_op, sd.CreateObject):
            if alter_table:
                self.pgops.add(alter_table)
            return

        ops = dbops.CommandGroup()

        # For multi pointers, if there is no fill expression, we
        # synthesize a bogus one so that an error will trip if there
        # are any objects with empty values.
        if fill_expr is None and is_multi and is_required:
            if (
                ptr.get_cardinality(schema).is_multi()
                and fill_expr is None
                and (target := ptr.get_target(schema))
            ):
                fill_ast = ql_ast.TypeCast(
                    expr=ql_ast.Set(elements=[]),
                    type=s_utils.typeref_to_ast(schema, target),
                )
                fill_expr = s_expr.Expression.from_ast(
                    qltree=fill_ast, schema=schema
                )

        if fill_expr is not None:
            _, fill_sql_expr, orig_rel_alias, _ = (
                self._compile_conversion_expr(
                    pointer=ptr,
                    conv_expr=fill_expr,
                    schema=schema,
                    orig_schema=orig_schema,
                    context=context,
                    orig_rel_is_always_source=True,
                )
            )

            if is_lprop:
                obj_id_ref = f'{qi(orig_rel_alias)}.source'
            else:
                obj_id_ref = f'{qi(orig_rel_alias)}.id'

            if is_required and not is_multi:
                fill_sql_expr = textwrap.dedent(f'''\
                    edgedb.raise_on_null(
                        ({fill_sql_expr}),
                        'not_null_violation',
                        msg => 'missing value for required property',
                        detail => '{{"object_id": "' || {obj_id_ref} || '"}}',
                        "column" => {ql(str(ptr.id))}
                    )
                ''')

            tab = q(*ptr_stor_info.table_name)
            target_col = ptr_stor_info.column_name

            if not is_multi:
                # For singleton pointers we simply update the
                # requisite column of the host source in every
                # row where it is NULL.
                update_qry = textwrap.dedent(f'''\
                    UPDATE {tab} AS {qi(orig_rel_alias)}
                    SET {qi(target_col)} = ({fill_sql_expr})
                    WHERE {qi(target_col)} IS NULL
                ''')
                ops.add_command(dbops.Query(update_qry))
            else:
                # For multi pointers we have to INSERT the
                # result of USING into the link table for
                # every source object that has _no entries_
                # in said link table.
                source = ptr.get_source(orig_schema)
                src_tab = q(*common.get_backend_name(
                    orig_schema,
                    source,
                    catenate=False,
                ))

                update_qry = textwrap.dedent(f'''\
                    INSERT INTO {tab} (source, target)
                    (
                        SELECT
                            {qi(orig_rel_alias)}.id,
                            q.val
                        FROM
                            (
                                SELECT *
                                FROM {src_tab}
                                WHERE id != ALL (
                                    SELECT source FROM {tab}
                                )
                            ) AS {qi(orig_rel_alias)},
                            LATERAL (
                                {fill_sql_expr}
                            ) AS q(val)
                        WHERE
                            q.val IS NOT NULL
                    )
                ''')

                ops.add_command(dbops.Query(update_qry))

                check_qry = textwrap.dedent(f'''\
                    SELECT
                        edgedb.raise(
                            NULL::text,
                            'not_null_violation',
                            msg => 'missing value for required property',
                            detail => '{{"object_id": "' || id || '"}}',
                            "column" => {ql(str(ptr.id))}
                        )
                    FROM {src_tab}
                    WHERE id != ALL (SELECT source FROM {tab})
                    LIMIT 1
                    INTO _dummy_text;
                ''')

                if is_required:
                    ops.add_command(dbops.Query(check_qry))

        if alter_table:
            ops.add_command(alter_table)

        self.pgops.add(ops)

    def _drop_constraints(self, pointer, schema, context):
        # We need to be able to drop all the constraints referencing a
        # pointer before modifying its type, and then recreate them
        # once the change is done.
        # We look at all referrers to the pointer (and not just the
        # constraints directly on the pointer) because we want to
        # pick up object constraints that reference it as well.
        for cnstr in schema.get_referrers(
                pointer, scls_type=s_constr.Constraint):
            self.pgops.add(
                ConstraintCommand.delete_constraint(cnstr, schema, context))

    def _recreate_constraints(self, pointer, schema, context):
        for cnstr in schema.get_referrers(
                pointer, scls_type=s_constr.Constraint):
            self.pgops.add(
                ConstraintCommand.create_constraint(cnstr, schema, context))

    def _alter_pointer_type(self, pointer, schema, orig_schema, context):
        old_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=orig_schema)
        new_target = pointer.get_target(schema)

        ptr_table = old_ptr_stor_info.table_type == 'link'
        is_link = isinstance(pointer, s_links.Link)
        is_lprop = pointer.is_link_property(schema)
        is_multi = ptr_table and not is_lprop
        is_required = pointer.get_required(schema)
        changing_col_type = not is_link or old_ptr_stor_info.column_type != ('uuid', )
        source_ctx = self.get_referrer_context_or_die(context)
        ptr_op = self.get_parent_op(context)
        if is_multi:
            source_op = ptr_op
        else:
            source_op = source_ctx.op

        # Ignore type narrowing resulting from a creation of a subtype
        # as there isn't any data in the link yet.
        if is_link and isinstance(source_ctx.op, sd.CreateObject):
            return

        new_target = pointer.get_target(schema)
        orig_target = pointer.get_target(orig_schema)
        new_type = types.pg_type_from_object(
            schema, new_target, persistent_tuples=True)

        source = source_op.scls
        using_eql_expr = self.cast_expr

        # For links, when the new type is a supertype of the old, no
        # SQL-level changes are necessary, unless an explicit conversion
        # expression was specified.
        if (
            is_link
            and using_eql_expr is None
            and orig_target.issubclass(orig_schema, new_target)
        ):
            return

        # We actually have work to do, so drop any constraints we have
        self._drop_constraints(pointer, schema, context)

        if using_eql_expr is None and not is_link:
            # A lack of an explicit EdgeQL conversion expression means
            # that the new type is assignment-castable from the old type
            # in the EdgeDB schema.  BUT, it would not necessarily be
            # assignment-castable in Postgres, especially if the types are
            # compound.  Thus, generate an explicit cast expression.
            pname = pointer.get_shortname(schema).name
            using_eql_expr = s_expr.Expression.from_ast(
                ql_ast.TypeCast(
                    expr=ql_ast.Path(
                        partial=True,
                        steps=[
                            ql_ast.Ptr(
                                ptr=ql_ast.ObjectRef(name=pname),
                                type='property' if is_lprop else None,
                            ),
                        ],
                    ),
                    type=s_utils.typeref_to_ast(schema, new_target),
                ),
                schema=orig_schema,
            )

        # There are two major possibilities about the USING claus:
        # 1) trivial case, where the USING clause refers only to the
        # columns of the source table, in which case we simply compile that
        # into an equivalent SQL USING clause, and 2) complex case, which
        # supports arbitrary queries, but requires a temporary column,
        # which is populated with the transition query and then used as the
        # source for the SQL USING clause.
        using_eql_expr, using_sql_expr, orig_rel_alias, sql_expr_is_trivial = (
            self._compile_conversion_expr(
                pointer=pointer,
                conv_expr=using_eql_expr,
                schema=schema,
                orig_schema=orig_schema,
                context=context,
            )
        )

        expr_is_nullable = using_eql_expr.cardinality.can_be_zero()

        need_temp_col = (
            (is_multi and expr_is_nullable)
            or (changing_col_type and not sql_expr_is_trivial)
        )

        if changing_col_type:
            self.drop_inhview(schema, context, source)
            self.alter_ancestor_source_inhviews(
                schema, context, pointer,
                exclude_children=frozenset((source,)))

        tab = q(*old_ptr_stor_info.table_name)
        target_col = old_ptr_stor_info.column_name
        aux_ptr_table = None
        aux_ptr_col = None

        if is_link:
            old_lb_ptr_stor_info = types.get_pointer_storage_info(
                pointer, link_bias=True, schema=orig_schema)

            if (
                old_lb_ptr_stor_info is not None
                and old_lb_ptr_stor_info.table_type == 'link'
            ):
                aux_ptr_table = old_lb_ptr_stor_info.table_name
                aux_ptr_col = old_lb_ptr_stor_info.column_name

        if not sql_expr_is_trivial:
            if need_temp_col:
                alter_table = source_op.get_alter_table(
                    schema, context, force_new=True, manual=True)
                temp_column = dbops.Column(
                    name=f'??{pointer.id}_{common.get_unique_random_name()}',
                    type=qt(new_type),
                )
                alter_table.add_operation(
                    dbops.AlterTableAddColumn(temp_column))
                self.pgops.add(alter_table)
                target_col = temp_column.name

            if is_multi:
                obj_id_ref = f'{qi(orig_rel_alias)}.source'
            else:
                obj_id_ref = f'{qi(orig_rel_alias)}.id'

            if is_required and not is_multi:
                using_sql_expr = textwrap.dedent(f'''\
                    edgedb.raise_on_null(
                        ({using_sql_expr}),
                        'not_null_violation',
                        msg => 'missing value for required property',
                        detail => '{{"object_id": "' || {obj_id_ref} || '"}}',
                        "column" => {ql(str(pointer.id))}
                    )
                ''')

            update_qry = textwrap.dedent(f'''\
                UPDATE {tab} AS {qi(orig_rel_alias)}
                SET {qi(target_col)} = ({using_sql_expr})
            ''')

            self.pgops.add(dbops.Query(update_qry))
            actual_using_expr = qi(target_col)
        else:
            actual_using_expr = using_sql_expr

        if changing_col_type or need_temp_col:
            alter_table = source_op.get_alter_table(
                schema, context, force_new=True, manual=True)

        if is_multi:
            # Remove all rows where the conversion expression produced NULLs.
            col = qi(target_col)
            if pointer.get_required(schema):
                clean_nulls = dbops.Query(textwrap.dedent(f'''\
                    WITH d AS (
                        DELETE FROM {tab} WHERE {col} IS NULL RETURNING source
                    )
                    SELECT
                        edgedb.raise(
                            NULL::text,
                            'not_null_violation',
                            msg => 'missing value for required property',
                            detail => '{{"object_id": "' || l.source || '"}}',
                            "column" => {ql(str(pointer.id))}
                        )
                    FROM
                        {tab} AS l
                    WHERE
                        l.source IN (SELECT source FROM d)
                        AND True = ALL (
                            SELECT {col} IS NULL
                            FROM {tab} AS l2
                            WHERE l2.source = l.source
                        )
                    LIMIT
                        1
                    INTO _dummy_text;
                '''))
            else:
                clean_nulls = dbops.Query(textwrap.dedent(f'''\
                    DELETE FROM {tab} WHERE {col} IS NULL
                '''))

            self.pgops.add(clean_nulls)

        elif aux_ptr_table is not None:
            # SINGLE links with link properties are represented in
            # _two_ tables (the host type table and a link table with
            # properties), and we must update both.
            actual_col = qi(old_ptr_stor_info.column_name)

            if expr_is_nullable and not is_required:
                cleanup_qry = textwrap.dedent(f'''\
                    DELETE FROM {q(*aux_ptr_table)} AS aux
                    USING {tab} AS main
                    WHERE
                        main.id = aux.source
                        AND {actual_col} IS NULL
                ''')
                self.pgops.add(dbops.Query(cleanup_qry))

            update_qry = textwrap.dedent(f'''\
                UPDATE {q(*aux_ptr_table)} AS aux
                SET {qi(aux_ptr_col)} = main.{actual_col}
                FROM {tab} AS main
                WHERE
                    main.id = aux.source
            ''')
            self.pgops.add(dbops.Query(update_qry))

        if changing_col_type:
            # In case the column has a default, clear it out before
            # changing the type
            if is_lprop or self.is_sequence_ptr(pointer, orig_schema):
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnDefault(
                        column_name=old_ptr_stor_info.column_name,
                        default=None))

            alter_type = dbops.AlterTableAlterColumnType(
                old_ptr_stor_info.column_name,
                common.quote_type(new_type),
                using_expr=actual_using_expr,
            )

            alter_table.add_operation(alter_type)
        elif need_temp_col:
            move_data = dbops.Query(textwrap.dedent(f'''\
                UPDATE
                    {q(*old_ptr_stor_info.table_name)} AS {qi(orig_rel_alias)}
                SET
                    {qi(old_ptr_stor_info.column_name)} = ({qi(target_col)})
            '''))
            self.pgops.add(move_data)

        if need_temp_col:
            alter_table.add_operation(dbops.AlterTableDropColumn(temp_column))

        if changing_col_type or need_temp_col:
            self.pgops.add(alter_table)

        self._recreate_constraints(pointer, schema, context)

        if changing_col_type:
            self.create_inhview(schema, context, source, alter_ancestors=False)
            self.alter_ancestor_source_inhviews(schema, context, pointer)

    def _compile_conversion_expr(
        self,
        *,
        pointer: s_pointers.Pointer,
        conv_expr: s_expr.Expression,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
        orig_rel_is_always_source: bool = False,
        target_as_singleton: bool = True,
    ) -> Tuple[
        s_expr.Expression,  # Possibly-amended EdgeQL conversion expression
        str,                # SQL text
        str,                # original relation alias
        bool,               # whether SQL expression is trivial
    ]:
        old_ptr_stor_info = types.get_pointer_storage_info(
            pointer, schema=orig_schema)

        ptr_table = old_ptr_stor_info.table_type == 'link'
        is_link = isinstance(pointer, s_links.Link)
        is_lprop = pointer.is_link_property(schema)
        is_multi = ptr_table and not is_lprop
        is_required = pointer.get_required(schema)

        new_target = pointer.get_target(schema)
        expr_is_trivial = False

        if conv_expr.irast is not None:
            ir = conv_expr.irast
        else:
            conv_expr = self._compile_expr(
                orig_schema,
                context,
                conv_expr,
                target_as_singleton=target_as_singleton,
            )
            ir = conv_expr.irast

        assert ir is not None

        if ir.stype != new_target and not is_link:
            # The result of an EdgeQL USING clause does not match
            # the target type exactly, but is castable.  Like in the
            # case of an empty USING clause, we still have to make
            # ane explicit EdgeQL cast rather than rely on Postgres
            # casting.
            conv_expr = self._compile_expr(
                orig_schema,
                context,
                s_expr.Expression.from_ast(
                    ql_ast.TypeCast(
                        expr=conv_expr.qlast,
                        type=s_utils.typeref_to_ast(schema, new_target),
                    ),
                    schema=orig_schema,
                ),
                target_as_singleton=target_as_singleton,
            )

            ir = conv_expr.irast

        if params := irutils.get_parameters(ir):
            param = list(params)[0]
            if param.is_global:
                if param.is_implicit_global:
                    problem = 'functions that reference globals'
                else:
                    problem = 'globals'
            else:
                problem = 'parameters'
            raise errors.UnsupportedFeatureError(
                f'{problem} may not be used when converting/populating '
                f'data in migrations',
                context=self.source_context,
            )
        expr_is_nullable = conv_expr.cardinality.can_be_zero()

        refs = irutils.get_longest_paths(ir.expr)
        ref_tables = schemamech.get_ref_storage_info(ir.schema, refs)

        local_table_only = all(
            t == old_ptr_stor_info.table_name
            for t in ref_tables
        )

        # TODO: implement IR complexity inference
        can_translate_to_sql_value_expr = False

        expr_is_trivial = (
            # Only allow trivial USING if we can compile the
            # EdgeQL expression into a trivial SQL value expression.
            can_translate_to_sql_value_expr
            # No link expr is trivially translatable into
            # a USING SQL clause.
            and not is_link
            # SQL SET TYPE cannot contain references
            # outside of the local table.
            and local_table_only
            # Changes to a multi-pointer might involve contraction of
            # the overall cardinality, i.e. the deletion some rows.
            and not is_multi
            # If the property is required, and the USING expression
            # was not proven by the compiler to not return ZERO, we
            # must inject an explicit NULL guard, as the SQL null
            # violation error is very nondescript in the context of
            # a table rewrite, making it hard to pinpoint the failing
            # object.
            and (not is_required or not expr_is_nullable)
        )

        alias = f'alias_{uuidgen.uuid1mc()}'

        if not expr_is_trivial:
            # Non-trivial conversion expression means that we
            # are compiling a full-blown EdgeQL statement as
            # opposed to compiling a scalar fragment in trivial
            # expression mode.
            external_rvars = {}

            if is_lprop:
                # For linkprops we actually want the source path.
                # To make it work for abstract links, get the source
                # path out of the IR's output (to take advantage
                # of the types we made up for it).
                # FIXME: Maybe we shouldn't be compiling stuff
                # for abstract links!
                tgt_path_id = ir.singletons[0]
            else:
                tgt_path_id = irpathid.PathId.from_pointer(
                    orig_schema,
                    pointer,
                )

            ptr_path_id = tgt_path_id.ptr_path()
            src_path_id = ptr_path_id.src_path()

            if ptr_table and not orig_rel_is_always_source:
                rvar = compiler.new_external_rvar(
                    rel_name=(alias,),
                    path_id=ptr_path_id,
                    outputs={
                        (src_path_id, ('identity',)): 'source',
                    },
                )
                external_rvars[ptr_path_id, 'source'] = rvar
                external_rvars[ptr_path_id, 'value'] = rvar
                external_rvars[src_path_id, 'identity'] = rvar
                if local_table_only and not is_lprop:
                    external_rvars[src_path_id, 'source'] = rvar
                    external_rvars[src_path_id, 'value'] = rvar
                elif is_lprop:
                    external_rvars[tgt_path_id, 'identity'] = rvar
                    external_rvars[tgt_path_id, 'value'] = rvar
            else:
                src_rvar = compiler.new_external_rvar(
                    rel_name=(alias,),
                    path_id=src_path_id,
                    outputs={},
                )
                external_rvars[src_path_id, 'identity'] = src_rvar
                external_rvars[src_path_id, 'value'] = src_rvar
                external_rvars[src_path_id, 'source'] = src_rvar
        else:
            external_rvars = None

        sql_tree = compiler.compile_ir_to_sql_tree(
            ir,
            output_format=compiler.OutputFormat.NATIVE_INTERNAL,
            singleton_mode=expr_is_trivial,
            external_rvars=external_rvars,
            backend_runtime_params=context.backend_runtime_params,
        )

        sql_text = codegen.generate_source(sql_tree)

        return (conv_expr, sql_text, alias, expr_is_trivial)

    def schedule_endpoint_delete_action_update(
            self, link, orig_schema, schema, context):
        endpoint_delete_actions = context.get(
            sd.DeltaRootContext).op.update_endpoint_delete_actions
        link_ops = endpoint_delete_actions.link_ops

        if isinstance(self, sd.DeleteObject):
            for i, (_, ex_link, _, _) in enumerate(link_ops):
                if ex_link == link:
                    link_ops.pop(i)
                    break

        link_ops.append((self, link, orig_schema, schema))


class LinkMetaCommand(CompositeMetaCommand, PointerMetaCommand):

    @staticmethod
    def resolve_col_type(prop, schema):
        if prop is None:
            return 'uuid'

        target = prop.get_target(schema)
        if target.is_sequence(schema):
            return 'int8'

        col_tuple = types.pg_type_from_object(schema, target)

        if len(col_tuple) == 2:
            return f"{col_tuple[0]}.{common.quote_ident(col_tuple[1])}"
        else:
            return '.'.join(col_tuple)

    @classmethod
    def _create_table(
            cls, link, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_backend_name(schema, link, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = 'source'
        tgt_col = 'target'

        src_col_type = cls.resolve_col_type(
            link.get_source_property(schema),
            schema
        )
        tgt_col_type = cls.resolve_col_type(
            link.get_target_property(schema),
            schema
        )

        columns.append(
            dbops.Column(
                name=src_col, type=src_col_type, required=True))
        columns.append(
            dbops.Column(
                name=tgt_col, type=tgt_col_type, required=True))

        constraints.append(
            dbops.UniqueConstraint(
                table_name=new_table_name,
                columns=[src_col, tgt_col]))

        if not link.generic(schema) and link.scalar():
            tgt_prop = link.getptr(schema, 'target')
            tgt_ptr = types.get_pointer_storage_info(
                tgt_prop, schema=schema)
            columns.append(
                dbops.Column(
                    name=tgt_ptr.column_name,
                    type=common.qname(*tgt_ptr.column_type)))

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        ct = dbops.CreateTable(table=table)

        index_name = common.edgedb_name_to_pg_name(
            str(link.get_name(schema)) + 'target_id_default_idx')
        index = dbops.Index(index_name, new_table_name, unique=False)
        index.add_columns([tgt_col])
        ci = dbops.CreateIndex(index)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        c.add_command(
            dbops.Comment(
                table,
                str(link.get_verbosename(schema, with_parent=True)),
            ),
        )

        create_c.add_command(c)

        if create_children:
            for l_descendant in link.descendants(schema):
                if has_table(l_descendant, schema):
                    lc = LinkMetaCommand._create_table(
                        l_descendant, schema, context, conditional=True,
                        create_bases=False, create_children=False)
                    create_c.add_command(lc)

        return create_c

    def _create_link(
        self,
        link: s_props.Property,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:

        objtype = context.get(s_objtypes.ObjectTypeCommandContext)
        source = link.get_source(schema)

        if source is not None:
            source_is_view = (
                source.is_view(schema)
                or source.is_compound_type(schema)
                or source.get_is_derived(schema)
            )
        else:
            source_is_view = None

        if has_table(self.scls, schema):
            self.create_table(self.scls, schema, context)

        if (
            source is not None
            and not source_is_view
            and not link.is_pure_computable(schema)
        ):
            ptr_stor_info = types.get_pointer_storage_info(
                link, resolve_type=False, schema=schema)

            fills_required = any(
                x.fill_expr for x in
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))
            sets_required = bool(
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))

            if ptr_stor_info.table_type == 'ObjectType':
                cols = self.get_columns(
                    link, schema, None, sets_required)
                table_name = common.get_backend_name(
                    schema, objtype.scls, catenate=False)
                objtype_alter_table = objtype.op.get_alter_table(
                    schema, context, manual=True)

                for col in cols:
                    cmd = dbops.AlterTableAddColumn(col)
                    objtype_alter_table.add_operation(cmd)

                    if col.name == '__type__':
                        constr_name = common.edgedb_name_to_pg_name(
                            str(objtype.op.classname) + '.class_check')

                        constr_expr = dbops.Query(textwrap.dedent(f"""\
                            SELECT
                                '"__type__" = ' ||
                                quote_literal({ql(str(objtype.scls.id))})
                        """), type='text')

                        cid_constraint = dbops.CheckConstraint(
                            self.table_name,
                            constr_name,
                            constr_expr,
                            inherit=False,
                        )

                        objtype_alter_table.add_operation(
                            dbops.AlterTableAddConstraint(cid_constraint),
                        )

                self.pgops.add(objtype_alter_table)

                index_name = common.get_backend_name(
                    schema, link, catenate=False, aspect='index'
                )[1]

                pg_index = dbops.Index(
                    name=index_name, table_name=table_name,
                    unique=False, columns=[c.name for c in cols],
                    inherit=True)

                ci = dbops.CreateIndex(pg_index)
                self.pgops.add(ci)

                self.schedule_inhview_source_update(
                    schema,
                    context,
                    link,
                    s_objtypes.ObjectTypeCommandContext,
                )
            else:
                self.schedule_inhview_update(
                    schema,
                    context,
                    link,
                    s_objtypes.ObjectTypeCommandContext,
                )

            if (
                (default := link.get_default(schema))
                and not link.is_pure_computable(schema)
                and not fills_required
            ):
                self._alter_pointer_optionality(
                    schema, schema, context, fill_expr=default)
            # If we're creating a required multi pointer without a SET
            # REQUIRED USING inside, run the alter_pointer_optionality
            # path to produce an error if there is existing data.
            elif (
                link.get_cardinality(schema).is_multi()
                and link.get_required(schema)
                and not link.is_pure_computable(schema)
                and not sets_required
            ):
                self._alter_pointer_optionality(
                    schema, schema, context, fill_expr=None)

            if not link.is_pure_computable(schema):
                self.schedule_endpoint_delete_action_update(
                    link, orig_schema, schema, context)

    def _delete_link(
        self,
        link: s_props.Link,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:

        old_table_name = common.get_backend_name(
            schema, link, catenate=False)

        if (
            not link.generic(orig_schema)
            and has_table(link.get_source(orig_schema), orig_schema)
            and not link.is_pure_computable(orig_schema)
        ):
            ptr_stor_info = types.get_pointer_storage_info(
                link, schema=orig_schema)

            objtype = context.get(s_objtypes.ObjectTypeCommandContext)

            if (not isinstance(objtype.op, s_objtypes.DeleteObjectType)
                    and ptr_stor_info.table_type == 'ObjectType'):
                self.recreate_inhview(
                    schema,
                    context,
                    objtype.scls,
                    exclude_ptrs=frozenset((link,)),
                    alter_ancestors=False,
                )
                self.alter_ancestor_source_inhviews(
                    schema, context, link
                )
                alter_table = objtype.op.get_alter_table(
                    schema, context, manual=True)
                col = dbops.Column(
                    name=ptr_stor_info.column_name,
                    type=common.qname(*ptr_stor_info.column_type))
                col = dbops.AlterTableDropColumn(col)
                alter_table.add_operation(col)
                self.pgops.add(alter_table)

            self.attach_alter_table(context)

        if has_table(link, orig_schema):
            self.drop_inhview(orig_schema, context, link, conditional=True)
            self.alter_ancestor_inhviews(
                orig_schema, context, link,
                exclude_children=frozenset((link,)))
            condition = dbops.TableExists(name=old_table_name)
            self.pgops.add(
                dbops.DropTable(name=old_table_name, conditions=[condition]))

        self.schedule_endpoint_delete_action_update(
            link, orig_schema, schema, context)


class CreateLink(LinkMetaCommand, adapts=s_links.CreateLink):
    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._create_begin(schema, context)

        link = self.scls
        if link in context.external_objs:
            return schema

        self.table_name = common.get_backend_name(schema, link, catenate=False)

        self._create_link(link, schema, orig_schema, context)

        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        link = self.scls
        if link in context.external_objs and has_table(link, schema):
            self.schedule_external_type_inhview_create(link, schema, context)
        return schema

    def _create_finalize(self, schema, context):
        schema = super()._create_finalize(schema, context)
        self.apply_scheduled_inhview_updates(schema, context)
        return schema


class RenameLink(LinkMetaCommand, adapts=s_links.RenameLink):
    pass


class RebaseLink(LinkMetaCommand, adapts=s_links.RebaseLink):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema
        if has_table(self.scls, schema):
            self.update_base_inhviews_on_rebase(
                schema, orig_schema, context, self.scls)

        schema = super()._alter_innards(schema, context)

        if not self.scls.is_pure_computable(schema):
            self.schedule_endpoint_delete_action_update(
                self.scls, orig_schema, schema, context)

        return schema


class SetLinkType(LinkMetaCommand, adapts=s_links.SetLinkType):

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        pop = self.get_parent_op(context)
        orig_type = self.scls.get_target(orig_schema)
        new_type = self.scls.get_target(schema)
        if (
            not pop.maybe_get_object_aux_data('from_alias')
            and not self.scls.is_pure_computable(schema)
            and (orig_type != new_type or self.cast_expr is not None)
        ):
            self._alter_pointer_type(self.scls, schema, orig_schema, context)
            self.schedule_endpoint_delete_action_update(
                self.scls, orig_schema, schema, context)
        return schema


class AlterLinkUpperCardinality(
    LinkMetaCommand,
    adapts=s_links.AlterLinkUpperCardinality,
):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema
        pop = self.get_parent_op(context)

        # We need to run the parent change *before* the children,
        # or else the view update in the child might fail if a
        # link table isn't created in the parent yet.
        if (
            not self.scls.generic(schema)
            and not self.scls.is_pure_computable(schema)
            and not pop.maybe_get_object_aux_data('from_alias')
        ):
            orig_card = self.scls.get_cardinality(orig_schema)
            new_card = self.scls.get_cardinality(schema)
            if orig_card != new_card:
                self._alter_pointer_cardinality(schema, orig_schema, context)

        return super()._alter_innards(schema, context)


class AlterLinkLowerCardinality(
    LinkMetaCommand,
    adapts=s_links.AlterLinkLowerCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        pop = self.get_parent_op(context)
        orig_schema = schema
        schema = super().apply(schema, context)

        if not self.scls.generic(schema):
            orig_required = self.scls.get_required(orig_schema)
            new_required = self.scls.get_required(schema)
            if (
                not pop.maybe_get_object_aux_data('from_alias')
                and not self.scls.is_endpoint_pointer(schema)
                and not self.scls.is_pure_computable(schema)
                and orig_required != new_required
            ):
                self._alter_pointer_optionality(
                    schema, orig_schema, context, fill_expr=self.fill_expr)

        return schema


class AlterLinkOwned(LinkMetaCommand, adapts=s_links.AlterLinkOwned):
    pass


class AlterLink(LinkMetaCommand, adapts=s_links.AlterLink):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema

        link = self.scls
        is_abs = link.generic(schema)
        is_comp = link.is_pure_computable(schema)
        was_comp = link.is_pure_computable(orig_schema)

        if not is_abs and (was_comp and not is_comp):
            self._create_link(link, schema, orig_schema, context)
        elif not is_abs and (not was_comp and is_comp):
            self._delete_link(link, schema, orig_schema, context)

        schema = super()._alter_innards(schema, context)

        # We check whether otd has changed, rather than whether
        # it is an attribute on this alter, because it might
        # live on a nested SetOwned, for example.
        otd_changed = (
            link.get_on_target_delete(orig_schema) !=
            link.get_on_target_delete(schema)
        )
        osd_changed = (
            link.get_on_source_delete(orig_schema) !=
            link.get_on_source_delete(schema)
        )
        card_changed = (
            link.get_cardinality(orig_schema) !=
            link.get_cardinality(schema)
        )
        if (
            (otd_changed or osd_changed or card_changed)
            and not link.is_pure_computable(schema)
        ):
            self.schedule_endpoint_delete_action_update(
                link, orig_schema, schema, context)

        return schema

    def _alter_finalize(self, schema, context):
        orig_schema = context.current().original_schema
        schema = super()._alter_finalize(schema, context)
        if not self.maybe_get_object_aux_data('from_alias'):
            self._validate_link_path(self.scls, schema, context)
            if self._alter_link_path(
                orig_schema=orig_schema,
                schema=schema, context=context
            ):
                self.apply_scheduled_inhview_updates(schema, context)
        return schema

    def _get_link_path_alter_info(
        self,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema
    ) -> Optional[_LinkAlterInfo]:
        old_link = orig_schema.get(self.classname, None)
        if old_link is None:
            return
        new_link = schema.get(self.classname, None)
        if new_link is None:
            return

        type_altered = old_link.get_target(orig_schema) \
            != new_link.get_target(schema)

        return _LinkAlterInfo(
            new_target=new_link.get_target_property(schema),
            new_source=new_link.get_source_property(schema),
            old_target=old_link.get_target_property(orig_schema),
            old_source=old_link.get_source_property(orig_schema),
            type_altered=type_altered
        )

    def _alter_link_path(
        self,
        orig_schema: s_schema.Schema,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> bool:
        link = self.scls
        if link.generic(schema):
            return False

        alter_info = self._get_link_path_alter_info(schema, orig_schema)
        if alter_info is None:
            return False

        stor_info = types.get_pointer_storage_info(link, schema=schema)
        is_line_link = stor_info.table_type == 'ObjectType'

        source_altered = alter_info.source_altered and not is_line_link
        target_altered = alter_info.target_altered

        if not (source_altered or target_altered):
            return False

        if is_line_link:
            link_main = link.get_source_type(schema)
            if self.get_annotation('is_propagated'):
                drop_inhview = [link_main] + list(link_main.get_ancestors(schema).objects(schema))
            else:
                drop_inhview = [link_main]
        else:
            link_main = link
            drop_inhview = [link_main]

        for obj in drop_inhview:
            self.drop_inhview(schema, context, obj, conditional=True)

        alter_table_before = self.get_alter_table(
            schema, context, table_name=stor_info.table_name,
            force_new=True, manual=True
        )
        self.pgops.add(alter_table_before)
        alter_table_after = self.get_alter_table(
            schema, context, table_name=stor_info.table_name,
            force_new=True, manual=True
        )

        if source_altered:
            add_col, update_col, drop_col = self._create_temp_column(
                stor_info.table_name,
                'source', link.get_source_type(schema),
                alter_info.new_source, alter_info.old_source, schema,
            )
            alter_table_before.add_operation(add_col)
            alter_table_after.add_operation(update_col)
            alter_table_after.add_operation(drop_col)

        if target_altered:
            add_col, update_col, drop_col = self._create_temp_column(
                stor_info.table_name,
                stor_info.column_name, link.get_target(schema),
                alter_info.new_target, alter_info.old_target, schema,
                link_type_changed=alter_info.type_altered
            )
            alter_table_before.add_operation(add_col)
            alter_table_after.add_operation(update_col)
            alter_table_after.add_operation(drop_col)

        self.pgops.add(alter_table_after)
        self.schedule_inhview_update(
            schema,
            context,
            link_main,
            s_objtypes.ObjectTypeCommandContext,
        )
        self.schedule_endpoint_delete_action_update(link, orig_schema, schema, context)
        return True

    def _create_temp_column(
        self,
        main_table: str,
        link_field: str,
        link_obj: s_types.Type,
        new_pointer: Optional[s_pointers.Pointer],
        old_pointer: Optional[s_pointers.Pointer],
        schema: s_schema.Schema,
        link_type_changed: bool = False
    ) -> Tuple[
        dbops.AlterTableAddColumn,
        dbops.AlterTableAlterColumnType,
        dbops.AlterTableDropColumn,
    ]:
        link_table = common.get_backend_name(schema, link_obj)
        temp_column = self._get_temp_column(new_pointer, schema)

        if link_type_changed:
            # 如果DDL中有SetLinkType，link字段会被重置为id列对应的值
            old_col = 'id'
        else:
            old_col = self._get_id_or_property_id(old_pointer)
        new_col = self._get_id_or_property_id(new_pointer)
        upd_src_qry = textwrap.dedent(
            f"""\
                UPDATE {q(*main_table)} AS main
                SET {q(temp_column.name)} = link.{new_col}
                FROM {link_table} AS link
                WHERE
                    main.{q(link_field)} = link.{old_col}
            """
        )
        self.pgops.add(dbops.Query(upd_src_qry))

        return (
            dbops.AlterTableAddColumn(temp_column),
            dbops.AlterTableAlterColumnType(
                link_field, self.resolve_col_type(new_pointer, schema),
                using_expr=qi(temp_column.name)
            ),
            dbops.AlterTableDropColumn(temp_column)
        )

    @staticmethod
    def _get_id_or_property_id(
        ptr: Optional[s_pointers.Pointer],
        quoted: bool = True,
    ):
        ptr_stor = 'id' if ptr is None else str(ptr.id)
        if quoted:
            return q(ptr_stor)
        return ptr_stor

    def _get_temp_column(self,
        ptr: Optional[s_pointers.Pointer],
        schema: s_schema.Schema,
    ):
        col_prefix = self._get_id_or_property_id(ptr, quoted=False)
        col_type = self.resolve_col_type(ptr, schema)
        temp_column = dbops.Column(
            name=f'??{col_prefix}_{common.get_unique_random_name()}',
            type=col_type,
        )
        return temp_column


class DeleteLink(LinkMetaCommand, adapts=s_links.DeleteLink):
    def _delete_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema
        link = schema.get(self.classname)

        schema = super()._delete_innards(schema, context)

        if link.get_external(schema):
            return schema

        self._delete_link(link, schema, orig_schema, context)

        return schema

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        link = schema.get(self.classname)
        has_extern_table = (
            link.get_external(schema)
            and has_table(link, schema))

        schema = super().apply(schema, context)

        self.apply_scheduled_inhview_updates(schema, context)
        if has_extern_table:
            self.pgops.add(dbops.DropView(name=('edgedbpub', str(link.id))))
            self.pgops.add(dbops.DropView(name=('edgedbpub', str(link.id) + '_t')))
            return schema

        return schema


class PropertyMetaCommand(CompositeMetaCommand, PointerMetaCommand):
    @classmethod
    def _create_table(
            cls, prop, schema, context, conditional=False, create_bases=True,
            create_children=True):
        new_table_name = common.get_backend_name(schema, prop, catenate=False)

        create_c = dbops.CommandGroup()

        constraints = []
        columns = []

        src_col = common.edgedb_name_to_pg_name('source')

        columns.append(
            dbops.Column(
                name=src_col, type='uuid', required=True))

        id = sn.QualName(
            module=prop.get_name(schema).module, name=str(prop.id))
        index_name = common.convert_name(id, 'idx0', catenate=True)

        pg_index = dbops.Index(
            name=index_name, table_name=new_table_name,
            unique=False, columns=[src_col])

        ci = dbops.CreateIndex(pg_index)

        if not prop.generic(schema):
            tgt_cols = cls.get_columns(prop, schema, None)
            columns.extend(tgt_cols)

        table = dbops.Table(name=new_table_name)
        table.add_columns(columns)
        table.constraints = constraints

        ct = dbops.CreateTable(table=table)

        if conditional:
            c = dbops.CommandGroup(
                neg_conditions=[dbops.TableExists(new_table_name)])
        else:
            c = dbops.CommandGroup()

        c.add_command(ct)
        c.add_command(ci)

        c.add_command(
            dbops.Comment(
                table,
                str(prop.get_verbosename(schema, with_parent=True)),
            ),
        )

        create_c.add_command(c)

        if create_children:
            for p_descendant in prop.descendants(schema):
                if has_table(p_descendant, schema):
                    pc = PropertyMetaCommand._create_table(
                        p_descendant, schema, context, conditional=True,
                        create_bases=False, create_children=False)
                    create_c.add_command(pc)

        return create_c

    def _create_property(
        self,
        prop: s_props.Property,
        src: Optional[s_sources.SourceCommandContext],
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        propname = prop.get_shortname(schema).name

        if has_table(prop, schema):
            self.create_table(prop, schema, context)

        if (
            src
            and has_table(src.scls, schema)
            and not prop.is_pure_computable(schema)
        ):
            if (
                isinstance(src.scls, s_links.Link)
                and not has_table(src.scls, orig_schema)
            ):
                ct = src.op._create_table(src.scls, schema, context)
                self.pgops.add(ct)

            ptr_stor_info = types.get_pointer_storage_info(
                prop, resolve_type=False, schema=schema)

            fills_required = any(
                x.fill_expr for x in
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))
            sets_required = bool(
                self.get_subcommands(
                    type=s_pointers.AlterPointerLowerCardinality))

            if (
                not isinstance(src.scls, s_objtypes.ObjectType)
                or ptr_stor_info.table_type == 'ObjectType'
            ):
                if (
                    not isinstance(src.scls, s_links.Link)
                    or propname not in {'source', 'target'}
                ):
                    alter_table = src.op.get_alter_table(
                        schema,
                        context,
                        force_new=True,
                        manual=True,
                    )

                    default_value = self.get_pointer_default(
                        prop, schema, context)

                    if (
                        isinstance(src.scls, s_links.Link)
                        and not default_value
                        and prop.get_default(schema)
                    ):
                        raise errors.UnsupportedFeatureError(
                            f'default value for '
                            f'{prop.get_verbosename(schema, with_parent=True)}'
                            f' is too complicated; link property defaults '
                            f'must not depend on database contents',
                            context=self.source_context)

                    cols = self.get_columns(
                        prop, schema, default_value, sets_required)

                    for col in cols:
                        cmd = dbops.AlterTableAddColumn(col)
                        alter_table.add_operation(cmd)

                        if col.name == 'id':
                            constraint = dbops.PrimaryKey(
                                table_name=alter_table.name,
                                columns=[col.name],
                            )
                            alter_table.add_operation(
                                dbops.AlterTableAddConstraint(constraint),
                            )

                    self.pgops.add(alter_table)

                self.schedule_inhview_source_update(
                    schema,
                    context,
                    prop,
                    s_sources.SourceCommandContext,
                )

            if (
                (default := prop.get_default(schema))
                and not prop.is_pure_computable(schema)
                and not fills_required
            ):
                self._alter_pointer_optionality(
                    schema, schema, context, fill_expr=default)
            # If we're creating a required multi pointer without a SET
            # REQUIRED USING inside, run the alter_pointer_optionality
            # path to produce an error if there is existing data.
            elif (
                prop.get_cardinality(schema).is_multi()
                and prop.get_required(schema)
                and not prop.is_pure_computable(schema)
                and not sets_required
            ):
                self._alter_pointer_optionality(
                    schema, schema, context, fill_expr=None)

            if not prop.is_pure_computable(schema):
                self.schedule_endpoint_delete_action_update(
                    prop, orig_schema, schema, context)

    def _delete_property(
        self,
        prop: s_props.Property,
        source: s_sources.Source,
        source_op,
        schema: s_schema.Schema,
        orig_schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if has_table(source, schema):
            ptr_stor_info = types.get_pointer_storage_info(
                prop,
                schema=schema,
                link_bias=prop.is_link_property(schema),
            )

            if (
                ptr_stor_info.table_type == 'ObjectType'
                or prop.is_link_property(schema)
            ):
                alter_table = source_op.get_alter_table(
                    schema, context, force_new=True, manual=True)

                # source and target don't have a proper inheritence
                # hierarchy, so we can't do the source trick for them
                is_endpoint_ptr = prop.is_endpoint_pointer(schema)
                self.recreate_inhview(
                    schema,
                    context,
                    source,
                    exclude_ptrs=frozenset((prop,)),
                    alter_ancestors=is_endpoint_ptr,
                )
                if not is_endpoint_ptr:
                    self.alter_ancestor_source_inhviews(
                        schema, context, prop
                    )

                col = dbops.AlterTableDropColumn(
                    dbops.Column(name=ptr_stor_info.column_name,
                                 type=ptr_stor_info.column_type))

                alter_table.add_operation(col)

                self.pgops.add(alter_table)
        elif (
            prop.is_link_property(schema)
            and has_table(source, orig_schema)
        ):
            self.drop_inhview(orig_schema, context, source)
            self.alter_ancestor_inhviews(
                orig_schema, context, source,
                exclude_children=frozenset((source,)))
            old_table_name = common.get_backend_name(
                orig_schema, source, catenate=False)
            self.pgops.add(dbops.DropTable(name=old_table_name))

        if has_table(prop, orig_schema):
            self.drop_inhview(orig_schema, context, prop)
            self.alter_ancestor_inhviews(
                schema, context, prop, exclude_children={prop})
            old_table_name = common.get_backend_name(
                orig_schema, prop, catenate=False)
            self.pgops.add(dbops.DropTable(name=old_table_name))
            self.schedule_endpoint_delete_action_update(
                prop, orig_schema, schema, context)


class CreateProperty(PropertyMetaCommand, adapts=s_props.CreateProperty):
    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = schema
        schema = super()._create_begin(schema, context)
        prop = self.scls

        if prop in context.external_objs:
            return schema

        src = context.get(s_sources.SourceCommandContext)

        self._create_property(prop, src, schema, orig_schema, context)

        return schema


class RenameProperty(PropertyMetaCommand, adapts=s_props.RenameProperty):
    pass


class RebaseProperty(PropertyMetaCommand, adapts=s_props.RebaseProperty):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        orig_schema = context.current().original_schema
        if has_table(self.scls, schema):
            self.update_base_inhviews_on_rebase(
                schema, orig_schema, context, self.scls)

        schema = super()._alter_innards(schema, context)

        if not self.scls.is_pure_computable(schema):
            self.schedule_endpoint_delete_action_update(
                self.scls, orig_schema, schema, context)

        return schema


class SetPropertyType(PropertyMetaCommand, adapts=s_props.SetPropertyType):
    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        pop = self.get_parent_op(context)
        orig_schema = schema
        schema = super()._alter_begin(schema, context)
        orig_type = self.scls.get_target(orig_schema)
        new_type = self.scls.get_target(schema)
        if (
            not pop.maybe_get_object_aux_data('from_alias')
            and not self.scls.is_pure_computable(schema)
            and not self.scls.is_endpoint_pointer(schema)
            and (orig_type != new_type or self.cast_expr is not None)
        ):
            self._alter_pointer_type(self.scls, schema, orig_schema, context)
        return schema


class AlterPropertyUpperCardinality(
    PropertyMetaCommand,
    adapts=s_props.AlterPropertyUpperCardinality,
):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        pop = self.get_parent_op(context)
        orig_schema = context.current().original_schema

        # We need to run the parent change *before* the children,
        # or else the view update in the child might fail if a
        # link table isn't created in the parent yet.
        if (
            not self.scls.generic(schema)
            and not self.scls.is_pure_computable(schema)
            and not self.scls.is_endpoint_pointer(schema)
            and not pop.maybe_get_object_aux_data('from_alias')
        ):
            orig_card = self.scls.get_cardinality(orig_schema)
            new_card = self.scls.get_cardinality(schema)
            if orig_card != new_card:
                self._alter_pointer_cardinality(schema, orig_schema, context)

        return super()._alter_innards(schema, context)


class AlterPropertyLowerCardinality(
    PropertyMetaCommand,
    adapts=s_props.AlterPropertyLowerCardinality,
):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        pop = self.get_parent_op(context)
        orig_schema = schema
        schema = super().apply(schema, context)

        if not self.scls.generic(schema):
            orig_required = self.scls.get_required(orig_schema)
            new_required = self.scls.get_required(schema)
            if (
                not pop.maybe_get_object_aux_data('from_alias')
                and not self.scls.is_endpoint_pointer(schema)
                and not self.scls.is_pure_computable(schema)
                and orig_required != new_required
            ):
                self._alter_pointer_optionality(
                    schema, orig_schema, context, fill_expr=self.fill_expr)

        return schema


class AlterPropertyOwned(
    PropertyMetaCommand,
    adapts=s_props.AlterPropertyOwned,
):
    pass


class AlterProperty(PropertyMetaCommand, adapts=s_props.AlterProperty):
    def _alter_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        prop = self.scls
        orig_schema = context.current().original_schema

        src = context.get(s_sources.SourceCommandContext)
        is_comp = prop.is_pure_computable(schema)
        was_comp = prop.is_pure_computable(orig_schema)

        if src and (was_comp and not is_comp):
            self._create_property(prop, src, schema, orig_schema, context)
        elif src and (not was_comp and is_comp):
            self._delete_property(
                prop, src.scls, src.op, schema, orig_schema, context)

        schema = super()._alter_innards(schema, context)

        if self.metadata_only:
            return schema

        if not is_comp:
            orig_def_val = self.get_pointer_default(prop, orig_schema, context)
            def_val = self.get_pointer_default(prop, schema, context)

            if orig_def_val != def_val:
                if prop.get_cardinality(schema).is_multi():
                    source_op = self
                else:
                    source_op = context.get_ancestor(
                        s_sources.SourceCommandContext, self).op
                alter_table = source_op.get_alter_table(
                    schema, context, manual=True)

                ptr_stor_info = types.get_pointer_storage_info(
                    prop, schema=schema)
                alter_table.add_operation(
                    dbops.AlterTableAlterColumnDefault(
                        column_name=ptr_stor_info.column_name,
                        default=def_val))

                self.pgops.add(alter_table)

            card = self.get_resolved_attribute_value(
                'cardinality',
                schema=schema,
                context=context,
            )
            if card:
                self.schedule_endpoint_delete_action_update(
                    prop, orig_schema, schema, context)

        return schema


class DeleteProperty(PropertyMetaCommand, adapts=s_props.DeleteProperty):

    def _delete_innards(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._delete_innards(schema, context)
        prop = self.scls

        if prop.get_source(schema).get_external(schema):
            return schema

        orig_schema = context.current().original_schema

        source_ctx = self.get_referrer_context(context)
        if source_ctx is not None:
            source = source_ctx.scls
            source_op = source_ctx.op
        else:
            source = source_op = None

        if source and not prop.is_pure_computable(schema):
            self._delete_property(
                prop, source, source_op, schema, orig_schema, context)

        return schema


class UpdateEndpointDeleteActions(MetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.link_ops = []
        self.changed_targets = set()

    def _get_link_table_union(self, schema, links, include_children) -> str:
        selects = []
        aspect = 'inhview' if include_children else None
        for link in links:
            selects.append(textwrap.dedent('''\
                (SELECT
                    {id}::uuid AS __sobj_id__,
                    {src} as source,
                    {tgt} as target
                FROM {table})
            ''').format(
                id=ql(str(link.id)),
                src=common.quote_ident('source'),
                tgt=common.quote_ident('target'),
                table=common.get_backend_name(
                    schema,
                    link,
                    aspect=aspect,
                ),
            ))

        return '(' + '\nUNION ALL\n    '.join(selects) + ') as q'

    def _filter_duplicated_inhview_links(self, schema, links):
        """
        只能在aspect = inhview 时调用
        有的link可能来自于继承，如果父表link已经出现，
        由于父表的view中已经出现该link的信息，没有必要重复union
        """
        # TODO
        all_source_links = frozenset(
            (link.get_source(schema), link.get_displayname(schema), link.get_target(schema))
            for link in links
        )

        for link in links:
            for ancestor in link.get_source(schema).get_ancestors(schema):
                pass

        return links

    def _get_inline_link_table_union(
            self, schema, links, include_children) -> str:
        selects = []
        aspect = 'inhview' if include_children else None
        for link in links:
            link_psi = types.get_pointer_storage_info(link, schema=schema)
            link_col = link_psi.column_name
            selects.append(textwrap.dedent('''\
                (SELECT
                    {id}::uuid AS __sobj_id__,
                    {src} as source,
                    {tgt} as target
                FROM {table})
            ''').format(
                id=ql(str(link.id)),
                src=common.quote_ident('id'),
                tgt=common.quote_ident(link_col),
                table=common.get_backend_name(
                    schema,
                    link.get_source(schema),
                    aspect=aspect,
                ),
            ))

        return '(' + '\nUNION ALL\n    '.join(selects) + ') as q'

    def get_target_objs(self, link, schema):
        tgt = link.get_target(schema)
        if union := tgt.get_union_of(schema).objects(schema):
            objs = set(union)
        else:
            objs = {tgt}
        objs |= {
            x for obj in objs for x in obj.descendants(schema)}
        return {obj for obj in objs if not obj.is_view(schema)}

    def get_orphan_link_ancestors(self, link, schema):
        val = s_links.LinkSourceDeleteAction.DeleteTargetIfOrphan
        if link.get_on_source_delete(schema) != val:
            return set()
        ancestors = {
            x
            for base in link.get_bases(schema).objects(schema)
            for x in self.get_orphan_link_ancestors(base, schema)
        }
        if ancestors:
            return ancestors
        else:
            return {link}

    def get_trigger_name(self, schema, target,
                         disposition, deferred=False, inline=False):
        if disposition == 'target':
            aspect = 'target-del'
        else:
            aspect = 'source-del'

        if deferred:
            aspect += '-def'
        else:
            aspect += '-imm'

        if inline:
            aspect += '-inl'
        else:
            aspect += '-otl'

        aspect += '-t'

        # Postgres applies triggers in alphabetical order, and
        # get_backend_name produces essentially cryptographically
        # random trigger names.
        #
        # All we want for now is for source triggers to apply first,
        # though, so that a loop of objects with
        # 'on source delete delete target' + 'on target delete restrict'
        # succeeds.
        #
        # Fortunately S comes before T.
        order_prefix = disposition[0]

        return order_prefix + common.get_backend_name(
            schema, target, catenate=False, aspect=aspect)[1]

    def get_trigger_proc_name(self, schema, target,
                              disposition, deferred=False, inline=False):
        if disposition == 'target':
            aspect = 'target-del'
        else:
            aspect = 'source-del'

        if deferred:
            aspect += '-def'
        else:
            aspect += '-imm'

        if inline:
            aspect += '-inl'
        else:
            aspect += '-otl'

        aspect += '-f'

        return common.get_backend_name(
            schema, target, catenate=False, aspect=aspect)

    def get_trigger_proc_text(self, target, links, *,
                              disposition, inline, schema):
        if inline:
            return self._get_inline_link_trigger_proc_text(
                target, links, disposition=disposition, schema=schema)
        else:
            return self._get_outline_link_trigger_proc_text(
                target, links, disposition=disposition, schema=schema)

    def _get_dunder_type_trigger_proc_text(self, target, *, schema):
        body = textwrap.dedent('''\
            SELECT
                CASE WHEN tp.builtin
                    THEN 'edgedbstd'
                    ELSE 'edgedbpub'
                END AS sname
                INTO schema_name
            FROM edgedb."_SchemaType" as tp
            WHERE tp.id = OLD.id;

            SELECT EXISTS (
                SELECT FROM pg_tables
                WHERE  schemaname = "schema_name"
                AND    tablename  = OLD.id::text
            ) INTO table_exists;

            IF table_exists THEN
                target_sql = format('SELECT EXISTS (SELECT FROM %I.%I LIMIT 1)', "schema_name", OLD.id::text);
                EXECUTE target_sql into del_prohibited;
            ELSE
                del_prohibited = FALSE;
            END IF;

            IF del_prohibited THEN
            RAISE foreign_key_violation
                USING
                    TABLE = TG_TABLE_NAME,
                    SCHEMA = TG_TABLE_SCHEMA,
                    MESSAGE = 'deletion of {tgtname} (' || OLD.id
                        || ') is prohibited by link target policy',
                    DETAIL = 'Object is still referenced in link __type__'
                        || ' of ' || edgedb._get_schema_object_name(OLD.id) || ' ('
                        || OLD.id || ').';
            END IF;
        '''.format(tgtname=target.get_displayname(schema)))

        text = textwrap.dedent('''\
            DECLARE
                table_exists bool;
                del_prohibited bool;
                schema_name text;
                target_sql text;
            BEGIN
                {body}
                RETURN OLD;
            END;
        ''').format(body=body)
        return text

    def _get_outline_link_trigger_proc_text(
            self, target, links, *, disposition, schema):

        chunks = []

        DA = s_links.LinkTargetDeleteAction

        if disposition == 'target':
            groups = itertools.groupby(
                links, lambda l: l.get_on_target_delete(schema))
            near_endpoint, far_endpoint = 'target', 'source'
            resolve_target = target
        else:
            groups = itertools.groupby(
                links, lambda l: (
                    l.get_on_source_delete(schema)
                    if isinstance(l, s_links.Link)
                    else s_links.LinkSourceDeleteAction.Allow))
            near_endpoint, far_endpoint = 'source', 'target'
            resolve_target = None
        defined_vars = {}

        def _declare_var(var_prefix, index, var_type):
            var_name = f'{var_prefix}_{index}'
            if var_name not in defined_vars:
                defined_vars[var_name] = f"{var_name} {var_type};"

            return var_name

        for action, links in groups:
            if action is DA.Restrict or action is DA.DeferredRestrict:
                group_key = lambda l: self._resolve_link_group(
                    l, schema,
                    GB.TARGET_T | GB.SOURCE_T | GB.get_flag(near_endpoint),
                    resolve_target=resolve_target
                )
                for idx, ((tgt_type, src_type, ne_field), links) in enumerate(
                    itertools.groupby(links, group_key)
                ):
                    # Inherited link targets with restrict actions are
                    # elided by apply() to enable us to use inhviews here
                    # when looking for live references.
                    srcid = _declare_var('srcid', idx, src_type)
                    tgtid = _declare_var('tgtid', idx, tgt_type)

                    tables = self._get_link_table_union(
                        schema, links, include_children=True)

                    text = textwrap.dedent('''\
                        SELECT
                            q.__sobj_id__, q.source, q.target
                            INTO link_type_id, {srcid}, {tgtid}
                        FROM
                            {tables}
                        WHERE
                            q.{near_endpoint} = OLD.{id}
                        LIMIT 1;

                        IF FOUND THEN
                            SELECT
                                edgedb.shortname_from_fullname(link.name),
                                edgedb._get_schema_object_name(link.{far_endpoint})
                                INTO linkname, endname
                            FROM
                                edgedb."_SchemaLink" AS link
                            WHERE
                                link.id = link_type_id;
                            RAISE foreign_key_violation
                                USING
                                    TABLE = TG_TABLE_NAME,
                                    SCHEMA = TG_TABLE_SCHEMA,
                                    MESSAGE = 'deletion of {tgtname} (' || {tgtid}
                                        || ') is prohibited by link target policy',
                                    DETAIL = 'Object is still referenced in link '
                                        || linkname || ' of ' || endname || ' ('
                                        || {srcid} || ').';
                        END IF;
                    ''').format(
                        srcid=srcid,
                        tgtid=tgtid,
                        tables=tables,
                        id=ne_field,
                        tgtname=target.get_displayname(schema),
                        near_endpoint=near_endpoint,
                        far_endpoint=far_endpoint,
                    )

                    chunks.append(text)

            elif (
                action == s_links.LinkTargetDeleteAction.Allow
                or action == s_links.LinkSourceDeleteAction.Allow
            ):
                group_key = lambda l: self._resolve_link_group(
                    l, schema,
                    GB.SOURCE_T | GB.get_flag(near_endpoint),
                    resolve_target=resolve_target
                )
                for idx, ((src_type, ne_field), links) in enumerate(
                    itertools.groupby(links, group_key)
                ):
                    for link in links:
                        link_table = common.get_backend_name(
                            schema, link)

                        # Since enforcement of 'required' on multi links
                        # is enforced manually on the query side and (not
                        # through constraints/triggers of its own), we
                        # also need to do manual enforcement of it when
                        # deleting a required multi link.
                        if link.get_required(schema) and disposition == 'target':
                            srcid = _declare_var('srcid', idx, src_type)
                            required_text = textwrap.dedent('''\
                                SELECT q.source INTO {srcid}
                                FROM {link_table} as q
                                    WHERE q.target = OLD.{id}
                                    AND NOT EXISTS (
                                        SELECT FROM {link_table} as q2
                                        WHERE q.source = q2.source
                                              AND q2.target != OLD.{id}
                                    );

                                IF FOUND THEN
                                    RAISE not_null_violation
                                        USING
                                            TABLE = TG_TABLE_NAME,
                                            SCHEMA = TG_TABLE_SCHEMA,
                                            MESSAGE = 'missing value',
                                            COLUMN = '{link_id}';
                                END IF;
                            ''').format(
                                srcid=srcid,
                                link_table=link_table,
                                link_id=str(link.id),
                                id=ne_field
                            )

                            chunks.append(required_text)

                        # Otherwise just delete it from the link table.
                        text = textwrap.dedent('''\
                            DELETE FROM
                                {link_table}
                            WHERE
                                {endpoint} = OLD.{id};
                        ''').format(
                            link_table=link_table,
                            endpoint=common.quote_ident(near_endpoint),
                            id=ne_field
                        )

                        chunks.append(text)

            elif action == s_links.LinkTargetDeleteAction.DeleteSource:
                sources = collections.defaultdict(list)
                for link in links:
                    group_by = self._resolve_link_group(
                        link, schema,
                        GB.SOURCE_T | GB.TARGET_T | GB.SOURCE_F | GB.TARGET_F,
                        resolve_target=resolve_target
                    )
                    sources[(group_by, link.get_source(schema))].append(link)

                for (group_by, source), source_links in sources.items():
                    *_, src_field, tgt_field = group_by

                    tables = self._get_link_table_union(
                        schema, source_links, include_children=False)

                    text = textwrap.dedent('''\
                        DELETE FROM
                            {source_table}
                        WHERE
                            {source_table}.{src_field} IN (
                                SELECT source
                                FROM {tables}
                                WHERE target = OLD.{tgt_field}
                            );
                    ''').format(
                        source_table=common.get_backend_name(schema, source),
                        src_field=src_field,
                        tgt_field=tgt_field,
                        tables=tables,
                    )

                    chunks.append(text)

            elif (
                action == s_links.LinkSourceDeleteAction.DeleteTarget
                or action ==
                    s_links.LinkSourceDeleteAction.DeleteTargetIfOrphan
            ):
                for link in links:
                    link_table = common.get_backend_name(schema, link)
                    objs = self.get_target_objs(link, schema)
                    src_field, tgt_field = self._resolve_link_group(
                        link, schema, GB.SOURCE_F | GB.TARGET_F,
                        resolve_target=resolve_target
                    )

                    # If the link is DELETE TARGET IF ORPHAN, build
                    # filters to ignore any objects that aren't
                    # orphans (wrt to this link).
                    orphan_check = ''
                    for orphan_check_root in self.get_orphan_link_ancestors(
                            link, schema):
                        check_table = common.get_backend_name(
                            schema, orphan_check_root, aspect='inhview')

                        orphan_check += f'''\
                            AND NOT EXISTS (
                                SELECT FROM {check_table} as q2
                                WHERE q.target = q2.target
                                      AND q2.source != OLD.{src_field}
                            )
                        '''.strip()

                    # We find all the objects to delete in a CTE, then
                    # delete the link table entries, and then delete
                    # the targets. We apply the non-orphan filter when
                    # finding the objects.
                    prefix = textwrap.dedent(f'''\
                        WITH range AS (
                            SELECT target FROM {link_table} as q
                            WHERE q.source = OLD.{src_field}
                            {orphan_check}
                        ),
                        del AS (
                            DELETE FROM
                                {link_table}
                            WHERE
                                source = OLD.{src_field}
                        )
                    ''').strip()
                    parts = [prefix]

                    for i, obj in enumerate(objs):
                        tgt_table = common.get_backend_name(schema, obj)
                        text = textwrap.dedent(f'''\
                            d{i} AS (
                                DELETE FROM
                                    {tgt_table}
                                WHERE
                                    {tgt_table}.{tgt_field} IN (
                                        SELECT target
                                        FROM range
                                    )
                            )
                        ''').strip()
                        parts.append(text)

                    full = ',\n'.join(parts) + "\nSELECT '' INTO _dummy_text;"
                    chunks.append(full)

        text = textwrap.dedent('''\
            DECLARE
                link_type_id uuid;
                linkname text;
                endname text;
                _dummy_text text;
            {variables}
            BEGIN
            {chunks}
                RETURN OLD;
            END;
        ''').format(
            variables=textwrap.indent('\n'.join(defined_vars.values()), ' ' * 4),
            chunks='\n\n'.join(chunks)
        )

        return text

    def _get_link_field(self, link, disposition: str, schema, target=None):
        if disposition == 'target':
            prop = link.get_target_property(schema)
            if (
                prop is not None
                and target is not None
                and (src := prop.get_source(schema)) != target
                and (target.issubclass(schema, src))
            ):
                prop_name = prop.get_local_name(schema)
                prop = target.maybe_get_ptr(schema, prop_name)
        else:
            prop = link.get_source_property(schema)

        return 'id' if prop is None else q(str(prop.id))

    def _resolve_link_group(
        self,
        link,
        schema: s_schema.Schema,
        groupby: GB,
        resolve_target: s_objtypes.ObjectType = None,
    ):
        is_link = isinstance(link, s_links.Link)
        resolve_col = LinkMetaCommand.resolve_col_type

        group_var = []
        if groupby & GB.SOURCE_T:
            if not is_link:
                group_var.append('uuid')
            else:
                src_prop = link.get_source_property(schema)
                group_var.append(resolve_col(src_prop, schema))

        if groupby & GB.TARGET_T:
            if not is_link:
                group_var.append('uuid')
            else:
                tgt_prop = link.get_target_property(schema)
                group_var.append(resolve_col(tgt_prop, schema))

        if groupby & GB.SOURCE_F:
            if not is_link:
                group_var.append('id')
            else:
                group_var.append(self._get_link_field(link, 'source', schema))

        if groupby & GB.TARGET_F:
            if not is_link:
                group_var.append('id')
            else:
                group_var.append(self._get_link_field(link, 'target', schema, target=resolve_target))

        return tuple(group_var)

    def _get_inline_link_trigger_proc_text(
            self, target, links, *, disposition, schema):

        chunks = []

        DA = s_links.LinkTargetDeleteAction

        if disposition == 'target':
            groups = itertools.groupby(
                links, lambda l: l.get_on_target_delete(schema))
            resolve_target = target
        else:
            groups = itertools.groupby(
                links, lambda l: l.get_on_source_delete(schema))
            resolve_target = None

        near_endpoint, far_endpoint = 'target', 'source'
        defined_vars = []

        for action, links in groups:
            if action is DA.Restrict or action is DA.DeferredRestrict:
                grp_key = lambda l: self._resolve_link_group(
                    l, schema,
                    groupby=GB.SOURCE_T | GB.TARGET_T | GB.get_flag(near_endpoint),
                    resolve_target=resolve_target
                )

                for idx, ((src_type, tgt_type, ne_field), links) in enumerate(
                    itertools.groupby(links, grp_key)
                ):
                    # Inherited link targets with restrict actions are
                    # elided by apply() to enable us to use inhviews here
                    # when looking for live references.
                    srcid_placeholder = f'srcid_{idx}'
                    tgtid_placeholder = f'tgtid_{idx}'
                    defined_vars.extend([
                        f"{srcid_placeholder} {src_type};",
                        f"{tgtid_placeholder} {tgt_type};"
                    ])
                    tables = self._get_inline_link_table_union(
                        schema, links, include_children=True)

                    text = textwrap.dedent('''\
                        SELECT
                            q.__sobj_id__, q.source, q.target
                            INTO link_type_id, {srcid}, {tgtid}
                        FROM
                            {tables}
                        WHERE
                            q.{near_endpoint} = OLD.{id}
                        LIMIT 1;

                        IF FOUND THEN
                            SELECT
                                edgedb.shortname_from_fullname(link.name),
                                edgedb._get_schema_object_name(link.{far_endpoint})
                                INTO linkname, endname
                            FROM
                                edgedb."_SchemaLink" AS link
                            WHERE
                                link.id = link_type_id;
                            RAISE foreign_key_violation
                                USING
                                    TABLE = TG_TABLE_NAME,
                                    SCHEMA = TG_TABLE_SCHEMA,
                                    MESSAGE = 'deletion of {tgtname} (' || {tgtid}
                                        || ') is prohibited by link target policy',
                                    DETAIL = 'Object is still referenced in link '
                                        || linkname || ' of ' || endname || ' ('
                                        || {srcid} || ').';
                        END IF;
                    ''').format(
                        srcid=srcid_placeholder,
                        tgtid=tgtid_placeholder,
                        tables=tables,
                        id=ne_field,
                        tgtname=target.get_displayname(schema),
                        near_endpoint=near_endpoint,
                        far_endpoint=far_endpoint,
                    )

                    chunks.append(text)

            elif action == s_links.LinkTargetDeleteAction.Allow:
                for link in links:
                    link_psi = types.get_pointer_storage_info(
                        link, schema=schema)
                    link_col = link_psi.column_name
                    source_table = common.get_backend_name(
                        schema, link.get_source(schema))

                    link_src_field = self._get_link_field(link, 'source', schema)

                    text = textwrap.dedent(f'''\
                        UPDATE
                            {source_table}
                        SET
                            {qi(link_col)} = NULL
                        WHERE
                            {qi(link_col)} = OLD.{link_src_field};
                    ''')

                    chunks.append(text)

            elif action == s_links.LinkTargetDeleteAction.DeleteSource:
                sources = collections.defaultdict(list)
                for link in links:
                    link_tgt_field = self._get_link_field(link, 'target', schema)
                    sources[(link_tgt_field, link.get_source(schema))].append(link)

                for (tgt_field, source), source_links in sources.items():
                    tables = self._get_inline_link_table_union(
                        schema, source_links, include_children=False)

                    text = textwrap.dedent('''\
                        DELETE FROM
                            {source_table}
                        WHERE
                            {source_table}.id IN (
                                SELECT source
                                FROM {tables}
                                WHERE target = OLD.{id}
                            );
                    ''').format(
                        source_table=common.get_backend_name(schema, source),
                        id=tgt_field,
                        tables=tables,
                    )

                    chunks.append(text)

            elif (
                action == s_links.LinkSourceDeleteAction.DeleteTarget
                or action ==
                    s_links.LinkSourceDeleteAction.DeleteTargetIfOrphan
            ):
                for link in links:
                    objs = self.get_target_objs(link, schema)

                    link_psi = types.get_pointer_storage_info(
                        link, schema=schema)
                    link_col = common.quote_ident(link_psi.column_name)

                    # If the link is DELETE TARGET IF ORPHAN, filter out
                    # any objects that aren't orphans (wrt to this link).
                    orphan_check = ''
                    for orphan_check_root in self.get_orphan_link_ancestors(
                            link, schema):
                        check_source = orphan_check_root.get_source(schema)
                        check_table = common.get_backend_name(
                            schema, check_source, aspect='inhview')

                        check_link_psi = types.get_pointer_storage_info(
                            orphan_check_root, schema=schema)
                        check_link_col = common.quote_ident(
                            check_link_psi.column_name)

                        orphan_check += f'''\
                            AND NOT EXISTS (
                                SELECT FROM {check_table} as q2
                                WHERE q2.{check_link_col} = OLD.{link_col}
                                      AND q2.id != OLD.id
                            )
                        '''.strip()

                    # Do the orphan check (which trivially succeeds if
                    # the link isn't IF ORPHAN)
                    text = textwrap.dedent(f'''\
                        SELECT (
                            SELECT true
                            {orphan_check}
                        ) INTO ok;
                    ''').strip()

                    chunks.append(text)
                    tgt_field = self._get_link_field(link, 'target', schema)
                    for obj in objs:
                        tgt_table = common.get_backend_name(schema, obj)
                        text = textwrap.dedent(f'''\
                            IF ok THEN
                                DELETE FROM
                                    {tgt_table}
                                WHERE
                                    {tgt_table}.{tgt_field} = OLD.{link_col};
                            END IF;
                        ''')
                        chunks.append(text)

        text = textwrap.dedent('''\
            DECLARE
                link_type_id uuid;
                linkname text;
                endname text;
                ok bool;
                links text[];
            {variables}
            BEGIN
            {chunks}
                RETURN OLD;
            END;
        ''').format(
            variables=textwrap.indent('\n'.join(defined_vars), ' '*4),
            chunks='\n\n'.join(chunks)
        )

        return text

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if not self.link_ops and not self.changed_targets:
            return schema

        DA = s_links.LinkTargetDeleteAction

        affected_sources = set()
        affected_targets = {t for _, t in self.changed_targets}
        modifications = any(
            isinstance(op, RebaseObjectType) and op.removed_bases
            for op, _ in self.changed_targets
        )

        for link_op, link, orig_schema, eff_schema in self.link_ops:
            if (
                isinstance(link_op, (DeleteProperty, DeleteLink))
                or (
                    link.is_pure_computable(eff_schema)
                    and not link.is_pure_computable(orig_schema)
                )
            ):
                source = link.get_source(orig_schema)
                if source:
                    current_source = schema.get_by_id(source.id, None)
                    if (current_source is not None
                            and not current_source.is_view(schema)):
                        modifications = True
                        affected_sources.add(current_source)

            if not eff_schema.has_object(link.id):
                continue

            # If our link has a restrict policy, we don't need to update
            # the target on changes to inherited links.
            # Most importantly, this optimization lets us avoid updating
            # the triggers for every schema::Type subtype every time a
            # new object type is created containing a __type__ link.
            action = (
                link.get_on_target_delete(eff_schema)
                if isinstance(link, s_links.Link) else None)
            target_is_affected = not (
                (action is DA.Restrict or action is DA.DeferredRestrict)
                and (
                    link.field_is_inherited(eff_schema, 'on_target_delete')
                    or link.get_explicit_field_value(
                        eff_schema, 'on_target_delete', None) is None
                )
                and link.get_implicit_bases(eff_schema)
            ) and isinstance(link, s_links.Link)

            if (
                link.generic(eff_schema)
                or (
                    link.is_pure_computable(eff_schema)
                    and link.is_pure_computable(orig_schema)
                )
            ):
                continue

            source = link.get_source(eff_schema)
            target = link.get_target(eff_schema)

            if not isinstance(source, s_objtypes.ObjectType):
                continue

            if not isinstance(link_op, (CreateProperty, CreateLink)):
                modifications = True

            if isinstance(link_op, (DeleteProperty, DeleteLink)):
                current_target = schema.get_by_id(target.id, None)
                if target_is_affected and current_target is not None:
                    affected_targets.add(current_target)
            else:
                if source.is_view(eff_schema):
                    continue

                current_source = schema.get_by_id(source.id, None)
                if current_source:
                    affected_sources.add(current_source)

                if target_is_affected:
                    affected_targets.add(target)

                if isinstance(link_op, (SetLinkType, SetPropertyType)):
                    orig_target = link.get_target(orig_schema)
                    if target != orig_target:
                        current_orig_target = schema.get_by_id(
                            orig_target.id, None)
                        if current_orig_target is not None:
                            affected_targets.add(current_orig_target)

        for source in affected_sources:
            links = []
            inline_links = []

            for link in source.get_pointers(schema).objects(schema):
                if link.is_pure_computable(schema):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=schema)

                if ptr_stor_info.table_type == 'link':
                    links.append(link)
                elif (
                    isinstance(link, s_links.Link)
                    and link.get_on_source_delete(schema) !=
                    s_links.LinkSourceDeleteAction.Allow
                ):
                    inline_links.append(link)

            links.sort(
                key=lambda l: (
                    (l.get_on_target_delete(schema),)
                    if isinstance(l, s_links.Link) else (),
                    l.get_name(schema)))

            inline_links.sort(
                key=lambda l: (
                    (l.get_on_target_delete(schema),)
                    if isinstance(l, s_links.Link) else (),
                    l.get_name(schema)))

            if links or modifications:
                self._update_action_triggers(
                    schema, source, links, disposition='source')

            if inline_links or modifications:
                self._update_action_triggers(
                    schema, source, inline_links,
                    inline=True, disposition='source')

        # All descendants of affected targets also need to have their
        # triggers updated, so track them down.
        all_affected_targets = set()
        for target in affected_targets:
            union_of = target.get_union_of(schema)
            if union_of:
                objtypes = tuple(union_of.objects(schema))
            else:
                objtypes = (target,)

            for objtype in objtypes:
                all_affected_targets.add(objtype)
                for descendant in objtype.descendants(schema):
                    if has_table(descendant, schema):
                        all_affected_targets.add(descendant)

        for target in all_affected_targets:
            deferred_links = []
            deferred_inline_links = []
            links = []
            inline_links = []
            dunder_type_links = []

            inbound_links = schema.get_referrers(
                target, scls_type=s_links.Link, field_name='target')

            # We need to look at all inbound links to all ancestors
            for ancestor in target.get_ancestors(schema).objects(schema):
                inbound_links |= schema.get_referrers(
                    ancestor, scls_type=s_links.Link, field_name='target')

            for link in inbound_links:
                if link.is_pure_computable(schema):
                    continue
                action = link.get_on_target_delete(schema)

                # Enforcing link deletion policies on targets are
                # handled by looking at the inheritance views, when
                # restrict is the policy.
                # If the policy is allow or delete source, we need to
                # actually process this for each link.
                if (
                    (action is DA.Restrict or action is DA.DeferredRestrict)
                    and link.field_is_inherited(schema, 'on_target_delete')
                    and link.get_implicit_bases(schema)
                ):
                    continue

                source = link.get_source(schema)
                if source.is_view(schema):
                    continue
                ptr_stor_info = types.get_pointer_storage_info(
                    link, schema=schema)
                if ptr_stor_info.column_name == '__type__':
                    assert action is DA.Restrict
                    dunder_type_links.append(link)
                elif ptr_stor_info.table_type != 'link':
                    if action is DA.DeferredRestrict:
                        deferred_inline_links.append(link)
                    else:
                        inline_links.append(link)
                else:
                    if action is DA.DeferredRestrict:
                        deferred_links.append(link)
                    else:
                        links.append(link)

            links.sort(
                key=lambda l: (l.get_on_target_delete(schema),
                               l.get_name(schema)))

            inline_links.sort(
                key=lambda l: (l.get_on_target_delete(schema),
                               l.get_name(schema)))

            deferred_links.sort(
                key=lambda l: l.get_name(schema))

            deferred_inline_links.sort(
                key=lambda l: l.get_name(schema))

            if dunder_type_links:
                self._update_dunder_type_link_triggers(schema, target)

            if links or modifications:
                self._update_action_triggers(
                    schema, target, links, disposition='target')

            if inline_links or modifications:
                self._update_action_triggers(
                    schema, target, inline_links,
                    disposition='target', inline=True)

            if deferred_links or modifications:
                self._update_action_triggers(
                    schema, target, deferred_links,
                    disposition='target', deferred=True)

            if deferred_inline_links or modifications:
                self._update_action_triggers(
                    schema, target, deferred_inline_links,
                    disposition='target', deferred=True,
                    inline=True)

        return schema

    def _update_dunder_type_link_triggers(
        self,
        schema,
        objtype: s_objtypes.ObjectType,
    ) -> None:
        table_name = common.get_backend_name(
            schema, objtype, catenate=False)
        aspect = 'dunder-type-link-'
        trigger_name = common.get_backend_name(
            schema, objtype, catenate=False, aspect=aspect + 't')[1]

        proc_name = common.get_backend_name(
            schema, objtype, catenate=False, aspect=aspect + 'f')

        trigger = dbops.Trigger(
            name=trigger_name, table_name=table_name,
            events=('delete',), procedure=proc_name,
            is_constraint=True, inherit=True, deferred=False)

        proc_text = self._get_dunder_type_trigger_proc_text(
            objtype, schema=schema)

        trig_func = dbops.Function(
            name=proc_name, text=proc_text, volatility='volatile',
            returns='trigger', language='plpgsql')

        self.pgops.add(dbops.CreateOrReplaceFunction(trig_func))

        self.pgops.add(dbops.CreateTrigger(
            trigger, neg_conditions=[dbops.TriggerExists(
                trigger_name=trigger_name, table_name=table_name
            )]
        ))

    def _update_action_triggers(
            self,
            schema,
            objtype: s_objtypes.ObjectType,
            links: List[s_links.Link], *,
            disposition: str,
            deferred: bool=False,
            inline: bool=False) -> None:

        table_name = common.get_backend_name(
            schema, objtype, catenate=False)

        trigger_name = self.get_trigger_name(
            schema, objtype, disposition=disposition,
            deferred=deferred, inline=inline)

        proc_name = self.get_trigger_proc_name(
            schema, objtype, disposition=disposition,
            deferred=deferred, inline=inline)

        trigger = dbops.Trigger(
            name=trigger_name, table_name=table_name,
            events=('delete',), procedure=proc_name,
            is_constraint=True, inherit=True, deferred=deferred)

        if links:
            proc_text = self.get_trigger_proc_text(
                objtype, links, disposition=disposition,
                inline=inline, schema=schema)

            trig_func = dbops.Function(
                name=proc_name, text=proc_text, volatility='volatile',
                returns='trigger', language='plpgsql')

            self.pgops.add(dbops.CreateOrReplaceFunction(trig_func))

            self.pgops.add(dbops.CreateTrigger(
                trigger, neg_conditions=[dbops.TriggerExists(
                    trigger_name=trigger_name, table_name=table_name
                )]
            ))
        else:
            self.pgops.add(
                dbops.DropTrigger(
                    trigger,
                    conditions=[dbops.TriggerExists(
                        trigger_name=trigger_name,
                        table_name=table_name,
                    )]
                )
            )

            self.pgops.add(
                dbops.DropFunction(
                    name=proc_name,
                    args=[],
                    conditions=[dbops.FunctionExists(
                        name=proc_name,
                        args=[],
                    )]
                )
            )


class CreateExternalView(MetaCommand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.external_views: List[dbops.View] = []

    def collect_external_objects(
        self,
        obj: so.Object,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ):
        key = obj.get_displayname(schema)
        if (src_is_link := isinstance(obj, s_links.Link)):
            key = (obj.get_source_type(schema).get_displayname(schema), key)

        if key not in context.external_view:
            raise errors.SchemaDefinitionError(
                f"Failed to find view definition for external object {key} ")

        view_def = context.external_view[key]
        columns = []

        for ptr in context.external_objs:
            if not isinstance(ptr, s_pointers.Pointer):
                continue

            if (
                ptr.get_source(schema) != obj
                or ptr.is_pure_computable(schema)
                or has_table(ptr, schema)
            ):
                continue

            ptrname = ptr.get_shortname(schema).name

            if ptrname == 'id':
                columns.append("edgedbext.uuid_generate_v1mc() AS id")
            elif ptrname == '__type__':
                columns.append(f"'{(str(obj.id))}'::uuid AS __type__")
            else:
                if ptrname not in view_def.columns:
                    raise errors.SchemaDefinitionError(
                        f"Missing column definition for "
                        f"{ptr.get_verbosename(schema, with_parent=True)}")

                if src_is_link and ptrname in ('target', 'source'):
                    colview_name = ptrname
                else:
                    colview_name = str(ptr.id)

                columns.append(f"{q(view_def.columns[ptrname])} AS {q(colview_name)}")

        column_str = ',\n'.join(columns)
        query = f"SELECT {column_str} FROM {view_def.relation}"
        self.external_views.append(dbops.View(query=query, name=('edgedbpub', str(obj.id))))
        self.external_views.append(dbops.View(query=query, name=('edgedbpub', str(obj.id) + '_t')))


    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        for view in self.external_views:
            self.pgops.add(dbops.CreateView(view=view, or_replace=True))
        return schema


class ModuleMetaCommand(MetaCommand):
    pass


class CreateModule(ModuleMetaCommand, adapts=s_mod.CreateModule):
    pass


class AlterModule(ModuleMetaCommand, adapts=s_mod.AlterModule):
    pass


class DeleteModule(ModuleMetaCommand, adapts=s_mod.DeleteModule):
    pass


class DatabaseMixin:
    def ensure_has_create_database(self, backend_params):
        if not backend_params.has_create_database:
            self.pgops.add(
                dbops.Query(
                    f'''
                    SELECT
                        edgedb.raise(
                            NULL::uuid,
                            msg => 'operation is not supported by the backend',
                            exc => 'feature_not_supported'
                        )
                    INTO _dummy_text
                    '''
                )
            )


class CreateDatabase(MetaCommand, DatabaseMixin, adapts=s_db.CreateDatabase):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_database(backend_params)

        schema = super().apply(schema, context)
        db = self.scls
        tenant_id = self._get_tenant_id(context)
        db_name = common.get_database_backend_name(
            str(self.classname), tenant_id=tenant_id)
        tpl_name = common.get_database_backend_name(
            self.template or edbdef.EDGEDB_TEMPLATE_DB, tenant_id=tenant_id)
        self.pgops.add(
            dbops.CreateDatabase(
                dbops.Database(
                    db_name,
                    metadata=dict(
                        id=str(db.id),
                        tenant_id=tenant_id,
                        builtin=self.get_attribute_value('builtin'),
                        name=str(self.classname),
                    ),
                ),
                template=tpl_name,
            )
        )
        return schema


class DropDatabase(MetaCommand, DatabaseMixin, adapts=s_db.DropDatabase):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_database(backend_params)

        schema = super().apply(schema, context)
        tenant_id = self._get_tenant_id(context)
        db_name = common.get_database_backend_name(
            str(self.classname), tenant_id=tenant_id)
        self.pgops.add(dbops.DropDatabase(db_name))
        return schema


class RoleMixin:
    def ensure_has_create_role(self, backend_params):
        if not backend_params.has_create_role:
            self.pgops.add(
                dbops.Query(
                    f'''
                    SELECT
                        edgedb.raise(
                            NULL::uuid,
                            msg => 'operation is not supported by the backend',
                            exc => 'feature_not_supported'
                        )
                    INTO _dummy_text
                    '''
                )
            )


class CreateRole(MetaCommand, RoleMixin, adapts=s_roles.CreateRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_role(backend_params)

        schema = super().apply(schema, context)
        role = self.scls

        membership = list(role.get_bases(schema).names(schema))
        passwd = role.get_password(schema)
        superuser_flag = False

        members = set()

        role_name = str(role.get_name(schema))

        instance_params = backend_params.instance_params
        tenant_id = instance_params.tenant_id

        if role.get_superuser(schema):
            membership.append(edbdef.EDGEDB_SUPERGROUP)

            # If the cluster is not exposing an explicit superuser role,
            # we will make the created Postgres role superuser if we can
            if not instance_params.base_superuser:
                superuser_flag = backend_params.has_superuser_access

        if backend_params.session_authorization_role is not None:
            # When we connect to the backend via a proxy role, we
            # must ensure that role is a member of _every_ EdgeDB
            # role so that `SET ROLE` can work properly.
            members.add(backend_params.session_authorization_role)

        role = dbops.Role(
            name=common.get_role_backend_name(role_name, tenant_id=tenant_id),
            allow_login=True,
            superuser=superuser_flag,
            password=passwd,
            membership=[
                common.get_role_backend_name(parent_role, tenant_id=tenant_id)
                for parent_role in membership
            ],
            metadata=dict(
                id=str(role.id),
                name=role_name,
                tenant_id=tenant_id,
                password_hash=passwd,
                builtin=role.get_builtin(schema),
            ),
        )
        self.pgops.add(dbops.CreateRole(role))
        return schema


class AlterRole(MetaCommand, RoleMixin, adapts=s_roles.AlterRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        role = self.scls

        backend_params = self._get_backend_params(context)
        instance_params = backend_params.instance_params
        tenant_id = instance_params.tenant_id
        role_name = str(role.get_name(schema))

        kwargs = {}
        if self.has_attribute_value('password'):
            passwd = self.get_attribute_value('password')
            if backend_params.has_create_role:
                # Only modify Postgres password of roles managed by EdgeDB
                kwargs['password'] = passwd
            kwargs['metadata'] = dict(
                id=str(role.id),
                name=role_name,
                tenant_id=tenant_id,
                password_hash=passwd,
                builtin=role.get_builtin(schema),
            )

        pg_role_name = common.get_role_backend_name(
            role_name, tenant_id=tenant_id)
        if self.has_attribute_value('superuser'):
            self.ensure_has_create_role(backend_params)
            membership = list(role.get_bases(schema).names(schema))
            membership.append(edbdef.EDGEDB_SUPERGROUP)
            self.pgops.add(
                dbops.AlterRoleAddMembership(
                    name=pg_role_name,
                    membership=[
                        common.get_role_backend_name(
                            parent_role, tenant_id=tenant_id)
                        for parent_role in membership
                    ],
                )
            )

            superuser_flag = False

            # If the cluster is not exposing an explicit superuser role,
            # we will make the modified Postgres role superuser if we can
            if not instance_params.base_superuser:
                superuser_flag = backend_params.has_superuser_access

            kwargs['superuser'] = superuser_flag

        if backend_params.has_create_role:
            dbrole = dbops.Role(name=pg_role_name, **kwargs)
        else:
            dbrole = dbops.SingleRole(**kwargs)
        self.pgops.add(dbops.AlterRole(dbrole))

        return schema


class RebaseRole(MetaCommand, RoleMixin, adapts=s_roles.RebaseRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_role(backend_params)

        schema = super().apply(schema, context)
        role = self.scls

        tenant_id = self._get_tenant_id(context)

        for dropped in self.removed_bases:
            self.pgops.add(dbops.AlterRoleDropMember(
                name=common.get_role_backend_name(
                    str(dropped.name), tenant_id=tenant_id),
                member=common.get_role_backend_name(
                    str(role.get_name(schema)), tenant_id=tenant_id),
            ))

        for bases, _pos in self.added_bases:
            for added in bases:
                self.pgops.add(dbops.AlterRoleAddMember(
                    name=common.get_role_backend_name(
                        str(added.name), tenant_id=tenant_id),
                    member=common.get_role_backend_name(
                        str(role.get_name(schema)), tenant_id=tenant_id),
                ))

        return schema


class DeleteRole(MetaCommand, RoleMixin, adapts=s_roles.DeleteRole):
    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        backend_params = self._get_backend_params(context)
        self.ensure_has_create_role(backend_params)

        schema = super().apply(schema, context)
        tenant_id = self._get_tenant_id(context)
        self.pgops.add(dbops.DropRole(
            common.get_role_backend_name(
                str(self.classname), tenant_id=tenant_id)))
        return schema


class CreateExtensionPackage(
    MetaCommand,
    adapts=s_exts.CreateExtensionPackage,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        ext_id = str(self.scls.id)
        name__internal = str(self.scls.get_name(schema))
        name = self.scls.get_displayname(schema)
        version = self.scls.get_version(schema)._asdict()
        version['stage'] = version['stage'].name.lower()

        metadata = {
            ext_id: {
                'id': ext_id,
                'name': name,
                'name__internal': name__internal,
                'script': self.scls.get_script(schema),
                'version': version,
                'builtin': self.scls.get_builtin(schema),
                'internal': self.scls.get_internal(schema),
            }
        }

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        if backend_params.has_create_database:
            tenant_id = self._get_tenant_id(context)
            tpl_db_name = common.get_database_backend_name(
                edbdef.EDGEDB_TEMPLATE_DB, tenant_id=tenant_id)

            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.Database(name=tpl_db_name),
                    section='ExtensionPackage',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='ExtensionPackage',
                    metadata=metadata
                )
            )

        return schema


class DeleteExtensionPackage(
    MetaCommand,
    adapts=s_exts.DeleteExtensionPackage,
):

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)

        ctx_backend_params = context.backend_runtime_params
        if ctx_backend_params is not None:
            backend_params = cast(
                params.BackendRuntimeParams, ctx_backend_params)
        else:
            backend_params = params.get_default_runtime_params()

        ext_id = str(self.scls.id)
        metadata = {
            ext_id: None
        }

        if backend_params.has_create_database:
            tenant_id = self._get_tenant_id(context)
            tpl_db_name = common.get_database_backend_name(
                edbdef.EDGEDB_TEMPLATE_DB, tenant_id=tenant_id)
            self.pgops.add(
                dbops.UpdateMetadataSection(
                    dbops.Database(name=tpl_db_name),
                    section='ExtensionPackage',
                    metadata=metadata
                )
            )
        else:
            self.pgops.add(
                dbops.UpdateSingleDBMetadataSection(
                    edbdef.EDGEDB_TEMPLATE_DB,
                    section='ExtensionPackage',
                    metadata=metadata
                )
            )

        return schema


class ExtensionCommand(MetaCommand):
    pass


class CreateExtension(ExtensionCommand, adapts=s_exts.CreateExtension):
    pass


class DeleteExtension(ExtensionCommand, adapts=s_exts.DeleteExtension):
    pass


class DeltaRoot(MetaCommand, adapts=sd.DeltaRoot):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._renames = {}
        self.config_ops = []

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        self.update_endpoint_delete_actions = UpdateEndpointDeleteActions()
        self.create_extern_view_actions = CreateExternalView()

        schema = super().apply(schema, context)

        self.update_endpoint_delete_actions.apply(schema, context)
        self.pgops.add(self.update_endpoint_delete_actions)
        self.create_extern_view_actions.apply(schema, context)
        self.pgops.add(self.create_extern_view_actions)

        return schema

    def is_material(self):
        return True

    def generate(self, block: dbops.PLBlock) -> None:
        for op in self.pgops:
            op.generate(block)


class MigrationCommand(MetaCommand):
    pass


class CreateMigration(
    MigrationCommand,
    adapts=s_migrations.CreateMigration,
):
    pass


class AlterMigration(
    MigrationCommand,
    adapts=s_migrations.AlterMigration,
):
    pass


class DeleteMigration(
    MigrationCommand,
    adapts=s_migrations.DeleteMigration,
):
    pass
