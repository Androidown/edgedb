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

import contextlib
from typing import *

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb import errors
from edb.common import debug

from . import abc as s_abc
from . import constraints
from . import delta as sd
from . import indexes
from . import inheriting
from . import properties
from . import name as sn
from . import objects as so
from . import pointers
from . import referencing
from . import sources
from . import utils

if TYPE_CHECKING:
    from . import objtypes as s_objtypes
    from . import types as s_types
    from . import schema as s_schema
    from edb.common import parsing


LinkTargetDeleteAction = qltypes.LinkTargetDeleteAction
LinkSourceDeleteAction = qltypes.LinkSourceDeleteAction


def merge_actions(
    target: so.InheritingObject,
    sources: List[so.Object],
    field_name: str,
    *,
    ignore_local: bool = False,
    schema: s_schema.Schema,
    **kwargs,
) -> Any:
    if not ignore_local:
        ours = target.get_explicit_local_field_value(schema, field_name, None)
    else:
        ours = None
    if ours is None:
        current = None
        current_from = None

        for source in sources:
            theirs = source.get_explicit_field_value(schema, field_name, None)
            if theirs is not None:
                if current is None:
                    current = theirs
                    current_from = source
                elif current != theirs:
                    target_source = target.get_source(schema)
                    current_from_source = current_from.get_source(schema)
                    source_source = source.get_source(schema)

                    tgt_repr = (
                        f'{target_source.get_displayname(schema)}.'
                        f'{target.get_displayname(schema)}'
                    )
                    cf_repr = (
                        f'{current_from_source.get_displayname(schema)}.'
                        f'{current_from.get_displayname(schema)}'
                    )
                    other_repr = (
                        f'{source_source.get_displayname(schema)}.'
                        f'{source.get_displayname(schema)}'
                    )

                    raise errors.SchemaError(
                        f'cannot implicitly resolve the '
                        f'`on target delete` action for '
                        f'{tgt_repr!r}: it is defined as {current} in '
                        f'{cf_repr!r} and as {theirs} in {other_repr!r}; '
                        f'to resolve, declare `on target delete` '
                        f'explicitly on {tgt_repr!r}'
                    )
        return current
    else:
        return ours


def raise_link_path_conflict(
    path0, path1,
    aspect: str,
    schema: s_schema.Schema
):
    link0, prop0 = path0
    link1, prop1 = path1

    source0 = link0.get_source(schema)
    source1 = link1.get_source(schema)

    src0_name = source0.get_displayname(schema)
    src1_name = source1.get_displayname(schema)

    prop0_name = 'id' if prop0 is None else prop0.get_displayname(schema)
    prop1_name = 'id' if prop1 is None else prop1.get_displayname(schema)

    msg = f"Cannot inherit from {src0_name} and {src1_name} " \
          f"because they have different {aspect} at link '{link0.get_displayname(schema)}': " \
          f"'{prop0_name}' for {src0_name} but '{prop1_name}' for {src1_name}."
    raise errors.UnsupportedFeatureError(msg)


def raise_linkpath_overload_prohibited(
    ours,
    theirs,
    aspect: str,
    schema: s_schema.Schema,
):
    our_link, _ = ours
    _, their_prop = theirs
    our_src = our_link.get_source(schema)
    our_src_name = our_src.get_displayname(schema)

    if their_prop is None:
        their_prop_name = 'id'
    else:
        their_prop_name = their_prop.get_displayname(schema)

    msg = f"Overload link while changing its {aspect} is prohibited, " \
          f"{aspect} of '{our_src_name}.{our_link.get_displayname(schema)}' " \
          f"must be '{their_prop_name}'."
    raise errors.UnsupportedFeatureError(msg)


def merge_target_property(
    target: Link,
    sources: List[Link],
    field_name: str,
    *,
    ignore_local: bool = False,
    schema: s_schema.Schema,
    is_propagated: bool = False,
    **kwargs,
):
    last = None
    ours = (target, target.get_explicit_field_value(schema, field_name, None))
    std_link = schema.get('std::link')
    inherit_from_std = len(sources) == 1 and sources[0] == std_link

    if inherit_from_std:
        return ours[1]

    for source in sources:
        theirs = (source, source.get_explicit_field_value(schema, field_name, None))

        if last is not None and last[1] != theirs[1]:
            raise_link_path_conflict(
                last, theirs, field_name, schema)
        else:
            last = theirs

    if is_propagated:
        return last[1]

    if ours[1] is None:
        if last is not None:
            return last[1]
        else:
            return None
    elif last is not None and last[1] != ours[1]:
        raise_linkpath_overload_prohibited(ours, last, field_name, schema)
    else:
        return ours[1]


def merge_source_property(
    target: Link,
    sources: List[Link],
    field_name: str,
    *,
    ignore_local: bool = False,
    schema: s_schema.Schema,
    is_propagated: bool = False,
    **kwargs,
):
    last = None
    source_field = None
    ours_prop = target.get_explicit_field_value(schema, field_name, None)
    std_link = schema.get('std::link')
    inherit_from_std = len(sources) == 1 and sources[0] == std_link

    if inherit_from_std:
        return ours_prop

    our_field = None if ours_prop is None else ours_prop.get_local_name(schema)
    ours = (target, ours_prop)

    for source in sources:
        theirs = source.get_explicit_field_value(schema, field_name, None)
        their_field = 'id' if theirs is None else theirs.get_local_name(schema)
        current = (source, theirs)

        if source_field is not None and source_field != their_field:
            raise_link_path_conflict(
                last, current, field_name, schema)

        source_field = their_field
        last = current

    if source_field is not None:
        if (
            not is_propagated
            and our_field is not None
            and source_field != our_field
        ):
            raise_linkpath_overload_prohibited(
                ours, last, field_name, schema)

        link_source = target.get_source(schema)

        if link_source is not None:
            src_prop = link_source.maybe_get_ptr(
                schema, source_field, type=properties.Property)
        else:
            src_prop = None
    else:
        src_prop = None

    return src_prop


class Link(
    sources.Source,
    pointers.Pointer,
    s_abc.Link,
    qlkind=qltypes.SchemaObjectClass.LINK,
    data_safe=False,
):

    on_target_delete = so.SchemaField(
        LinkTargetDeleteAction,
        default=LinkTargetDeleteAction.Restrict,
        coerce=True,
        compcoef=0.9,
        merge_fn=merge_actions)

    on_source_delete = so.SchemaField(
        LinkSourceDeleteAction,
        default=LinkSourceDeleteAction.Allow,
        coerce=True,
        compcoef=0.9,
        merge_fn=merge_actions)

    source_property = so.SchemaField(
        properties.Property,
        default=None,
        compcoef=None,
        inheritable=True,
        merge_fn=merge_source_property
    )

    target_property = so.SchemaField(
        properties.Property,
        default=None,
        compcoef=None,
        inheritable=True,
        merge_fn=merge_target_property
    )

    if debug.flags.disable_link_path:
        def get_source_property(self, schema: s_schema.Schema):
            return None

        def get_target_property(self, schema: s_schema.Schema):
            return None

    def get_target(self, schema: s_schema.Schema) -> s_objtypes.ObjectType:
        return self.get_field_value(  # type: ignore[no-any-return]
            schema, 'target')

    def is_link_property(self, schema: s_schema.Schema) -> bool:
        return False

    def is_property(self, schema: s_schema.Schema) -> bool:
        return False

    def scalar(self) -> bool:
        return False

    def has_user_defined_properties(self, schema: s_schema.Schema) -> bool:
        return bool([p for p in self.get_pointers(schema).objects(schema)
                     if not p.is_special_pointer(schema)])

    def get_source_type(
        self,
        schema: s_schema.Schema
    ) -> s_types.Type:
        from . import types as s_types
        source = self.get_source(schema)
        assert isinstance(source, s_types.Type)
        return source

    def compare(
        self,
        other: so.Object,
        *,
        our_schema: s_schema.Schema,
        their_schema: s_schema.Schema,
        context: so.ComparisonContext,
    ) -> float:
        if not isinstance(other, Link):
            if isinstance(other, pointers.Pointer):
                return 0.0
            else:
                raise NotImplementedError()

        return super().compare(
            other, our_schema=our_schema,
            their_schema=their_schema, context=context)

    def set_target(
        self,
        schema: s_schema.Schema,
        target: s_types.Type,
    ) -> s_schema.Schema:
        schema = super().set_target(schema, target)
        tgt_prop = self.getptr(schema, sn.UnqualName('target'))
        schema = tgt_prop.set_target(schema, target)
        return schema

    @classmethod
    def get_root_classes(cls) -> Tuple[sn.QualName, ...]:
        return (
            sn.QualName(module='std', name='link'),
            sn.QualName(module='schema', name='__type__'),
        )

    @classmethod
    def get_default_base_name(self) -> sn.QualName:
        return sn.QualName('std', 'link')


class LinkSourceCommandContext(sources.SourceCommandContext):
    pass


class LinkSourceCommand(inheriting.InheritingObjectCommand[sources.Source_T]):
    pass


class LinkCommandContext(pointers.PointerCommandContext[Link],
                         constraints.ConsistencySubjectCommandContext,
                         properties.PropertySourceContext,
                         indexes.IndexSourceCommandContext):
    pass


class LinkCommand(
    properties.PropertySourceCommand[Link],
    pointers.PointerCommand[Link],
    context_class=LinkCommandContext,
    referrer_context_class=LinkSourceCommandContext,
):

    def _resolve_linkpath_attr(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Dict[str, so.Object]:
        # canonicalize linkpath attributes
        cmd_target_prop = self._get_attribute_set_cmd('target_property')
        cmd_source_prop = self._get_attribute_set_cmd('source_property')

        is_alter_link = isinstance(self, AlterLink)

        if is_alter_link:
            if cmd_target_prop is not None:
                cmd_target_prop.old_value = self.scls.get_explicit_field_value(
                    schema, 'target_property', default=None)
            if cmd_source_prop is not None:
                cmd_source_prop.old_value = self.scls.get_explicit_field_value(
                    schema, 'source_property', default=None)

        altered_props = {}

        if cmd_target_prop is not None:
            if is_alter_link:
                target_ref = self.get_local_attribute_value('target')
                if target_ref is None:
                    target = self.scls.get_target(schema)
                else:
                    target = target_ref.resolve(schema)
            else:
                target_shell = self.get_local_attribute_value('target')
                if target_shell is None:
                    target = self.get_attribute_value('target')
                else:
                    target = target_shell.resolve(schema)

            if target.is_compound_type(schema):
                raise errors.UnsupportedFeatureError(
                    'Setting link path on compound type is not yet supported.',
                    context=self.get_attribute_source_context('target')
                )

            altered = self._finalize_alter_attr(
                cmd_target_prop,
                'target_property',
                source=target,
                schema=schema,
                context=context,
            )
            altered_props.update(altered)

        if cmd_source_prop is not None:
            if is_alter_link:
                source = self.scls.get_source_type(schema)
            else:
                maybe_source_shell = self.get_local_attribute_value('source')
                if isinstance(maybe_source_shell, so.ObjectShell):
                    source = maybe_source_shell.resolve(schema)
                else:
                    source = maybe_source_shell

            altered = self._finalize_alter_attr(
                cmd_source_prop,
                'source_property',
                source=source,
                schema=schema,
                context=context,
            )
            altered_props.update(altered)

        return altered_props

    def _finalize_alter_attr(
        self,
        command,
        attrname: str,
        source: s_objtypes.ObjectType,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> Dict[str, so.Object]:
        name = command.new_value
        old_value = command.old_value
        sourcectx = command.source_context

        if isinstance(name, properties.Property):
            name = name.get_displayname(schema)

        if name == 'id' or name is None:
            new_val, discard = None, old_value is None
        else:
            uq_name = sn.UnqualName(name)
            ptr = source.maybe_get_ptr(schema, uq_name)
            if ptr is not None:
                new_val, discard = ptr, ptr == old_value
            else:
                vname = source.get_verbosename(schema, with_parent=True)
                err = errors.InvalidReferenceError(
                    f'{vname} has no property {name!r}',
                    context=sourcectx
                )
                utils.enrich_schema_lookup_error(
                    err,
                    uq_name,
                    modaliases=context.modaliases,
                    item_type=properties.Property,
                    collection=source.get_pointers(schema).objects(schema),
                    schema=schema,
                )
                raise err

        if discard:
            self.discard(command)
            return {}
        else:
            self.set_attribute_value(
                attrname, new_val, source_context=sourcectx)
            return {attrname: new_val}

    def _append_subcmd_ast(
        self,
        schema: s_schema.Schema,
        node: qlast.DDLOperation,
        subcmd: sd.Command,
        context: sd.CommandContext,
    ) -> None:
        if (
            isinstance(subcmd, pointers.PointerCommand)
            and subcmd.classname != self.classname

        ):
            pname = sn.shortname_from_fullname(subcmd.classname)
            if pname.name in {'source', 'target'}:
                return

        super()._append_subcmd_ast(schema, node, subcmd, context)

    def validate_object(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> None:
        """Check that link definition is sound."""
        super().validate_object(schema, context)

        scls = self.scls
        assert isinstance(scls, Link)

        if not scls.get_owned(schema):
            return

        target = scls.get_target(schema)
        assert target is not None

        if not target.is_object_type():
            srcctx = self.get_attribute_source_context('target')
            raise errors.InvalidLinkTargetError(
                f'invalid link target type, expected object type, got '
                f'{target.get_verbosename(schema)}',
                context=srcctx,
            )

        if target.is_free_object_type(schema):
            srcctx = self.get_attribute_source_context('target')
            raise errors.InvalidLinkTargetError(
                f'{target.get_verbosename(schema)} is not a valid link target',
                context=srcctx,
            )

        if (
            not scls.is_pure_computable(schema)
            and not scls.get_from_alias(schema)
            and target.is_view(schema)
        ):
            srcctx = self.get_attribute_source_context('target')
            raise errors.InvalidLinkTargetError(
                f'invalid link type: {target.get_displayname(schema)!r}'
                f' is an expression alias, not a proper object type',
                context=srcctx,
            )

        self._validate_link_path(scls, schema, context)

    def _validate_link_path(
        self,
        scls: Link,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ):
        if (
            scls.get_target_property(schema) is None
            and (scls.get_target(schema) is not None
                 and scls.get_target(schema).get_external(schema))
        ):
            raise errors.SchemaDefinitionError(
                f"target_property is required in {self.get_friendly_description()} "
                f"from {scls.get_source(schema).get_verbosename(schema)} "
                f"to external {scls.get_target(schema).get_verbosename(schema, with_parent=True)}."
            )

        if (
            (src_prop := scls.get_source_property(schema)) is not None
            and not src_prop.is_exclusive(schema)
        ):
            srcctx = self.get_attribute_source_context('source_property')
            raise errors.SchemaDefinitionError(
                f'invalid link source property for {scls.get_verbosename(schema, with_parent=True)}, '
                f'{src_prop.get_verbosename(schema, with_parent=True)} is not exclusive.',
                context=srcctx
            )

        if (
            (tgt_prop := scls.get_target_property(schema)) is not None
            and not tgt_prop.is_exclusive(schema)
        ):
            srcctx = self.get_attribute_source_context('target_property')
            raise errors.SchemaDefinitionError(
                f'invalid link target property for {scls.get_verbosename(schema, with_parent=True)}, '
                f'{tgt_prop.get_verbosename(schema, with_parent=True)} is not exclusive.',
                context=srcctx
            )

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        node = super()._get_ast(schema, context, parent_node=parent_node)
        # __type__ link is special, and while it exists on every object
        # it does not have a defined default in the schema (and therefore
        # it isn't marked as required.)  We intervene here to mark all
        # __type__ links required when rendering for SDL/TEXT.
        if context.declarative and node is not None:
            assert isinstance(node, (qlast.CreateConcreteLink,
                                     qlast.CreateLink))
            if node.name.name == '__type__':
                assert isinstance(node, qlast.CreateConcretePointer)
                node.is_required = True
        return node

    def _reinherit_classref_dict(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> Tuple[s_schema.Schema,
               Dict[sn.Name, Type[sd.ObjectCommand[so.Object]]]]:
        if self.scls.get_computable(schema) and refdict.attr != 'pointers':
            # If the link is a computable, the inheritance would only
            # happen in the case of aliasing, and in that case we only
            # need to inherit the link properties and nothing else.
            return schema, {}

        return super()._reinherit_classref_dict(schema, context, refdict)


class CreateLink(
    pointers.CreatePointer[Link],
    LinkCommand,
):
    astnode = [qlast.CreateConcreteLink, qlast.CreateLink]
    referenced_astnode = qlast.CreateConcreteLink

    def _create_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        with contextlib.ExitStack() as stack:
            if not context.canonical:
                stack.enter_context(self._hide_linkpath_props())
            schema = super()._create_begin(schema, context)

        if not context.canonical and not context.mark_derived:
            altered_props = self._resolve_linkpath_attr(schema, context)
            if altered_props:
                schema = self.scls.update(schema, altered_props)

        return schema

    @contextlib.contextmanager
    def _hide_linkpath_props(self):
        tgt_op = self._get_attribute_set_cmd('target_property')
        src_op = self._get_attribute_set_cmd('source_property')
        self.discard(tgt_op)
        self.discard(src_op)
        try:
            yield
        finally:
            if tgt_op is not None:
                self.add(tgt_op)
            if src_op is not None:
                self.add(src_op)

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        if isinstance(astnode, qlast.CreateConcreteLink):
            assert isinstance(cmd, pointers.PointerCommand)
            cmd._process_create_or_alter_ast(schema, astnode, context)
        else:
            # this is an abstract property then
            if cmd.get_attribute_value('default') is not None:
                raise errors.SchemaDefinitionError(
                    f"'default' is not a valid field for an abstract link",
                    context=astnode.context)
        assert isinstance(cmd, sd.Command)
        return cmd

    def get_ast_attr_for_field(
        self,
        field: str,
        astnode: Type[qlast.DDLOperation],
    ) -> Optional[str]:
        if (
            field == 'required'
            and issubclass(astnode, qlast.CreateConcreteLink)
        ):
            return 'is_required'
        elif (
            field == 'cardinality'
            and issubclass(astnode, qlast.CreateConcreteLink)
        ):
            return 'cardinality'
        else:
            return super().get_ast_attr_for_field(field, astnode)

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        objtype = self.get_referrer_context(context)

        if op.property == 'target' and objtype:
            # Due to how SDL is processed the underlying AST may be an
            # AlterConcreteLink, which requires different handling.
            if isinstance(node, qlast.CreateConcreteLink):
                if not node.target:
                    expr = self.get_attribute_value('expr')
                    if expr is not None:
                        node.target = expr.qlast
                    else:
                        t = op.new_value
                        assert isinstance(t, (so.Object, so.ObjectShell))
                        node.target = utils.typeref_to_ast(schema, t)
            else:
                old_type = pointers.merge_target(
                    self.scls,
                    list(self.scls.get_bases(schema).objects(schema)),
                    'target',
                    ignore_local=True,
                    schema=schema,
                )
                assert isinstance(op.new_value, (so.Object, so.ObjectShell))
                new_type = (
                    op.new_value.resolve(schema)
                    if isinstance(op.new_value, so.ObjectShell)
                    else op.new_value)

                new_type_ast = utils.typeref_to_ast(schema, op.new_value)
                cast_expr = None
                # If the type isn't assignment castable, generate a
                # USING with a nonsense cast. It shouldn't matter,
                # since there should be no data to cast, but the DDL side
                # of things doesn't know that since the command is split up.
                if old_type and not old_type.assignment_castable_to(
                        new_type, schema):
                    cast_expr = qlast.TypeCast(
                        type=new_type_ast,
                        expr=qlast.Set(elements=[]),
                    )
                node.commands.append(
                    qlast.SetPointerType(
                        value=new_type_ast,
                        cast_expr=cast_expr,
                    )
                )

        elif op.property == 'on_target_delete':
            node.commands.append(qlast.OnTargetDelete(cascade=op.new_value))
        elif op.property == 'on_source_delete':
            node.commands.append(qlast.OnSourceDelete(cascade=op.new_value))
        elif op.property == 'target_property':
            ref = qlast.ObjectRef(name=op.new_value.get_displayname(schema))
            link_path_op = qlast.get_ddl_subcommand(node, qlast.SetLinkPath)
            if link_path_op is not None:
                link_path_op.target = ref
            else:
                node.commands.append(qlast.SetLinkPath(target=ref))
        elif op.property == 'source_property':
            ref = qlast.ObjectRef(name=op.new_value.get_displayname(schema))
            link_path_op = qlast.get_ddl_subcommand(node, qlast.SetLinkPath)
            if link_path_op is not None:
                link_path_op.source = ref
            else:
                node.commands.append(qlast.SetLinkPath(
                    source=ref, target=qlast.ObjectRef(name='id')))
        else:
            super()._apply_field_ast(schema, context, node, op)

    def inherit_classref_dict(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        refdict: so.RefDict,
    ) -> sd.CommandGroup:
        if self.scls.get_computable(schema) and refdict.attr != 'pointers':
            # If the link is a computable, the inheritance would only
            # happen in the case of aliasing, and in that case we only
            # need to inherit the link properties and nothing else.
            return sd.CommandGroup()

        cmd = super().inherit_classref_dict(schema, context, refdict)

        if refdict.attr != 'pointers':
            return cmd

        parent_ctx = self.get_referrer_context(context)
        if parent_ctx is None:
            return cmd

        base_prop_name = sn.QualName('std', 'source')
        s_name = sn.get_specialized_name(
            sn.QualName('__', 'source'), str(self.classname))
        src_prop_name = sn.QualName(
            name=s_name, module=self.classname.module)

        src_prop = properties.CreateProperty(
            classname=src_prop_name,
            is_strong_ref=True,
        )
        src_prop.set_attribute_value('name', src_prop_name)
        src_prop.set_attribute_value(
            'bases',
            so.ObjectList.create(schema, [schema.get(base_prop_name)]),
        )
        src_prop.set_attribute_value(
            'source',
            self.scls,
        )
        src_prop.set_attribute_value(
            'target',
            parent_ctx.op.scls,
        )
        src_prop.set_attribute_value('required', True)
        src_prop.set_attribute_value('readonly', True)
        src_prop.set_attribute_value('owned', True)
        src_prop.set_attribute_value('from_alias',
                                     self.scls.get_from_alias(schema))
        src_prop.set_attribute_value('cardinality',
                                     qltypes.SchemaCardinality.One)

        cmd.prepend(src_prop)

        base_prop_name = sn.QualName('std', 'target')
        s_name = sn.get_specialized_name(
            sn.QualName('__', 'target'), str(self.classname))
        tgt_prop_name = sn.QualName(
            name=s_name, module=self.classname.module)

        tgt_prop = properties.CreateProperty(
            classname=tgt_prop_name,
            is_strong_ref=True,
        )

        tgt_prop.set_attribute_value('name', tgt_prop_name)
        tgt_prop.set_attribute_value(
            'bases',
            so.ObjectList.create(schema, [schema.get(base_prop_name)]),
        )
        tgt_prop.set_attribute_value(
            'source',
            self.scls,
        )
        tgt_prop.set_attribute_value(
            'target',
            self.get_attribute_value('target'),
        )
        tgt_prop.set_attribute_value('required', False)
        tgt_prop.set_attribute_value('readonly', True)
        tgt_prop.set_attribute_value('owned', True)
        tgt_prop.set_attribute_value('from_alias',
                                     self.scls.get_from_alias(schema))
        tgt_prop.set_attribute_value('cardinality',
                                     qltypes.SchemaCardinality.One)

        cmd.prepend(tgt_prop)

        return cmd


class RenameLink(
    LinkCommand,
    referencing.RenameReferencedInheritingObject[Link],
):
    pass


class RebaseLink(
    LinkCommand,
    referencing.RebaseReferencedInheritingObject[Link],
):
    pass


class SetLinkType(
    pointers.SetPointerType[Link],
    referrer_context_class=LinkSourceCommandContext,
    field='target',
):

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_begin(schema, context)
        scls = self.scls

        new_target = scls.get_target(schema)

        if not context.canonical:
            # We need to update the target link prop as well
            tgt_prop = scls.getptr(schema, sn.UnqualName('target'))
            tgt_prop_alter = tgt_prop.init_delta_command(
                schema, sd.AlterObject)
            tgt_prop_alter.set_attribute_value('target', new_target)
            self.add(tgt_prop_alter)

        return schema

    def _alter_finalize(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super()._alter_finalize(schema, context)
        link = self.scls
        link_op = self.get_parent_op(context)

        if (
            not link_op.get_annotation('implicit_propagation')
            and context.canonical
            and not link_op.get_annotation('set_linkpath')
            and (tgt_prop := link.get_target_property(schema)) is not None
        ):
            orig_target = tgt_prop.get_source(schema)
            new_target = link.get_target(schema)

            action = self.get_friendly_description()
            link_desc = link.get_verbosename(schema=schema, with_parent=True)
            if (
                orig_target != new_target
            ):
                raise errors.SchemaDefinitionError(
                    f"cannot {action} because this affects "
                    f"'target_property' of {link_desc}.",
                    details=(
                        f"target_property '{tgt_prop.get_displayname(schema)}' "
                        f"belongs to {orig_target.get_verbosename(schema)}, "
                        f"not {new_target.get_verbosename(schema)}"
                    )
                )

        if (
            not link_op.get_annotation('implicit_propagation')
            and context.canonical
            and link_op.get_annotation('set_linkpath')
            and link.descendants(schema)
        ):
            action = self.get_friendly_description()
            raise errors.UnsupportedFeatureError(
                f"{action} on a pathed parent link is not yet supported. "
            )
        return schema


class AlterLinkUpperCardinality(
    pointers.AlterPointerUpperCardinality[Link],
    referrer_context_class=LinkSourceCommandContext,
    field='cardinality',
):
    pass


class AlterLinkLowerCardinality(
    pointers.AlterPointerLowerCardinality[Link],
    referrer_context_class=LinkSourceCommandContext,
    field='required',
):
    pass


class AlterLinkOwned(
    referencing.AlterOwned[Link],
    pointers.PointerCommandOrFragment[Link],
    referrer_context_class=LinkSourceCommandContext,
    field='owned',
):
    pass


class SetTargetDeletePolicy(sd.Command):
    astnode = qlast.OnTargetDelete

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.AlterObjectProperty:
        return sd.AlterObjectProperty(
            property='on_target_delete'
        )

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.OnTargetDelete)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, sd.AlterObjectProperty)
        cmd.new_value = astnode.cascade
        return cmd


class SetSourceDeletePolicy(sd.Command):
    astnode = qlast.OnSourceDelete

    @classmethod
    def _cmd_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.AlterObjectProperty:
        return sd.AlterObjectProperty(
            property='on_source_delete'
        )

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> sd.Command:
        assert isinstance(astnode, qlast.OnSourceDelete)
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, sd.AlterObjectProperty)
        cmd.new_value = astnode.cascade
        return cmd


class AlterLink(
    LinkCommand,
    pointers.AlterPointer[Link],
):
    astnode = [qlast.AlterConcreteLink, qlast.AlterLink]
    referenced_astnode = qlast.AlterConcreteLink

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.DDLOperation,
        context: sd.CommandContext,
    ) -> AlterLink:
        cmd = super()._cmd_tree_from_ast(schema, astnode, context)
        assert isinstance(cmd, AlterLink)
        if isinstance(astnode, qlast.CreateConcreteLink):
            cmd._process_create_or_alter_ast(schema, astnode, context)
        else:
            cmd._process_alter_ast(schema, astnode, context)
        return cmd

    def _apply_field_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        node: qlast.DDLOperation,
        op: sd.AlterObjectProperty,
    ) -> None:
        if op.property == 'target':
            if op.new_value:
                assert isinstance(op.new_value, so.ObjectShell)
                node.commands.append(
                    qlast.SetPointerType(
                        value=utils.typeref_to_ast(schema, op.new_value),
                    ),
                )
        elif op.property == 'computable':
            if not op.new_value:
                node.commands.append(
                    qlast.SetField(
                        name='expr',
                        value=None,
                        special_syntax=True,
                    ),
                )
        elif op.property == 'on_target_delete':
            node.commands.append(qlast.OnTargetDelete(cascade=op.new_value))
        elif op.property == 'on_source_delete':
            node.commands.append(qlast.OnSourceDelete(cascade=op.new_value))
        else:
            super()._apply_field_ast(schema, context, node, op)

    def _alter_begin(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        if (
            context.canonical
            and not self.get_annotation('link_path_resolved')
            and not self.maybe_get_object_aux_data('from_alias')
        ):
            altered_props = self._resolve_linkpath_attr(schema, context)
            if altered_props:
                schema = self.scls.update(schema, altered_props)
                if not self.get_annotation('is_propagated'):
                    self._propagate_field_alter(
                        schema, context, self.scls,
                        tuple(altered_props), mark_propagate=True
                    )
            self.set_annotation('link_path_resolved', True)
        return super()._alter_begin(schema, context)


class DeleteLink(
    LinkCommand,
    pointers.DeletePointer[Link],
):
    astnode = [qlast.DropConcreteLink, qlast.DropLink]
    referenced_astnode = qlast.DropConcreteLink

    # NB: target type cleanup (e.g. target compound type) is done by
    #     the DeleteProperty handler for the @target property.

    def _get_ast(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        *,
        parent_node: Optional[qlast.DDLOperation] = None,
    ) -> Optional[qlast.DDLOperation]:
        if self.get_orig_attribute_value('from_alias'):
            # This is an alias type, appropriate DDL would be generated
            # from the corresponding Alter/DeleteAlias node.
            return None
        else:
            return super()._get_ast(schema, context, parent_node=parent_node)


class SetLinkPath(sd.Command):
    astnode = qlast.SetLinkPath

    @classmethod
    def _cmd_tree_from_ast(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.SetLinkPath,
        context: sd.CommandContext,
    ):
        this_op = cls.get_parent_op(context)

        if isinstance(this_op, CreateLink):
            cls._attach_cmd(schema, astnode, context)
        else:
            cmd = super()._cmd_tree_from_ast(schema, astnode, context)
            cmd.astnode = astnode
            return cmd

    @classmethod
    def _attach_cmd(
        cls,
        schema: s_schema.Schema,
        astnode: qlast.SetLinkPath,
        context: sd.CommandContext,
    ):
        this_op = cls.get_parent_op(context)
        alter_tgt_prop = sd.AlterObjectProperty(
            property='target_property',
            new_value=astnode.target.name,
            source_context=astnode.target.context
        )
        this_op.add(alter_tgt_prop)
        cls._validate_cmd(
            alter_tgt_prop, schema, context, astnode.context
        )

        if astnode.source is not None:
            this_op.add(
                sd.AlterObjectProperty(
                    property='source_property',
                    new_value=astnode.source.name,
                    source_context=astnode.source.context
                )
            )
        this_op.set_annotation('set_linkpath', True)

    def apply(
        self,
        schema: s_schema.Schema,
        context: sd.CommandContext,
    ) -> s_schema.Schema:
        schema = super().apply(schema, context)
        if not context.canonical:
            self._attach_cmd(schema, self.astnode, context)
            self.get_parent_op(context).discard(self)
        return schema

    @classmethod
    def _validate_cmd(
        cls,
        cmd: sd.AlterObjectProperty,
        schema: s_schema.Schema,
        context: sd.CommandContext,
        src_contex: parsing.ParserContext
    ):
        link_op = cls.get_parent_op(context)
        if isinstance(link_op, CreateLink):
            return

        link = link_op.get_object(schema, context)
        assert isinstance(link, Link)
        ancestors = link.get_ancestors(schema).objects(schema)
        if len(ancestors) > 1:
            ans = ancestors[-2]
            op = cmd.get_friendly_description(parent_op=link_op, schema=schema)
            raise errors.QueryError(
                f"{op} is prohibited, alter that on "
                f"'{ans.get_source_type(schema).get_displayname(schema)}' instead.",
                context=src_contex
            )

    @classmethod
    def get_parent_op(
        cls,
        context: sd.CommandContext,
    ) -> LinkCommand:
        op = context.current().op
        assert isinstance(op, LinkCommand)
        return op

