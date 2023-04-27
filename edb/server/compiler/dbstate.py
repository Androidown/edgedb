#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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

import pickle
from typing import *  # NoQA

import dataclasses
import enum
import time
import uuid

import immutables

from edb import errors

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.schema import delta as s_delta
from edb.schema import migrations as s_migrations
from edb.schema import objects as s_obj
from edb.schema import schema as s_schema

from edb.server import config

from . import enums
from . import sertypes


class TxAction(enum.IntEnum):

    START = 1
    COMMIT = 2
    ROLLBACK = 3

    DECLARE_SAVEPOINT = 4
    RELEASE_SAVEPOINT = 5
    ROLLBACK_TO_SAVEPOINT = 6


class MigrationAction(enum.IntEnum):

    START = 1
    POPULATE = 2
    DESCRIBE = 3
    ABORT = 4
    COMMIT = 5
    REJECT_PROPOSED = 6


@dataclasses.dataclass(frozen=True)
class BaseQuery:

    sql: Tuple[bytes, ...]


@dataclasses.dataclass(frozen=True)
class NullQuery(BaseQuery):

    sql: Tuple[bytes, ...] = tuple()
    is_transactional: bool = True
    has_dml: bool = False


@dataclasses.dataclass(frozen=True)
class Query(BaseQuery):

    sql_hash: bytes

    cardinality: enums.Cardinality

    out_type_data: bytes
    out_type_id: bytes
    in_type_data: bytes
    in_type_id: bytes
    in_type_args: Optional[List[Param]] = None

    globals: Optional[List[str]] = None

    is_transactional: bool = True
    has_dml: bool = False
    single_unit: bool = False
    cacheable: bool = True
    # Set of object ids that used in this Query
    ref_ids: Optional[Set[uuid.UUID]] = None


@dataclasses.dataclass(frozen=True)
class SimpleQuery(BaseQuery):

    sql: Tuple[bytes, ...]
    is_transactional: bool = True
    has_dml: bool = False
    single_unit: bool = False
    # XXX: Temporary hack, since SimpleQuery will die
    in_type_args: Optional[List[Param]] = None


@dataclasses.dataclass(frozen=True)
class SessionStateQuery(BaseQuery):

    config_scope: Optional[qltypes.ConfigScope] = None
    is_backend_setting: bool = False
    requires_restart: bool = False
    config_op: Optional[config.Operation] = None
    is_transactional: bool = True
    single_unit: bool = False
    globals: Optional[List[str]] = None


@dataclasses.dataclass(frozen=True)
class DDLQuery(BaseQuery):

    user_schema: s_schema.FlatSchema
    global_schema: Optional[s_schema.FlatSchema] = None
    cached_reflection: Any = None
    is_transactional: bool = True
    single_unit: bool = False
    create_db: Optional[str] = None
    create_ns: Optional[str] = None
    drop_db: Optional[str] = None
    create_db_template: Optional[str] = None
    has_role_ddl: bool = False
    ddl_stmt_id: Optional[str] = None
    config_ops: List[config.Operation] = dataclasses.field(default_factory=list)
    schema_refl_sqls: Tuple[bytes, ...] = None
    stdview_sqls: Tuple[bytes, ...] = None


@dataclasses.dataclass(frozen=True)
class TxControlQuery(BaseQuery):

    action: TxAction
    cacheable: bool

    modaliases: Optional[immutables.Map]
    is_transactional: bool = True
    single_unit: bool = False

    user_schema: Optional[s_schema.FlatSchema] = None
    global_schema: Optional[s_schema.FlatSchema] = None
    cached_reflection: Any = None

    sp_name: Optional[str] = None
    sp_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class MigrationControlQuery(BaseQuery):

    action: MigrationAction
    tx_action: Optional[TxAction]
    cacheable: bool

    modaliases: Optional[immutables.Map]
    is_transactional: bool = True
    single_unit: bool = False

    user_schema: Optional[s_schema.FlatSchema] = None
    cached_reflection: Any = None
    ddl_stmt_id: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class Param:
    name: str
    required: bool
    array_type_id: Optional[uuid.UUID]
    outer_idx: int


#############################


@dataclasses.dataclass
class QueryUnit:

    sql: Tuple[bytes, ...]

    # Status-line for the compiled command; returned to front-end
    # in a CommandComplete protocol message if the command is
    # executed successfully.  When a QueryUnit contains multiple
    # EdgeQL queries, the status reflects the last query in the unit.
    status: bytes

    # Output format of this query unit
    output_format: enums.OutputFormat = enums.OutputFormat.NONE

    # Set only for units that contain queries that can be cached
    # as prepared statements in Postgres.
    sql_hash: bytes = b''

    # True if all statments in *sql* can be executed inside a transaction.
    # If False, they will be executed separately.
    is_transactional: bool = True

    # Capabilities used in this query
    capabilities: enums.Capability = enums.Capability(0)

    # True if this unit contains SET commands.
    has_set: bool = False

    # True if this unit contains ALTER/DROP/CREATE ROLE commands.
    has_role_ddl: bool = False

    # If tx_id is set, it means that the unit
    # starts a new transaction.
    tx_id: Optional[int] = None

    # True if this unit is single 'COMMIT' command.
    # 'COMMIT' is always compiled to a separate QueryUnit.
    tx_commit: bool = False

    # True if this unit is single 'ROLLBACK' command.
    # 'ROLLBACK' is always compiled to a separate QueryUnit.
    tx_rollback: bool = False

    # True if this unit is single 'ROLLBACK TO SAVEPOINT' command.
    # 'ROLLBACK TO SAVEPOINT' is always compiled to a separate QueryUnit.
    tx_savepoint_rollback: bool = False
    tx_savepoint_declare: bool = False

    # True if this unit is `ABORT MIGRATION` command within a transaction,
    # that means abort_migration and tx_rollback cannot be both True
    tx_abort_migration: bool = False

    # For SAVEPOINT commands, the name and sp_id
    sp_name: Optional[str] = None
    sp_id: Optional[str] = None

    # True if it is safe to cache this unit.
    cacheable: bool = False

    # If non-None, contains a name of the DB that is about to be
    # created/deleted. If it's the former, the IO process needs to
    # introspect the new db. If it's the later, the server should
    # close all inactive unused pooled connections to it.
    create_db: Optional[str] = None
    drop_db: Optional[str] = None

    # If non-None, contains a name of the DB that will be used as
    # a template database to create the database. The server should
    # close all inactive unused pooled connections to the template db.
    create_db_template: Optional[str] = None

    # If non-None, contains a name of the DB that is about to be
    # created/deleted.
    create_ns: Optional[str] = None

    # If non-None, the DDL statement will emit data packets marked
    # with the indicated ID.
    ddl_stmt_id: Optional[str] = None

    # Cardinality of the result set.  Set to NO_RESULT if the
    # unit represents multiple queries compiled as one script.
    cardinality: enums.Cardinality = \
        enums.Cardinality.NO_RESULT

    out_type_data: bytes = sertypes.NULL_TYPE_DESC
    out_type_id: bytes = sertypes.NULL_TYPE_ID.bytes
    in_type_data: bytes = sertypes.NULL_TYPE_DESC
    in_type_id: bytes = sertypes.NULL_TYPE_ID.bytes
    in_type_args: Optional[List[Param]] = None
    globals: Optional[List[str]] = None

    # Set only when this unit contains a CONFIGURE INSTANCE command.
    system_config: bool = False
    # Set only when this unit contains a CONFIGURE DATABASE command.
    database_config: bool = False
    # Set only when this unit contains a SET_GLOBAL command.
    set_global: bool = False
    # Whether any configuration change requires a server restart
    config_requires_restart: bool = False
    # Set only when this unit contains a CONFIGURE command which
    # alters a backend configuration setting.
    backend_config: bool = False
    config_ops: List[config.Operation] = dataclasses.field(default_factory=list)
    modaliases: Optional[immutables.Map] = None

    # If present, represents the future schema state after
    # the command is run. The schema is pickled.
    user_schema: Optional[bytes] = None
    cached_reflection: Optional[bytes] = None
    # The pickled user_shema mutation log, aim to replace user_schema
    user_schema_mutation: Optional[bytes] = None
    user_schema_mutation_obj: Optional[s_schema.SchemaMutationLogger] = None
    # Record affected object ids for cache clear
    affected_obj_ids: Optional[Set[uuid.UUID]] = None

    # If present, represents the future global schema state
    # after the command is run. The schema is pickled.
    global_schema: Optional[bytes] = None
    # schema reflection sqls, only available if this is a ddl stmt.
    schema_refl_sqls: Tuple[bytes, ...] = None
    stdview_sqls: Tuple[bytes, ...] = None

    @property
    def has_ddl(self) -> bool:
        return bool(self.capabilities & enums.Capability.DDL)

    def update_user_schema(self, base_schema: s_schema.FlatSchema):
        if self.user_schema_mutation is not None:
            mut: s_schema.SchemaMutationLogger = pickle.loads(self.user_schema_mutation)
            self.user_schema_mutation_obj = mut
            return mut.apply(base_schema)
        else:
            return base_schema

    @property
    def tx_control(self) -> bool:
        return (
            bool(self.tx_id)
            or self.tx_rollback
            or self.tx_commit
            or self.tx_savepoint_declare
            or self.tx_savepoint_rollback
        )


@dataclasses.dataclass
class QueryUnitGroup:

    # All capabilities used by any query units in this group
    capabilities: enums.Capability = enums.Capability(0)

    # True if it is safe to cache this unit.
    cacheable: bool = True

    # True if any query unit has transaction control commands, like COMMIT,
    # ROLLBACK, START TRANSACTION or SAVEPOINT-related commands
    tx_control: bool = False

    # Cardinality of the result set.  Set to NO_RESULT if the
    # unit group is not expected or desired to return data.
    cardinality: enums.Cardinality = enums.Cardinality.NO_RESULT

    out_type_data: bytes = sertypes.NULL_TYPE_DESC
    out_type_id: bytes = sertypes.NULL_TYPE_ID.bytes
    in_type_data: bytes = sertypes.NULL_TYPE_DESC
    in_type_id: bytes = sertypes.NULL_TYPE_ID.bytes
    in_type_args: Optional[List[Param]] = None
    globals: Optional[List[str]] = None
    # The pickled user_shema mutation log, aim to replace user_schema
    user_schema_mutation: Optional[bytes] = None

    units: List[QueryUnit] = dataclasses.field(default_factory=list)
    # Set of object ids that used in this Query
    ref_ids: Optional[Set[uuid.UUID]] = None
    # Record affected object ids for cache clear
    affected_obj_ids: Optional[Set[uuid.UUID]] = None

    def __iter__(self):
        return iter(self.units)

    def __len__(self):
        return len(self.units)

    def __getitem__(self, item):
        return self.units[item]

    def append(self, query_unit: QueryUnit):
        self.capabilities |= query_unit.capabilities

        if not query_unit.cacheable:
            self.cacheable = False

        if query_unit.tx_control:
            self.tx_control = True

        self.cardinality = query_unit.cardinality
        self.out_type_data = query_unit.out_type_data
        self.out_type_id = query_unit.out_type_id
        self.in_type_data = query_unit.in_type_data
        self.in_type_id = query_unit.in_type_id
        self.in_type_args = query_unit.in_type_args
        if query_unit.globals is not None:
            if self.globals is None:
                self.globals = []
            self.globals.extend(query_unit.globals)

        self.units.append(query_unit)


#############################


class ProposedMigrationStep(NamedTuple):

    statements: Tuple[str, ...]
    confidence: float
    prompt: str
    prompt_id: str
    data_safe: bool
    required_user_input: Tuple[Tuple[str, str]]
    # This isn't part of the output data, but is used to figure out
    # what to prohibit when something is rejected.
    operation_key: s_delta.CommandKey

    def to_json(self) -> Dict[str, Any]:
        user_input_list = []
        for var_name, var_desc in self.required_user_input:
            user_input_list.append({
                'placeholder': var_name,
                'prompt': var_desc,
            })

        return {
            'statements': [{'text': stmt} for stmt in self.statements],
            'confidence': self.confidence,
            'prompt': self.prompt,
            'prompt_id': self.prompt_id,
            'data_safe': self.data_safe,
            'required_user_input': user_input_list,
        }


class MigrationState(NamedTuple):

    parent_migration: Optional[s_migrations.Migration]
    initial_schema: s_schema.Schema
    initial_savepoint: Optional[str]
    target_schema: s_schema.Schema
    guidance: s_obj.DeltaGuidance
    accepted_cmds: Tuple[qlast.Command, ...]
    last_proposed: Optional[Tuple[ProposedMigrationStep, ...]]


class TransactionState(NamedTuple):

    id: int
    name: Optional[str]
    user_schema: s_schema.FlatSchema
    global_schema: s_schema.FlatSchema
    modaliases: immutables.Map
    session_config: immutables.Map
    database_config: immutables.Map
    system_config: immutables.Map
    cached_reflection: immutables.Map[str, Tuple[str, ...]]
    tx: Transaction
    mutation_idx: int
    migration_state: Optional[MigrationState] = None


def _clear_savepoints_user_schema(
    state: CompilerConnectionState,
    savepoints: Dict[int, TransactionState],
    tx_memo: Dict[Transaction, Transaction]
):
    new_sp = {}
    for spid, sp in savepoints.items():
        new_sp[spid] = sp._replace(
            user_schema=None,  # noqa
            tx=sp.tx.clear_user_schema(state, tx_memo)
        )
    return new_sp


def _restore_savepoints_user_schema(
    base_schema: s_schema.FlatSchema,
    savepoints: Dict[int, TransactionState],
    state: CompilerConnectionState,
    tx_memo: Dict[Transaction, Transaction]
):
    mutations = state._mutations  # noqa
    new_sp = {}
    for spid, sp in savepoints.items():
        mut = s_schema.SchemaMutationLogger.merge(mutations[:sp.mutation_idx])
        new_sp[spid] = sp._replace(
            user_schema=mut.apply(base_schema),
            tx=sp.tx.restore_user_schema(base_schema, state, tx_memo)
        )
    return new_sp


class Transaction:

    _savepoints: Dict[int, TransactionState]
    _constate: CompilerConnectionState

    def __init__(
        self,
        constate: CompilerConnectionState,
        *,
        user_schema: s_schema.FlatSchema,
        global_schema: s_schema.FlatSchema,
        modaliases: immutables.Map,
        session_config: immutables.Map,
        database_config: immutables.Map,
        system_config: immutables.Map,
        cached_reflection: immutables.Map[str, Tuple[str, ...]],
        implicit: bool = True,
    ) -> None:

        assert not isinstance(user_schema, s_schema.ChainedSchema)

        self._constate = constate

        self._id = constate._new_txid()
        self._implicit = implicit

        self._current = TransactionState(
            id=self._id,
            name=None,
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=modaliases,
            session_config=session_config,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=cached_reflection,
            tx=self,
            mutation_idx=len(constate._mutations)
        )

        self._state0 = self._current
        self._savepoints = {}

    def _create_template(self, constate: CompilerConnectionState):
        new = Transaction.__new__(Transaction)
        new._constate = constate
        new._id = self._id
        new._implicit = self._implicit
        return new

    def restore_user_schema(
        self,
        user_schema,
        state: CompilerConnectionState,
        tx_memo: Dict[Transaction, Transaction]
    ) -> Transaction:
        if self in tx_memo:
            return tx_memo[self]

        tx_memo[self] = new = self._create_template(state)
        curr_schema = s_schema.SchemaMutationLogger.merge(state._mutations).apply(user_schema)
        new._current = self._current._replace(
            user_schema=curr_schema,
            tx=self._current.tx.restore_user_schema(user_schema, state, tx_memo)
        )
        new._state0 = self._state0._replace(
            user_schema=user_schema,
            tx=self._state0.tx.restore_user_schema(user_schema, state, tx_memo)
        )
        new._savepoints = _restore_savepoints_user_schema(
            user_schema, self._savepoints, state, tx_memo
        )
        return new

    def clear_user_schema(
        self,
        state: CompilerConnectionState,
        tx_memo: Dict[Transaction, Transaction]
    ) -> Transaction:
        if self in tx_memo:
            return tx_memo[self]

        tx_memo[self] = new = self._create_template(state)

        new._current = self._current._replace(
            user_schema=None,
            tx=self._current.tx.clear_user_schema(state, tx_memo)
        )
        new._state0 = self._state0._replace(
            user_schema=None,
            tx=self._state0.tx.clear_user_schema(state, tx_memo)
        )
        new._savepoints = _clear_savepoints_user_schema(state, self._savepoints, tx_memo)
        return new

    @property
    def id(self):
        return self._id

    def is_implicit(self):
        return self._implicit

    def make_explicit(self):
        if self._implicit:
            self._implicit = False
        else:
            raise errors.TransactionError('already in explicit transaction')

    def declare_savepoint(self, name: str):
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        return self._declare_savepoint(name)

    def start_migration(self) -> str:
        name = str(uuid.uuid4())
        self._declare_savepoint(name)
        return name

    def _declare_savepoint(self, name: str):
        sp_id = self._constate._new_txid()
        sp_state = self._current._replace(
            id=sp_id, name=name,
            mutation_idx=len(self._constate._mutations)
        )
        self._savepoints[sp_id] = sp_state
        self._constate._savepoints_log[sp_id] = sp_state
        return sp_id

    def rollback_to_savepoint(self, name: str) -> TransactionState:
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        return self._rollback_to_savepoint(name)

    def abort_migration(self, name: str):
        self._rollback_to_savepoint(name)

    def _rollback_to_savepoint(self, name) -> TransactionState:
        sp_ids_to_erase = []
        for sp in reversed(self._savepoints.values()):
            if sp.name == name:
                self._current = sp
                break

            sp_ids_to_erase.append(sp.id)
        else:
            raise errors.TransactionError(f'there is no {name!r} savepoint')

        for sp_id in sp_ids_to_erase:
            self._savepoints.pop(sp_id)

        return sp

    def release_savepoint(self, name: str):
        if self.is_implicit():
            raise errors.TransactionError(
                'savepoints can only be used in transaction blocks')

        self._release_savepoint(name)

    def commit_migration(self, name: str):
        self._release_savepoint(name)

    def _release_savepoint(self, name: str):
        sp_ids_to_erase = []
        for sp in reversed(self._savepoints.values()):
            sp_ids_to_erase.append(sp.id)

            if sp.name == name:
                break
        else:
            raise errors.TransactionError(f'there is no {name!r} savepoint')

        for sp_id in sp_ids_to_erase:
            self._savepoints.pop(sp_id)

    def get_schema(self, std_schema: s_schema.FlatSchema) -> s_schema.Schema:
        assert isinstance(std_schema, s_schema.FlatSchema)
        return s_schema.ChainedSchema(
            std_schema,
            self._current.user_schema,
            self._current.global_schema,
        )

    def get_user_schema(self) -> s_schema.FlatSchema:
        return self._current.user_schema

    def get_user_schema_if_updated(self) -> Optional[s_schema.FlatSchema]:
        if self._current.user_schema is self._state0.user_schema:
            return None
        else:
            return self._current.user_schema

    def get_global_schema(self) -> s_schema.FlatSchema:
        return self._current.global_schema

    def get_global_schema_if_updated(self) -> Optional[s_schema.FlatSchema]:
        if self._current.global_schema is self._state0.global_schema:
            return None
        else:
            return self._current.global_schema

    def get_modaliases(self) -> immutables.Map:
        return self._current.modaliases

    def get_session_config(self) -> immutables.Map:
        return self._current.session_config

    def get_database_config(self) -> immutables.Map:
        return self._current.database_config

    def get_system_config(self) -> immutables.Map:
        return self._current.system_config

    def get_cached_reflection_if_updated(self):
        if self._current.cached_reflection == self._state0.cached_reflection:
            return None
        else:
            return self._current.cached_reflection

    def get_cached_reflection(self) -> immutables.Map[str, Tuple[str, ...]]:
        return self._current.cached_reflection

    def get_migration_state(self) -> Optional[MigrationState]:
        return self._current.migration_state

    def update_schema(self, new_schema: s_schema.Schema):
        assert isinstance(new_schema, s_schema.ChainedSchema)
        self._current = self._current._replace(
            user_schema=new_schema.get_top_schema(),
            global_schema=new_schema.get_global_schema(),
        )

    def update_modaliases(self, new_modaliases: immutables.Map):
        self._current = self._current._replace(modaliases=new_modaliases)

    def update_session_config(self, new_config: immutables.Map):
        self._current = self._current._replace(session_config=new_config)

    def update_database_config(self, new_config: immutables.Map):
        self._current = self._current._replace(database_config=new_config)

    def update_cached_reflection(
        self,
        new: immutables.Map[str, Tuple[str, ...]],
    ) -> None:
        self._current = self._current._replace(cached_reflection=new)

    def update_migration_state(
        self, mstate: Optional[MigrationState]
    ) -> None:
        self._current = self._current._replace(migration_state=mstate)


class CompilerConnectionState:

    __slots__ = ('_savepoints_log', '_current_tx', '_tx_count', '_mutations')

    _savepoints_log: Dict[int, TransactionState]

    def __init__(
        self,
        *,
        user_schema: s_schema.Schema,
        global_schema: s_schema.Schema,
        modaliases: immutables.Map,
        session_config: immutables.Map,
        database_config: immutables.Map,
        system_config: immutables.Map,
        cached_reflection: FrozenSet[str]
    ):
        self._tx_count = time.monotonic_ns()
        self._init_current_tx(
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=modaliases,
            session_config=session_config,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=cached_reflection,
        )
        self._savepoints_log = {}

    def _create_template(self) -> CompilerConnectionState:
        new = CompilerConnectionState.__new__(CompilerConnectionState)
        new._mutations = self._mutations
        new._tx_count = self._tx_count
        return new

    def reset_mutation(self):
        self._mutations = self._mutations[:1]

    def get_mutation(self) -> Optional[s_schema.SchemaMutationLogger]:
        valid_mutations = self._mutations[1:]
        if valid_mutations:
            return s_schema.SchemaMutationLogger.merge(valid_mutations)

    @property
    def base_user_schema_id(self):
        return self._mutations[0].id

    def sync_mutation(self, sp_name):
        if sp_name is None:
            return

        target_sp = None
        for sp in self._savepoints_log.values():
            if sp.name == sp_name:
                target_sp = sp

        if target_sp is None:
            raise ValueError(f'Failed to find any savepoint with name: {sp_name}.')
        self._mutations = self._mutations[: target_sp.mutation_idx]

    def record_mutation(self, mut: s_schema.SchemaMutationLogger):
        if mut.ops:
            self._mutations.append(mut)

    def restore(self, user_schema: s_schema.FlatSchema) -> CompilerConnectionState:
        new = self._create_template()
        tx_memo = {}
        new._current_tx = self._current_tx.restore_user_schema(
            user_schema, new, tx_memo)
        new._savepoints_log = _restore_savepoints_user_schema(
            user_schema, self._savepoints_log, new, tx_memo)
        return new

    def compress(self) -> CompilerConnectionState:
        new = self._create_template()
        tx_memo = {}
        new._current_tx = self._current_tx.clear_user_schema(new, tx_memo)
        new._savepoints_log = _clear_savepoints_user_schema(
            new, self._savepoints_log, tx_memo)
        return new

    def _new_txid(self):
        self._tx_count += 1
        return self._tx_count

    def _init_current_tx(
        self,
        *,
        user_schema,
        global_schema,
        modaliases,
        session_config,
        database_config,
        system_config,
        cached_reflection,
        reset_mutation=True,
    ):
        assert isinstance(user_schema, s_schema.FlatSchema)
        if reset_mutation:
            self._mutations = [user_schema.get_mutation()]
        self._current_tx = Transaction(
            self,
            user_schema=user_schema,
            global_schema=global_schema,
            modaliases=modaliases,
            session_config=session_config,
            database_config=database_config,
            system_config=system_config,
            cached_reflection=cached_reflection,
        )

    def can_sync_to_savepoint(self, spid):
        return spid in self._savepoints_log

    def sync_to_savepoint(self, spid: int) -> None:
        """Synchronize the compiler state with the current DB state."""

        if not self.can_sync_to_savepoint(spid):
            raise RuntimeError(f'failed to lookup savepoint with id={spid}')

        sp = self._savepoints_log[spid]
        self._mutations = self._mutations[:sp.mutation_idx]
        self._current_tx = sp.tx
        self._current_tx._current = sp
        self._current_tx._id = spid

        # Cleanup all savepoints declared after the one we rolled back to
        # in the transaction we have now set as current.
        for id in tuple(self._current_tx._savepoints):
            if id > spid:
                self._current_tx._savepoints.pop(id)

        # Cleanup all savepoints declared after the one we rolled back to
        # in the global savepoints log.
        for id in tuple(self._savepoints_log):
            if id > spid:
                self._savepoints_log.pop(id)

    def current_tx(self) -> Transaction:
        return self._current_tx

    def start_tx(self):
        if self._current_tx.is_implicit():
            self._current_tx.make_explicit()
        else:
            raise errors.TransactionError('already in transaction')

    def rollback_tx(self):
        # Note that we might not be in a transaction as we allow
        # ROLLBACKs outside of transaction blocks (just like Postgres).

        prior_state = self._current_tx._state0

        self._init_current_tx(
            user_schema=prior_state.user_schema,
            global_schema=prior_state.global_schema,
            modaliases=prior_state.modaliases,
            session_config=prior_state.session_config,
            database_config=prior_state.database_config,
            system_config=prior_state.system_config,
            cached_reflection=prior_state.cached_reflection,
        )

        return prior_state

    def commit_tx(self):
        if self._current_tx.is_implicit():
            raise errors.TransactionError('cannot commit: not in transaction')

        latest_state = self._current_tx._current

        self._init_current_tx(
            user_schema=latest_state.user_schema,
            global_schema=latest_state.global_schema,
            modaliases=latest_state.modaliases,
            session_config=latest_state.session_config,
            database_config=latest_state.database_config,
            system_config=latest_state.system_config,
            cached_reflection=latest_state.cached_reflection,
            reset_mutation=False
        )

        return latest_state

    def sync_tx(self, txid: int) -> None:
        if self._current_tx.id == txid:
            return

        if self.can_sync_to_savepoint(txid):
            self.sync_to_savepoint(txid)
            return

        raise errors.InternalServerError(
            f'failed to lookup transaction or savepoint with id={txid}'
        )  # pragma: no cover
