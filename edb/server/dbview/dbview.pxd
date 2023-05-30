#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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


cimport cython

from libc.stdint cimport uint64_t

cdef DEFAULT_STATE

cpdef enum SideEffects:

    SchemaChanges = 1 << 0
    DatabaseConfigChanges = 1 << 1
    InstanceConfigChanges = 1 << 2
    RoleChanges = 1 << 3
    GlobalSchemaChanges = 1 << 4


@cython.final
cdef class QueryRequestInfo:
    cdef public object source  # edgeql.Source
    cdef public tuple protocol_version
    cdef public object output_format
    cdef public object input_format
    cdef public bint expect_one
    cdef public int implicit_limit
    cdef public int force_limit
    cdef public bint inline_typeids
    cdef public bint inline_typenames
    cdef public bint inline_objectids
    cdef public uint64_t allow_capabilities
    cdef public object module
    cdef public object namespace
    cdef public bint read_only
    cdef public object external_view
    cdef public bint testmode
    cdef int cached_hash


@cython.final
cdef class CompiledQuery:
    cdef public object query_unit_group
    cdef public object first_extra  # Optional[int]
    cdef public object extra_counts
    cdef public object extra_blobs


cdef class DatabaseIndex:
    cdef:
        dict _dbs
        object _server
        object _sys_config
        object _comp_sys_config
        object _std_schema
        object _global_schema
        object _factory


cdef class NameSpace:
    cdef:
        public object _eql_to_compiled
        public object _eql_to_compiled_disk
        public object _object_id_to_eql
        DatabaseIndex _dbindex
        object _state_serializers
        str _sql_bak_dir
        bint _log_cache

        readonly str name
        public object user_schema
        public object reflection_cache
        public object backend_ids
        public object extensions


cdef class Database:

    cdef:
        DatabaseIndex _index
        object _views
        object _introspection_lock
        object _state_serializers
        str _sql_bak_dir
        bint _log_cache

        readonly str name
        public object ns_map
        readonly object dbver
        readonly object db_config

    cdef schedule_config_update(self)

    cdef _invalidate_caches(self)
    cdef _cache_compiled_query(self, key, query_unit)
    cdef _new_view(self, query_cache, protocol_version)
    cdef _remove_view(self, view)
    cdef _update_backend_ids(self, namespace, new_types)
    cdef _set_and_signal_new_user_schema(
        self,
        namespace,
        new_schema,
        reflection_cache=?,
        backend_ids=?,
        db_config=?,
        affecting_ids=?,
    )
    cdef get_state_serializer(self, namespace, protocol_version)


cdef class DatabaseConnectionView:

    cdef:
        Database _db
        bint _query_cache_enabled
        object _protocol_version

        object _db_config_temp
        object _db_config_dbver

        # State properties
        object _config
        object _in_tx_config

        object _globals
        object _in_tx_globals

        object _modaliases
        object _in_tx_modaliases

        object _state_serializer
        object _in_tx_state_serializer
        object _command_state_serializer

        tuple _session_state_db_cache
        tuple _session_state_cache

        object _txid
        object _in_tx_db_config
        object _in_tx_savepoints
        object _in_tx_user_schema
        object _in_tx_base_user_schema
        object _in_tx_user_schema_mut_pickled
        object _in_tx_global_schema_pickled
        object _in_tx_global_schema
        object _in_tx_new_types
        object _in_tx_stdview_sqls
        object _in_tx_sp_sqls
        int _in_tx_dbver
        bint _in_tx
        bint _in_tx_with_ddl
        bint _in_tx_with_role_ddl
        bint _in_tx_with_sysconfig
        bint _in_tx_with_dbconfig
        bint _in_tx_with_set
        bint _tx_error

        uint64_t _capability_mask

        object _last_comp_state
        object _last_comp_state_id

        object __weakref__

    cdef _reset_tx_state(self)

    cdef clear_tx_error(self)
    cdef rollback_tx_to_savepoint(self, name)
    cdef declare_savepoint(self, namespace, name, spid)
    cdef recover_aliases_and_config(self, modaliases, config, globals)
    cpdef abort_tx(self)

    cpdef in_tx(self)
    cpdef in_tx_error(self)

    cdef cache_compiled_query(self, object key, object query_unit)
    cdef lookup_compiled_query(self, object key)

    cdef tx_error(self)

    cpdef start(self, query_unit)
    cdef _start_tx(self, namespace)
    cdef _apply_in_tx(self, query_unit)
    cdef start_implicit(self, query_unit)
    cpdef on_error(self)
    cdef commit_implicit_tx(
        self, namespace, user_schema, user_schema_unpacked,
        user_schema_mutation, global_schema,
        cached_reflection, affecting_ids,
    )

    cpdef get_session_config(self)
    cdef set_session_config(self, new_conf)

    cpdef get_globals(self)
    cpdef set_globals(self, new_globals)

    cdef get_state_serializer(self, namespace)
    cdef set_state_serializer(self, new_serializer)

    cdef update_database_config(self)
    cdef get_database_config(self)
    cdef set_database_config(self, new_conf)

    cdef get_system_config(self)
    cdef get_compilation_system_config(self)

    cdef set_modaliases(self, new_aliases)
    cpdef get_modaliases(self)

    cdef bytes serialize_state(self)
    cpdef bint is_state_desc_changed(self, namespace)
    cdef describe_state(self, namespace)
    cpdef encode_state(self)
    cpdef decode_state(self, type_id, data, namespace)
    cdef inline recode_global(self, serializer, namespace, k, v)
