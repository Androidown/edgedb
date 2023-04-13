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


cdef class StatementsCache:

    cdef:
        public object _dict
        int _maxsize
        object _dict_move_to_end
        object _dict_get
        object _remove_on_ddl

    cpdef get(self, key, default)
    cpdef add_to_remove_on_ddl(self, key)
    cpdef should_remove_on_ddl(self, key)
    cpdef needs_cleanup(self)
    cpdef cleanup_one(self)
