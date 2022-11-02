#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


# A dim node setup with only one parent link and computable children.
type Tree {
    required property name -> str {
        constraint exclusive;
    }
    property val -> str;

    link parent -> Tree {
        on id to name;
    };
    multi link children := .<parent[IS Tree];
}

# A dim node setup with several parent links and computable children.
type Graph {
    required property name -> str {
        constraint exclusive;
    }
    property val -> str;

    multi link parent -> Graph {
        on id to name;
    };
    multi link children := .<parent[IS Graph];
}
