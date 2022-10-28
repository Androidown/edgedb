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



INSERT Tree {name := '0', val := 'a'};
INSERT Tree {
    name := '00',
    parent := (SELECT DETACHED Tree FILTER .name = '0'),
    val := 'aa'
};
INSERT Tree {
    name := '01',
    parent := (SELECT DETACHED Tree FILTER .name = '0'),
    val := 'ab'
};
INSERT Tree {
    name := '02',
    parent := (SELECT DETACHED Tree FILTER .name = '0'),
    val := 'ac'
};
INSERT Tree {
    name := '000',
    parent := (SELECT DETACHED Tree FILTER .name = '00'),
    val := 'aaa'
};
INSERT Tree {
    name := '010',
    parent := (SELECT DETACHED Tree FILTER .name = '01'),
    val := 'aba'
};

INSERT Tree {name := '1', val := 'b'};
INSERT Tree {
    name := '10',
    parent := (SELECT DETACHED Tree FILTER .name = '1'),
    val := 'ba'
};
INSERT Tree {
    name := '11',
    parent := (SELECT DETACHED Tree FILTER .name = '1'),
    val := 'bb'
};
INSERT Tree {
    name := '12',
    parent := (SELECT DETACHED Tree FILTER .name = '1'),
    val := 'bc'
};
INSERT Tree {
    name := '13',
    parent := (SELECT DETACHED Tree FILTER .name = '1'),
    val := 'bd'
};

INSERT Graph {name := '0', val := 'Duty'};
INSERT Graph {name := '1', val := 'Project'};

INSERT Graph {
    name := '01',
    parent := (SELECT DETACHED Graph FILTER .name = '0'),
    val := 'Dev'
};
INSERT Graph {
    name := '02',
    parent := (SELECT DETACHED Graph FILTER .name = '0'),
    val := 'Test'
};
INSERT Graph {
    name := '03',
    parent := (SELECT DETACHED Graph FILTER .name = '0'),
    val := 'MainTain'
};

INSERT Graph {
    name := '11',
    parent := (SELECT DETACHED Graph FILTER .name = '1'),
    val := 'Project1'
};
INSERT Graph {
    name := '12',
    parent := (SELECT DETACHED Graph FILTER .name = '1'),
    val := 'Project2'
};
INSERT Graph {
    name := '13',
    parent := (SELECT DETACHED Graph FILTER .name = '1'),
    val := 'Project3'
};

INSERT Graph {
    name := '0001',
    parent := (SELECT DETACHED Graph FILTER .name IN {'01', '11'}),
    val := 'Alice'
};
INSERT Graph {
    name := '0002',
    parent := (SELECT DETACHED Graph FILTER .name IN {'01', '12'}),
    val := 'Bob'
};
INSERT Graph {
    name := '0003',
    parent := (SELECT DETACHED Graph FILTER .name IN {'01', '13'}),
    val := 'Cindy'
};

INSERT Graph {
    name := '0004',
    parent := (SELECT DETACHED Graph
                FILTER .name IN {'02', '11', '12', '13'}),
    val := 'Dannie'
};
INSERT Graph {
    name := '0005',
    parent := (SELECT DETACHED Graph
                FILTER .name IN {'03', '11'}),
    val := 'Enne'
};
INSERT Graph {
    name := '0006',
    parent := (SELECT DETACHED Graph
                FILTER .name IN {'03', '12', '13'}),
    val := 'Frank'
};
