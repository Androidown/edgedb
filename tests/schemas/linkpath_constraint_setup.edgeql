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

Insert Pokemon { species := 'snake' };
Insert Pokemon { species := 'frog' };
Insert Pokemon { species := 'dog', nickname := 'Zacian' };
Insert Pokemon { species := 'rat', nickname := 'Pikachu' };
Insert Pokemon { species := 'penguin', nickname := 'Pochyaman' };

Insert Animal { species := 'rat' };
Insert Animal { species := 'penguin' };
Insert Animal { species := 'snake' };
Insert Animal { species := 'frog' };
Insert Animal { species := 'dog' };
Insert Animal { species := 'chicken' };
Insert Animal { species := 'duck' };

Insert FlyingType { species := 'chicken', nickname := 'BoBo', number := 10 };
Insert FlyingType { species := 'duck', nickname := 'Kodaku', number := 11 };

Insert Trainer {
    name := 'Satosi',
    fav_pet := (SELECT Pokemon filter .species = 'rat'),
    pets := (SELECT Pokemon filter .species in {'rat', 'dog', 'duck'}),
};

Insert Trainer {
    name := 'Hikari',
    fav_pet := (SELECT Pokemon filter .species = 'penguin'),
    pets := (SELECT Pokemon filter .species in {'penguin', 'frog', 'chicken', 'snake'}),
};

