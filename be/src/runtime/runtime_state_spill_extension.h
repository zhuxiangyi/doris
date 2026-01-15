// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file contains extensions to RuntimeState for object storage spill support
// Add these methods to runtime_state.h

#pragma once

namespace doris {

// Add these methods to RuntimeState class:

/*
    bool enable_spill_to_object_storage() const {
        if (_query_options.__isset.enable_spill_to_object_storage) {
            return _query_options.enable_spill_to_object_storage;
        }
        return config::enable_spill_to_object_storage;
    }

    std::string spill_object_storage_vault_id() const {
        if (_query_options.__isset.spill_object_storage_vault_id &&
            !_query_options.spill_object_storage_vault_id.empty()) {
            return _query_options.spill_object_storage_vault_id;
        }
        return config::spill_object_storage_vault_id;
    }

    bool spill_prefer_object_storage() const {
        if (_query_options.__isset.spill_prefer_object_storage) {
            return _query_options.spill_prefer_object_storage;
        }
        return config::spill_prefer_object_storage;
    }
*/

} // namespace doris

