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

#pragma once

#include "common/status.h"
#include "util/runtime_profile.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;

// SpillWriter 抽象基类
class SpillWriterBase {
public:
    virtual ~SpillWriterBase() = default;

    virtual Status open() = 0;
    virtual Status close() = 0;
    virtual Status write(RuntimeState* state, const Block& block, size_t& written_bytes) = 0;

    virtual int64_t get_id() const = 0;
    virtual int64_t get_written_bytes() const = 0;
    virtual const std::string& get_file_path() const = 0;

    virtual void set_counters(RuntimeProfile* operator_profile) = 0;
};

} // namespace vectorized
} // namespace doris

