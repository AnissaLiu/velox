/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "folly/CPortability.h"

#include "velox/functions/Macros.h"

namespace facebook::velox::functions::flinksql {

// Flink string expressions whose contract differs from the velox baselines —
// e.g. by return type (Flink CHAR_LENGTH counts code points but types the
// result INTEGER, while presto's length returns BIGINT) or by behavior on
// inputs the baseline treats differently. Functions that are semantically
// identical to a baseline registration stay mapped there instead of being
// duplicated here.
//
// Implementation pattern: one struct per function with
// VELOX_DEFINE_FUNCTION_TYPES, registered with explicit signatures in
// registration/StringFunctionsRegistration.cpp (see velox/functions/prestosql
// for the convention). Reusable pieces (UTF-8 code point counting, hex
// encoding) live in velox/functions/lib.

// TODO: implement the Flink string expressions here.

} // namespace facebook::velox::functions::flinksql
