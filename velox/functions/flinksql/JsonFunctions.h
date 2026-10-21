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

// Flink JSON expressions: path-based extraction and construction —
// JSON_VALUE (scalar out), JSON_QUERY (object/array out, with the array
// wrapper option), JSON_EXISTS (boolean probe) and the JSON_OBJECT /
// JSON_ARRAY constructors, each with Flink's option clauses
// ({NULL|ERROR|DEFAULT} ON EMPTY / ON ERROR, {ABSENT|NULL} ON NULL).
//
// These live here rather than mapping to a baseline because the contracts
// differ: presto's JSON functions operate on the JSON type (varbinary
// payload) via json_extract-style signatures, while Flink takes and returns
// VARCHAR and folds the option clauses into the call; spark's
// get_json_object differs in path language and return conventions.
//
// Implementation pattern: one struct per function with
// VELOX_DEFINE_FUNCTION_TYPES, registered with explicit signatures in
// registration/JsonFunctionsRegistration.cpp (see velox/functions/prestosql
// for the convention). Parsing can lean on the
// simdjson dependency (see velox/functions/prestosql/JsonFunctions.cpp for
// the established usage) plus a small SQL/JSON path subset evaluator.

// TODO: implement the Flink JSON expressions here.

} // namespace facebook::velox::functions::flinksql
