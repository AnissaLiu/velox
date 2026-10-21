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

// Flink conditional expressions whose null semantics diverge from the
// baselines — e.g. IFNULL(a, b), a two-argument coalesce where the default
// null behavior would wrongly null out the result whenever the first
// argument is null (custom callNullable handling is required).
//
// Implementation pattern: one struct per function with
// VELOX_DEFINE_FUNCTION_TYPES plus call()/callNullable(), registered with
// explicit signatures in registration/ConditionalFunctionsRegistration.cpp
// (see velox/functions/prestosql for the convention).

// TODO: implement the Flink conditional expressions here.

} // namespace facebook::velox::functions::flinksql
