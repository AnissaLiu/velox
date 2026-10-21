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

// Flink logical expressions whose semantics diverge from (or are absent
// from) the velox baselines, e.g. the null-absorbing IS TRUE family
// (IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE map null to a definite
// boolean, unlike a comparison against a literal). Boolean AND/OR/NOT follow
// standard three-valued logic already provided by the baselines and stay
// mapped there.
//
// Implementation pattern: one struct per function —
// VELOX_DEFINE_FUNCTION_TYPES plus call()/callNullable(), registered with
// explicit signatures in registration/LogicalFunctionsRegistration.cpp (see
// velox/functions/prestosql for the convention).

// TODO: implement the Flink logical expressions here.

} // namespace facebook::velox::functions::flinksql
