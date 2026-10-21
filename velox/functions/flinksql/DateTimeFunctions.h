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

// Flink date/time expressions: session-timezone-sensitive functions such as
// CURRENT_DATE / CURRENT_TIME, evaluated per record in streaming mode (the
// baseline current_date conventions may differ). Read the zone once per
// function instance through initialize() from
// core::QueryConfig::sessionTimezone(), which gluten-flink populates from
// Flink's table.local-time-zone.
//
// Implementation pattern: one struct per function with
// VELOX_DEFINE_FUNCTION_TYPES plus call()/callNullable(), registered with
// explicit signatures in registration/DateTimeFunctionsRegistration.cpp.

// TODO: implement the Flink date/time expressions here.

} // namespace facebook::velox::functions::flinksql
