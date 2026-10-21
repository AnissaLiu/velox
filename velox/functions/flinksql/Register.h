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

#include <string>

namespace facebook::velox::functions::flinksql {

// Register Flink SQL functions.
//
// Idempotent within a process when called with overwrite=true at the
// exec::registerStatefulVectorFunction layer; existing registrations with the
// same name are replaced.
//
// The per-family registration entry points below follow the prestosql
// registration/ layout: each family owns one registration file under
// registration/ and one implementation header beside this file; the families
// are aggregated here.
void registerFunctions(const std::string& prefix = "");

void registerArithmeticFunctions(const std::string& prefix = "");
void registerConditionalFunctions(const std::string& prefix = "");
void registerDateTimeFunctions(const std::string& prefix = "");
void registerHashFunctions(const std::string& prefix = "");
void registerJsonFunctions(const std::string& prefix = "");
void registerLogicalFunctions(const std::string& prefix = "");
void registerStringFunctions(const std::string& prefix = "");

} // namespace facebook::velox::functions::flinksql
