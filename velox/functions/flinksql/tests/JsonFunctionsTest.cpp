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
#include "velox/functions/flinksql/JsonFunctions.h"
#include "velox/functions/flinksql/Register.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::functions::flinksql::test {

// TODO: test the Flink JSON expressions once implemented. Pattern: register the family in
// SetUpTestCase, drive calls through testUnary/testBinary helpers built on
// core::CallTypedExpr, and anchor on: scalar/object/array extraction over
// nested documents; a missing path (NULL result and each ON EMPTY option);
// malformed JSON (each ON ERROR option); NULL input; the JSON_QUERY array
// wrapper options; and the constructors' ABSENT/NULL ON NULL handling.
class FlinkJsonTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    // flinksql::registerJsonFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }
};

} // namespace facebook::velox::functions::flinksql::test
