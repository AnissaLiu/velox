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
#include "velox/functions/flinksql/ArithmeticFunctions.h"
#include "velox/functions/flinksql/Register.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/parse/TypeResolver.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

namespace facebook::velox::functions::flinksql::test {

// TODO: test the Flink arithmetic expressions once implemented. Pattern:
// register the family in SetUpTestCase, drive calls through testUnary/
// testBinary helpers built on core::CallTypedExpr, and anchor on the
// Flink-defined rounding/tie semantics (see the family header for the
// semantic anchors).
class FlinkArithmeticTest : public functions::test::FunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    // flinksql::registerArithmeticFunctions("");
    memory::MemoryManager::testingSetInstance({});
  }
};

// Wiring smoke test: the aggregate entry point links and every family
// registration call runs (the skeletons register no functions yet).
TEST_F(FlinkArithmeticTest, aggregateRegistrationRuns) {
  flinksql::registerFunctions("");
  SUCCEED();
}

} // namespace facebook::velox::functions::flinksql::test
