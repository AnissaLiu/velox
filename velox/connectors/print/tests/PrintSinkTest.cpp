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

#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

#include "velox/common/base/PrefixSortConfig.h"
#include "velox/common/base/SpillConfig.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/print/PrintSink.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::connector::print::test {

namespace {
constexpr std::string_view kRowKindColumnName = "$row_kind";
} // namespace

class PrintSinkTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::initialize({});
    pool_ = memory::memoryManager()->addLeafPool("PrintSinkTest");
  }

  // Builds a ConnectorQueryCtx with given session properties and timezone.
  // PrintSink only reads `sessionProperties()` (parallelism, task_index) and
  // `sessionTimezone()`, so the rest of the context is left at defaults.
  // The config and spill config are owned by the fixture so they outlive the
  // returned context (which holds raw pointers to them).
  std::unique_ptr<ConnectorQueryCtx> makeQueryCtx(
      std::unordered_map<std::string, std::string> sessionProps = {},
      const std::string& timezone = "UTC") {
    auto config = std::make_unique<config::ConfigBase>(
        std::unordered_map<std::string, std::string>(sessionProps), false);
    auto spillCfg = std::make_unique<common::SpillConfig>();
    auto ctx = std::make_unique<ConnectorQueryCtx>(
        pool_.get(),
        pool_.get(),
        config.get(),
        spillCfg.get(),
        common::PrefixSortConfig{},
        /*expressionEvaluator=*/nullptr,
        /*cache=*/nullptr,
        /*queryId=*/"q1",
        /*taskId=*/"t1",
        /*planNodeId=*/"n1",
        /*driverId=*/0,
        timezone);
    configs_.push_back(std::move(config));
    spillCfgs_.push_back(std::move(spillCfg));
    return ctx;
  }

  // Swaps std::cout's streambuf with the supplied buffer for the lifetime of
  // the guard so tests can read PrintSink's stdout output.
  struct ScopedStdoutCapture {
    std::stringstream stream;
    std::streambuf* oldBuf;
    ScopedStdoutCapture() {
      oldBuf = std::cout.rdbuf(stream.rdbuf());
    }
    ~ScopedStdoutCapture() {
      std::cout.rdbuf(oldBuf);
    }
  };

  // Constructs a RowVector with two BIGINT data columns followed by an
  // optional trailing $row_kind TINYINT column carrying the provided kinds.
  RowVectorPtr makeRow(
      const std::vector<int64_t>& a,
      const std::vector<int64_t>& b,
      const std::vector<int8_t>* rowKinds = nullptr) {
    auto n = a.size();
    auto aVec =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), n, pool_.get());
    auto bVec =
        BaseVector::create<FlatVector<int64_t>>(BIGINT(), n, pool_.get());
    for (size_t i = 0; i < n; ++i) {
      aVec->set(i, a[i]);
      bVec->set(i, b[i]);
    }
    std::vector<VectorPtr> children = {aVec, bVec};
    std::vector<std::string> names = {"a", "b"};
    std::vector<TypePtr> types = {BIGINT(), BIGINT()};
    if (rowKinds != nullptr) {
      auto kindVec =
          BaseVector::create<FlatVector<int8_t>>(TINYINT(), n, pool_.get());
      for (size_t i = 0; i < n; ++i) {
        kindVec->set(i, (*rowKinds)[i]);
      }
      children.push_back(kindVec);
      names.emplace_back(std::string(kRowKindColumnName));
      types.push_back(TINYINT());
    }
    return std::make_shared<RowVector>(
        pool_.get(),
        ROW(std::move(names), std::move(types)),
        /*nulls=*/nullptr,
        n,
        std::move(children));
  }

  static std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<std::unique_ptr<config::ConfigBase>> configs_;
  std::vector<std::unique_ptr<common::SpillConfig>> spillCfgs_;
};

std::shared_ptr<memory::MemoryPool> PrintSinkTest::pool_;

TEST_F(PrintSinkTest, PrefixNoIdentifierSingleParallelism) {
  EXPECT_EQ("", PrintSink::computePrefix("", 1, 0));
}

TEST_F(PrintSinkTest, PrefixNoIdentifierMultiParallelism) {
  EXPECT_EQ("1> ", PrintSink::computePrefix("", 2, 0));
  EXPECT_EQ("2> ", PrintSink::computePrefix("", 2, 1));
}

TEST_F(PrintSinkTest, PrefixWithIdentifierSingleParallelism) {
  EXPECT_EQ("foo> ", PrintSink::computePrefix("foo", 1, 0));
}

TEST_F(PrintSinkTest, PrefixWithIdentifierMultiParallelism) {
  EXPECT_EQ("foo:1> ", PrintSink::computePrefix("foo", 2, 0));
  EXPECT_EQ("foo:2> ", PrintSink::computePrefix("foo", 2, 1));
}

// Without a trailing $row_kind column, every row renders as +I[...].
TEST_F(PrintSinkTest, AppendOnlyRowsRenderInsertPrefix) {
  auto inputType = ROW({{"a", BIGINT()}, {"b", BIGINT()}});
  auto queryCtx = makeQueryCtx();
  PrintSink sink(inputType, "id", /*isStdErr=*/false, queryCtx.get());

  ScopedStdoutCapture capture;
  sink.appendData(makeRow({1, 2, 3}, {10, 20, 30}));
  auto out = capture.stream.str();

  EXPECT_EQ(out, "id> +I[1, 10]\nid> +I[2, 20]\nid> +I[3, 30]\n");
}

// Trailing $row_kind column drives the per-row +I/-U/+U/-D prefix.
TEST_F(PrintSinkTest, RowKindColumnDrivesPrefix) {
  auto inputType =
      ROW({{"a", BIGINT()}, {"b", BIGINT()}, {"$row_kind", TINYINT()}});
  auto queryCtx = makeQueryCtx();
  PrintSink sink(inputType, "id", /*isStdErr=*/false, queryCtx.get());

  ScopedStdoutCapture capture;
  std::vector<int8_t> kinds = {0, 1, 2, 3};
  sink.appendData(makeRow(
      /*a=*/{1, 2, 3, 4},
      /*b=*/{10, 20, 30, 40},
      /*rowKinds=*/&kinds));
  auto out = capture.stream.str();

  EXPECT_EQ(
      out, "id> +I[1, 10]\nid> -U[2, 20]\nid> +U[3, 30]\nid> -D[4, 40]\n");
}

// print-identifier prefix is rendered before the row prefix.
TEST_F(PrintSinkTest, PrintIdentifierIsPrepended) {
  auto inputType = ROW({{"a", BIGINT()}});
  auto queryCtx = makeQueryCtx();
  PrintSink sink(inputType, "table_q", /*isStdErr=*/false, queryCtx.get());

  ScopedStdoutCapture capture;
  auto aVec = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_.get());
  aVec->set(0, 7);
  std::vector<VectorPtr> children = {aVec};
  auto row = std::make_shared<RowVector>(
      pool_.get(),
      inputType,
      /*nulls=*/nullptr,
      1,
      std::move(children));
  sink.appendData(row);
  auto out = capture.stream.str();

  EXPECT_EQ(out, "table_q> +I[7]\n");
}

// With parallelism > 1, the task index suffix is rendered in the prefix.
TEST_F(PrintSinkTest, TaskIndexRenderedWhenParallel) {
  auto inputType = ROW({{"a", BIGINT()}, {"b", BIGINT()}});
  std::unordered_map<std::string, std::string> props = {
      {"parallelism", "2"}, {"task_index", "1"}};
  auto queryCtx = makeQueryCtx(props);
  PrintSink sink(inputType, "id", /*isStdErr=*/false, queryCtx.get());

  ScopedStdoutCapture capture;
  sink.appendData(makeRow({1}, {10}));
  auto out = capture.stream.str();

  EXPECT_EQ(out, "id:2> +I[1, 10]\n");
}

// Null data fields render as the null keyword in place.
TEST_F(PrintSinkTest, NullDataFieldRendersNullKeyword) {
  auto inputType = ROW({{"a", BIGINT()}, {"b", BIGINT()}});
  auto queryCtx = makeQueryCtx();
  PrintSink sink(inputType, "id", /*isStdErr=*/false, queryCtx.get());

  ScopedStdoutCapture capture;
  auto n = 2;
  auto aVec = BaseVector::create<FlatVector<int64_t>>(BIGINT(), n, pool_.get());
  auto bVec = BaseVector::create<FlatVector<int64_t>>(BIGINT(), n, pool_.get());
  aVec->set(0, 1);
  aVec->set(1, 2);
  bVec->set(0, 10);
  bVec->setNull(1, true);
  auto row = std::make_shared<RowVector>(
      pool_.get(),
      inputType,
      /*nulls=*/nullptr,
      n,
      std::vector<VectorPtr>{aVec, bVec});
  sink.appendData(row);
  auto out = capture.stream.str();

  EXPECT_EQ(out, "id> +I[1, 10]\nid> +I[2, null]\n");
}

// A nested ROW field renders its own +I prefix regardless of the top-level
// row kind, matching Flink where nested RowData always carries INSERT.
TEST_F(PrintSinkTest, NestedRowFieldRendersOwnInsertPrefix) {
  auto nestedType = ROW({{"x", BIGINT()}, {"y", BIGINT()}});
  auto inputType =
      ROW({{"a", BIGINT()}, {"b", nestedType}, {"$row_kind", TINYINT()}});
  auto queryCtx = makeQueryCtx();
  PrintSink sink(inputType, "id", /*isStdErr=*/false, queryCtx.get());

  ScopedStdoutCapture capture;
  auto xVec = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_.get());
  auto yVec = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_.get());
  xVec->set(0, 1);
  yVec->set(0, 2);
  auto nested = std::make_shared<RowVector>(
      pool_.get(),
      nestedType,
      /*nulls=*/nullptr,
      1,
      std::vector<VectorPtr>{xVec, yVec});
  auto aVec = BaseVector::create<FlatVector<int64_t>>(BIGINT(), 1, pool_.get());
  aVec->set(0, 7);
  auto kindVec =
      BaseVector::create<FlatVector<int8_t>>(TINYINT(), 1, pool_.get());
  kindVec->set(0, 1); // UPDATE_BEFORE
  auto row = std::make_shared<RowVector>(
      pool_.get(),
      inputType,
      /*nulls=*/nullptr,
      1,
      std::vector<VectorPtr>{aVec, nested, kindVec});
  sink.appendData(row);
  auto out = capture.stream.str();

  EXPECT_EQ(out, "id> -U[7, +I[1, 2]]\n");
}

// A trailing $row_kind column whose type is not TINYINT fails loudly at sink
// construction instead of mis-rendering rows later.
TEST_F(PrintSinkTest, NonTinyIntRowKindColumnThrowsAtConstruction) {
  auto inputType = ROW({{"a", BIGINT()}, {"$row_kind", BIGINT()}});
  auto queryCtx = makeQueryCtx();
  VELOX_ASSERT_THROW(
      PrintSink(inputType, "id", /*isStdErr=*/false, queryCtx.get()),
      "trailing $row_kind column must be TINYINT");
}

// Nulls in the $row_kind column are rejected at appendData time; every row
// must carry a kind byte for the prefix to be derivable.
TEST_F(PrintSinkTest, NullRowKindThrowsAtAppendData) {
  auto inputType =
      ROW({{"a", BIGINT()}, {"b", BIGINT()}, {"$row_kind", TINYINT()}});
  auto queryCtx = makeQueryCtx();
  PrintSink sink(inputType, "id", /*isStdErr=*/false, queryCtx.get());

  auto n = 2;
  auto aVec = BaseVector::create<FlatVector<int64_t>>(BIGINT(), n, pool_.get());
  auto bVec = BaseVector::create<FlatVector<int64_t>>(BIGINT(), n, pool_.get());
  aVec->set(0, 1);
  bVec->set(0, 10);
  aVec->set(1, 2);
  bVec->set(1, 20);
  auto kindVec =
      BaseVector::create<FlatVector<int8_t>>(TINYINT(), n, pool_.get());
  kindVec->set(0, 0);
  kindVec->setNull(1, true);
  auto row = std::make_shared<RowVector>(
      pool_.get(),
      inputType,
      /*nulls=*/nullptr,
      n,
      std::vector<VectorPtr>{aVec, bVec, kindVec});

  VELOX_ASSERT_THROW(sink.appendData(row), "must not contain nulls");
}

} // namespace facebook::velox::connector::print::test
