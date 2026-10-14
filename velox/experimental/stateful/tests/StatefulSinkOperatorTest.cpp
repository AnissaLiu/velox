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

#include "velox/experimental/stateful/StatefulSinkOperator.h"

#include <folly/ScopeGuard.h>
#include <folly/dynamic.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/Connector.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Driver.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/Task.h"
#include "velox/exec/Values.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StatefulPlanner.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::stateful::test {
namespace {

// An exec::Operator that captures the last RowVector received via addInput, so
// tests can assert exactly what StatefulSinkOperator fed to the underlying
// operator (exec::TableWriter in production).
class SpyOperator : public exec::Operator {
 public:
  explicit SpyOperator(exec::DriverCtx* driverCtx)
      : Operator(driverCtx, ROW({"c"}, {BIGINT()}), 0, "spy_sink", "SpySink") {}

  bool needsInput() const override {
    return true;
  }

  void addInput(RowVectorPtr input) override {
    captured_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  exec::BlockingReason isBlocked(ContinueFuture*) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return false;
  }

  RowVectorPtr capturedInput() const {
    return captured_;
  }

 private:
  RowVectorPtr captured_;
};

// A ConnectorInsertTableHandle with configurable supportsRowKind, standing in
// for changelog-aware sinks such as print.
class CaptureInsertTableHandle : public connector::ConnectorInsertTableHandle {
 public:
  explicit CaptureInsertTableHandle(bool supportsRowKind)
      : supportsRowKind_(supportsRowKind) {}

  bool supportsRowKind() const override {
    return supportsRowKind_;
  }

  std::string toString() const override {
    return "CaptureInsertTableHandle";
  }

  folly::dynamic serialize() const override {
    return folly::dynamic::object;
  }

 private:
  const bool supportsRowKind_;
};

// Records every RowVector appended, so tests can assert what the real
// TableWriter delivered to the connector sink.
class CaptureDataSink : public connector::DataSink {
 public:
  explicit CaptureDataSink(std::shared_ptr<std::vector<RowVectorPtr>> inputs)
      : inputs_(std::move(inputs)) {}

  void appendData(RowVectorPtr input) override {
    inputs_->push_back(std::move(input));
  }

  bool finish() override {
    return true;
  }

  std::vector<std::string> close() override {
    return {};
  }

  void abort() override {}

  Stats stats() const override {
    return {};
  }

 private:
  std::shared_ptr<std::vector<RowVectorPtr>> inputs_;
};

class CaptureConnector : public connector::Connector {
 public:
  CaptureConnector(
      const std::string& id,
      std::shared_ptr<std::vector<RowVectorPtr>> inputs)
      : Connector(id), inputs_(std::move(inputs)) {}

  std::unique_ptr<connector::DataSource> createDataSource(
      const RowTypePtr& /*outputType*/,
      const std::shared_ptr<connector::ConnectorTableHandle>& /*tableHandle*/,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& /*columnHandles*/,
      connector::ConnectorQueryCtx* /*connectorQueryCtx*/) override {
    VELOX_NYI();
  }

  std::unique_ptr<connector::DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<connector::ConnectorInsertTableHandle>
      /*connectorInsertTableHandle*/,
      connector::ConnectorQueryCtx* /*connectorQueryCtx*/,
      connector::CommitStrategy /*commitStrategy*/) override {
    return std::make_unique<CaptureDataSink>(inputs_);
  }

 private:
  std::shared_ptr<std::vector<RowVectorPtr>> inputs_;
};

class StatefulSinkOperatorTest : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();

    core::PlanFragment planFragment;
    planFragment.planNode = std::make_shared<core::ValuesNode>(
        core::PlanNodeId{"values"}, std::vector<RowVectorPtr>{plainBatch()});
    executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    task_ = exec::Task::create(
        "StatefulSinkOperatorTest_task",
        std::move(planFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        exec::Task::ExecutionMode::kParallel);
    driver_ = exec::Driver::testingCreate();
    driverCtx_ = std::make_unique<exec::DriverCtx>(task_, 0, 0, 0, 0);
    driverCtx_->driver = driver_.get();
  }

  void TearDown() override {
    driverCtx_.reset();
    driver_.reset();
    task_.reset();
    executor_.reset();
    OperatorTestBase::TearDown();
  }

  RowVectorPtr plainBatch() {
    return makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  }

  // User column "c" plus a trailing $row_kind TINYINT column carrying all four
  // RowKind ordinals, as it would arrive merged across the JNI boundary.
  RowVectorPtr mergedBatch() {
    auto value = makeFlatVector<int64_t>({10, 20, 30, 40});
    auto rowKind = makeFlatVector<int8_t>({0, 1, 2, 3});
    return makeRowVector({"c", "$row_kind"}, {value, rowKind});
  }

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_;
  std::shared_ptr<exec::Task> task_;
  std::shared_ptr<exec::Driver> driver_;
  std::unique_ptr<exec::DriverCtx> driverCtx_;
};

// supportsRowKind=true: a changelog StreamRecord is re-merged into a RowVector
// carrying the trailing $row_kind column before being fed to the underlying
// operator.
TEST_F(
    StatefulSinkOperatorTest,
    addInputFeedsMergedRowVectorWhenSupportsRowKind) {
  auto spy = std::make_unique<SpyOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  StatefulSinkOperator sinkOp(std::move(spy), {}, /*supportsRowKind=*/true);

  sinkOp.addInput(StreamRecord::create("sink", mergedBatch()));

  auto captured = spyPtr->capturedInput();
  ASSERT_NE(captured, nullptr);
  // User column "c" plus trailing $row_kind.
  ASSERT_EQ(captured->type()->size(), 2);
  EXPECT_EQ(
      asRowType(captured->type())->nameOf(captured->type()->size() - 1),
      "$row_kind");
  auto rowKindCol = std::dynamic_pointer_cast<const SimpleVector<int8_t>>(
      captured->childAt(captured->type()->size() - 1));
  ASSERT_NE(rowKindCol, nullptr);
  EXPECT_EQ(rowKindCol->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(
      rowKindCol->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(rowKindCol->valueAt(2), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  EXPECT_EQ(rowKindCol->valueAt(3), static_cast<int8_t>(RowKind::DELETE));
  auto values = captured->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 10);
  EXPECT_EQ(values->valueAt(1), 20);
  EXPECT_EQ(values->valueAt(2), 30);
  EXPECT_EQ(values->valueAt(3), 40);
}

// supportsRowKind=false: the record value is fed as-is; the trailing $row_kind
// column is NOT appended (the non-changelog sink path).
TEST_F(
    StatefulSinkOperatorTest,
    addInputFeedsPlainRecordWhenNotSupportsRowKind) {
  auto spy = std::make_unique<SpyOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  StatefulSinkOperator sinkOp(std::move(spy), {}, /*supportsRowKind=*/false);

  // Even though the input record carries rowKind, it is dropped on this path.
  sinkOp.addInput(StreamRecord::create("sink", mergedBatch()));

  auto captured = spyPtr->capturedInput();
  ASSERT_NE(captured, nullptr);
  // Only the user column; no trailing $row_kind.
  EXPECT_EQ(captured->type()->size(), 1);
  EXPECT_EQ(asRowType(captured->type())->nameOf(0), "c");
  auto values = captured->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 10);
  EXPECT_EQ(values->valueAt(1), 20);
  EXPECT_EQ(values->valueAt(2), 30);
  EXPECT_EQ(values->valueAt(3), 40);
}

// supportsRowKind=true on an appendOnly record (no rowKind): toMergedRowVector
// appends a constant INSERT trailing column so the schema stays N+1.
TEST_F(StatefulSinkOperatorTest, addInputAppendsConstantInsertForAppendOnly) {
  auto spy = std::make_unique<SpyOperator>(driverCtx_.get());
  auto* spyPtr = spy.get();
  StatefulSinkOperator sinkOp(std::move(spy), {}, /*supportsRowKind=*/true);

  sinkOp.addInput(std::make_shared<StreamRecord>("sink", plainBatch()));

  auto captured = spyPtr->capturedInput();
  ASSERT_NE(captured, nullptr);
  ASSERT_EQ(captured->type()->size(), 2);
  EXPECT_EQ(
      asRowType(captured->type())->nameOf(captured->type()->size() - 1),
      "$row_kind");
  auto rowKindCol = std::dynamic_pointer_cast<const SimpleVector<int8_t>>(
      captured->childAt(captured->type()->size() - 1));
  ASSERT_NE(rowKindCol, nullptr);
  EXPECT_EQ(rowKindCol->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(rowKindCol->valueAt(1), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(rowKindCol->valueAt(2), static_cast<int8_t>(RowKind::INSERT));
  auto values = captured->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 1);
  EXPECT_EQ(values->valueAt(1), 2);
  EXPECT_EQ(values->valueAt(2), 3);
}

// Real-path: StatefulPlanner::plan over a real TableWriteNode whose
// ConnectorInsertTableHandle declares supportsRowKind. The real
// exec::TableWriter resolves every column name against
// sources()[0]->outputType() in its ctor, so planning alone fails if
// augmentTableWriteForRowKind ever stops keeping the augmented columns and
// the source schema consistent. addInput then threads the merged RowVector
// through TableWriter's name-based mapping into the connector DataSink.
TEST_F(StatefulSinkOperatorTest, realPlannerPathThreadsRowKindIntoDataSink) {
  const std::string connectorId = "stateful-sink-capture";
  auto sinkInputs = std::make_shared<std::vector<RowVectorPtr>>();
  connector::registerConnector(
      std::make_shared<CaptureConnector>(connectorId, sinkInputs));
  auto unregister = folly::makeGuard(
      [&connectorId] { connector::unregisterConnector(connectorId); });

  auto insertHandle = std::make_shared<core::InsertTableHandle>(
      connectorId, std::make_shared<CaptureInsertTableHandle>(true));
  auto source = std::make_shared<core::ValuesNode>(
      core::PlanNodeId{"vals"},
      std::vector<RowVectorPtr>{
          makeRowVector({"c"}, {makeFlatVector<int64_t>({1})})});
  auto tableWrite = std::make_shared<core::TableWriteNode>(
      core::PlanNodeId{"write"},
      ROW({"c"}, {BIGINT()}),
      std::vector<std::string>{"c"},
      nullptr /* aggregationNode */,
      insertHandle,
      false /* hasPartitioningScheme */,
      exec::TableWriteTraits::outputType(nullptr),
      connector::CommitStrategy::kNoCommit,
      source);

  core::PlanFragment planFragment;
  planFragment.planNode = std::make_shared<StatefulPlanNode>(
      tableWrite, std::vector<core::PlanNodePtr>{});
  auto chain = StatefulPlanner::plan(planFragment, driverCtx_.get(), nullptr);

  chain->initialize();
  chain->addInput(StreamRecord::create("sink", mergedBatch()));
  chain->finish();

  ASSERT_EQ(sinkInputs->size(), 1);
  const auto& received = sinkInputs->at(0);
  // User column plus trailing $row_kind, in TableWriter's mapped order.
  ASSERT_EQ(received->type()->size(), 2);
  EXPECT_EQ(asRowType(received->type())->nameOf(0), "c");
  EXPECT_EQ(asRowType(received->type())->nameOf(1), "$row_kind");
  auto values = received->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 10);
  EXPECT_EQ(values->valueAt(1), 20);
  EXPECT_EQ(values->valueAt(2), 30);
  EXPECT_EQ(values->valueAt(3), 40);
  auto kinds = received->childAt(1)->asFlatVector<int8_t>();
  ASSERT_NE(kinds, nullptr);
  EXPECT_EQ(kinds->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(kinds->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(kinds->valueAt(2), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  EXPECT_EQ(kinds->valueAt(3), static_cast<int8_t>(RowKind::DELETE));
}

// columnNames() (the names written through to the connector) may differ from
// columns()->names() (the upstream source names). Augmentation must preserve
// the write names and only append $row_kind, so the sink receives user_c +
// $row_kind rather than the source column name _c0.
TEST_F(
    StatefulSinkOperatorTest,
    plannerPreservesWriteColumnNamesWhenAugmenting) {
  const std::string connectorId = "stateful-sink-capture-names";
  auto sinkInputs = std::make_shared<std::vector<RowVectorPtr>>();
  connector::registerConnector(
      std::make_shared<CaptureConnector>(connectorId, sinkInputs));
  auto unregister = folly::makeGuard(
      [&connectorId] { connector::unregisterConnector(connectorId); });

  auto insertHandle = std::make_shared<core::InsertTableHandle>(
      connectorId, std::make_shared<CaptureInsertTableHandle>(true));
  auto source = std::make_shared<core::ValuesNode>(
      core::PlanNodeId{"vals"},
      std::vector<RowVectorPtr>{
          makeRowVector({"_c0"}, {makeFlatVector<int64_t>({10})})});
  // Source column _c0 is written to the sink under the name user_c.
  auto tableWrite = std::make_shared<core::TableWriteNode>(
      core::PlanNodeId{"write"},
      ROW({"_c0"}, {BIGINT()}),
      std::vector<std::string>{"user_c"},
      nullptr /* aggregationNode */,
      insertHandle,
      false /* hasPartitioningScheme */,
      exec::TableWriteTraits::outputType(nullptr),
      connector::CommitStrategy::kNoCommit,
      source);

  core::PlanFragment planFragment;
  planFragment.planNode = std::make_shared<StatefulPlanNode>(
      tableWrite, std::vector<core::PlanNodePtr>{});
  auto chain = StatefulPlanner::plan(planFragment, driverCtx_.get(), nullptr);

  chain->initialize();
  chain->addInput(StreamRecord::create(
      "sink",
      makeRowVector(
          {"_c0", "$row_kind"},
          {makeFlatVector<int64_t>({10, 20, 30, 40}),
           makeFlatVector<int8_t>({0, 1, 2, 3})})));
  chain->finish();

  ASSERT_EQ(sinkInputs->size(), 1);
  const auto& received = sinkInputs->at(0);
  ASSERT_EQ(received->type()->size(), 2);
  EXPECT_EQ(asRowType(received->type())->nameOf(0), "user_c");
  EXPECT_EQ(asRowType(received->type())->nameOf(1), "$row_kind");
  auto values = received->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 10);
  EXPECT_EQ(values->valueAt(1), 20);
  EXPECT_EQ(values->valueAt(2), 30);
  EXPECT_EQ(values->valueAt(3), 40);
  auto kinds = received->childAt(1)->asFlatVector<int8_t>();
  ASSERT_NE(kinds, nullptr);
  EXPECT_EQ(kinds->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(kinds->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(kinds->valueAt(2), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  EXPECT_EQ(kinds->valueAt(3), static_cast<int8_t>(RowKind::DELETE));
}

// supportsRowKind=false planner path: the TableWriteNode is left unaugmented
// and the sink receives the original columns only. The planner reads the flag
// in two places (transformOperator for schema augmentation and
// transformGenericOperator for the merge); this pins both to the false branch.
TEST_F(StatefulSinkOperatorTest, plannerSkipsRowKindWhenNotSupported) {
  const std::string connectorId = "stateful-sink-capture-plain";
  auto sinkInputs = std::make_shared<std::vector<RowVectorPtr>>();
  connector::registerConnector(
      std::make_shared<CaptureConnector>(connectorId, sinkInputs));
  auto unregister = folly::makeGuard(
      [&connectorId] { connector::unregisterConnector(connectorId); });

  auto insertHandle = std::make_shared<core::InsertTableHandle>(
      connectorId, std::make_shared<CaptureInsertTableHandle>(false));
  auto source = std::make_shared<core::ValuesNode>(
      core::PlanNodeId{"vals"},
      std::vector<RowVectorPtr>{
          makeRowVector({"c"}, {makeFlatVector<int64_t>({1})})});
  auto tableWrite = std::make_shared<core::TableWriteNode>(
      core::PlanNodeId{"write"},
      ROW({"c"}, {BIGINT()}),
      std::vector<std::string>{"c"},
      nullptr /* aggregationNode */,
      insertHandle,
      false /* hasPartitioningScheme */,
      exec::TableWriteTraits::outputType(nullptr),
      connector::CommitStrategy::kNoCommit,
      source);

  core::PlanFragment planFragment;
  planFragment.planNode = std::make_shared<StatefulPlanNode>(
      tableWrite, std::vector<core::PlanNodePtr>{});
  auto chain = StatefulPlanner::plan(planFragment, driverCtx_.get(), nullptr);

  chain->initialize();
  // Even though the record carries per-row kinds, they are dropped on this
  // path and the schema stays N columns.
  chain->addInput(StreamRecord::create("sink", mergedBatch()));
  chain->finish();

  ASSERT_EQ(sinkInputs->size(), 1);
  const auto& received = sinkInputs->at(0);
  ASSERT_EQ(received->type()->size(), 1);
  EXPECT_EQ(asRowType(received->type())->nameOf(0), "c");
  auto values = received->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 10);
  EXPECT_EQ(values->valueAt(1), 20);
  EXPECT_EQ(values->valueAt(2), 30);
  EXPECT_EQ(values->valueAt(3), 40);
}

// Aggregation path: a TableWriteNode carrying a real AggregationNode (global
// sum) with a row-kind-aware sink. The merged N+1 column input reaches the
// connector DataSink while the embedded HashAggregation keeps reading its
// channels from the original N-column plan: appending the trailing $row_kind
// column must not shift channel-based addressing.
TEST_F(StatefulSinkOperatorTest, plannerAggregationPathWithRowKind) {
  const std::string connectorId = "stateful-sink-capture-agg";
  auto sinkInputs = std::make_shared<std::vector<RowVectorPtr>>();
  connector::registerConnector(
      std::make_shared<CaptureConnector>(connectorId, sinkInputs));
  auto unregister = folly::makeGuard(
      [&connectorId] { connector::unregisterConnector(connectorId); });

  auto insertHandle = std::make_shared<core::InsertTableHandle>(
      connectorId, std::make_shared<CaptureInsertTableHandle>(true));
  auto source = std::make_shared<core::ValuesNode>(
      core::PlanNodeId{"vals"},
      std::vector<RowVectorPtr>{
          makeRowVector({"c"}, {makeFlatVector<int64_t>({1})})});

  // Global sum over "c"; the aggregation's declared input stays the original
  // N-column source (the planner passes aggregationNode through unchanged).
  core::AggregationNode::Aggregate sumAgg;
  sumAgg.call = std::make_shared<core::CallTypedExpr>(
      BIGINT(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c")},
      "sum");
  sumAgg.rawInputTypes = {BIGINT()};
  auto aggregation = std::make_shared<core::AggregationNode>(
      core::PlanNodeId{"agg"},
      core::AggregationNode::Step::kSingle,
      std::vector<core::FieldAccessTypedExprPtr>{} /* groupingKeys */,
      std::vector<core::FieldAccessTypedExprPtr>{} /* preGroupedKeys */,
      std::vector<std::string>{"c_sum"},
      std::vector<core::AggregationNode::Aggregate>{sumAgg},
      false /* ignoreNullKeys */,
      source);

  auto tableWrite = std::make_shared<core::TableWriteNode>(
      core::PlanNodeId{"write"},
      ROW({"c"}, {BIGINT()}),
      std::vector<std::string>{"c"},
      aggregation,
      insertHandle,
      false /* hasPartitioningScheme */,
      exec::TableWriteTraits::outputType(aggregation),
      connector::CommitStrategy::kNoCommit,
      source);

  core::PlanFragment planFragment;
  planFragment.planNode = std::make_shared<StatefulPlanNode>(
      tableWrite, std::vector<core::PlanNodePtr>{});
  auto chain = StatefulPlanner::plan(planFragment, driverCtx_.get(), nullptr);

  chain->initialize();
  chain->addInput(StreamRecord::create("sink", mergedBatch()));

  // finish() discards sink-operator output, so drain the wrapped TableWriter
  // directly to read the aggregation stats row.
  auto& writer = chain->op();
  writer->noMoreInput();
  auto stats = writer->getOutput();
  ASSERT_NE(stats, nullptr);
  ASSERT_EQ(stats->size(), 1);
  ASSERT_EQ(stats->type()->size(), 4);
  EXPECT_EQ(asRowType(stats->type())->nameOf(3), "c_sum");
  auto sums = stats->childAt(3)->asFlatVector<int64_t>();
  ASSERT_NE(sums, nullptr);
  EXPECT_EQ(sums->valueAt(0), 100); // 10 + 20 + 30 + 40

  // The connector side still receives user column plus trailing $row_kind.
  ASSERT_EQ(sinkInputs->size(), 1);
  const auto& received = sinkInputs->at(0);
  ASSERT_EQ(received->type()->size(), 2);
  EXPECT_EQ(asRowType(received->type())->nameOf(0), "c");
  EXPECT_EQ(asRowType(received->type())->nameOf(1), "$row_kind");
  auto values = received->childAt(0)->asFlatVector<int64_t>();
  ASSERT_NE(values, nullptr);
  EXPECT_EQ(values->valueAt(0), 10);
  EXPECT_EQ(values->valueAt(1), 20);
  EXPECT_EQ(values->valueAt(2), 30);
  EXPECT_EQ(values->valueAt(3), 40);
  auto kinds = received->childAt(1)->asFlatVector<int8_t>();
  ASSERT_NE(kinds, nullptr);
  EXPECT_EQ(kinds->valueAt(0), static_cast<int8_t>(RowKind::INSERT));
  EXPECT_EQ(kinds->valueAt(1), static_cast<int8_t>(RowKind::UPDATE_BEFORE));
  EXPECT_EQ(kinds->valueAt(2), static_cast<int8_t>(RowKind::UPDATE_AFTER));
  EXPECT_EQ(kinds->valueAt(3), static_cast<int8_t>(RowKind::DELETE));
}

} // namespace
} // namespace facebook::velox::stateful::test

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init(&argc, &argv, false);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
