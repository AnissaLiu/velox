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
#include "velox/experimental/stateful/WindowAggregator.h"

#include "velox/exec/AggregationMasks.h"
#include "velox/experimental/stateful/RowKind.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/agg/AggregateInfos.h"
#include "velox/exec/Aggregate.h"
#include "velox/experimental/stateful/window/TimeWindowUtil.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::stateful {

WindowAggregator::WindowAggregator(
    std::unique_ptr<exec::Operator> localAggregator,
    std::vector<StatefulOperatorPtr> targets,
    std::shared_ptr<const StreamWindowAggregationNode> node,
    exec::DriverCtx* driverCtx)
    : StatefulOperator(
          std::make_unique<EmptyOperator>(0, driverCtx, node),
          std::move(targets)),
      node_(std::move(node)),
      localAggregator_(std::move(localAggregator)) {}

void WindowAggregator::initialize() {
  if (localAggregator_) {
    localAggregator_->initialize();
  }
  const auto* aggNode =
      std::dynamic_pointer_cast<const core::AggregationNode>(
          node_->aggregation())
          .get();
  VELOX_CHECK_NOT_NULL(
      aggNode, "StreamWindowAggregationNode must carry an AggregationNode");
  auto inputType = std::dynamic_pointer_cast<const RowType>(
      aggNode->sources()[0]->outputType());
  VELOX_CHECK_NOT_NULL(
      inputType, "AggregationNode input type must be a RowType");
  numKeys_ = static_cast<int32_t>(node_->keyedStateSpec()->keyChannels().size());
  numLeadingColumns_ = numKeys_ + 2; // key columns + window_start + window_end
  aggregates_ = makeAggregateInfos(
      aggNode->aggregates(),
      inputType,
      node_->outputType(),
      numLeadingColumns_,
      op()->operatorCtx()->driverCtx()->queryConfig(),
      memoryPool());
}

void WindowAggregator::initializeStateBackend(StateBackend* stateBackend) {
  const auto& spec = *node_->keyedStateSpec();
  keyedBackend_ = createKeyedStateBackend<RowContainerStateKey>(
      *stateBackend, spec, memoryPool());
  keySelector_ = std::make_unique<KeySelector>(
      keyedBackend_->keyIndex(), spec.keyChannels());

  sliceAssigner_ = std::make_unique<SliceAssigner>(
      nullptr, /* keySelector unused for batch-level slice */
      node_->size(),
      node_->step(),
      node_->offset(),
      Window::getType(node_->windowType()),
      node_->rowtimeIndex());

  // Aggregate functions were created in initialize() (op() available from
  // the EmptyOperator set in the constructor).


  std::vector<exec::Accumulator> accumulators;
  accumulators.reserve(aggregates_.size());
  for (const auto& aggregate : aggregates_) {
    accumulators.emplace_back(
        aggregate.function.get(), aggregate.intermediateType);
  }
  AggregatingStateDescriptor descriptor(
      "window-agg-accumulators",
      std::move(accumulators),
      [this](char** rows, size_t numRows) { initializeRows(rows, numRows); },
      [this](char* row, const std::vector<VectorPtr>& columns) {
        mergeRow(row, columns);
      },
      memoryPool());
  state_ =
      keyedBackend_->getOrCreateAggregatingState<SliceEndNs>(descriptor);

  auto* valueRows = state_->valueRows();
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    auto& function = aggregates_[i].function;
    function->setAllocator(&valueRows->stringAllocator());
    const auto rowColumn = valueRows->columnAt(static_cast<int32_t>(i));
    function->setOffsets(
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        rowColumn.initializedByte(),
        rowColumn.initializedMask(),
        valueRows->rowSizeOffset());
  }

  StatefulOperator::initializeStateBackend(stateBackend);
}

void WindowAggregator::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  checkInputRowKinds(*record);
  VELOX_CHECK(!input_, "Last input has not been processed");
  input_ = record->record();
}

void WindowAggregator::advance() {
  if (!input_) {
    if (keyedBackend_) {
      keyedBackend_->advance();
    }
    return;
  }

  const auto numRows = input_->size();
  if (numRows == 0) {
    input_.reset();
    keyedBackend_->advance();
    return;
  }

  // Group rows by sliceEnd using the batch-level SliceAssigner.
  auto sliceEndToData = sliceAssigner_->assignSliceEnd(input_);
  for (const auto& [sliceEnd, sliceData] : sliceEndToData) {
    SliceEndNs ns(sliceEnd);

    // Late check (event-time): skip if all owning windows fired.
    if (node_->isEventTime() &&
        TimeWindowUtil::isWindowFired(sliceEnd, currentProgress_, 0)) {
      continue; // TODO: late-drop counter.
    }

    // Probe keys for this slice's data.
    keySelector_->probe(sliceData);
    const auto distinctKeys = keySelector_->distinctKeys();
    const auto sliceNumDistinct =
        static_cast<vector_size_t>(distinctKeys.size());
    const auto sliceNumRows = sliceData->size();

    // Batch state rows for the distinct keys under this namespace.
    stateRows_.resize(sliceNumDistinct);
    state_->rows(
        folly::Range<const RowContainerStateKey*>(
            distinctKeys.data(),
            static_cast<size_t>(sliceNumDistinct)),
        ns,
        stateRows_.data());

    // Accumulate raw input (same pattern as GroupAggregator::accumulateInput).
    const SelectivityVector allRows(sliceNumRows);
    std::vector<std::optional<column_index_t>> maskChannels;
    maskChannels.reserve(aggregates_.size());
    for (const auto& aggregate : aggregates_) {
      maskChannels.push_back(aggregate.mask);
    }
    exec::AggregationMasks masks(std::move(maskChannels));
    masks.addInput(sliceData, allRows);

    // Map each row to its distinct key index, then to its state row.
    rowToDistinct_.clear();
    for (vector_size_t d = 0; d < sliceNumDistinct; ++d) {
      rowToDistinct_[distinctKeys[d].row()] = d;
    }
    rowGroups_.resize(sliceNumRows);
    const auto perRowKeys = keySelector_->keys();
    for (vector_size_t row = 0; row < sliceNumRows; ++row) {
      rowGroups_[row] = stateRows_[rowToDistinct_[perRowKeys[row].row()]];
    }

    for (size_t i = 0; i < aggregates_.size(); ++i) {
      auto& aggregate = aggregates_[i];
      std::vector<VectorPtr> args;
      args.reserve(aggregate.inputs.size());
      for (size_t j = 0; j < aggregate.inputs.size(); ++j) {
        if (aggregate.constantInputs[j] != nullptr) {
          args.push_back(aggregate.constantInputs[j]);
        } else {
          args.push_back(sliceData->childAt(aggregate.inputs[j]));
        }
      }
      const auto* activeRows = masks.activeRows(i);
      aggregate.function->addRawInput(
          rowGroups_.data(),
          activeRows != nullptr ? *activeRows : allRows,
          args,
          false /* mayPushdown */);
    }

    // Register fire entries for each distinct key under this slice.
    for (vector_size_t d = 0; d < sliceNumDistinct; ++d) {
      registerFire(distinctKeys[d], sliceEnd);
    }
  }

  input_.reset();
  keyedBackend_->advance();
}

void WindowAggregator::processWatermark(int64_t timestamp) {
  if (timestamp > currentProgress_) {
    fireDue(timestamp);
    currentProgress_ = timestamp;
  }
  // Propagate watermark downstream (after fire, matching Flink order).
  StatefulOperator::processWatermark(timestamp);
}

void WindowAggregator::onProcessingTime(int64_t timestamp) {
  if (timestamp > lastTriggeredProcessingTime_) {
    fireDue(timestamp);
    lastTriggeredProcessingTime_ = timestamp;
  }
}

void WindowAggregator::close() {
  // Flush trailing windows (bounded input).
  fireDue(std::numeric_limits<int64_t>::max());
  StatefulOperator::close();
  if (localAggregator_) {
    localAggregator_->close();
  }
  input_.reset();
  state_.reset();
  keyedBackend_.reset();
  keySelector_.reset();
  fireIndex_.clear();
  currentProgress_ = 0;
  lastTriggeredProcessingTime_ = 0;
}

void WindowAggregator::fireDue(int64_t progress) {
  while (!fireIndex_.empty() && fireIndex_.begin()->first <= progress) {
    auto it = fireIndex_.begin();
    auto key = it->second.key;
    auto windowEnd = it->second.ns.sliceEnd();
    fireIndex_.erase(it);
    fireWindow(key, windowEnd);
  }
}

void WindowAggregator::fireWindow(
    const RowContainerStateKey& key,
    int64_t windowEnd) {
  auto ns = SliceEndNs(windowEnd);
  char* row = state_->row(key, ns);
  if (row == nullptr) {
    return; // Window already cleared or never materialized.
  }
  emitWindowResult(key, windowStart(windowEnd), windowEnd);
  // Stage 1 (TUMBLE): clear the entire (key, slice) after fire.
  // TODO stage 2: HOP — merge covering slices, clear first slice, chain.
  // TODO stage 2: CUMULATE — emit cumulative value per step, clear on final step.
  state_->remove(key, ns);
}

void WindowAggregator::emitWindowResult(
    const RowContainerStateKey& key,
    int64_t windowStartMs,
    int64_t windowEndMs) {
  auto ns = SliceEndNs(windowEndMs);
  char* row = state_->row(key, ns);
  VELOX_CHECK_NOT_NULL(row, "fire on a window without state");

  const auto& outputType = node_->outputType();
  const auto numOutputCols = static_cast<int32_t>(outputType->size());
  std::vector<VectorPtr> children(numOutputCols);

  auto* keyRows = keySelector_->keyRowContainer();

  // Key columns extracted from the key row.
  const folly::Range<const vector_size_t*> oneRow(
      &kZeroIndex, 1);
  const char* keyRowPtr = key.row();
  for (int32_t k = 0; k < numKeys_; ++k) {
    children[k] = BaseVector::create(outputType->childAt(k), 1, memoryPool());
    keyRows->extractColumn(
        &keyRowPtr, oneRow, k, 0, children[k]);
  }

  // Window columns as TIMESTAMP.
  auto startVec = BaseVector::create(outputType->childAt(numKeys_), 1, memoryPool());
  if (auto* tsVec = startVec->asFlatVector<Timestamp>()) {
    tsVec->set(0, Timestamp::fromMillis(windowStartMs));
  }
  children[numKeys_] = startVec;

  auto endVec = BaseVector::create(outputType->childAt(numKeys_ + 1), 1, memoryPool());
  if (auto* tsVec = endVec->asFlatVector<Timestamp>()) {
    tsVec->set(0, Timestamp::fromMillis(windowEndMs));
  }
  children[numKeys_ + 1] = endVec;

  // Extract final aggregate values from the accumulator row.
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    auto result = BaseVector::create(
        outputType->childAt(numLeadingColumns_ + i), 1, memoryPool());
    aggregates_[i].function->extractValues(&row, 1, &result);
    children[numLeadingColumns_ + i] = result;
  }

  auto output = std::make_shared<RowVector>(
      memoryPool(), outputType, nullptr, 1, std::move(children));
  pushOutput(std::make_shared<StreamRecord>(
      getPlanNodeId(), std::move(output)));
}

int64_t WindowAggregator::windowStart(int64_t windowEnd) const {
  return windowEnd - node_->size();
}

void WindowAggregator::registerFire(
    const RowContainerStateKey& key,
    int64_t windowEnd) {
  // Due timestamp = window_end - 1 (matching Flink's maxTimestamp).
  fireIndex_.emplace(windowEnd - 1, FireEntry{key, SliceEndNs(windowEnd)});
}

void WindowAggregator::initializeRows(char** rows, size_t numRows) {
  std::vector<vector_size_t> indices(numRows);
  for (size_t i = 0; i < numRows; ++i) {
    indices[i] = static_cast<vector_size_t>(i);
  }
  for (auto& aggregate : aggregates_) {
    aggregate.function->initializeNewGroups(
        rows,
        folly::Range<const vector_size_t*>(indices.data(), indices.size()));
  }
}

void WindowAggregator::mergeRow(
    char* row,
    const std::vector<VectorPtr>& columns) {
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_[i].function->addSingleGroupIntermediateResults(
        row, SelectivityVector(1), {columns[i]}, false);
  }
}

void WindowAggregator::checkInputRowKinds(const StreamRecord& record) const {
  if (record.appendOnly()) {
    return;
  }
  const auto& rowKind = *record.rowKind();
  // TODO: support retraction semantics (L2); every row must be INSERT until then.
  for (vector_size_t row = 0; row < rowKind.size(); ++row) {
    VELOX_CHECK_EQ(
        rowKind.valueAt(row),
        static_cast<int8_t>(RowKind::INSERT),
        "WindowAggregator received a non-INSERT row; retraction is not "
        "supported yet (L2)");
  }
}

} // namespace facebook::velox::stateful
