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

#include <cstdlib>

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
  VELOX_CHECK_NOT_NULL(
      node_->keyedStateSpec(),
      "StreamWindowAggregationNode must carry a KeyedStateSpec");
  const WindowType wt = static_cast<WindowType>(node_->windowType());
  // TODO(L5): support CUMULATE and SESSION windows in a follow-up task;
  // running them through the aligned-window logic would silently
  // produce wrong results.
  if (wt != WindowType::HOP && wt != WindowType::TUMBLE) {
    VELOX_UNSUPPORTED(
        "WindowAggregator does not support CUMULATE/SESSION windows yet (L5)");
  }
  VELOX_CHECK_GT(
      node_->size(), 0, "window size must be positive, got {}", node_->size());
  VELOX_CHECK_LT(
      std::abs(node_->offset()),
      node_->size(),
      "window offset must be in (-size, size)");
  if (wt == WindowType::HOP) {
    VELOX_CHECK_GT(
        node_->step(),
        0,
        "HOP window slide must be positive, got {}",
        node_->step());
    VELOX_CHECK_EQ(
        node_->size() % node_->step(),
        0,
        "HOP window size must be a multiple of slide (size={}, slide={})",
        node_->size(),
        node_->step());
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
  declaredInputType_ = inputType;
  // Keyless windows (empty grouping): gluten appends a constant BIGINT
  // "$keyless_key" column to the declared input type and keys on it (the
  // keyed state backend requires at least one key channel); runtime
  // batches are one column narrower and get the column wrapped on in
  // addInput.
  const auto& keyChannels = node_->keyedStateSpec()->keyChannels();
  keyless_ = keyChannels.size() == 1 &&
      keyChannels[0] == inputType->size() - 1 &&
      std::string(inputType->nameOf(inputType->size() - 1)) == "$keyless_key";
  numKeys_ = static_cast<int32_t>(keyChannels.size());
  // Window columns carry windowStartIndex/windowEndIndex; -1 falls back
  // to the legacy layout right after the key columns.
  windowStartIdx_ =
      node_->windowStartIndex() >= 0 ? node_->windowStartIndex() : numKeys_;
  windowEndIdx_ =
      node_->windowEndIndex() >= 0 ? node_->windowEndIndex() : numKeys_ + 1;
  // The aggregate output columns sit where the plan's projection puts
  // them - either right after the keys (q12-style: key, aggs, window) or
  // after the window columns (wa-e1-style: key, window, aggs). The base
  // is the first output index that is neither a key column nor a window
  // column; the run must be contiguous and cover every remaining column.
  const auto& outputType = node_->outputType();
  std::vector<bool> taken(outputType->size(), false);
  for (int32_t k = 0; k < numKeys_; ++k) {
    taken[k] = true;
  }
  taken[windowStartIdx_] = true;
  taken[windowEndIdx_] = true;
  const int32_t freeColumns =
      static_cast<int32_t>(outputType->size()) - numKeys_ - 2;
  VELOX_CHECK_EQ(
      freeColumns,
      static_cast<int32_t>(aggNode->aggregates().size()),
      "Output schema must be keys + window_start/end + aggregates, got {}",
      outputType->toString());
  numLeadingColumns_ = -1;
  for (int32_t c = 0; c < static_cast<int32_t>(outputType->size()); ++c) {
    if (!taken[c]) {
      numLeadingColumns_ = c;
      break;
    }
  }
  VELOX_CHECK_GE(
      numLeadingColumns_,
      0,
      "No output column left for aggregates in {}",
      outputType->toString());
  // Count star detection (Flink indexOfCountStar): the first
  // zero-argument UNFILTERED COUNT call (AggregateUtil requires
  // filterArg < 0 - a filtered COUNT(*) is not a count star, the
  // empty-window check stays disabled and zero-count windows emit).
  // Hopping windows require one - Flink checkArgument.
  countStarIndex_ = findCountStarIndex(
      aggNode->aggregates(), wt == WindowType::HOP);
  aggregates_ = makeAggregateInfos(
      aggNode->aggregates(),
      inputType,
      node_->outputType(),
      numLeadingColumns_,
      op()->operatorCtx()->driverCtx()->queryConfig(),
      memoryPool());
}

void WindowAggregator::initializeStateBackend(StateBackend* stateBackend) {
  tz_ = TimeWindowUtil::locateTimeZone(node_->shiftTimeZone());
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
      node_->rowtimeIndex(),
      tz_);

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
    if (static_cast<int32_t>(i) == countStarIndex_) {
      countStarOffset_ = countStarColumnOffset(
          aggregates_[i].intermediateType->kind(), rowColumn.offset());
    }
  }

  StatefulOperator::initializeStateBackend(stateBackend);

  // Processing-time windows fire on the wall clock: the wake-up timer
  // asks Java to drain the task at the next due time and the fire itself
  // runs on the task thread (checkWatermarkStatus / advance).
  if (!node_->isEventTime()) {
    procTimeScheduler_ = std::make_unique<SystemProcessingTimeScheduler>();
  }
}

void WindowAggregator::addInput(StreamElementPtr input) {
  auto record = std::static_pointer_cast<StreamRecord>(input);
  checkInputRowKinds(*record);
  VELOX_CHECK(!input_, "Last input has not been processed");
  input_ = keyless_ ? wrapKeylessColumn(record->record()) : record->record();
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
    // Rows accumulate into their own slice's accumulator (Flink
    // AbstractSliceWindowAggProcessor#processElement buffers per
    // (key, sliceEnd)); a HOP window merges its covered slices on the
    // heap at fire time (fireHopWindow). Redirecting accumulation into
    // a "first slice" namespace is wrong for shared slices.
    SliceEndNs ns(sliceEnd);

    // Late check (event-time, two-phase per Flink
    // AbstractSliceWindowAggProcessor#processElement):
    // Phase 1: slice fired? Phase 2: all owning windows fired?
    // TUMBLE: lastWindowEnd == sliceEnd (single window) -> phase 1
    // implies drop. HOP: lastWindowEnd = sliceEnd - sliceSize + size.
    if (node_->isEventTime() &&
        TimeWindowUtil::isWindowFired(sliceEnd, currentProgress_, tz_)) {
      int64_t lastWindowEnd = sliceAssigner_->getLastWindowEnd(sliceEnd);
      if (TimeWindowUtil::isWindowFired(lastWindowEnd, currentProgress_, tz_)) {
        lateDroppedRows_ += sliceData->size();
        continue; // All owning windows fired: drop.
      }
      // Partially fired: accumulate into the (unfired) own slice below
      // and register the first unfired owning window for fire.
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

    // Register fire for the first unfired window owning this slice,
    // scanning forward from sliceEnd by the slice interval (Flink
    // registers a timer at sliceEnd for the earliest owning window and
    // the late path scans to the first unfired one; for a fresh slice
    // the scan exits immediately).
    int64_t fireWindowEnd = sliceEnd;
    if (node_->isEventTime()) {
      const int64_t sliceSize = sliceAssigner_->getSliceEndInterval();
      while (TimeWindowUtil::isWindowFired(
          fireWindowEnd, currentProgress_, tz_)) {
        // Clamp before overflow: isWindowFired is false at INT64_MAX.
        if (fireWindowEnd >
            std::numeric_limits<int64_t>::max() - sliceSize) {
          fireWindowEnd = std::numeric_limits<int64_t>::max();
          break;
        }
        fireWindowEnd += sliceSize;
      }
    }
    for (vector_size_t d = 0; d < sliceNumDistinct; ++d) {
      registerFire(distinctKeys[d], fireWindowEnd);
    }
  }

  input_.reset();
  keyedBackend_->advance();
  // Processing-time windows fire opportunistically after each batch (the
  // wall clock only moves forward); idle periods are covered by the
  // wake-up timer + checkWatermarkStatus tick.
  if (!node_->isEventTime() && procTimeScheduler_) {
    fireDue(TimeWindowUtil::getCurrentProcessingTime());
    armProcTimeWakeUp();
  }
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
  armProcTimeWakeUp();
}

void WindowAggregator::checkWatermarkStatus(int64_t now) {
  // Processing-time tick on the task thread: StatefulTask::next invokes
  // checkWatermarkStatus(now) on idle drains. This is where proc-time
  // windows actually fire (the background wake-up timer only asks Java
  // to drain the task; the operator state is never touched off-thread).
  if (!node_->isEventTime() && keyedBackend_) {
    fireDue(now);
    armProcTimeWakeUp();
  }
  StatefulOperator::checkWatermarkStatus(now);
}

void WindowAggregator::close() {
  shutdownProcTimeScheduler();
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
  registeredFires_.clear();
  currentProgress_ = std::numeric_limits<int64_t>::min();
  lastTriggeredProcessingTime_ = 0;
  lateDroppedRows_ = 0;
}

void WindowAggregator::fireDue(int64_t progress) {
  while (!fireIndex_.empty() && fireIndex_.begin()->first <= progress) {
    auto it = fireIndex_.begin();
    auto key = it->second.key;
    auto windowEnd = it->second.ns.sliceEnd();
    registeredFires_.erase({key.row(), windowEnd});
    fireIndex_.erase(it);
    fireWindow(key, windowEnd);
  }
  if (!node_->isEventTime() && procTimeScheduler_) {
    armProcTimeWakeUp();
  }
}

void WindowAggregator::fireWindow(
    const RowContainerStateKey& key,
    int64_t windowEnd) {
  const WindowType wt = static_cast<WindowType>(node_->windowType());
  if (wt == WindowType::HOP) {
    fireHopWindow(key, windowEnd);
    return;
  }
  // TUMBLE: the slice is the window. Read-only peek: a miss means the
  // window never materialized or was already cleared; creating an empty
  // accumulator here would emit a bogus empty window row.
  auto ns = SliceEndNs(windowEnd);
  char* row = state_->get(key, ns);
  if (row == nullptr) {
    return;
  }
  // Empty-window suppression (Flink WindowIsEmptySupplier): a window
  // whose count star reads 0 emits nothing. Cleanup still runs (Flink
  // fireWindow suppresses collect, then clearWindow removes the state).
  if (!countStarIsZero(row, countStarOffset_)) {
    emitWindowResult(key, windowStart(windowEnd), windowEnd, row);
  }
  state_->remove(key, ns);
}

void WindowAggregator::fireHopWindow(
    const RowContainerStateKey& key,
    int64_t windowEnd) {
  // Heap merge of the slices covered by [windowEnd - size, windowEnd):
  // firstSliceEnd .. windowEnd stepping by the slice interval (Flink
  // SharedSliceAssigner#mergeSlices / HoppingSlicesIterable).
  const int64_t sliceSize = sliceAssigner_->getSliceEndInterval();
  const int64_t windowStartMs = windowStart(windowEnd);
  const int64_t firstSliceEnd = windowStartMs + sliceSize;

  // Read-only peeks: a window with no materialized slice is empty —
  // no emission, no chain continuation (Flink empty-window semantics).
  char* firstRow = state_->get(key, SliceEndNs(firstSliceEnd));
  bool hasData = firstRow != nullptr;
  for (int64_t e = firstSliceEnd + sliceSize;
       e <= windowEnd && !hasData;
       e += sliceSize) {
    hasData = state_->get(key, SliceEndNs(e)) != nullptr;
  }
  if (!hasData) {
    return;
  }

  // The merge target is the first slice; materialize its accumulator
  // when the window's data starts in a later slice (Flink mergeSlices
  // merges into the first slice of the window).
  if (firstRow == nullptr) {
    firstRow = state_->row(key, SliceEndNs(firstSliceEnd));
  }

  // Merge the remaining covered slices into the first slice. Source
  // slices are NOT removed: they are shared with later windows and are
  // only cleared when they become a later window's first slice.
  for (int64_t e = firstSliceEnd + sliceSize; e <= windowEnd; e += sliceSize) {
    if (char* sourceRow = state_->get(key, SliceEndNs(e))) {
      mergeAccumulatorRows(firstRow, sourceRow);
    }
  }

  // Empty-window suppression on the merged result (Flink checks the
  // emptySupplier after mergeSlices, before collect). Emission only;
  // cleanup and the probe-based chain still run - the chain continues
  // only into windows whose state exists, whose fires are suppressed
  // the same way, so the observable stream matches Flink exactly (the
  // probe keeps the R1 key-liveness invariant; a count-gated chain
  // would register entries for keys with no state and dangle).
  if (!countStarIsZero(firstRow, countStarOffset_)) {
    emitWindowResult(key, windowStartMs, windowEnd, firstRow);
  }

  // ExpiredSlices: HOP cleans up only the first slice of the fired
  // window (Flink SliceAssigners#SharedSliceAssigner.expiredSlices does
  // not differentiate).
  state_->remove(key, SliceEndNs(firstSliceEnd));

  // Chain continuation: the fired window is non-empty, so register the
  // next window at windowEnd + sliceSize (Flink nextTriggerWindow has
  // no upper bound; an empty window stops the chain above). Deduplicated
  // with any registration the next window's own slices already made.
  // Register ONLY when the next window actually has state: an entry for
  // a key with no live state would dangle after the KeyTable reclaims
  // the key row at the next batch boundary (fire entries hold row
  // pointers that StateMap equality comparisons dereference).
  const int64_t nextWindowEnd = windowEnd + sliceSize;
  const int64_t nextFirstSliceEnd =
      windowStart(nextWindowEnd) + sliceSize;
  bool nextHasState = false;
  for (int64_t e = nextFirstSliceEnd; e <= nextWindowEnd; e += sliceSize) {
    if (state_->get(key, SliceEndNs(e)) != nullptr) {
      nextHasState = true;
      break;
    }
  }
  if (nextHasState) {
    registerFire(key, nextWindowEnd);
  }
}

void WindowAggregator::mergeAccumulatorRows(char* targetRow, char* sourceRow) {
  // One-row intermediate extraction per aggregate, merged into the
  // target accumulator — the same path restore uses (mergeRow).
  std::vector<VectorPtr> columns(aggregates_.size());
  for (size_t i = 0; i < aggregates_.size(); ++i) {
    columns[i] = BaseVector::create(
        aggregates_[i].intermediateType, 1, memoryPool());
    aggregates_[i].function->extractAccumulators(&sourceRow, 1, &columns[i]);
  }
  mergeRow(targetRow, columns);
}

void WindowAggregator::emitWindowResult(
    const RowContainerStateKey& key,
    int64_t windowStartMs,
    int64_t windowEndMs,
    char* row) {
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

  // Window columns as TIMESTAMP, at the projection positions the plan
  // carries (windowStartIdx_/windowEndIdx_, resolved at initialize from
  // windowStartIndex/windowEndIndex).
  auto startVec =
      BaseVector::create(outputType->childAt(windowStartIdx_), 1, memoryPool());
  if (auto* tsVec = startVec->asFlatVector<Timestamp>()) {
    tsVec->set(0, Timestamp::fromMillis(windowStartMs));
  }
  children[windowStartIdx_] = startVec;

  auto endVec = BaseVector::create(outputType->childAt(windowEndIdx_), 1, memoryPool());
  if (auto* tsVec = endVec->asFlatVector<Timestamp>()) {
    tsVec->set(0, Timestamp::fromMillis(windowEndMs));
  }
  children[windowEndIdx_] = endVec;

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
  // Deduplicated per (key row, windowEnd): Flink's timer service
  // registers one timer per (key, namespace), and a duplicate fire
  // entry would re-emit an already-cleared window (the second fire
  // would read a freshly created empty accumulator).
  if (registeredFires_.insert({key.row(), windowEnd}).second) {
    fireIndex_.emplace(
        TimeWindowUtil::toEpochMillsForTimer(windowEnd - 1, tz_),
        FireEntry{key, SliceEndNs(windowEnd)});
  }
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

void WindowAggregator::rebuildFireIndex() {
  // One fire entry per live (key, window) state entry; deduplicated by
  // registerFire against anything the current session already armed.
  // currentProgress_ restarts at Long.MIN (progress is not persisted,
  // L3): the first watermark drives everything whose due time it passes,
  // which is the "fire immediately what is due" behavior.
  state_->forEachEntry([this](
                           const RowContainerStateKey& key,
                           const SliceEndNs& ns) {
    registerFire(key, ns.sliceEnd());
  });
  armProcTimeWakeUp();
}

void WindowAggregator::armProcTimeWakeUp() {
  if (!procTimeScheduler_ || fireIndex_.empty()) {
    return;
  }
  const int64_t nextDue = fireIndex_.begin()->first;
  if (procTimerName_.has_value() && armedDueTs_ <= nextDue) {
    return; // Already armed at an earlier-or-equal due time.
  }
  if (procTimerName_.has_value()) {
    procTimeScheduler_->cancel(procTimerName_.value());
    procTimeScheduler_->unregister(procTimerName_.value());
    procTimerName_ = std::nullopt;
  }
  const auto registered = procTimeScheduler_->registerTimer(
      nextDue, ProcessingTimerTask(nextDue, [this](int64_t timestamp) {
        // Wake-up only: ask Java to drain the task (mailbox), which runs
        // task.advance() -> checkWatermarkStatus on the task thread where
        // the actual fire happens. The operator state is never touched
        // from this background thread (WatermarkAssigner idle-timer
        // pattern).
        auto bridge = nativeCallbackBridge();
        if (bridge) {
          bridge->onProcessingTime(timestamp);
        }
      }));
  if (registered.has_value()) {
    procTimerName_ = registered;
    armedDueTs_ = nextDue;
  }
}

void WindowAggregator::shutdownProcTimeScheduler() {
  if (procTimeScheduler_) {
    if (procTimerName_.has_value()) {
      procTimeScheduler_->cancel(procTimerName_.value());
      procTimerName_ = std::nullopt;
    }
    procTimeScheduler_->close();
    procTimeScheduler_.reset();
  }
}

RowVectorPtr WindowAggregator::wrapKeylessColumn(const RowVectorPtr& input) {
  const auto numRows = input->size();
  auto children = input->children();
  children.push_back(std::make_shared<ConstantVector<int64_t>>(
      memoryPool(), numRows, /*isNull=*/false, BIGINT(), int64_t(0)));
  return std::make_shared<RowVector>(
      memoryPool(),
      declaredInputType_,
      nullptr,
      numRows,
      std::move(children));
}

} // namespace facebook::velox::stateful
