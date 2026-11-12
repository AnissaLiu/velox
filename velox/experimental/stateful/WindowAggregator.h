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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "velox/exec/AggregateInfo.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/stateful/EmptyOperator.h"
#include "velox/experimental/stateful/KeySelector.h"
#include "velox/experimental/stateful/StatefulOperator.h"
#include "velox/experimental/stateful/StatefulPlanNode.h"
#include "velox/experimental/stateful/StreamElement.h"
#include "velox/experimental/stateful/state/HeapKeyedStateBackend.h"
#include "velox/experimental/stateful/state/Namespace.h"
#include "velox/experimental/stateful/state/State.h"
#include "velox/experimental/stateful/window/SliceAssigner.h"
#include "velox/experimental/stateful/window/WindowNamespace.h"

namespace facebook::velox::stateful {

/// Streaming window TVF aggregation (TUMBLE / HOP / CUMULATE / SESSION)
/// over the keyed state backend, the counterpart of Flink's
/// WindowAggOperator + XXXWindowAggProcessor family.
///
/// State model: AggregatingState<K, Ns> where K is the grouping key row
/// and Ns is the window namespace (SliceEndNs for aligned windows,
/// TimeWindowNs for SESSION). Window results are emitted when the
/// watermark (event-time) or the processing-time clock crosses
/// window_end - 1ms; fire is driven by a fire index (an ordered map from
/// due timestamp to dirty (key, window) pairs) rather than per-(key, ns)
/// timers. Output is append-only (+I); empty windows are not emitted
/// (state exists only for windows that received data). Late elements are
/// dropped only when every window owning the slice has already fired;
/// otherwise they are accumulated into an unfired window.
///
/// Limitations (fail loud): append-only input only (needRetraction=true
/// is rejected at translation time; a non-+I rowKind fails at runtime);
/// DISTINCT/sorted aggregates are rejected by makeAggregateInfos.
class WindowAggregator : public StatefulOperator {
 public:
  WindowAggregator(
      std::unique_ptr<exec::Operator> localAggregator,
      std::vector<StatefulOperatorPtr> targets,
      std::shared_ptr<const StreamWindowAggregationNode> node,
      exec::DriverCtx* driverCtx);

  void initialize() override;

  /// Builds the keyed state backend from the plan's KeyedStateSpec,
  /// binds the key selector, creates the AggregatingState with the
  /// window namespace, and wires the aggregate function offsets.
  void initializeStateBackend(StateBackend* stateBackend) override;

  void addInput(StreamElementPtr input) override;

  void advance() override;

  void close() override;

  std::string name() const override {
    return "WindowAggregator";
  }

  /// Fires all windows whose dueTs <= watermark, then propagates the
  /// watermark downstream. Fire precedes propagation (Flink semantics).
  void processWatermark(int64_t timestamp) override;

  /// Processing-time trigger: fires all windows whose dueTs <= timestamp,
  /// deduplicated by lastTriggeredProcessingTime_. Called from the
  /// NativeCallbackBridge proc-time callback path.
  void onProcessingTime(int64_t timestamp);

  HeapKeyedStateBackend<RowContainerStateKey>& keyedStateBackend() {
    return *keyedBackend_;
  }

 private:
  /// A dirty (key, window) pair pending fire. The key is a copy (row
  /// pointer + cached hash + keyGroup), re-probed at fire time: the
  /// KeyTable reclaims zero-reference key rows at batch boundaries, and
  /// a fire entry registered in batch N may be consumed in batch N+M.
  /// Invariant: a fire entry (key, W) exists -> W's state entry exists
  /// -> the key row has not been reclaimed.
  struct FireEntry {
    RowContainerStateKey key;
    SliceEndNs ns;
  };

  /// Fires all fire-index entries with dueTs <= progress, per window type.
  void fireDue(int64_t progress);

  /// Fires one (key, windowEnd): reads the accumulator, emits the
  /// result row (+I), then clears per window type.
  void fireWindow(const RowContainerStateKey& key, int64_t windowEnd);

  /// Emits the result row: [key columns, window_start, window_end, agg values].
  void emitWindowResult(
      const RowContainerStateKey& key,
      int64_t windowStart,
      int64_t windowEnd);

  /// Per-window-type cleanup after fire.
  void clearWindow(const RowContainerStateKey& key, int64_t windowEnd);

  /// Late-data check: true if every window owning this slice has fired.
  bool isLate(int64_t sliceEnd);

  /// The interval used to advance from one window to the next in the
  /// unfired-window scan (late-data path). HOP -> step, CUMULATE -> step,
  /// TUMBLE/SESSION -> size.
  int64_t windowStep() const;

  /// The interval used to compute the next trigger watermark from a
  /// current watermark: same as windowStep().
  int64_t nextTriggerInterval() const;

  /// Window start from window end (all window types: end - size).
  int64_t windowStart(int64_t windowEnd) const;

  /// Adds a (key, windowEnd) pair to the fire index at dueTs = windowEnd - 1.
  void registerFire(
      const RowContainerStateKey& key,
      int64_t windowEnd);

  /// Aggregate metadata resolution from the plan's AggregationNode.
  std::vector<exec::AggregateInfo> makeWindowAggregateInfos();

  /// Batch accumulator initialization for freshly materialized state rows.
  void initializeRows(char** rows, size_t numRows);

  /// Merges intermediate columns into a state row (restore path).
  void mergeRow(char* row, const std::vector<VectorPtr>& columns);

  /// Asserts the input record carries only +I rowKinds.
  void checkInputRowKinds(const StreamRecord& record) const;

  const std::shared_ptr<const StreamWindowAggregationNode> node_;
  std::unique_ptr<exec::Operator> localAggregator_;
  std::vector<exec::AggregateInfo> aggregates_;
  std::unique_ptr<HeapKeyedStateBackend<RowContainerStateKey>> keyedBackend_;
  std::unique_ptr<KeySelector> keySelector_;
  std::unique_ptr<SliceAssigner> sliceAssigner_;
  std::shared_ptr<AggregatingState<RowContainerStateKey, SliceEndNs>> state_;

  /// Fire index: ordered map from due timestamp to dirty (key, window)
  /// pairs. begin()->first is the next trigger watermark.
  std::multimap<int64_t, FireEntry> fireIndex_;

  int64_t currentProgress_ = 0;
  int64_t lastTriggeredProcessingTime_ = 0;
  int32_t numKeys_ = 0;
  int32_t numLeadingColumns_ = 0;
  RowVectorPtr input_;
  std::vector<char*> stateRows_;
  std::vector<char*> rowGroups_;
  std::unordered_map<const char*, vector_size_t> rowToDistinct_;

  static constexpr vector_size_t kZeroIndex = 0;
};

} // namespace facebook::velox::stateful
