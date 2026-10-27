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

#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/DateTimeImpl.h"

#include "velox/type/Timestamp.h"

namespace facebook::velox::functions::flinksql {

namespace detail {

constexpr int64_t kMillisPerDay = 86'400'000;
constexpr int64_t kMicrosPerMilli = 1'000;

// Flink adds interval millis to a TIMESTAMP keeping its sub-millisecond part
// (presto's fromMillis(a.toMillis()+b) would truncate it).
inline Timestamp plusMillisKeepMicros(
    const Timestamp& ts,
    const int64_t millis) {
  return Timestamp::fromMicrosNoError(ts.toMicros() + millis * kMicrosPerMilli);
}

// Flink's DATE + day-time interval keeps the date type and truncates the
// time-of-day part of the interval toward negative infinity (25h -> +1 day).
inline int32_t plusMillisTruncateToDay(
    const int32_t date,
    const int64_t millis) {
  const int64_t days =
      millis / kMillisPerDay - (millis % kMillisPerDay < 0 ? 1 : 0);
  return static_cast<int32_t>(date + days);
}

} // namespace detail

// Timestamp +/- IntervalDayTime
template <typename T>
struct TimestampPlusIntervalDayTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b) {
    result = detail::plusMillisKeepMicros(a, b);
    return true;
  }
};

template <typename T>
struct TimestampMinusIntervalDayTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalDayTime>& b) {
    result = detail::plusMillisKeepMicros(a, -b);
    return true;
  }
};

// Timestamp +/- IntervalYearMonth: Flink uses a timezone-naive wall-calendar
// add with end-of-month clamping; the no-timezone overload of addToTimestamp
// matches exactly (presto's session-timezone round-trip would diverge across
// DST boundaries).
template <typename T>
struct TimestampPlusIntervalYearMonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = addToTimestamp(a, DateTimeUnit::kMonth, b);
    return true;
  }
};

template <typename T>
struct TimestampMinusIntervalYearMonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& a,
      const arg_type<IntervalYearMonth>& b) {
    result = addToTimestamp(a, DateTimeUnit::kMonth, -b);
    return true;
  }
};

// Date +/- interval: Flink keeps the DATE type.
template <typename T>
struct DatePlusIntervalDayTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    result = detail::plusMillisTruncateToDay(date, interval);
    return true;
  }
};

template <typename T>
struct DatePlusIntervalYearMonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToDate(date, DateTimeUnit::kMonth, interval);
    return true;
  }
};

template <typename T>
struct DateMinusIntervalDayTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalDayTime>& interval) {
    result = detail::plusMillisTruncateToDay(date, -interval);
    return true;
  }
};

template <typename T>
struct DateMinusIntervalYearMonthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<Date>& result,
      const arg_type<Date>& date,
      const arg_type<IntervalYearMonth>& interval) {
    result = addToDate(date, DateTimeUnit::kMonth, -interval);
    return true;
  }
};

// Interval * BIGINT for the TIMESTAMPADD(column, ...) rewrite
// ts + (interval * n). Flink wraps silently on extreme products.
template <typename T>
struct IntervalDayTimeMultiplyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<IntervalDayTime>& result,
      const arg_type<IntervalDayTime>& a,
      const arg_type<int64_t>& b) {
    result = a * b;
    return true;
  }
};

template <typename T>
struct IntervalYearMonthMultiplyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE bool call(
      out_type<IntervalYearMonth>& result,
      const arg_type<IntervalYearMonth>& a,
      const arg_type<int64_t>& b) {
    result = static_cast<int32_t>(static_cast<int64_t>(a) * b);
    return true;
  }
};

} // namespace facebook::velox::functions::flinksql
