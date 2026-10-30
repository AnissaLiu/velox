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

#include <chrono>
#include <limits>
#include <string_view>

#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/lib/DateTimeFormatterBuilder.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/type/Timestamp.h"

namespace facebook::velox::functions::flinksql {

namespace detail {

// Flink renders an empty SimpleDateFormat pattern to an empty string; the
// Joda builder rejects an empty pattern, so build a token-less formatter
// instead.
inline std::shared_ptr<DateTimeFormatter> emptyJodaFormatter() {
  DateTimeFormatterBuilder builder(0);
  builder.setType(DateTimeFormatterType::JODA);
  return builder.build();
}

// Builds a Joda formatter, returning nullptr on an invalid pattern so the
// caller can defer the failure to call(): Flink never touches the formatter
// for all-null rows, so an invalid constant format must only fail the query
// when at least one row is actually evaluated.
inline std::shared_ptr<DateTimeFormatter> tryGetJodaFormatter(
    const std::string_view format) {
  if (format.empty()) {
    return emptyJodaFormatter();
  }
  auto formatter = buildJodaDateTimeFormatter(format);
  if (formatter.hasError()) {
    return nullptr;
  }
  return formatter.value();
}

// Flink lenient behavior for DST gap times: Java SimpleDateFormat
// interprets a non-existent local time (spring-forward gap) using the
// pre-transition UTC offset. Velox's toGMT() throws instead; this helper
// recovers by computing the epoch with the pre-gap offset, which is found
// by converting a guaranteed-to-exist time (one second before the gap).
inline void resolveDstGap(Timestamp& timestamp, const tz::TimeZone& tz) {
  const auto wall = std::chrono::seconds(timestamp.getSeconds());
  // One second before the gap is guaranteed to exist as a local time.
  const auto utcBefore = tz.to_sys(
      wall - std::chrono::seconds(1), tz::TimeZone::TChoose::kEarliest);
  // Pre-transition offset (positive for east of UTC).
  const auto offset = (wall - std::chrono::seconds(1)) - utcBefore;
  // Apply the pre-transition offset to the gap time (Java lenient).
  timestamp = Timestamp((wall - offset).count(), timestamp.getNanos());
}

// Flink renders FROM_UNIXTIME in the session timezone regardless of the
// adjust_timestamp_to_timezone flag (that flag governs TIMESTAMP
// storage/comparison, not rendering).
inline const tz::TimeZone* getSessionTimeZone(const core::QueryConfig& config) {
  const auto sessionTzName = config.sessionTimezone();
  if (!sessionTzName.empty()) {
    return tz::locateZone(sessionTzName);
  }
  return nullptr;
}

// Flink's SimpleDateFormat is lenient in two ways that velox's Joda
// formatter is not:
// 1. Calendar-invalid dates (e.g. '2025-02-30') roll to the next valid
//    date ('2025-03-02').
// 2. Trailing text after a valid prefix is ignored (e.g.
//    '2026-09-11 10:00:00 xyz' parses as '2026-09-11 10:00:00').
// This helper first tries the full input; on failure it retries with
// the longest prefix that ends at a whitespace or format boundary.
// Only when no prefix is parseable does it return the error.
inline folly::Expected<DateTimeResult, Status> parseLenient(
    const std::shared_ptr<DateTimeFormatter>& formatter,
    const std::string_view input) {
  auto result = formatter->parse(input);
  if (!result.hasError()) {
    return result;
  }
  // Trailing-text tolerance: find the longest whitespace-delimited prefix
  // and try parsing that. Flink's SimpleDateFormat.parse() ignores text
  // after the matched pattern.
  for (size_t end = input.size(); end > 0; --end) {
    if (end == input.size() &&
        std::isspace(static_cast<unsigned char>(input[end - 1]))) {
      continue;
    }
    if (std::isspace(static_cast<unsigned char>(input[end - 1]))) {
      auto prefixResult = formatter->parse(input.substr(0, end - 1));
      if (!prefixResult.hasError()) {
        return prefixResult;
      }
    }
  }
  // Try the full input one more time in case the initial failure was a
  // calendar-invalid date (Joda strict) rather than trailing text — the
  // caller's gap/lenient handling may resolve it downstream.
  return result;
}

// Flink fails the job on an invalid SimpleDateFormat pattern
// (IllegalArgumentException from the formatter constructor).
inline std::shared_ptr<DateTimeFormatter> getJodaFormatterOrThrow(
    const std::string_view format) {
  if (format.empty()) {
    return emptyJodaFormatter();
  }
  auto formatter = buildJodaDateTimeFormatter(format);
  if (formatter.hasError()) {
    VELOX_USER_FAIL(
        "Invalid datetime format pattern '{}': {}",
        format,
        formatter.error().message());
  }
  return formatter.value();
}

// Whether the pattern parses an explicit year field.
inline bool hasYearSpecifier(const DateTimeFormatter& formatter) {
  for (const auto& token : formatter.tokens()) {
    if (token.type != DateTimeToken::Type::kPattern) {
      continue;
    }
    switch (token.pattern.specifier) {
      case DateTimeFormatSpecifier::YEAR:
      case DateTimeFormatSpecifier::YEAR_OF_ERA:
      case DateTimeFormatSpecifier::CENTURY_OF_ERA:
      case DateTimeFormatSpecifier::WEEK_YEAR:
        return true;
      default:
        break;
    }
  }
  return false;
}

// Whether the pattern parses month/day fields of the MM/dd family — the
// tokens that make velox's Joda emulation default a missing year to 2000
// instead of 1970 (DateTimeFormatter.cpp "fallsback to 2000").
inline bool hasMonthOrDaySpecifier(const DateTimeFormatter& formatter) {
  for (const auto& token : formatter.tokens()) {
    if (token.type != DateTimeToken::Type::kPattern) {
      continue;
    }
    switch (token.pattern.specifier) {
      case DateTimeFormatSpecifier::MONTH_OF_YEAR:
      case DateTimeFormatSpecifier::MONTH_OF_YEAR_TEXT:
      case DateTimeFormatSpecifier::DAY_OF_MONTH:
        return true;
      default:
        break;
    }
  }
  return false;
}

// Flink/Java defaults a missing year to 1970 while velox's Joda emulation
// uses 2000 once month or day fields are present.
inline bool defaultsMissingYearTo2000(const DateTimeFormatter& formatter) {
  return !hasYearSpecifier(formatter) && hasMonthOrDaySpecifier(formatter);
}

// Replaces the wall-clock year with 1970 (the Java default for missing
// years). Feb 29 rolls to Mar 1 through the epoch recomposition, matching
// Java's lenient calendar.
inline void applyEpochDefaultYear(Timestamp& timestamp) {
  std::tm dateTime{};
  if (Timestamp::epochToCalendarUtc(timestamp.getSeconds(), dateTime)) {
    dateTime.tm_year = 70;
    timestamp = Timestamp(
        Timestamp::calendarUtcToEpoch(dateTime), timestamp.getNanos());
  }
}

} // namespace detail

// FROM_UNIXTIME(unixtime): render epoch seconds with the default pattern
// 'yyyy-MM-dd HH:mm:ss' in the session timezone.
template <typename T>
struct FromUnixtimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/) {
    sessionTimeZone_ = detail::getSessionTimeZone(config);
    formatter_ = detail::getJodaFormatterOrThrow(kDefaultFormat_);
    maxResultSize_ = formatter_->maxResultSize(sessionTimeZone_);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& second) {
    formatSeconds(result, second);
    return true;
  }

 protected:
  void formatSeconds(out_type<Varchar>& result, const int64_t second) {
    const Timestamp timestamp{second, 0};
    result.reserve(maxResultSize_);
    const int32_t resultSize = formatter_->format(
        timestamp, sessionTimeZone_, maxResultSize_, result.data(), true);
    result.resize(resultSize);
  }

  constexpr static std::string_view kDefaultFormat_{"yyyy-MM-dd HH:mm:ss"};
  const tz::TimeZone* sessionTimeZone_{nullptr};
  std::shared_ptr<DateTimeFormatter> formatter_;
  uint32_t maxResultSize_{0};
};

// FROM_UNIXTIME(unixtime, format)
template <typename T>
struct FromUnixtimeWithFormatFunction : public FromUnixtimeFunction<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int64_t>* /*unixtime*/,
      const arg_type<Varchar>* format) {
    this->sessionTimeZone_ = detail::getSessionTimeZone(config);
    if (format != nullptr) {
      this->formatter_ = detail::tryGetJodaFormatter(std::string_view(*format));
      isConstantFormat_ = true;
      if (this->formatter_ != nullptr) {
        this->maxResultSize_ =
            this->formatter_->maxResultSize(this->sessionTimeZone_);
      }
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<int64_t>& second,
      const arg_type<Varchar>& format) {
    if (!isConstantFormat_) {
      this->formatter_ =
          detail::getJodaFormatterOrThrow(std::string_view(format));
      this->maxResultSize_ =
          this->formatter_->maxResultSize(this->sessionTimeZone_);
    } else if (this->formatter_ == nullptr) {
      VELOX_USER_FAIL(
          "Invalid datetime format pattern: '{}'", std::string_view(format));
    }
    this->formatSeconds(result, second);
    return true;
  }

 private:
  bool isConstantFormat_{false};
};

// UNIX_TIMESTAMP(): current epoch seconds. Non-deterministic so velox keeps
// evaluating per row like Flink (deterministic no-arg calls are constant
// folded once per query).
template <typename T>
struct UnixTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void call(int64_t& result) {
    result = Timestamp::now().getSeconds();
  }
};

// UNIX_TIMESTAMP(s): parse 'yyyy-MM-dd HH:mm:ss' in the session timezone.
// Flink returns Long.MIN_VALUE (not NULL) when parsing fails.
template <typename T>
struct UnixTimestampParseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/) {
    setFormatter(detail::getJodaFormatterOrThrow(kDefaultFormat_));
    setTimezone(config);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input) {
    parseToSeconds(result, std::string_view(input));
    return true;
  }

 protected:
  void setFormatter(std::shared_ptr<DateTimeFormatter> formatter) {
    formatter_ = std::move(formatter);
    needsEpochDefaultYear_ = detail::defaultsMissingYearTo2000(*formatter_);
  }
  void setTimezone(const core::QueryConfig& config) {
    const auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTimeZone_ = tz::locateZone(sessionTzName);
    }
  }

  const tz::TimeZone* getTimeZone(const DateTimeResult& dateTime) const {
    // If timezone was not parsed, fallback to the session timezone.
    return dateTime.timezone ? dateTime.timezone : sessionTimeZone_;
  }
  // Flink truncates total milliseconds toward zero (ts / 1000 in Java);
  // getSeconds() alone would diverge for negative sub-second timestamps.
  void parseToSeconds(int64_t& result, const std::string_view input) {
    auto dateTimeResult = detail::parseLenient(formatter_, input);
    if (dateTimeResult.hasError()) {
      result = std::numeric_limits<int64_t>::min();
      return;
    }
    auto timestamp = dateTimeResult.value().timestamp;
    if (needsEpochDefaultYear_) {
      detail::applyEpochDefaultYear(timestamp);
    }
    const auto* tz = getTimeZone(dateTimeResult.value());
    if (tz != nullptr) {
      try {
        timestamp.toGMT(*tz);
      } catch (const VeloxUserError&) {
        // Flink lenient: a DST gap local time (e.g. 2005-03-27 02:00:00
        // in MET where 02:00 does not exist) is interpreted with the
        // pre-transition offset — the same instant as the first valid
        // time after the gap (Java SimpleDateFormat behavior).
        detail::resolveDstGap(timestamp, *tz);
      }
    }
    const int64_t millis = timestamp.getSeconds() * 1000 +
        static_cast<int64_t>(timestamp.getNanos()) / 1000000;
    result = millis / 1000;
  }

  constexpr static std::string_view kDefaultFormat_{"yyyy-MM-dd HH:mm:ss"};
  std::shared_ptr<DateTimeFormatter> formatter_;
  bool needsEpochDefaultYear_{false};
  const tz::TimeZone* sessionTimeZone_{tz::locateZone(0)};
};

// UNIX_TIMESTAMP(s, format)
template <typename T>
struct UnixTimestampParseWithFormatFunction
    : public UnixTimestampParseFunction<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    this->setTimezone(config);
    if (format != nullptr) {
      auto formatter = detail::tryGetJodaFormatter(std::string_view(*format));
      if (formatter != nullptr) {
        this->setFormatter(std::move(formatter));
      }
      isConstantFormat_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (!isConstantFormat_) {
      this->setFormatter(
          detail::getJodaFormatterOrThrow(std::string_view(format)));
    } else if (this->formatter_ == nullptr) {
      VELOX_USER_FAIL(
          "Invalid datetime format pattern: '{}'", std::string_view(format));
    }
    this->parseToSeconds(result, std::string_view(input));
    return true;
  }

 private:
  bool isConstantFormat_{false};
};

} // namespace facebook::velox::functions::flinksql
