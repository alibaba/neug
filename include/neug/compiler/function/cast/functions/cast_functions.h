/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/cast/functions/numeric_cast.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

struct CastToString {
  template <typename T>
  static inline void operation(T& input, common::neug_string_t& result,
                               common::ValueVector& inputVector,
                               common::ValueVector& resultVector) {
    auto str = common::TypeUtils::toString(input, (void*) &inputVector);
    common::StringVector::addString(&resultVector, result, str);
  }
};

struct CastNodeToString {
  static inline void operation(common::struct_entry_t& input,
                               common::neug_string_t& result,
                               common::ValueVector& inputVector,
                               common::ValueVector& resultVector) {
    auto str = common::TypeUtils::nodeToString(input, &inputVector);
    common::StringVector::addString(&resultVector, result, str);
  }
};

struct CastRelToString {
  static inline void operation(common::struct_entry_t& input,
                               common::neug_string_t& result,
                               common::ValueVector& inputVector,
                               common::ValueVector& resultVector) {
    auto str = common::TypeUtils::relToString(input, &inputVector);
    common::StringVector::addString(&resultVector, result, str);
  }
};

struct CastDateToTimestamp {
  template <typename T>
  static inline void operation(common::date_t& input, T& result) {
    // base case: timestamp
    result = common::Timestamp::fromDateTime(input, common::dtime_t{});
  }
};

template <>
inline void CastDateToTimestamp::operation(common::date_t& input,
                                           common::timestamp_ns_t& result) {
  operation<common::timestamp_t>(input, result);
  result =
      common::timestamp_ns_t{common::Timestamp::getEpochNanoSeconds(result)};
}

template <>
inline void CastDateToTimestamp::operation(common::date_t& input,
                                           common::timestamp_ms_t& result) {
  operation<common::timestamp_t>(input, result);
  result.value /= common::Interval::MICROS_PER_MSEC;
}

template <>
inline void CastDateToTimestamp::operation(common::date_t& input,
                                           common::timestamp_sec_t& result) {
  operation<common::timestamp_t>(input, result);
  result.value /= common::Interval::MICROS_PER_SEC;
}

struct CastToDate {
  template <typename T>
  static inline void operation(T& input, common::date_t& result);
};

template <>
inline void CastToDate::operation(common::timestamp_t& input,
                                  common::date_t& result) {
  result = common::Timestamp::getDate(input);
}

template <>
inline void CastToDate::operation(common::timestamp_ns_t& input,
                                  common::date_t& result) {
  auto tmp = common::Timestamp::fromEpochNanoSeconds(input.value);
  operation<common::timestamp_t>(tmp, result);
}

template <>
inline void CastToDate::operation(common::timestamp_ms_t& input,
                                  common::date_t& result) {
  auto tmp = common::Timestamp::fromEpochMilliSeconds(input.value);
  operation<common::timestamp_t>(tmp, result);
}

template <>
inline void CastToDate::operation(common::timestamp_sec_t& input,
                                  common::date_t& result) {
  auto tmp = common::Timestamp::fromEpochSeconds(input.value);
  operation<common::timestamp_t>(tmp, result);
}

struct CastToDouble {
  template <typename T>
  static inline void operation(T& input, double& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within DOUBLE range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToDouble::operation(common::int128_t& input, double& result) {
  if (!common::Int128_t::tryCast(input, result)) {  // LCOV_EXCL_START
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within DOUBLE range",
                             common::TypeUtils::toString(input)));
  }  // LCOV_EXCL_STOP
}

struct CastToFloat {
  template <typename T>
  static inline void operation(T& input, float& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within FLOAT range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToFloat::operation(common::int128_t& input, float& result) {
  if (!common::Int128_t::tryCast(input, result)) {  // LCOV_EXCL_START
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within FLOAT range",
                             common::TypeUtils::toString(input)));
  };  // LCOV_EXCL_STOP
}

struct CastToInt128 {
  template <typename T>
  static inline void operation(T& input, common::int128_t& result) {
    common::Int128_t::tryCastTo(input, result);
  }
};

struct CastToInt64 {
  template <typename T>
  static inline void operation(T& input, int64_t& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within INT64 range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToInt64::operation(common::int128_t& input, int64_t& result) {
  if (!common::Int128_t::tryCast(input, result)) {
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within INT64 range",
                             common::TypeUtils::toString(input)));
  };
}

struct CastToSerial {
  template <typename T>
  static inline void operation(T& input, int64_t& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within INT64 range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToSerial::operation(common::int128_t& input, int64_t& result) {
  if (!common::Int128_t::tryCast(input, result)) {
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within INT64 range",
                             common::TypeUtils::toString(input)));
  };
}

struct CastToInt32 {
  template <typename T>
  static inline void operation(T& input, int32_t& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within INT32 range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToInt32::operation(common::int128_t& input, int32_t& result) {
  if (!common::Int128_t::tryCast(input, result)) {
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within INT32 range",
                             common::TypeUtils::toString(input)));
  };
}

struct CastToInt16 {
  template <typename T>
  static inline void operation(T& input, int16_t& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within INT16 range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToInt16::operation(common::int128_t& input, int16_t& result) {
  if (!common::Int128_t::tryCast(input, result)) {
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within INT16 range",
                             common::TypeUtils::toString(input)));
  };
}

struct CastToInt8 {
  template <typename T>
  static inline void operation(T& input, int8_t& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within INT8 range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToInt8::operation(common::int128_t& input, int8_t& result) {
  if (!common::Int128_t::tryCast(input, result)) {
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within INT8 range",
                             common::TypeUtils::toString(input)));
  };
}

struct CastToUInt64 {
  template <typename T>
  static inline void operation(T& input, uint64_t& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within UINT64 range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToUInt64::operation(common::int128_t& input, uint64_t& result) {
  if (!common::Int128_t::tryCast(input, result)) {
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within UINT64 range",
                             common::TypeUtils::toString(input)));
  };
}

struct CastToUInt32 {
  template <typename T>
  static inline void operation(T& input, uint32_t& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within UINT32 range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToUInt32::operation(common::int128_t& input, uint32_t& result) {
  if (!common::Int128_t::tryCast(input, result)) {
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within UINT32 range",
                             common::TypeUtils::toString(input)));
  };
}

struct CastToUInt16 {
  template <typename T>
  static inline void operation(T& input, uint16_t& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within UINT16 range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToUInt16::operation(common::int128_t& input, uint16_t& result) {
  if (!common::Int128_t::tryCast(input, result)) {
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within UINT16 range",
                             common::TypeUtils::toString(input)));
  };
}

struct CastToUInt8 {
  template <typename T>
  static inline void operation(T& input, uint8_t& result) {
    if (!tryCastWithOverflowCheck(input, result)) {
      THROW_OVERFLOW_EXCEPTION(
          common::stringFormat("Value {} is not within UINT8 range",
                               common::TypeUtils::toString(input)));
    }
  }
};

template <>
inline void CastToUInt8::operation(common::int128_t& input, uint8_t& result) {
  if (!common::Int128_t::tryCast(input, result)) {
    THROW_OVERFLOW_EXCEPTION(
        common::stringFormat("Value {} is not within UINT8 range",
                             common::TypeUtils::toString(input)));
  };
}

struct CastBetweenTimestamp {
  template <typename SRC_TYPE, typename DST_TYPE>
  static void operation(const SRC_TYPE& input, DST_TYPE& result) {
    // base case: same type
    result.value = input.value;
  }
};

template <>
inline void CastBetweenTimestamp::operation(const common::timestamp_t& input,
                                            common::timestamp_ns_t& output) {
  output.value = common::Timestamp::getEpochNanoSeconds(input);
}

template <>
inline void CastBetweenTimestamp::operation(const common::timestamp_t& input,
                                            common::timestamp_ms_t& output) {
  output.value = common::Timestamp::getEpochMilliSeconds(input);
}

template <>
inline void CastBetweenTimestamp::operation(const common::timestamp_t& input,
                                            common::timestamp_sec_t& output) {
  output.value = common::Timestamp::getEpochSeconds(input);
}

template <>
inline void CastBetweenTimestamp::operation(const common::timestamp_ms_t& input,
                                            common::timestamp_t& output) {
  output = common::Timestamp::fromEpochMilliSeconds(input.value);
}

template <>
inline void CastBetweenTimestamp::operation(const common::timestamp_ms_t& input,
                                            common::timestamp_ns_t& output) {
  operation<common::timestamp_ms_t, common::timestamp_t>(input, output);
  operation<common::timestamp_t, common::timestamp_ns_t>(output, output);
}

template <>
inline void CastBetweenTimestamp::operation(const common::timestamp_ms_t& input,
                                            common::timestamp_sec_t& output) {
  operation<common::timestamp_ms_t, common::timestamp_t>(input, output);
  operation<common::timestamp_t, common::timestamp_sec_t>(output, output);
}

template <>
inline void CastBetweenTimestamp::operation(const common::timestamp_ns_t& input,
                                            common::timestamp_t& output) {
  output = common::Timestamp::fromEpochNanoSeconds(input.value);
}

template <>
inline void CastBetweenTimestamp::operation(const common::timestamp_ns_t& input,
                                            common::timestamp_ms_t& output) {
  operation<common::timestamp_ns_t, common::timestamp_t>(input, output);
  operation<common::timestamp_t, common::timestamp_ms_t>(output, output);
}

template <>
inline void CastBetweenTimestamp::operation(const common::timestamp_ns_t& input,
                                            common::timestamp_sec_t& output) {
  operation<common::timestamp_ns_t, common::timestamp_t>(input, output);
  operation<common::timestamp_t, common::timestamp_sec_t>(output, output);
}

template <>
inline void CastBetweenTimestamp::operation(
    const common::timestamp_sec_t& input, common::timestamp_t& output) {
  output = common::Timestamp::fromEpochSeconds(input.value);
}

template <>
inline void CastBetweenTimestamp::operation(
    const common::timestamp_sec_t& input, common::timestamp_ns_t& output) {
  operation<common::timestamp_sec_t, common::timestamp_t>(input, output);
  operation<common::timestamp_t, common::timestamp_ns_t>(output, output);
}

template <>
inline void CastBetweenTimestamp::operation(
    const common::timestamp_sec_t& input, common::timestamp_ms_t& output) {
  operation<common::timestamp_sec_t, common::timestamp_t>(input, output);
  operation<common::timestamp_t, common::timestamp_ms_t>(output, output);
}

}  // namespace function
}  // namespace neug
