/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/storages/csr/generic_view_utils.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/runtime/rt_any.h"  // For EdgeRecord

namespace gs {

bool nbr_data_eq(const char* data_ptr, const char* expected_ptr,
                 const PropertyType& type) {
  if (type == PropertyType::Bool()) {
    return *reinterpret_cast<const bool*>(data_ptr) ==
           *reinterpret_cast<const bool*>(expected_ptr);
  } else if (type == PropertyType::Int32()) {
    return *reinterpret_cast<const int32_t*>(data_ptr) ==
           *reinterpret_cast<const int32_t*>(expected_ptr);
  } else if (type == PropertyType::UInt32()) {
    return *reinterpret_cast<const uint32_t*>(data_ptr) ==
           *reinterpret_cast<const uint32_t*>(expected_ptr);
  } else if (type == PropertyType::Int64()) {
    return *reinterpret_cast<const int64_t*>(data_ptr) ==
           *reinterpret_cast<const int64_t*>(expected_ptr);
  } else if (type == PropertyType::UInt64()) {
    return *reinterpret_cast<const uint64_t*>(data_ptr) ==
           *reinterpret_cast<const uint64_t*>(expected_ptr);
  } else if (type == PropertyType::Float()) {
    return *reinterpret_cast<const float*>(data_ptr) ==
           *reinterpret_cast<const float*>(expected_ptr);
  } else if (type == PropertyType::Double()) {
    return *reinterpret_cast<const double*>(data_ptr) ==
           *reinterpret_cast<const double*>(expected_ptr);
  } else if (type == PropertyType::Date()) {
    return *reinterpret_cast<const Date*>(data_ptr) ==
           *reinterpret_cast<const Date*>(expected_ptr);
  } else if (type == PropertyType::DateTime()) {
    return *reinterpret_cast<const DateTime*>(data_ptr) ==
           *reinterpret_cast<const DateTime*>(expected_ptr);
  } else {
    THROW_NOT_IMPLEMENTED_EXCEPTION("Unsupported property type in nbr_data_eq" +
                                    type.ToString());
  }
  return false;
}

bool nbr_ts_eq(const char* ts_ptr, const char* expected_ts_ptr) {
  return *reinterpret_cast<const timestamp_t*>(ts_ptr) ==
         *reinterpret_cast<const timestamp_t*>(expected_ts_ptr);
}

int32_t find_exact_match_in_candidates(const std::vector<int32_t>& candidates,
                                       const void* start_ptr,
                                       int32_t nbr_stride, int32_t data_offset,
                                       int32_t ts_offset,
                                       const void* expected_prop,
                                       const PropertyType& type) {
  for (const auto& offset : candidates) {
    auto ptr = reinterpret_cast<const char*>(expected_prop);
    auto nbr_ptr = reinterpret_cast<const char*>(start_ptr) +
                   offset * nbr_stride + data_offset;
    if (nbr_data_eq(nbr_ptr, ptr, type)) {
      if (ts_offset > 0) {
        auto ts_ptr = reinterpret_cast<const char*>(start_ptr) +
                      offset * nbr_stride + ts_offset;
        if (!nbr_ts_eq(ts_ptr, ptr - data_offset + ts_offset)) {
          continue;
        }
        return offset;
      }
    }
  }
  return std::numeric_limits<int32_t>::max();
}

int32_t fuzzy_search_offset_from_nbr_list(const NbrList& nbr_list,
                                          vid_t expected_nbr,
                                          const void* expected_prop,
                                          const PropertyType& type) {
  std::vector<int32_t> candidates;
  const char* start_ptr = reinterpret_cast<const char*>(nbr_list.start_ptr);
  for (auto it = nbr_list.begin(); it != nbr_list.end(); ++it) {
    if (*it == expected_nbr) {
      candidates.emplace_back(
          (reinterpret_cast<const char*>(it.get_nbr_ptr()) - start_ptr) /
          nbr_list.cfg.stride);
    }
  }
  if (candidates.empty()) {
    return std::numeric_limits<int32_t>::max();
  } else if (candidates.size() == 1) {
    return candidates[0];
  } else {
    const auto& cfg = nbr_list.cfg;
    return find_exact_match_in_candidates(candidates, nbr_list.start_ptr,
                                          cfg.stride, cfg.data_offset,
                                          cfg.ts_offset, expected_prop, type);
  }
}

size_t get_offset_for_edge_record(const NbrList& nbr_list, vid_t expected_nbr,
                                  const void* expected_prop,
                                  const PropertyType& type) {
  auto casted_ptr =
      const_cast<char*>(reinterpret_cast<const char*>(expected_prop));
  auto data_offset = nbr_list.cfg.data_offset;
  auto nbr_stride = nbr_list.cfg.stride;
  auto ts_offset = nbr_list.cfg.ts_offset;
  assert(casted_ptr != nullptr);
  if (casted_ptr < nbr_list.start_ptr || casted_ptr >= nbr_list.end_ptr) {
    // If the pointer is invalid, do a fuzzy search.
    return fuzzy_search_offset_from_nbr_list(nbr_list, expected_nbr,
                                             expected_prop, type);
  } else {
    // If the pointer is valid.
    auto start_ptr =
        const_cast<char*>(reinterpret_cast<const char*>(nbr_list.start_ptr));
    auto offset = (casted_ptr - data_offset - start_ptr) / nbr_stride;
    {
      start_ptr += offset * nbr_stride;
      if (reinterpret_cast<const vid_t*>(start_ptr)[0] != expected_nbr) {
        THROW_INTERNAL_EXCEPTION(
            "EdgeRecord vertex id mismatch at calculated offset.");
      }
      if (!nbr_data_eq(static_cast<const char*>(start_ptr) + data_offset,
                       casted_ptr, type)) {
        THROW_INTERNAL_EXCEPTION(
            "EdgeRecord data mismatch at calculated offset.");
      }
      if (ts_offset > 0) {
        auto ts_ptr = start_ptr + ts_offset;
        if (!nbr_ts_eq(ts_ptr, casted_ptr - data_offset + ts_offset)) {
          THROW_INTERNAL_EXCEPTION(
              "EdgeRecord timestamp mismatch at calculated offset.");
        }
      }
      return offset;
    }
    return std::numeric_limits<size_t>::max();
  }
}

std::pair<int32_t, int32_t> record_to_csr_offset_pair(
    GenericView& oe, GenericView& ie, const gs::runtime::EdgeRecord& record,
    const std::vector<PropertyType>& props, timestamp_t ts) {
  NbrList cur_nbr_list, another_nbr_list;
  vid_t src, nbr;
  if (record.dir == runtime::Direction::kOut) {
    cur_nbr_list = oe.get_edges(record.src);
    another_nbr_list = ie.get_edges(record.dst);
    src = record.src;
    nbr = record.dst;
  } else {
    cur_nbr_list = ie.get_edges(record.dst);
    another_nbr_list = oe.get_edges(record.src);
    src = record.dst;
    nbr = record.src;
  }

  int32_t cur_offset, another_offset;
  PropertyType e_prop_type =
      props.size() == 1 ? props[0] : PropertyType::UInt64();
  cur_offset = gs::get_offset_for_edge_record(cur_nbr_list, nbr, record.prop,
                                              e_prop_type);
  assert(cur_offset != std::numeric_limits<int32_t>::max());

  another_offset = gs::fuzzy_search_offset_from_nbr_list(
      another_nbr_list, src, record.prop, e_prop_type);
  assert(another_offset != std::numeric_limits<int32_t>::max());
  if (record.dir == runtime::Direction::kOut) {
    return std::make_pair(cur_offset, another_offset);
  } else {
    return std::make_pair(another_offset, cur_offset);
  }
}

}  // namespace gs