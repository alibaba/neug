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

#include "neug/utils/service_utils.h"

#include <fcntl.h>
#if defined(__APPLE__)
#include <sys/sysctl.h>
#else
#include <sys/sysinfo.h>
#endif
#include <sys/types.h>
#include <unistd.h>
#include <cstdint>
#include <unordered_map>

namespace neug {

// get current executable's directory
std::string get_current_dir() {
  char buf[1024];
  int dirfd = open("/proc/self/", O_RDONLY | O_DIRECTORY);
  if (dirfd == -1) {
    // Handle error
  }

  ssize_t len = readlinkat(dirfd, "exe", buf, sizeof(buf) - 1);
  if (len == -1) {
    // Handle error
  }
  buf[len] = '\0';
  close(dirfd);
  std::string exe_path(buf);
  return exe_path.substr(0, exe_path.rfind('/'));
}

std::pair<uint64_t, uint64_t> get_total_physical_memory_usage() {
#ifdef __APPLE__
  // Get the total physical memory
  int64_t total_mem;
  size_t size = sizeof(total_mem);
  if (sysctlbyname("hw.memsize", &total_mem, &size, NULL, 0) != 0) {
    // Handle error
    LOG(ERROR) << "Failed to get total physical memory";
    return std::make_pair(0, 0);
  }
  // Get the free physical memory
  int64_t free_mem;
  size = sizeof(free_mem);
  if (sysctlbyname("vm.page_free_count", &free_mem, &size, NULL, 0) != 0) {
    // Handle error
    LOG(ERROR) << "Failed to get free physical memory";
    return std::make_pair(0, 0);
  }
  // Get the used physical memory
  int64_t used_mem = total_mem - free_mem;
  return std::make_pair(used_mem, total_mem);
#else
  struct sysinfo memInfo;

  sysinfo(&memInfo);
  uint64_t total_mem = memInfo.totalram;
  total_mem *= memInfo.mem_unit;

  uint64_t phy_mem_used = memInfo.totalram - memInfo.freeram;
  phy_mem_used *= memInfo.mem_unit;
  return std::make_pair(phy_mem_used, total_mem);
#endif
}

std::string memory_to_mb_str(uint64_t mem_bytes) {
  double mem_mb = mem_bytes / 1024.0 / 1024.0;
  return std::to_string(mem_mb) + "MB";
}

// Possible input: 1KB, 1B, 1K, 2Gi, 4GB
size_t human_readable_to_bytes(const std::string& human_readable_bytes) {
  // Check if the input is empty
  if (human_readable_bytes.empty()) {
    return 0;
  }

  // Define the multipliers for various size units
  static std::unordered_map<std::string, size_t> multipliers = {
      {"B", 1},
      {"KB", 1024ul},
      {"MB", 1024ul * 1024},
      {"GB", 1024ul * 1024 * 1024},
      {"KiB", 1024ul},
      {"MiB", 1024ul * 1024},
      {"GiB", 1024ul * 1024 * 1024}};

  size_t pos = 0;

  // Read and validate the numeric part
  while (pos < human_readable_bytes.size() &&
         (isdigit(human_readable_bytes[pos]) ||
          human_readable_bytes[pos] == '.' ||
          human_readable_bytes[pos] == ' ')) {
    pos++;
  }

  // If no numeric part, return 0
  if (pos == 0) {
    return 0;
  }

  // Extract the numeric portion as a string
  std::string number_str = human_readable_bytes.substr(0, pos);
  double number = std::stod(number_str);  // Convert to double for calculation

  // Read the unit part
  std::string unit;
  if (pos < human_readable_bytes.size()) {
    unit = human_readable_bytes.substr(pos);
  }

  // Normalize the unit to uppercase
  if (!unit.empty()) {
    std::transform(unit.begin(), unit.end(), unit.begin(), ::toupper);
  }

  // If the unit is not in the multipliers map, return 0
  if (multipliers.count(unit) == 0) {
    return 0;
  }

  // Calculate bytes
  return static_cast<size_t>(number * multipliers[unit]);
}

}  // namespace neug
