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

// Windows-only shim implementations for POSIX mmap/munmap/msync/truncate.
// Moved out of file_utils.h to avoid ODR conflicts with third-party headers
// that may also define these symbols.

#ifdef _WIN32

#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#include <windows.h>
#include <cstdint>

#include "neug/utils/io/file/file_utils.h"

int truncate(const char* path, int64_t length) {
  int fd = _open(path, _O_WRONLY, 0);
  if (fd < 0) {
    return -1;
  }
  errno_t ret = _chsize_s(fd, length);
  _close(fd);
  if (ret != 0) {
    errno = static_cast<int>(ret);
    return -1;
  }
  return 0;
}

void* mmap(void* addr, size_t len, int prot, int flags, int fd, off_t offset) {
  (void) addr;
  DWORD pageProtect = PAGE_NOACCESS;
  if (prot & PROT_WRITE) {
    pageProtect = (flags & MAP_PRIVATE) ? PAGE_WRITECOPY : PAGE_READWRITE;
  } else if (prot & PROT_READ) {
    pageProtect = PAGE_READONLY;
  }
  if (flags & MAP_ANONYMOUS) {
    void* ptr =
        VirtualAlloc(nullptr, len, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    return ptr ? ptr : MAP_FAILED;
  }
  HANDLE hFile =
      (fd == -1) ? INVALID_HANDLE_VALUE : (HANDLE) _get_osfhandle(fd);
  DWORD sizeHigh =
      static_cast<DWORD>((static_cast<uint64_t>(len) >> 32) & 0xFFFFFFFFULL);
  DWORD sizeLow = static_cast<DWORD>(len & 0xFFFFFFFFULL);
  HANDLE hMap = CreateFileMapping(hFile, nullptr, pageProtect, sizeHigh,
                                  sizeLow, nullptr);
  if (!hMap) {
    return MAP_FAILED;
  }
  DWORD access = FILE_MAP_READ;
  if (prot & PROT_WRITE) {
    access = (flags & MAP_PRIVATE) ? FILE_MAP_COPY : FILE_MAP_WRITE;
  }
  DWORD offsetHigh =
      static_cast<DWORD>((static_cast<uint64_t>(offset) >> 32) & 0xFFFFFFFFULL);
  DWORD offsetLow = static_cast<DWORD>(offset & 0xFFFFFFFFULL);
  void* ptr = MapViewOfFile(hMap, access, offsetHigh, offsetLow, len);
  CloseHandle(hMap);
  return ptr ? ptr : MAP_FAILED;
}

int munmap(void* addr, size_t len) {
  (void) len;
  // Try VirtualFree first (for anonymous mmap via VirtualAlloc);
  // fall back to UnmapViewOfFile (for file-backed mmap).
  if (VirtualFree(addr, 0, MEM_RELEASE)) {
    return 0;
  }
  return UnmapViewOfFile(addr) ? 0 : -1;
}

int msync(void* addr, size_t len, int flags) {
  (void) flags;
  return FlushViewOfFile(addr, len) ? 0 : -1;
}

#endif  // _WIN32
