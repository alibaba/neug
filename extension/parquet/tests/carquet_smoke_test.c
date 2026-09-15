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

#include <carquet/carquet.h>

#include <stdio.h>
#include <string.h>

int main(void) {
  int major = -1;
  int minor = -1;
  int patch = -1;
  carquet_version_components(&major, &minor, &patch);
  if (major != CARQUET_VERSION_MAJOR || minor != CARQUET_VERSION_MINOR ||
      patch != CARQUET_VERSION_PATCH ||
      strcmp(carquet_version(), CARQUET_VERSION_STRING) != 0) {
    fprintf(stderr, "Carquet header/library version mismatch\n");
    return 1;
  }

  const carquet_status_t status = carquet_init();
  if (status != CARQUET_OK) {
    fprintf(stderr, "Carquet initialization failed: %s\n",
            carquet_status_string(status));
    return 1;
  }
  carquet_cleanup();
  return 0;
}
