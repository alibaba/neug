/** Copyright 2020 Alibaba Group Holding Limited.
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

#include <carquet/carquet.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

int main(void) {
  carquet_reader_options_t reader_options;
  carquet_reader_options_init(&reader_options);
  if (reader_options.buffer_size != 64U * 1024U ||
      reader_options.num_threads != 0) {
    fprintf(stderr, "unexpected Carquet reader defaults\n");
    return 1;
  }

  carquet_batch_reader_config_t batch_config;
  carquet_batch_reader_config_init(&batch_config);
  if (batch_config.batch_size != 64 * 1024 || batch_config.num_threads != 0 ||
      batch_config.column_indices != NULL || batch_config.num_columns != 0 ||
      batch_config.row_group_filter != NULL ||
      batch_config.thread_pool != NULL) {
    fprintf(stderr, "unexpected Carquet batch reader defaults\n");
    return 1;
  }

  /* Compile-time probes for the option hooks NeuG must preserve. */
  reader_options.buffer_size = 4096;
  reader_options.num_threads = 1;
  batch_config.batch_size = 128;
  batch_config.num_threads = 1;
  batch_config.column_indices = NULL;
  batch_config.num_columns = 0;
  batch_config.row_group_filter = NULL;
  batch_config.thread_pool = NULL;

  carquet_status_t (*prebuffer)(carquet_reader_t*, int32_t, const int32_t*,
                                int32_t, carquet_error_t*) =
      carquet_reader_prebuffer;
  carquet_batch_reader_t* (*open_batch)(
      carquet_reader_t*, const carquet_batch_reader_config_t*,
      carquet_error_t*) = carquet_batch_reader_create;
  (void) prebuffer;
  (void) open_batch;

  return 0;
}
