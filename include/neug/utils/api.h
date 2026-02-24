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

// Helpers
#if defined _WIN32 || defined __CYGWIN__
#define NEUG_HELPER_DLL_IMPORT __declspec(dllimport)
#define NEUG_HELPER_DLL_EXPORT __declspec(dllexport)
#define NEUG_HELPER_DLL_LOCAL
#define NEUG_HELPER_DEPRECATED __declspec(deprecated)
#else
#define NEUG_HELPER_DLL_IMPORT __attribute__((visibility("default")))
#define NEUG_HELPER_DLL_EXPORT __attribute__((visibility("default")))
#define NEUG_HELPER_DLL_LOCAL __attribute__((visibility("hidden")))
#define NEUG_HELPER_DEPRECATED __attribute__((__deprecated__))
#endif

#ifdef NEUG_STATIC_DEFINE
#define NEUG_API
#else
#ifndef NEUG_API
#ifdef NEUG_EXPORTS
/* We are building this library */
#define NEUG_API NEUG_HELPER_DLL_EXPORT
#else
/* We are using this library */
#define NEUG_API NEUG_HELPER_DLL_IMPORT
#endif
#endif
#endif

#ifndef NEUG_DEPRECATED
#define NEUG_DEPRECATED NEUG_HELPER_DEPRECATED
#endif

#ifndef NEUG_DEPRECATED_EXPORT
#define NEUG_DEPRECATED_EXPORT NEUG_API NEUG_DEPRECATED
#endif
