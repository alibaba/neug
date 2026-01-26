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

#include "neug/compiler/common/task_system/task_scheduler.h"
using namespace neug::common;

namespace neug {
namespace common {

#ifndef __SINGLE_THREADED__
TaskScheduler::TaskScheduler(uint64_t numWorkerThreads)
    : stopWorkerThreads{false}, nextScheduledTaskID{0} {
  for (auto n = 0u; n < numWorkerThreads; ++n) {
    workerThreads.emplace_back([&] { runWorkerThread(); });
  }
}

TaskScheduler::~TaskScheduler() {
  lock_t lck{taskSchedulerMtx};
  stopWorkerThreads = true;
  lck.unlock();
  cv.notify_all();
  for (auto& thread : workerThreads) {
    thread.join();
  }
}

void TaskScheduler::scheduleTaskAndWaitOrError(
    const std::shared_ptr<Task>& task, processor::ExecutionContext* context,
    bool launchNewWorkerThread) {}

void TaskScheduler::runWorkerThread() {
  std::unique_lock<std::mutex> lck{taskSchedulerMtx, std::defer_lock};
  std::exception_ptr exceptionPtr = nullptr;
  std::shared_ptr<ScheduledTask> scheduledTask = nullptr;
  while (true) {
    lck.lock();
    if (scheduledTask != nullptr) {
      if (exceptionPtr != nullptr) {
        scheduledTask->task->setException(exceptionPtr);
        exceptionPtr = nullptr;
      }
      scheduledTask->task->deRegisterThreadAndFinalizeTask();
      scheduledTask = nullptr;
    }
    cv.wait(lck, [&] {
      scheduledTask = getTaskAndRegister();
      return scheduledTask != nullptr || stopWorkerThreads;
    });
    lck.unlock();
    if (stopWorkerThreads) {
      return;
    }
    try {
      scheduledTask->task->run();
    } catch (std::exception& e) { exceptionPtr = std::current_exception(); }
  }
}
#else
TaskScheduler::TaskScheduler(uint64_t)
    : stopWorkerThreads{false}, nextScheduledTaskID{0} {}

TaskScheduler::~TaskScheduler() { stopWorkerThreads = true; }

void TaskScheduler::scheduleTaskAndWaitOrError(
    const std::shared_ptr<Task>& task, processor::ExecutionContext* context,
    bool) {
  for (auto& dependency : task->children) {
    scheduleTaskAndWaitOrError(dependency, context);
  }
  task->registerThread();
  runTask(task.get());
  if (task->hasException()) {
    removeErroringTask(task->ID);
    std::rethrow_exception(task->getExceptionPtr());
  }
}
#endif

std::shared_ptr<ScheduledTask> TaskScheduler::pushTaskIntoQueue(
    const std::shared_ptr<Task>& task) {
  lock_t lck{taskSchedulerMtx};
  auto scheduledTask =
      std::make_shared<ScheduledTask>(task, nextScheduledTaskID++);
  taskQueue.push_back(scheduledTask);
  return scheduledTask;
}

std::shared_ptr<ScheduledTask> TaskScheduler::getTaskAndRegister() {
  if (taskQueue.empty()) {
    return nullptr;
  }
  auto it = taskQueue.begin();
  while (it != taskQueue.end()) {
    auto task = (*it)->task;
    if (!task->registerThread()) {
      if (task->isCompletedSuccessfully()) {
        it = taskQueue.erase(it);
      } else {
        ++it;
      }
    } else {
      return *it;
    }
  }
  return nullptr;
}

void TaskScheduler::removeErroringTask(uint64_t scheduledTaskID) {
  lock_t lck{taskSchedulerMtx};
  for (auto it = taskQueue.begin(); it != taskQueue.end(); ++it) {
    if (scheduledTaskID == (*it)->ID) {
      taskQueue.erase(it);
      return;
    }
  }
}

void TaskScheduler::runTask(Task* task) {
  try {
    task->run();
    task->deRegisterThreadAndFinalizeTask();
  } catch (std::exception& e) {
    task->setException(std::current_exception());
    task->deRegisterThreadAndFinalizeTask();
  }
}
}  // namespace common
}  // namespace neug
