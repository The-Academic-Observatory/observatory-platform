# Copyright 2019 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose

import logging
from typing import List

import ray


def wait_for_tasks(task_ids: List[object], wait_time: float = 10.) -> List[any]:
    """ Wait until a list of ray tasks are complete.

    :param task_ids: a list of ray task id objects to wait for.
    :param wait_time: the time to wait before processing ready results.
    :return: the finished task results.
    """

    logging.info(f"Waiting for tasks")
    results = []
    total_tasks_finished = 0

    while True:
        # Wait for until all all tasks complete or until wait time expires
        num_tasks = len(task_ids)
        ready_ids, remaining_ids = ray.wait(task_ids, num_returns=num_tasks, timeout=wait_time)
        num_ready = len(ready_ids)

        # Get all tasks that area ready
        for ready_id in ready_ids:
            result = ray.get(ready_id)
            results.append(result)

        # All ready tasks processed, so only tasks left are remaining tasks
        task_ids = remaining_ids
        total_tasks_finished += num_ready
        num_remaining = len(task_ids)

        logging.info(f"Tasks finished: {total_tasks_finished}, tasks: remaining: {num_remaining}.")

        # If all tasks finished then break
        if num_remaining <= 0:
            break

    return results
