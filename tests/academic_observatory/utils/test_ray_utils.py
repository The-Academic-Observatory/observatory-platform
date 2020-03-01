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

import unittest

import ray

from academic_observatory.utils import wait_for_tasks


@ray.remote
def exponent(x):
    return x * x


class TestRayUtils(unittest.TestCase):

    def test_wait_for_tasks(self):
        # Init ray
        ray.init(ignore_reinit_error=True)

        # Create tasks and save task ids
        task_ids = []
        for x in range(5):
            task_id = exponent.remote(x)
            task_ids.append(task_id)

        # Wait for tasks to complete
        results = wait_for_tasks(task_ids)

        # Check results
        expected_results = [0, 1, 4, 9, 16]
        self.assertListEqual(results, expected_results)
