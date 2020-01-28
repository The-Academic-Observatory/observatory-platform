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
