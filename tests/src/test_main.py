# Copyright © 2020 Red Hat Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Author: Dharmendra G Patel <dhpatel@redhat.com>
#
"""Test main class."""
import time
import unittest
from unittest import mock
from unittest.mock import patch
from src.main import main


class MockBQDataProcessing(mock.Mock):
    """Mock class for data processing."""

    def process(self, validate):
        """Process bg data."""
        time.sleep(1)


class TestMain(unittest.TestCase):
    """Main function unit test cases."""

    @patch('src.main.BQDataProcessing', new_callable=MockBQDataProcessing)
    def test_main(self, _mdp):
        """Execute main function to trigger big query data update."""
        try:
            main()
        except Exception:
            assert False, 'Exception raised'
