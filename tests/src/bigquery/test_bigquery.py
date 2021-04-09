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
"""Test big query."""
import pytest
import unittest
from unittest import mock
from unittest.mock import patch
from src.bigquery.bigquery import Bigquery


class DummyBigquery():
    """Mocks Google's Big Query Runner."""

    def __init__(self):
        """Set default counter and dummy job id."""
        self.counter = 0
        self.job_id = 12345

    def done(self):
        """Run the bigquery synchronously."""
        self.counter += 1
        return self.counter > 4

    def result(self, job_id=None, job_query_obj=None):
        """Get last query results."""
        bigquery_data = []

        with open('tests/data/pom.xml', 'r') as f:
            bigquery_data.append({
                'path': 'tests/data/pom.xml',
                'content': f.read(),
            })

        with open('tests/data/package.json', 'r') as f:
            bigquery_data.append({
                'path': 'tests/data/package.json',
                'content': f.read(),
            })

        with open('tests/data/requirements.txt', 'r') as f:
            bigquery_data.append({
                'path': 'tests/data/requirements.txt',
                'content': f.read(),
            })

        bigquery_data.append({
            'path': 'tests/data/invalid.file',
            'content': '',
        })

        bigquery_data.append({
            'path': 'tests/data/invalid.file',
            'content': 'dummy_content',
        })

        return bigquery_data


class MockQueryJobConfig(mock.Mock):
    """Job configuration mock class."""

    pass


class MockClient(mock.Mock):
    """Client mock class."""

    def query(self, query, job_config=None, job_id=None, job_id_prefix=None,
              location=None, project=None, retry=3):
        """Query function for big queries."""
        return DummyBigquery()

    def get_job(self, job_id, project=None, location=None, retry=3):
        """Get last executed job data."""
        return {'state': 'COMPLETED'}


class TestBigQuery(unittest.TestCase):
    """Unite test cases for big query class."""

    @patch('src.bigquery.bigquery.QueryJobConfig', new_callable=MockQueryJobConfig)
    @patch('src.bigquery.bigquery.Client', new_callable=MockClient)
    def test_big_query(self, _c, _qjc):
        """Test big query."""
        bq = Bigquery()
        assert bq.client is not None

    @patch('src.bigquery.bigquery.Client', new_callable=MockClient)
    def test_big_query_with_job_configuration(self, _c):
        """Test big query without job configuration."""
        bq = Bigquery(MockQueryJobConfig())
        assert bq.client is not None

    @patch('src.bigquery.bigquery.Client', new_callable=MockClient)
    def test_run_without_query(self, _c):
        """Test run without query."""
        bq = Bigquery(MockQueryJobConfig())

        with pytest.raises(Exception) as e:
            bq.run(None)

        assert str(e.value) == 'Client or query missing'

    @patch('src.bigquery.bigquery.Client', new_callable=MockClient)
    def test_run(self, _c):
        """Test run."""
        bq = Bigquery(MockQueryJobConfig())
        assert bq.run('Query string goes here') == 12345

    @patch('src.bigquery.bigquery.Client', new_callable=MockClient)
    def test_get_result_without_query(self, _c):
        """Test get result without query."""
        bq = Bigquery(MockQueryJobConfig())
        with pytest.raises(Exception) as e:
            for object in bq.get_result():
                assert object.get('path', None) is not None

        assert str(e.value) == 'Job is not initialized'

    @patch('src.bigquery.bigquery.Client', new_callable=MockClient)
    def test_get_result(self, _c):
        """Test get result."""
        bq = Bigquery(MockQueryJobConfig())
        bq.run('Query string goes here')

        total_count = 0
        for object in bq.get_result():
            assert object.get('path', None) is not None
            assert object.get('content', None) is not None

            total_count += 1

        assert total_count == 5
