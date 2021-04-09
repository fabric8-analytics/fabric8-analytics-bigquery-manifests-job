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
"""Test data job class."""
import unittest
from unittest import mock
from unittest.mock import patch
from src.job.data_job import DataJob


class MockPersistenceStore(mock.Mock):
    """Mocks persistence storage."""

    def update(self, data, bucket_name, filename='collated.json'):
        """Upload s3 bucket."""
        return True


class MockBigquery(mock.Mock):
    """Mocks Google's Big Query Runner."""

    def run(self, query):
        """Run the bigquery synchronously."""
        return 1234

    def get_result(self):
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


class TestDataJob(unittest.TestCase):
    """Unite test cases for big query class."""

    @patch('src.job.data_job.Bigquery', new_callable=MockBigquery)
    @patch('src.job.data_job.PersistenceStore', new_callable=MockPersistenceStore)
    def test_big_query(self, _bq, _ps):
        """Test data job init."""
        dj = DataJob()
        assert dj.big_query is not None
        assert dj.collectors is not None
        assert len(dj.collectors) == 3
        assert dj.data_store is not None

    @patch('src.job.data_job.Bigquery', new_callable=MockBigquery)
    @patch('src.job.data_job.PersistenceStore', new_callable=MockPersistenceStore)
    def test_big_query_data_processing(self, _bq, _ps):
        """Test data job run."""
        dj = DataJob()
        dj.run()

        for ecosystem, object in dj.collectors.items():
            if ecosystem == 'maven':
                maven_data = dict(object.counter.most_common())
                assert maven_data == {'org.apache.camel:camel-spring-boot-starter, '
                                      'org.springframework.boot:spring-boot-starter-web': 1}

            elif ecosystem == 'pypi':
                pypi_data = dict(object.counter.most_common())
                assert pypi_data == {'boto, chardet, cookies, cryptography, flask': 1}

            elif ecosystem == 'npm':
                npm_data = dict(object.counter.most_common())
                assert npm_data == {'request, winston, xml2object': 1}
