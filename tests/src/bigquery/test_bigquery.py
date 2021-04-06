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
import unittest
from unittest import mock
from unittest.mock import patch
from src.bigquery.bigquery import Bigquery, BQDataProcessing


class MockAmazonS3(mock.Mock):
    """Mocks AWS S3 storage."""

    def object_exists(self, fname):
        """Check if file exists in S3."""
        return False

    def write_json_file(self, fname, content):
        """Dump manifest json data to S3."""
        # Do nothing in this function
        assert len(fname) != 0
        assert len(content) != 0

    def connect(self):
        """Connect to AWS S3 instance."""
        return True


class MockBigquery(mock.Mock):
    """Mocks Google's Big Query Runner."""

    def run_query_sync(self):
        """Run the bigquery synchronously."""
        return 1234

    def get_result(self, job_id=None, job_query_obj=None):
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
        return {}

    def get_job(self, job_id, project=None, location=None, retry=3):
        """Get last executed job data."""
        return {'state': 'COMPLETED'}


class TestBigQuery(unittest.TestCase):
    """Unite test cases for big query class."""

    @patch('src.bigquery.base.QueryJobConfig', new_callable=MockQueryJobConfig)
    @patch('src.bigquery.base.Client', new_callable=MockClient)
    def test_big_query(self, _c, _qjc):
        """Test big query."""
        bq = Bigquery()
        assert len(bq.query) == 1065
        assert bq.client is not None

    @patch('src.bigquery.bigquery.AmazonS3', new_callable=MockAmazonS3)
    @patch('src.bigquery.bigquery.Bigquery', new_callable=MockBigquery)
    def test_big_query_data_processing(self, _bb, _aws):
        """Test big query data processing."""
        bqProcess = BQDataProcessing()
        bqProcess.process(True)

        assert len(bqProcess.collectors.items()) == 3

        for ecosystem, object in bqProcess.collectors.items():
            if ecosystem == 'maven':
                maven_data = dict(object.counter.most_common())
                assert len(maven_data) == 1
                assert maven_data == {'org.apache.camel:camel-spring-boot-starter, '
                                      'org.springframework.boot:spring-boot-starter-web': 1}

            if ecosystem == 'pypi':
                pypi_data = dict(object.counter.most_common())
                assert len(pypi_data) == 1
                assert pypi_data == {'boto, chardet, cookies, cryptography, flask': 1}

            if ecosystem == 'npm':
                npm_data = dict(object.counter.most_common())
                assert len(npm_data) == 1
                assert npm_data == {'request, winston, xml2object': 1}
