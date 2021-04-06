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
"""Test base class."""
import pytest
import unittest
from unittest import mock
from unittest.mock import patch
from src.bigquery.base import BigqueryBuilder, DataProcessing


class MockQueryJobConfig(mock.Mock):
    """Mock Job Config class."""

    pass


class MockClient(mock.Mock):
    """Mock Client class."""

    class JobQuery():
        """Mock job query class."""

        def __init__(self, job_id):
            """Set job id and run count."""
            self.job_id = job_id
            self.run_count = 0

        def done(self):
            """Return done status of a job query."""
            self.run_count += 1
            return self.run_count > 4

        def result(self):
            """Return result of job query."""
            return [
                {"path": "path/to/file/pom.txt", "content": "Pom content"},
                {"path": "path/to/file/package.json", "content": "Package content"}
            ]

    def query(self, query, job_config=None, job_id=None, job_id_prefix=None,
              location=None, project=None, retry=3):
        """Execute bg query and return job object."""
        return self.JobQuery(12345)

    def get_job(self, job_id, project=None, location=None, retry=3):
        """Mock to get job object."""
        return {'state': 'COMPLETED'}


class S3NotConnected:
    """S3 class that is not connected to S3."""

    def connect(self):
        """Mock function to connect to S3."""
        return 1

    def is_connected(self):
        """Mock function to get connection state."""
        return False

    def object_exists(self, fname):
        """Mock function to check if object exists."""
        return self.object_exists


class S3NewUpload(S3NotConnected):
    """S3 class with no current file."""

    def is_connected(self):
        """Mock function to get connection state."""
        return True

    def object_exists(self, fname):
        """Mock function to check if object exists."""
        return False

    def write_json_file(self, fname, content):
        """Mock to write json file."""
        pass


class S3ExistingEmptyUpload(S3NewUpload):
    """S3 class with existing file containing no content."""

    def object_exists(self, fname):
        """Mock function to check if object exists."""
        return True

    def read_json_file(self, fname):
        """Read json file mock function."""
        return {}


class S3ExistingUpload(S3ExistingEmptyUpload):
    """S3 class with existing file containing some content."""

    def read_json_file(self, fname):
        """Read json file mock function."""
        return {'test': 'cool'}


class TestBigqueryBuilder(unittest.TestCase):
    """Unit test cases for BigqueryBuilder class."""

    @patch('src.bigquery.base.QueryJobConfig', new_callable=MockQueryJobConfig)
    @patch('src.bigquery.base.Client', new_callable=MockClient)
    def test_init(self, _c, _qjc):
        """Test init use case."""
        bqBuilder = BigqueryBuilder()

        assert bqBuilder.query_job_config is not None
        assert bqBuilder.client is not None

    @patch('src.bigquery.base.QueryJobConfig', new_callable=MockQueryJobConfig)
    @patch('src.bigquery.base.Client', new_callable=MockClient)
    def test_run_query(self, _c, _qjc):
        """Run query with proper query."""
        bqBuilder = BigqueryBuilder()
        bqBuilder.query = 'DUMMY QUERY'
        bqBuilder.run_query_sync()

        assert bqBuilder.job_query_obj.job_id == 12345

    @patch('src.bigquery.base.QueryJobConfig', new_callable=MockQueryJobConfig)
    @patch('src.bigquery.base.Client', new_callable=MockClient)
    def test_run_query_none(self, _c, _qjc):
        """Run sync query with None query string."""
        bqBuilder = BigqueryBuilder()
        bqBuilder.query = None

        with pytest.raises(Exception) as e:
            bqBuilder.run_query_sync()

        assert str(e.value) == 'Client or query missing'

    @patch('src.bigquery.base.QueryJobConfig', new_callable=MockQueryJobConfig)
    @patch('src.bigquery.base.Client', new_callable=MockClient)
    def test_get_result(self, _c, _qjc):
        """Happy case of getting bg result."""
        bqBuilder = BigqueryBuilder()
        bqBuilder.query = 'DUMMY QUERY'
        bqBuilder.run_query_sync()

        total_count = 0
        for object in bqBuilder.get_result():
            assert object.get('path', None) is not None
            assert object.get('content', None) is not None

            total_count += 1

        assert total_count == 2

    @patch('src.bigquery.base.QueryJobConfig', new_callable=MockQueryJobConfig)
    @patch('src.bigquery.base.Client', new_callable=MockClient)
    def test_get_result_without_query(self, _c, _qjc):
        """Get BG result without query being fired."""
        bqBuilder = BigqueryBuilder()

        with pytest.raises(Exception) as e:
            for object in bqBuilder.get_result():
                assert object.get('path', None) is not None

        assert str(e.value) == 'Job is not initialized'


class TestDataProcessing(unittest.TestCase):
    """Unit test cases for Data processing class."""

    def test_init(self):
        """Test init use case."""
        dp = DataProcessing(s3_client=None)
        assert dp.s3_client is None

        dp = DataProcessing(s3_client=12345)
        assert dp.s3_client == 12345

    def test_upload_no_connection(self):
        """Test no connection use case."""
        dp = DataProcessing(s3_client=S3NotConnected())

        with pytest.raises(Exception) as e:
            dp.update_s3_bucket({}, 'bucket_name', 'filename.json')

        assert str(e.value) == 'Unable to connect to s3.'

    def test_upload_new_file(self):
        """Update data to a new file."""
        dp = DataProcessing(s3_client=S3NewUpload())

        try:
            dp.update_s3_bucket({}, 'bucket_name', 'filename.json')
        except Exception:
            assert False, 'Exception raised'

    def test_upload_existing_empty_file(self):
        """Upload new data with empty data in existing file."""
        dp = DataProcessing(s3_client=S3ExistingEmptyUpload())

        with pytest.raises(Exception) as e:
            dp.update_s3_bucket({}, 'bucket_name', 'filename.json')

        assert str(e.value) == 'Unable to get the json data path:bucket_name/filename.json'

    def test_upload_existing_file(self):
        """Upload data in S3 with existing data."""
        dp = DataProcessing(s3_client=S3ExistingUpload())

        try:
            dp.update_s3_bucket({'test': 'super cool'}, 'bucket_name', 'filename.json')
        except Exception:
            assert False, 'Exception raised'
