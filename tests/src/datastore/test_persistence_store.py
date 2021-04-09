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
"""Test persistence store class."""
import pytest
import unittest
from unittest import mock
from unittest.mock import patch
from src.datastore.persistence_store import PersistenceStore


class S3NotConnected(mock.Mock):
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


class TestPersistenceStore(unittest.TestCase):
    """Unit test cases for Data processing class."""

    @patch('src.datastore.persistence_store.AmazonS3', new_callable=S3NotConnected)
    def test_init(self, _s3):
        """Test init without client."""
        ps = PersistenceStore(s3_client=None)
        assert ps.s3_client is not None

    def test_init_with_client(self):
        """Test init without client."""
        ps = PersistenceStore(s3_client=S3NotConnected())
        assert ps.s3_client is not None

    def test_upload_no_connection(self):
        """Test no connection use case."""
        ps = PersistenceStore(s3_client=S3NotConnected())

        with pytest.raises(Exception) as e:
            ps.update({}, 'bucket_name', 'filename.json')

        assert str(e.value) == 'Unable to connect to s3.'

    def test_upload_new_file(self):
        """Update data to a new file."""
        ps = PersistenceStore(s3_client=S3NewUpload())

        try:
            ps.update({}, 'bucket_name', 'filename.json')
        except Exception:
            assert False, 'Exception raised'

    def test_upload_existing_empty_file(self):
        """Upload new data with empty data in existing file."""
        ps = PersistenceStore(s3_client=S3ExistingEmptyUpload())

        with pytest.raises(Exception) as e:
            ps.update({}, 'bucket_name', 'filename.json')

        assert str(e.value) == 'Unable to get the json data path:bucket_name/filename.json'

    def test_upload_existing_file(self):
        """Upload data in S3 with existing data."""
        ps = PersistenceStore(s3_client=S3ExistingUpload())

        try:
            ps.update({'test': 'super cool'}, 'bucket_name', 'filename.json')
        except Exception:
            assert False, 'Exception raised'
