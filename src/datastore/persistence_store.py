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
"""Implementation persistence store using S3."""
import logging
from rudra.data_store.aws import AmazonS3
from src.config.settings import SETTINGS, AWS_SETTINGS

logger = logging.getLogger(__name__)


class PersistenceStore:
    """Persistence store to save Bigquery Data, it uses AWS S3 as of now as data store."""

    def __init__(self, s3_client=None):
        """Initialize DataProcessing object."""
        self.s3_client = s3_client
        if s3_client:
            self.s3_client = s3_client
        else:
            self.s3_client = AmazonS3(
                region_name=AWS_SETTINGS.s3_region,
                bucket_name=AWS_SETTINGS.s3_bucket_name,
                aws_access_key_id=AWS_SETTINGS.s3_access_key_id,
                aws_secret_access_key=AWS_SETTINGS.s3_secret_access_key,
                local_dev=not SETTINGS.use_cloud_services
            )

    def _check_and_connect(self):
        if not self.s3_client.is_connected():
            self.s3_client.connect()
            if not self.s3_client.is_connected():
                raise Exception('Unable to connect to s3.')

    def update(self, data, filename='collated.json'):
        """Upload s3 bucket."""
        # connect after creating or with existing s3 client
        self._check_and_connect()

        if self.s3_client.object_exists(filename):
            logger.info('%s exists, updating it.', filename)
            json_data = self.s3_client.read_json_file(filename)
            if not json_data:
                raise Exception('Unable to get the json data path: '
                                '{}/{}'.format(AWS_SETTINGS.s3_bucket_name, filename))

            for key in data.keys():
                data[key].update(json_data.get(key, {}))

        self.s3_client.write_json_file(filename, data)
        logger.info('Updated file Succefully!')

    def upload_file(self, src, target):
        """Upload given file to s3."""
        self._check_and_connect()
        self.s3_client.upload_file(src, target)

    def download_file(self, src, target):
        """Download file into S3 Bucket."""
        self._check_and_connect()
        try:
            return self.s3_client._s3.Bucket(AWS_SETTINGS.s3_bucket_name).download_file(
                src, target)
        except Exception as exc:
            logger.error('An Exception occurred while downloading a file\n'
                         '{}'.format(str(exc)))

    def list_bucket_objects(self, prefix=None):
        """List all the objects in bucket."""
        self._check_and_connect()
        return self.s3_client.list_bucket_objects(prefix)

    def s3_delete_folder(self, folder_path=None):
        """Delete all objects in the folder."""
        self._check_and_connect()
        try:
            return self.s3_client._s3.Bucket(AWS_SETTINGS.s3_bucket_name).objects.filter(
                Prefix=folder_path).delete()
        except Exception as exc:
            logger.error('An Exception occurred while deleting a folder {}\n'
                         '{}'.format(folder_path, str(exc)))
