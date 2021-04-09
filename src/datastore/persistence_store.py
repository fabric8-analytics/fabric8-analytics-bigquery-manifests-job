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

    def update(self, data, bucket_name, filename='collated.json'):
        """Upload s3 bucket."""
        # connect after creating or with existing s3 client
        self.s3_client.connect()
        if not self.s3_client.is_connected():
            raise Exception('Unable to connect to s3.')

        json_data = dict()

        if self.s3_client.object_exists(filename):
            logger.info('%s exists, updating it.', filename)
            json_data = self.s3_client.read_json_file(filename)
            if not json_data:
                raise Exception(f'Unable to get the json data path:{bucket_name}/{filename}')

        json_data.update(data)
        self.s3_client.write_json_file(filename, json_data)
        logger.info('Updated file Succefully!')
