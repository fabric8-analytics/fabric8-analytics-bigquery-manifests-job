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
"""Implementation Bigquery builder base."""
import os
import json
import time
import tempfile
from rudra import logger
from src.config.settings import GCP_SETTINGS
from google.cloud.bigquery.job import QueryJobConfig
from google.cloud.bigquery.client import Client


class BigqueryBuilder:
    """BigqueryBuilder class Implementation."""

    def __init__(self):
        """Initialize the BigqueryBuilder object."""
        logger.info('Storing BigQuery Auth Credentials')
        key_file_contents = {
            "type": GCP_SETTINGS.type,
            "project_id": GCP_SETTINGS.project_id,
            "private_key_id": GCP_SETTINGS.private_key_id,
            "private_key": GCP_SETTINGS.private_key,
            "client_email": GCP_SETTINGS.client_email,
            "client_id": GCP_SETTINGS.client_id,
            "auth_uri": GCP_SETTINGS.auth_uri,
            "token_uri": GCP_SETTINGS.token_uri,
            "auth_provider_x509_cert_url": GCP_SETTINGS.auth_provider_x509_cert_url,
            "client_x509_cert_url": GCP_SETTINGS.client_x509_cert_url,
        }
        tfile = tempfile.NamedTemporaryFile(mode='w+', delete=True)
        tfile.write(json.dumps(key_file_contents))
        tfile.flush()
        tfile.seek(0)
        self.credential_path = tfile.name

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credential_path
        logger.info('GOOGLE_APPLICATION_CREDENTIALS %s', self.credential_path)

        logger.info('Creating new query job configuration')
        self.query_job_config = QueryJobConfig()
        self.client = Client(default_query_job_config=self.query_job_config)
        self.job_query_obj = None

        tfile.close()

    def run_query_sync(self):
        """Run the bigquery synchronously."""
        if self.client and self.query:
            self.job_query_obj = self.client.query(
                self.query, job_config=self.query_job_config)
            while not self.job_query_obj.done():
                time.sleep(0.1)
            return self.job_query_obj.job_id
        else:
            raise ValueError('Client or query missing')

    def get_result(self):
        """Get the result of the job."""
        if self.job_query_obj:
            for row in self.job_query_obj.result():
                yield ({k: v for k, v in row.items()})
        else:
            raise ValueError('Job is not initialized')


class DataProcessing:
    """Process the Bigquery Data."""

    def __init__(self, s3_client=None):
        """Initialize DataProcessing object."""
        self.s3_client = s3_client

    def update_s3_bucket(self, data,
                         bucket_name,
                         filename='collated.json'):
        """Upload s3 bucket."""
        # connect after creating or with existing s3 client
        self.s3_client.connect()
        if not self.s3_client.is_connected():
            raise ValueError('Unable to connect to s3.')

        json_data = dict()

        if self.s3_client.object_exists(filename):
            logger.info('%s exists, updating it.', filename)
            json_data = self.s3_client.read_json_file(filename)
            if not json_data:
                raise ValueError(f'Unable to get the json data path:{bucket_name}/{filename}')

        json_data.update(data)
        self.s3_client.write_json_file(filename, json_data)
        logger.info('Updated file Succefully!')
