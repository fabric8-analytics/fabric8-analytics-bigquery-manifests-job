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
from google.cloud import bigquery

_POLLING_DELAY = 1  # sec


class BigqueryBuilder:
    """BigqueryBuilder class Implementation."""

    def __init__(self, query_job_config=None):
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

        if isinstance(query_job_config, bigquery.job.QueryJobConfig):
            logger.info('Using given query job configuration')
            self.query_job_config = query_job_config
        else:
            logger.info('Creating new query job configuration')
            self.query_job_config = bigquery.job.QueryJobConfig()

        self.client = None

        if self.credential_path:
            self.client = bigquery.Client(
                default_query_job_config=self.query_job_config)
        else:
            raise ValueError("Please provide the the valid credential_path")

        tfile.close()

    def _run_query(self, job_config=None):
        if self.client and self.query:
            self.job_query_obj = self.client.query(
                self.query, job_config=job_config)
            while not self.job_query_obj.done():
                time.sleep(0.1)
            return self.job_query_obj.job_id
        else:
            raise ValueError

    def run_query_sync(self):
        """Run the bigquery synchronously."""
        return self._run_query()

    def run_query_async(self):
        """Run the bigquery asynchronously."""
        job_config = bigquery.QueryJobConfig()
        job_config.priority = bigquery.QueryPriority.BATCH
        return self._run_query(job_config=job_config)

    def get_status(self, job_id):
        """Get the job status of async query."""
        response = self.client.get_job(job_id)
        return response.state

    def get_result(self, job_id=None, job_query_obj=None):
        """Get the result of the job."""
        if job_id is None:
            job_query_obj = job_query_obj or self.job_query_obj
            for row in job_query_obj.result():
                yield ({k: v for k, v in row.items()})
        else:
            job_obj = self.client.get_job(job_id)
            while job_obj.state == 'PENDING':
                job_obj = self.client.get_job(job_id)
                logger.info("Job State for Job Id:{} is {}".format(
                    job_id, job_obj.state))
                time.sleep(_POLLING_DELAY)
            yield from self.get_result(job_query_obj=job_obj)

    def __iter__(self):
        """Iterate over the query result."""
        yield from self.get_result()


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
            raise ValueError("Unable to connect to s3.")

        json_data = dict()

        if self.s3_client.object_exists(filename):
            logger.info("{} exists, updating it.".format(filename))
            json_data = self.s3_client.read_json_file(filename)
            if not json_data:
                raise ValueError("Unable to get the json data path:{}/{}"
                                 .format(bucket_name, filename))

        json_data.update(data)
        self.s3_client.write_json_file(filename, json_data)
        logger.info("Updated file Succefully!")