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
"""Bigquery implementation to read big data for manifest files."""
import os
import json
import time
import tempfile
import logging
from src.config.settings import GCP_SETTINGS
from google.cloud.bigquery.job import QueryJobConfig
from google.cloud.bigquery.client import Client

logger = logging.getLogger(__name__)


class Bigquery():
    """Base big query class."""

    def __init__(self, query_job_config=None):
        """Initialize big query object."""
        self.client = None
        self.job_query_obj = None

        self._configure_gcp_client(query_job_config)

    def _configure_gcp_client(self, query_job_config):
        """Configure GCP client."""
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
        if query_job_config:
            self.query_job_config = query_job_config
        else:
            self.query_job_config = QueryJobConfig()
            self.query_job_config.use_legacy_sql = False
            self.query_job_config.use_query_cache = True

        self.client = Client(default_query_job_config=self.query_job_config)

        tfile.close()

    def run(self, query):
        """Run the bigquery synchronously."""
        if self.client and query:
            self.job_query_obj = self.client.query(
                query, job_config=self.query_job_config)
            while not self.job_query_obj.done():
                time.sleep(0.1)
            return self.job_query_obj.job_id
        else:
            raise Exception('Client or query missing')

    def get_result(self):
        """Get the result of the job."""
        if self.job_query_obj:
            for row in self.job_query_obj.result():
                yield ({k: v for k, v in row.items()})
        else:
            raise Exception('Job is not initialized')
