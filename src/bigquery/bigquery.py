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
import logging
from src.config.settings import SETTINGS
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
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SETTINGS.bigquery_credentials_filepath
        logger.info('Creating new query job configuration')
        if query_job_config:
            self.query_job_config = query_job_config
        else:
            self.query_job_config = QueryJobConfig(use_legacy_sql=False,
                                                   use_query_cache=True)

        self.client = Client(default_query_job_config=self.query_job_config)

    def run(self, query):
        """Run the bigquery synchronously."""
        if self.client and query:
            self.job_query_obj = self.client.query(query, job_config=self.query_job_config)
            return self.job_query_obj.job_id
        else:
            raise Exception('Client or query missing')

    def get_result(self):
        """Get the result of the job."""
        assert self.job_query_obj is not None, 'Job is not initialized'
        yield from self.job_query_obj.result()
