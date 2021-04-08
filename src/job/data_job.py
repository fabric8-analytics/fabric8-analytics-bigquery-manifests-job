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
"""Main job that queries, collected and update manifest files from big query."""
import time
import logging
from src.config.settings import AWS_SETTINGS
from src.datastore.persistence_store import PersistenceStore
from src.bigquery.bigquery import Bigquery
from src.bigquery.base_collector import BaseCollector
from src.bigquery.maven_collector import MavenCollector
from src.bigquery.npm_collector import NpmCollector
from src.bigquery.pypi_collector import PypiCollector

logger = logging.getLogger(__name__)


ECOSYSTEM_MANIFEST_MAP = {
    'maven': 'pom.xml',
    'npm': 'package.json',
    'pypi': 'requirements.txt',
}


class DataJob():
    """Big query data fetching and processing class."""

    def __init__(self):
        """Initialize the BigQueryDataProcessing object."""
        self.big_query = Bigquery()

        self.collectors = {}
        for ecosystem in ECOSYSTEM_MANIFEST_MAP.keys():
            self.collectors[ecosystem] = self._get_collector(ecosystem)

        self.data_store = PersistenceStore()

    def run(self):
        """Process Bigquery response data."""
        start = time.monotonic()
        index = 0
        logger.info('Running Bigquery synchronously')
        self.big_query.run(self._get_big_query())
        for object in self.big_query.get_result():
            index += 1

            path = object.get('path', None)
            content = object.get('content', None)

            if not path or not content:
                logger.warning('Either path %s or content %s is null', path, content)
                continue

            ecosystem = None
            for _ecosystem, manifest in ECOSYSTEM_MANIFEST_MAP.items():
                if path.endswith(manifest):
                    ecosystem = _ecosystem

            if not ecosystem:
                logger.warning('Could not find ecosystem for given path %s', path)
                continue

            self.collectors[ecosystem].parse_and_collect(content, True)

        logger.info('Processed %d manifests in time: %f', index, time.monotonic() - start)
        self._update_s3()

    def _get_big_query(self) -> str:
        return """
            SELECT con.content AS content, L.path AS path
            FROM `bigquery-public-data.github_repos.contents` AS con
            INNER JOIN (
                SELECT files.id AS id, files.path as path
                FROM `bigquery-public-data.github_repos.languages` AS langs
                INNER JOIN `bigquery-public-data.github_repos.files` AS files
                ON files.repo_name = langs.repo_name
                    WHERE (
                        (
                            REGEXP_CONTAINS(TO_JSON_STRING(language), r'(?i)java') AND
                            files.path LIKE '%{m}'
                        ) OR
                        (
                            REGEXP_CONTAINS(TO_JSON_STRING(language), r'(?i)python') AND
                            files.path LIKE '%{p}'
                        ) OR
                        (
                            files.path LIKE '%{n}'
                        )
                    )
            ) AS L
            ON con.id = L.id;
        """.format(m=ECOSYSTEM_MANIFEST_MAP['maven'],
                   p=ECOSYSTEM_MANIFEST_MAP['pypi'],
                   n=ECOSYSTEM_MANIFEST_MAP['npm'])

    def _get_collector(self, ecosystem) -> BaseCollector:
        if ecosystem == 'maven':
            return MavenCollector()

        if ecosystem == 'npm':
            return NpmCollector()

        if ecosystem == 'pypi':
            return PypiCollector()

    def _update_s3(self):
        logger.info('Updating file content to S3')
        data = {}
        for ecosystem, object in self.collectors.items():
            data[ecosystem] = dict(object.counter.most_common())

        filename = '{}/big-query-data/{}'.format(
            AWS_SETTINGS.deployment_prefix, AWS_SETTINGS.s3_collated_filename)

        self.data_store.update(data=data,
                               bucket_name=AWS_SETTINGS.s3_bucket_name,
                               filename=filename)

        logger.info('Succefully saved BigQuery data to persistance store')
