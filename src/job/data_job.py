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
import os
import time
import logging
from shutil import make_archive, unpack_archive, rmtree
from src.config.settings import SETTINGS, AWS_SETTINGS
from src.datastore.persistence_store import PersistenceStore
from src.bigquery.bigquery import Bigquery
from src.collector.base_collector import BaseCollector
from src.collector.maven_collector import MavenCollector
from src.collector.npm_collector import NpmCollector
from src.collector.pypi_collector import PypiCollector

logger = logging.getLogger(__name__)

S3_TEMP_FOLDER = 'big-query-data/manifest-data-zip'
CONTENT_BATCH_SIZE = 200 * 1024 * 1024   # 200 MB

ECOSYSTEM_MANIFEST_MAP = {
    'maven': 'pom.xml',
    'npm': 'package.json',
    'pypi': 'requirements.txt',
}


class DataJob():
    """Big query data fetching and processing class."""

    def __init__(self):
        """Initialize the BigQueryDataProcessing object."""
        self.ecosystemBatchData = {}
        self.ecosystemContentData = {}
        self.collectors = {}
        for ecosystem in ECOSYSTEM_MANIFEST_MAP.keys():
            self.collectors[ecosystem] = self._get_collector(ecosystem)
            self.ecosystemContentData[ecosystem] = {
                'size': 0,
                'count': 0
            }
            self.ecosystemBatchData[ecosystem] = {
                'batch_index': 1,
                'size': 0
            }

        self.data_store = PersistenceStore()

    def run(self):
        """Get big query data and update manifest data."""
        # Cleanup s3 before start, in case last run was not completed due to error.
        self._cleanup_s3()

        bq_start = time.monotonic()
        self._get_big_query_data()
        bq_end = time.monotonic()

        parse_start = time.monotonic()
        self._parse()
        parse_end = time.monotonic()

        self._cleanup_s3()

        logger.info('Ecosystem wise content data: %s', self.ecosystemContentData)
        logger.info('Ecosystem wise batch information: %s', self.ecosystemBatchData)
        logger.info('Big query data download took %0.2f seconds', bq_end - bq_start)
        logger.info('Data parsing took %0.2f seconds', parse_end - parse_start)

    def _parse(self):
        """Parse all ecosystem data."""
        s3_objects = self.data_store.list_bucket_objects(prefix=S3_TEMP_FOLDER)
        index = 0
        for s3_object in s3_objects:
            object_key = s3_object.key
            logger.info('Parsing S3 object %s', object_key)

            # Skip folder objects and other files that are not .zip
            if not object_key.endswith('.zip'):
                continue

            # Extract ecosystem
            ecosystem = None
            if object_key.startswith('{}/npm'.format(S3_TEMP_FOLDER)):
                ecosystem = 'npm'
            elif object_key.startswith('{}/maven'.format(S3_TEMP_FOLDER)):
                ecosystem = 'maven'
            elif object_key.startswith('{}/pypi'.format(S3_TEMP_FOLDER)):
                ecosystem = 'pypi'

            if not ecosystem:
                logger.warning('Could not find ecosystem for given object_key %s', object_key)
                continue

            index += 1

            # Create unzip directory
            unzip_dir = '{}/{}_unzip_dir/'.format(SETTINGS.local_working_directory, index)
            if not os.path.exists(unzip_dir):
                os.makedirs(unzip_dir)

            # Download zip content
            download_zip_path = '{}{}_downloaded.zip'.format(unzip_dir, index)
            self.data_store.download_file(object_key, download_zip_path)

            # Extract zip content
            unpack_archive(download_zip_path, unzip_dir, 'zip')

            # Loop through extract manifest files
            manifest_dir_path = '{}{}/'.format(unzip_dir, ecosystem)
            manifest_files = [manifest_dir_path + f for f in os.listdir(manifest_dir_path)]

            for manifest_file in manifest_files:
                if manifest_file.endswith(ECOSYSTEM_MANIFEST_MAP[ecosystem]):
                    with open(manifest_file, 'r') as fp:
                        content = fp.read()
                        logger.info('%d. Parsing file: %s', index, manifest_file)
                        self.collectors[ecosystem].parse_and_collect(content, True)
                else:
                    logger.warning('Skipping non-manifest file %s', manifest_file)

            rmtree(unzip_dir)
            logger.debug(f'Removed local unzip dir {unzip_dir}')

        self._update_s3()

    def _get_big_query_data(self):
        """Process Bigquery response data."""
        big_query = Bigquery()

        start = time.monotonic()
        index = 0

        # Create local structure to store content
        for _ecosystem, _ in ECOSYSTEM_MANIFEST_MAP.items():
            dir = '{}/{}/'.format(SETTINGS.local_working_directory, _ecosystem)
            if not os.path.exists(dir):
                os.makedirs(dir)
                print(f'Created dir {dir}')

        big_query.run(self._get_big_query())
        for object in big_query.get_result():
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

            if index % 1000 == 0:
                logger.info('[%d] Time lapsed: %f Processing path: %s',
                            index, time.monotonic() - start, path)

            contentSize = len(content)
            self.ecosystemContentData[ecosystem]['size'] += contentSize
            self.ecosystemContentData[ecosystem]['count'] += 1

            filename = '{}/{}/{}_{}'.format(SETTINGS.local_working_directory,
                                            ecosystem,
                                            self.ecosystemContentData[ecosystem]['count'],
                                            path.split('/')[-1])
            with open(filename, 'w') as fp:
                fp.write(content)
            self.ecosystemBatchData[ecosystem]['size'] += contentSize

            if self.ecosystemBatchData[ecosystem]['size'] > CONTENT_BATCH_SIZE:
                self._upload_batch_data(ecosystem)

        # Finally upload incomplete batches
        for ecosystem, _ in ECOSYSTEM_MANIFEST_MAP.items():
            if self.ecosystemBatchData[ecosystem]['size'] > 0:
                self._upload_batch_data(ecosystem)

        logger.info('Processed %d manifests, ecosystem data: %s',
                    index, self.ecosystemContentData)

    def _upload_batch_data(self, ecosystem):
        # Compress the current content, delete the content and reset batch size.
        compressFileName = '{}/{}/{}_{}'.format(SETTINGS.local_working_directory,
                                                ecosystem,
                                                self.ecosystemBatchData[ecosystem]['batch_index'],
                                                ecosystem)
        make_archive(compressFileName, 'zip', root_dir=SETTINGS.local_working_directory,
                     base_dir=ecosystem)

        compressFileName = compressFileName + '.zip'
        filename = '{}/{}/{}_{}.zip'.format(S3_TEMP_FOLDER, ecosystem,
                                            self.ecosystemBatchData[ecosystem]['batch_index'],
                                            ecosystem)
        self.data_store.upload_file(compressFileName, filename)

        dir = '{}/{}/'.format(SETTINGS.local_working_directory, ecosystem)
        rmtree(dir)
        os.makedirs(dir)

        self.ecosystemBatchData[ecosystem]['batch_index'] += 1
        self.ecosystemBatchData[ecosystem]['size'] = 0

        logger.debug('Processed batch %d, starting new batch %d',
                     self.ecosystemBatchData[ecosystem]["batch_index"] - 1,
                     self.ecosystemBatchData[ecosystem]["batch_index"])

    def _cleanup_s3(self):
        try:
            self.data_store.s3_delete_folder(S3_TEMP_FOLDER)
        except Exception as e:
            logger.warning('Exception :: Cleaning s3 %s throws %s',
                           S3_TEMP_FOLDER, str(e))

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
                            files.path LIKE '%/{m}'
                        ) OR
                        (
                            REGEXP_CONTAINS(TO_JSON_STRING(language), r'(?i)python') AND
                            files.path LIKE '%/{p}'
                        ) OR
                        (
                            files.path LIKE '%/{n}'
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

        filename = 'big-query-data/{}'.format(AWS_SETTINGS.s3_collated_filename)

        self.data_store.update(data=data, filename=filename)

        logger.info('Succefully saved BigQuery data to persistance store')
