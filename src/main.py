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
"""The main script for the Big Query manifests retrieval."""

import time
from rudra import logger
from src.job.data_job import DataJob


def main():
    """Retrieve, process and store the manifest files from Big Query."""
    logger.info('Initializing Big query object')
    dataJob = DataJob()

    logger.info('Starting big query job')
    start = time.monotonic()
    dataJob.run()
    logger.info('Finished big query job, time taken: %f', time.monotonic() - start)


if __name__ == '__main__':
    main()
