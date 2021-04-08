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
"""Handle Pypi manifests and extract dependencies."""
import logging
from rudra.utils.pypi_parser import pip_req
from rudra.utils.validation import BQValidation
from src.collector.base_collector import BaseCollector

logger = logging.getLogger(__name__)


class PypiCollector(BaseCollector):
    """Handle Pypi manifests and extract dependencies."""

    def __init__(self):
        """Initialize BG validation."""
        super().__init__('pypi')
        self.bq_validation = BQValidation()

    def parse_and_collect(self, content, validate):
        """Parse dependencies and add it to collection."""
        packages = None
        try:
            packages = sorted({p for p in pip_req.parse_requirements(content)})
            if validate:
                packages = sorted(self.bq_validation.validate_pypi(packages))
        except Exception as e:
            logger.warning('Error in content, it raises %s', e)

        self._update_counter(packages)
