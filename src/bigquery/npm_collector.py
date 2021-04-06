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
"""Handle NPM manifests and extract dependencies."""
import re
import demjson
from rudra import logger
from src.bigquery.base_collector import BaseCollector


class NpmCollector(BaseCollector):
    """Handle NPM manifests and extract dependencies."""

    def __init__(self):
        """Npm collector init."""
        super().__init__('npm')

    def parse_and_collect(self, content, _):
        """Parse dependencies and add it to collection."""
        content = content.decode() if not isinstance(content, str) else content
        dependencies = {}
        try:
            decoded_json = demjson.decode(content)
        except Exception as e:
            logger.warning('Error in content, it raises %s', e)
            decoded_json = self._handle_corrupt_packagejson(content)
        if decoded_json and isinstance(decoded_json, dict):
            dependencies = decoded_json.get('dependencies', {})

        self._update_counter(list(dependencies.keys() if isinstance(dependencies, dict) else []))

    def _handle_corrupt_packagejson(self, content):
        """Find dependencies from corrupted/invalid package.json."""
        dependencies_pattern = re.compile(
            r'dependencies[\'"](?:|.|\s+):(?:|.|\s+)\{(.*?)\}', flags=re.DOTALL)
        dependencies = list()
        try:
            match = dependencies_pattern.search(content)
            for line in match[1].splitlines():
                for dep in line.split(','):
                    dependency_pattern = (r"(?:\"|\')(?P<pkg>[^\"]*)(?:\"|\')(?=:)"
                                          r"(?:\:\s*)(?:\"|\')?(?P<ver>.*)(?:\"|\')")
                    matches = re.search(dependency_pattern,
                                        dep.strip(), re.MULTILINE | re.DOTALL)
                    if matches:
                        dependencies.append('"{}": "{}"'.format(
                            matches['pkg'], matches['ver']))

            return demjson.decode('{"dependencies": {%s}}' % ', '.join(dependencies))
        except Exception as e:
            logger.warning('Error in content, it raises %s', e)
            return {}
