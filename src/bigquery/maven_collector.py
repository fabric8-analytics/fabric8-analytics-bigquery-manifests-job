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
"""Handle maven manifests and extract dependencies."""
from rudra import logger
from rudra.utils.mercator import SimpleMercator
from src.bigquery.base_collector import BaseCollector


class MavenCollector(BaseCollector):
    """Handle maven manifests and extract dependencies."""

    def __init__(self):
        """Contructor for maven collectors."""
        super().__init__('maven')

    def parse_and_collect(self, content, _):
        """Parse dependencies and add it to collection."""
        result = list()
        allowed_scopes = ['compile', 'run', 'provided']

        try:
            mercator_ins = SimpleMercator(content)
            for dep in mercator_ins.get_dependencies():
                scope, aid, gid = str(dep.scope), str(
                    dep.artifact_id), str(dep.group_id)

                if scope in allowed_scopes and aid and gid:
                    result.append('{g}:{a}'.format(
                        g=gid.strip(), a=aid.strip()))
        except Exception as _exc:
            logger.warn("IGNORE THIS ERROR {}".format(_exc))
            logger.warn("CONTENT: {}".format(content))

        self._update_counter(result)
