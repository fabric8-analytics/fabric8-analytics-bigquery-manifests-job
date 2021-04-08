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
"""Base collector class to parse and extract dependencies from manifests."""
from collections import Counter


class BaseCollector:
    """Base class to handle manifests and extract dependencies."""

    def __init__(self, name):
        """Collector init."""
        self.name = name
        self.counter = Counter()

    def _update_counter(self, packages):
        """Add packages to a collection."""
        if packages:
            pkg_string = ', '.join(packages)
            self.counter.update([pkg_string])
