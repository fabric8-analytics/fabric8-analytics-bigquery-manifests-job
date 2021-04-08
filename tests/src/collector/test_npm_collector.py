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
"""Test npm manifests and extract dependencies."""
from src.collector.npm_collector import NpmCollector

MANIFEST_START = """
{
  "name": "goof",
  "version": "1.0.1",
  "description": "A vulnerable todo demo application",
  "homepage": "https://snyk.io/",
  "repository": {
    "type": "git",
    "url": "https://github.com/Snyk/snyk-todo-list-demo-app/"
  },
  "scripts": {
    "start": "node app.js",
    "build": "browserify -r jquery > public/js/bundle.js",
    "cleanup": "mongo express-todo --eval 'db.todos.remove({});'",
    "test": "snyk test"
  },
  "engines": {
    "node": "6.14.1"
  },
  "devDependencies": {
    "browserify": "^13.1.1",
    "snyk": "^1.244.0"
  },
  "dependencies": {
"""

MANIFEST_END = """
  },
  "license": "Apache-2.0"
}
"""

DEP_1 = """
    "body-parser": "1.9.0",
"""

DEP_2 = """
    "ejs": "1.0.0",
"""

INVALID_DEP_1 = """
    "core-js": "2.6.10",
"""


class TestNpmCollector:
    """NPM collector test cases."""

    def test_single_dep(self):
        """Test single dep."""
        collector = NpmCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'body-parser': 1
        }

    def test_multiple_manifest_with_single_dep(self):
        """Test muitple manifest with same deps."""
        collector = NpmCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'body-parser': 3
        }

    def test_multiple_dep(self):
        """Test mutiple deps."""
        collector = NpmCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'body-parser, ejs': 1
        }

    def test_multiple_manifest_multiple_dep(self):
        """Test multiple manifest with multiple deps."""
        collector = NpmCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'body-parser, ejs': 2
        }

    def test_multiple_manifests(self):
        """Test multiple manifests."""
        collector = NpmCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_2 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'body-parser, ejs': 2,
            'body-parser': 1,
            'ejs': 1
        }

    def test_invalid_dep(self):
        """Test manifest with invalid deps."""
        collector = NpmCollector()
        collector.parse_and_collect(MANIFEST_START + INVALID_DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'core-js': 1
        }

    def test_corrupt_manifest(self):
        """Test corrupt / incomplete manifest."""
        collector = NpmCollector()
        collector.parse_and_collect(
            MANIFEST_START.replace('"repository": {', '') + DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'body-parser': 1
        }

    def test_corrupt_dep_section(self):
        """Test corrupt / missing deps section in manifest."""
        collector = NpmCollector()
        collector.parse_and_collect(
            MANIFEST_START.replace('"dependencies": {', '') + DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {}

    def test_valid_and_corrupt_dep_section(self):
        """Test a mix of valid and corrupt deps in manifests."""
        collector = NpmCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(
            MANIFEST_START.replace('"dependencies": {', '') + DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'body-parser': 1
        }
