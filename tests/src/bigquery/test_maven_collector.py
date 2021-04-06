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
"""Test maven manifests and extract dependencies."""
from src.bigquery.maven_collector import MavenCollector

MANIFEST_START = """
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.redhat.bayessian.test</groupId>
  <artifactId>test-app-springboot</artifactId>
  <version>1.0</version>
  <dependencies>
"""

MANIFEST_END = """
  </dependencies>
</project>
"""

DEP_1 = """
    <dependency>
      <groupId>org.springframework</groupId>
      <artifactId>spring-websocket</artifactId>
      <version>4.3.7.RELEASE</version>
    </dependency>
"""

DEP_2 = """
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
      <version>1.5.2.RELEASE</version>
    </dependency>
"""

TEST_DEP_1 = """
    <dependency>
      <groupId>org.springframework</groupId>
      <artifactId>spring-messaging</artifactId>
      <version>4.3.7.RELEASE</version>
      <scope>test</scope>
    </dependency>
"""

TEST_DEP_2 = """
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter</artifactId>
      <version>1.5.2.RELEASE</version>
      <scope>test</scope>
    </dependency>
"""


class TestMavenCollector:
    """Maven collector test cases."""

    def test_single_dep(self):
        """Test single dep."""
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'org.springframework:spring-websocket': 1
        }

    def test_multiple_manifest_with_single_dep(self):
        """Test muitple manifest with same deps."""
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'org.springframework:spring-websocket': 3
        }

    def test_single_dep_test_dep(self):
        """Test single dep and a test dep."""
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + TEST_DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'org.springframework:spring-websocket': 1
        }

    def test_multiple_dep(self):
        """Test mutiple deps."""
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'org.springframework:spring-websocket, '
            'org.springframework.boot:spring-boot-starter-web': 1
        }

    def test_multiple_manifest_multiple_dep(self):
        """Test multiple manifest with multiple deps."""
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'org.springframework:spring-websocket, '
            'org.springframework.boot:spring-boot-starter-web': 2
        }

    def test_multiple_dep_test_dep(self):
        """Test multiple deps with a test dep."""
        collector = MavenCollector()
        collector.parse_and_collect(
            MANIFEST_START + DEP_1 + DEP_2 + TEST_DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'org.springframework:spring-websocket, '
            'org.springframework.boot:spring-boot-starter-web': 1
        }

    def test_multiple_dep_multiple_test_dep(self):
        """Test multiple deps and multiple test deps."""
        collector = MavenCollector()
        collector.parse_and_collect(
            MANIFEST_START + DEP_1 + TEST_DEP_1 + DEP_2 + TEST_DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'org.springframework:spring-websocket, '
            'org.springframework.boot:spring-boot-starter-web': 1
        }

    def test_multiple_manifests(self):
        """Test multiple manifests."""
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_2 + MANIFEST_END, True)
        collector.parse_and_collect(
            MANIFEST_START + DEP_1 + TEST_DEP_1 + DEP_2 + TEST_DEP_2 + MANIFEST_END, True)
        collector.parse_and_collect(
            MANIFEST_START + DEP_1 + TEST_DEP_1 + DEP_2 + TEST_DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'org.springframework:spring-websocket, '
            'org.springframework.boot:spring-boot-starter-web': 2,
            'org.springframework:spring-websocket': 1,
            'org.springframework.boot:spring-boot-starter-web': 1
        }

    def test_empty_manifest(self):
        """Test empty / invalid manifest."""
        collector = MavenCollector()
        collector.parse_and_collect(None, True)
        packages = dict(collector.counter.most_common())
        assert packages == {}

    def test_valid_and_empty_manifest(self):
        """Test a mix of empty and valid manifests."""
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(None, True)
        packages = dict(collector.counter.most_common())
        assert packages == {
            'org.springframework:spring-websocket': 1
        }
