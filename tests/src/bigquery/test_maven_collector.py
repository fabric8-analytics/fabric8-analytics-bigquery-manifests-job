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
import pytest
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

    def test_single_dep(self):
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 1
        assert list(packages.keys())[0] == 'org.springframework:spring-websocket'
        assert list(packages.values())[0] == 1

    def test_multiple_manifest_with_single_dep(self):
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 1
        assert list(packages.keys())[0] == 'org.springframework:spring-websocket'
        assert list(packages.values())[0] == 3

    def test_single_dep_test_dep(self):
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + TEST_DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 1
        assert list(packages.keys())[0] == 'org.springframework:spring-websocket'
        assert list(packages.values())[0] == 1

    def test_multiple_dep(self):
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 1
        assert list(packages.keys())[0] == 'org.springframework:spring-websocket, org.springframework.boot:spring-boot-starter-web'
        assert list(packages.values())[0] == 1

    def test_multiple_manifest_multiple_dep(self):
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 1
        assert list(packages.keys())[0] == 'org.springframework:spring-websocket, org.springframework.boot:spring-boot-starter-web'
        assert list(packages.values())[0] == 2

    def test_multiple_dep_test_dep(self):
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + DEP_2 + TEST_DEP_1 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 1
        assert list(packages.keys())[0] == 'org.springframework:spring-websocket, org.springframework.boot:spring-boot-starter-web'
        assert list(packages.values())[0] == 1

    def test_multiple_dep_multiple_test_dep(self):
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + TEST_DEP_1 + DEP_2 + TEST_DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 1
        assert list(packages.keys())[0] == 'org.springframework:spring-websocket, org.springframework.boot:spring-boot-starter-web'
        assert list(packages.values())[0] == 1

    def test_multiple_manifests(self):
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_2 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + TEST_DEP_1 + DEP_2 + TEST_DEP_2 + MANIFEST_END, True)
        collector.parse_and_collect(MANIFEST_START + DEP_1 + TEST_DEP_1 + DEP_2 + TEST_DEP_2 + MANIFEST_END, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 3
        assert list(packages.keys())[0] == 'org.springframework:spring-websocket, org.springframework.boot:spring-boot-starter-web'
        assert list(packages.keys())[1] == 'org.springframework:spring-websocket'
        assert list(packages.keys())[2] == 'org.springframework.boot:spring-boot-starter-web'

    def test_empty_manifest(self):
        collector = MavenCollector()
        collector.parse_and_collect(None, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 0

    def test_valid_and_empty_manifest(self):
        collector = MavenCollector()
        collector.parse_and_collect(MANIFEST_START + DEP_1 + MANIFEST_END, True)
        collector.parse_and_collect(None, True)
        packages = dict(collector.counter.most_common())
        assert len(packages) == 1
        assert list(packages.keys())[0] == 'org.springframework:spring-websocket'
        assert list(packages.values())[0] == 1
