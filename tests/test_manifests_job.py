# -*- coding: utf-8 -*-
"""Tests the ManifestsJob Class."""

import pytest
from unittest import mock
import json
import shutil
import pathlib
import tempfile
import uuid
import sqlite3

from rudra.data_store.local_data_store import LocalDataStore


class MockDB:
    """Mocks a database of manifests.

    Useful to simulate getting user stacks.
    """

    def __init__(self, db=':memory:'):
        """Initialize the MockDB object."""
        self.session = sqlite3.connect(db)
        self.cols = ['id', 'name', 'content']
        self._create_data()

    def _create_data(self):
        """Create sample manifest stacks for tests."""
        query = """
        drop table if exists manifests;
        create table manifests({id} int, {nm} char, {cn} char);
        insert into manifests ({id}, {nm}, {cn}) values (1, 'requirements.txt', '{rq}');
        insert into manifests ({id}, {nm}, {cn}) values (2, 'requirements.txt', '{rq}');
        insert into manifests ({id}, {nm}, {cn}) values (3, 'package.json', '{pk}');
        insert into manifests ({id}, {nm}, {cn}) values (4, 'package.json', '{pk}');
        insert into manifests ({id}, {nm}, {cn}) values (5, 'pom.xml', '{pm}');
        insert into manifests ({id}, {nm}, {cn}) values (6, 'pom.xml', '{pm}');
        """.format(**dict(zip(('id', 'nm', 'cn'), self.cols)),
                   rq=self.manifest_content('pypi'),
                   pm=self.manifest_content('maven'),
                   pk=self.manifest_content('npm')
                   )

        self.session.executescript(query)

    def manifest_content(self, eco):
        """Return manifest stacks based on ecosystem."""
        eco_map_manifest = {'pypi': 'requirements.txt',
                            'maven': 'pom.xml',
                            'npm': 'package.json'}

        dir_path = pathlib.Path(__file__).resolve().parents[0]
        test_dir_path = dir_path.joinpath(
            "data", eco_map_manifest.get(eco)).absolute()
        with open(test_dir_path) as f:
            return f.read()

    def run(self, query):
        """Run specific query for manifest retrieval."""
        res = self.session.execute(query).fetchall()
        return [dict(zip(self.cols, r)) for r in res]


class QueryJob:
    """Mocks the job runner aspect of BigQuery."""

    def __init__(self, qry=None, job_id=uuid.uuid4()):
        """Initialize the QueryJob object."""
        self.output = MockDB().run(qry)
        self.job_id = job_id
        self.state = 'PENDING'

    def result(self):
        """Get the output of input query."""
        return self.output

    def done(self):
        """Check if job run was complete."""
        return True

    def __iter__(self):
        """Return job output as an iterable."""
        return iter(self.output)


class MockBigQuery(mock.Mock):
    """Mocks Google's Big Query Runner."""

    QueryJobConfig = type('QueryJobConfig', (), {'priority': None})

    def __init__(self, *args, **kwargs):
        """Initialize the MockBigQuery object."""
        super().__init__(*args, **kwargs)
        self.job = self.Job

    class Client:
        """Mocks the Google Big Query Client."""

        def __init__(self, *args, **kwargs):
            """Initialize the Client object."""
            self._state_flag = False

        def query(self, qry, *args, **kwargs):
            """Create and execute a manifest retrieval query."""
            self.qry = qry
            return QueryJob(self.qry)

        def get_job(self, job_id):
            """Get job status details."""
            query_job = QueryJob(qry=self.qry, job_id=job_id)
            query_job.state = ['PENDING', 'DONE'][self._state_flag]
            self._state_flag = True
            return query_job

    class Job:
        """Mocks Big Query Jobs."""

        QueryJobConfig = type('QueryJobConfig', (), {})

    class QueryPriority:
        """Priority aspect of queries."""

        BATCH = 'batch'


class MockS3(LocalDataStore):
    """Mocks AWS S3 storage."""

    def __init__(self, *args, **kwargs):
        """Initialize the MockS3 object."""
        super().__init__(*args, **kwargs)
        self.is_connected = lambda: True
        self.bucket_name = 'developer-analytics-audit-report'

    def object_exists(self, fname):
        """Check if file exists in S3."""
        return pathlib.Path(self.src_dir).joinpath(fname).exists()

    def write_json_file(self, fname, content):
        """Dump manifest json data to S3."""
        fpath = pathlib.Path(self.src_dir).joinpath(fname)
        if not fpath.parent.exists():
            fpath.parent.mkdir(parents=True, exist_ok=True)
        with open(str(fpath.absolute()), 'w') as json_fileobj:
            return json.dump(content, json_fileobj)

    def connect(self):
        """Connect to AWS S3 instance."""
        return self.is_connected()

    def __del__(self):
        """Delete S3 directory."""
        shutil.rmtree(self.src_dir)

'''
@pytest.fixture
@mock.patch('rudra.data_store.bigquery.base.bigquery',
            new_callable=MockBigQuery)
def _data_process_client_maven(_mock_bigquery_obj):
    """Mock manifest processing for maven."""
    _mvn_ins = manifests_job.mvn_bq.MavenBigQuery()
    s3_client = MockS3(tempfile.mkdtemp())
    _mvn_ins.query = "select id, name, content from manifests\
            where name like '%pom.xml'"
    _client = manifests_job.mvn_bq.MavenBQDataProcessing(
        _mvn_ins, s3_client=s3_client)
    return _client, s3_client


@pytest.fixture
@mock.patch('rudra.data_store.bigquery.base.bigquery',
            new_callable=MockBigQuery)
def _data_process_client_npm(_mock_bigquery_obj):
    """Mock manifest processing for npm."""
    _npm_ins = manifests_job.npm_bq.NpmBigQuery()
    s3_client = MockS3(tempfile.mkdtemp())
    _npm_ins.query = "select id, name, content from manifests\
            where name like '%package.json'"
    _client = manifests_job.npm_bq.NpmBQDataProcessing(
        _npm_ins, s3_client=s3_client)
    return _client, s3_client


@pytest.fixture
@mock.patch('rudra.data_store.bigquery.base.bigquery',
            new_callable=MockBigQuery)
def _data_process_client_pypi(_mock_bigquery_obj):
    """Mock manifest processing for pypi."""
    _pypi_ins = manifests_job.pypi_bq.PyPiBigQuery()
    s3_client = MockS3(tempfile.mkdtemp())
    _pypi_ins.query = "select id, name, content from manifests\
            where name like '%requirements.txt'"
    _client = manifests_job.pypi_bq.PyPiBigQueryDataProcessing(
        _pypi_ins, s3_client=s3_client)
    return _client, s3_client


class TestManifestsJob:
    """Tests the functionality of the ManifestsJob class."""

    def test_run_maven_job(self, _data_process_client_maven):
        """Tests manifest retrieval and storage for maven ecosystem."""
        dp_client, s3_client = _data_process_client_maven
        dp_client.process()
        data = s3_client.read_json_file(dp_client.filename)
        assert 'maven' in data
        assert len(data['maven']) > 0
        for k, v in data['maven'].items():
            assert 'org.apache.camel:camel-spring-boot-starter' in k
            assert 'org.springframework.boot:spring-boot-starter-web' in k
            assert v == 2

    def test_run_npm_job(self, _data_process_client_npm):
        """Tests manifest retrieval and storage for npm ecosystem."""
        dp_client, s3_client = _data_process_client_npm
        dp_client.process()
        data = s3_client.read_json_file(dp_client.filename)
        assert 'npm' in data
        assert len(data['npm']) > 0
        for k, v in data['npm'].items():
            assert 'request' in k
            assert 'winston' in k
            assert 'xml2object' in k
            assert v == 2

    def test_run_pypi_job(self, _data_process_client_pypi):
        """Tests manifest retrieval and storage for pypi ecosystem."""
        dp_client, s3_client = _data_process_client_pypi
        dp_client.process(validate=True)
        data = s3_client.read_json_file(dp_client.filename)
        assert 'pypi' in data
        assert len(data['pypi']) > 0
        for k, v in data['pypi'].items():
            assert 'boto' in k
            assert 'chardet' in k
            assert 'flask' in k
            assert 'unknown1' not in k
            assert 'unknown2' not in k
            assert v == 2
'''
