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
"""Abstracts settings based on env variables."""
from pydantic import BaseSettings, Field


class AWSSettings(BaseSettings):
    """AWS resource settings."""

    deployment_prefix = Field(env="DEPLOYMENT_PREFIX", default="dev")
    use_cloud_services = Field(env="USE_CLOUD_SERVICES", default=True)
    s3_region = Field(env="AWS_S3_REGION", default="us-east-1")
    s3_access_key_id = Field(env="AWS_S3_ACCESS_KEY_ID", default="")
    s3_secret_access_key = Field(env="AWS_S3_SECRET_ACCESS_KEY", default="")
    s3_bucket_name = Field(env="AWS_S3_BUCKET_NAME", default="developer-analytics-audit-report")
    s3_collated_filename = Field(env="AWS_S3_COLLATED_FILENAME", default="collated.json")


class GCPSettings(BaseSettings):
    """Google cloud platform access settings."""

    type = Field(env="GCP_TYPE", default="")
    project_id = Field(env="GCP_PROJECT_ID", default="")
    private_key_id = Field(env="GCP_PRIVATE_KEY_ID", default="")
    private_key = Field(env="GCP_PRIVATE_KEY", default="")
    client_email = Field(env="GCP_CLIENT_EMAIL", default="")
    client_id = Field(env="GCP_CLIENT_ID", default="")
    auth_uri = Field(env="GCP_AUTH_URI", default="")
    token_uri = Field(env="GCP_TOKEN_URI", default="")
    auth_provider_x509_cert_url = Field(env="GCP_AUTH_PROVIDER_X509_CERT_URL", default="")
    client_x509_cert_url = Field(env="GCP_CLIENT_X509_CERT_URL", default="")


AWS_SETTINGS = AWSSettings()
GCP_SETTINGS = GCPSettings()
