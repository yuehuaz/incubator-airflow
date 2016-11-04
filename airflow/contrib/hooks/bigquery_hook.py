# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module contains a BigQuery Hook, as well as a very basic PEP 249
implementation for BigQuery.
"""

import logging
import time

from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

API_BASE_URL = 'https://cloud.google.com/bigquery/docs/reference/v2/jobs'


class BigQueryHook(object):
    def __init__(self,
                 project_id,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):

        self.region = 'global'
        self.service = self._get_conn()
        self.jobs = self.service.jobs()
        self.project_id = project_id

        # TODO: should probably use the same base class
        # super(DataProcClusterHook, self).__init__(gcp_conn_id, delegate_to)

    def _get_conn(self):
        """
        Returns a BigQuery service object.
        """
        credentials = GoogleCredentials.get_application_default()

        return build('bigquery', 'v2', credentials=credentials)

    def _split_project_dataset_table_input(self, var_name, project_dataset_table):
        """
        :param var_name: the name of the variable input, for logging and erroring purposes.
        :type var_name: str
        :param project_dataset_table: input string in (<project>.)<dataset>.<project> format.
            if project is not included in the string, self.project_id will be returned in the tuple.
        :type project_dataset_table: str
        :return: (project, dataset, table) tuple
        """
        table_split = project_dataset_table.split('.')
        assert len(table_split) == 2 or len(table_split) == 3, (
            'Expected {var} in the format of (<project.)<dataset>.<table>, '
            'got {input}').format(var=var_name, input=project_dataset_table)

        if len(table_split) == 2:
            logging.info('project not included in {var}: {input}; using project "{project}"'.format
                         (var=var_name, input=project_dataset_table, project=self.project_id))
            dataset, table = table_split
            return self.project_id, dataset, table
        else:
            project, dataset, table = table_split
            return project, dataset, table

    def run_query(
            self, bql, destination_dataset_table=False,
            write_disposition='WRITE_EMPTY',
            allow_large_results=False,
            udf_config=False,
            use_legacy_sql=False,
            maximum_billing_tier=1):
        """
        Executes a BigQuery SQL query. Optionally persists results in a BigQuery
        table. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param bql: The BigQuery SQL to execute.
        :type bql: string
        :param destination_dataset_table: The dotted <dataset>.<table>
            BigQuery table to save the query results.
        :param write_disposition: What to do if the table already exists in
            BigQuery.
        :param allow_large_results: Whether to allow large results.
        :type allow_large_results: boolean
        :param udf_config: The User Defined Function configuration for the query.
            See https://cloud.google.com/bigquery/user-defined-functions for details.
        :type udf_config: list
        :param use_legacy_sql: Whether to use legacy SQL or standard SQL
        :type use_legacy_sql: boolean
        :param maximum_billing_tier: Limits the billing tier for this query
        :type maximum_billing_tier: integer
        """
        configuration = {
            'query': {
                'query': bql,
                'useLegacySql': use_legacy_sql,
                'maximumBillingTier': maximum_billing_tier
            }
        }

        if destination_dataset_table:
            assert '.' in destination_dataset_table, (
                'Expected destination_dataset_table in the format of '
                '<dataset>.<table>. Got: {}').format(destination_dataset_table)
            destination_dataset, destination_table = \
                destination_dataset_table.split('.', 1)
            configuration['query'].update({
                'allowLargeResults': allow_large_results,
                'writeDisposition': write_disposition,
                'destinationTable': {
                    'projectId': self.project_id,
                    'datasetId': destination_dataset,
                    'tableId': destination_table,
                }
            })
        if udf_config:
            assert isinstance(udf_config, list)
            configuration['query'].update({
                'userDefinedFunctionResources': udf_config
            })

        return self.run_with_configuration(configuration)

    def run_load(self,
                 destination_project_dataset_table,
                 source_uris,
                 source_format='CSV',
                 create_disposition='CREATE_IF_NEEDED',
                 skip_leading_rows=0,
                 write_disposition='WRITE_EMPTY',
                 field_delimiter=','):
        """
        Executes a BigQuery load command to load data from Google Cloud Storage
        to BigQuery. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param destination_project_dataset_table:
            The dotted (<project>.)<dataset>.<table> BigQuery table to load data into.
            If <project> is not included, project will be the project defined in
            the connection json.
        :type destination_project_dataset_table: string
        :param schema_fields: The schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
        :type schema_fields: list
        :param source_uris: The source Google Cloud
            Storage URI (e.g. gs://some-bucket/some-file.txt). A single wild
            per-object name can be used.
        :type source_uris: list
        :param source_format: File format to export.
        :type source_format: string
        :param create_disposition: The create disposition if the table doesn't exist.
        :type create_disposition: string
        :param skip_leading_rows: Number of rows to skip when loading from a CSV.
        :type skip_leading_rows: int
        :param write_disposition: The write disposition if the table already exists.
        :type write_disposition: string
        :param field_delimiter: The delimiter to use when loading from a CSV.
        :type field_delimiter: string
        """
        destination_project, destination_dataset, destination_table = \
            self._split_project_dataset_table_input(
                'destination_project_dataset_table', destination_project_dataset_table)

        configuration = {
            'load': {
                'createDisposition': create_disposition,
                'destinationTable': {
                    'projectId': destination_project,
                    'datasetId': destination_dataset,
                    'tableId': destination_table,
                },
                'sourceFormat': source_format,
                'sourceUris': source_uris,
                'writeDisposition': write_disposition,
            }
        }

        if source_format == 'CSV':
            configuration['load']['skipLeadingRows'] = skip_leading_rows
            configuration['load']['fieldDelimiter'] = field_delimiter

        return self.run_with_configuration(configuration)

    def run_with_configuration(self, configuration):
        """
        Executes a BigQuery SQL query. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about the configuration parameter.

        :param configuration: The configuration parameter maps directly to
            BigQuery's configuration field in the job object. See
            https://cloud.google.com/bigquery/docs/reference/v2/jobs for
            details.
        """
        job_data = {
            'configuration': configuration
        }

        # Send query and wait for reply.
        query_reply = self.jobs \
            .insert(projectId=self.project_id, body=job_data) \
            .execute()
        job_id = query_reply['jobReference']['jobId']
        job = self.jobs.get(projectId=self.project_id, jobId=job_id).execute()

        # Wait for query to finish.
        while not job['status']['state'] == 'DONE':
            logging.info('Waiting for job to complete: %s, %s', self.project_id, job_id)
            time.sleep(5)
            job = self.jobs.get(projectId=self.project_id, jobId=job_id).execute()

        # Check if job had errors.
        if 'errorResult' in job['status']:
            raise Exception(
                'BigQuery job failed. Final error was: %s', job['status']['errorResult'])

        return job_id

    def get_schema(self, dataset_id, table_id):
        """
        Get the schema for a given datset.table.
        see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
        :param dataset_id: the dataset ID of the requested table
        :param table_id: the table ID of the requested table
        :return: a table schema
        """
        tables_resource = self.service.tables() \
            .get(projectId=self.project_id, datasetId=dataset_id, tableId=table_id) \
            .execute()
        return tables_resource['schema']
