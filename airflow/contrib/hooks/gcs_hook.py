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

import logging

from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from googleapiclient import errors
from oauth2client.client import GoogleCredentials

logging.getLogger("google_cloud_storage").setLevel(logging.INFO)


class GoogleCloudStorageHook(object):
    """
    Interact with Google Cloud Storage. This hook uses the Google Cloud Platform
    connection.
    """

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):

        self.service = self._get_conn()
        self.objects = self.service.objects()

    def _get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        credentials = GoogleCredentials.get_application_default()

        return build('storage', 'v1', credentials=credentials)

    def download(self, bucket, object, filename=False):
        """
        Get a file from Google Cloud Storage.
        :param bucket: The bucket to fetch from.
        :type bucket: string
        :param object: The object to fetch.
        :type object: string
        :param filename: If set, a local file path where the file should be written to.
        :type filename: string
        """
        downloaded_file_bytes = self.objects \
            .get_media(bucket=bucket, object=object) \
            .execute()

        # Write the file to local file path, if requested.
        if filename:
            write_argument = 'wb' if isinstance(downloaded_file_bytes, bytes) else 'w'
            with open(filename, write_argument) as file_fd:
                file_fd.write(downloaded_file_bytes)

        return downloaded_file_bytes

    def upload(self, bucket, object, filename, mime_type='application/octet-stream'):
        """
        Uploads a local file to Google Cloud Storage.
        :param bucket: The bucket to upload to.
        :type bucket: string
        :param object: The object name to set when uploading the local file.
        :type object: string
        :param filename: The local file path to the file to be uploaded.
        :type filename: string
        :param mime_type: The MIME type to set when uploading the file.
        :type mime_type: string
        """
        media = MediaFileUpload(filename, mime_type)
        self.objects \
            .insert(bucket=bucket, name=object, media_body=media) \
            .execute()

    def exists(self, bucket, object):
        """
        Checks for the existence of a file in Google Cloud Storage.
        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: string
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: string
        """
        try:
            self.objects \
                .get(bucket=bucket, object=object) \
                .execute()
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise

    def delete(self, bucket, object):
        """
        Delete a file from Google Cloud Storage.
        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: string
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: string
        """
        self.objects \
            .delete(bucket=bucket, object=object) \
            .execute()
