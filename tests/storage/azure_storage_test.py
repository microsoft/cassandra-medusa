# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import tempfile
import unittest

from medusa.storage.azure_storage import AzureStorage
from tests.storage.abstract_storage_test import AttributeDict
from azure.identity import ManagedIdentityCredential
from azure.core.credentials import AzureNamedKeyCredential


class AzureStorageTest(unittest.TestCase):

    credentials_file_content = """
    {
      "storage_account": "medusa-unit-test",
      "key": "randomString=="
    }
    """

    def test_make_connection_url(self):
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = AttributeDict({
                'region': 'region-from-config',
                'storage_provider': 'azure_blobs',
                'key_file': credentials_file.name,
                'bucket_name': 'bucket-from-config',
                'concurrent_transfers': '1',
                'host': None,
                'port': None,
            })
            azure_storage = AzureStorage(config)
            self.assertTrue(isinstance(azure_storage.credentials, AzureNamedKeyCredential))
            self.assertEqual("medusa-unit-test", azure_storage.credentials.named_key.name)
            self.assertEqual("randomString==", azure_storage.credentials.named_key.key)
            self.assertEqual(
                'https://medusa-unit-test.blob.core.windows.net/',
                azure_storage.azure_blob_service_url
            )

    def test_make_connection_url_with_custom_host(self):
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = AttributeDict({
                'region': 'region-from-config',
                'storage_provider': 'azure_blobs',
                'key_file': credentials_file.name,
                'bucket_name': 'bucket-from-config',
                'concurrent_transfers': '1',
                'host': 'custom.host.net',
                'port': None,
            })
            azure_storage = AzureStorage(config)
            self.assertEqual(
                'https://medusa-unit-test.blob.core.custom.host.net/',
                azure_storage.azure_blob_service_url
            )

    def test_make_connection_url_with_custom_host_port(self):
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(self.credentials_file_content.encode())
            credentials_file.flush()
            config = AttributeDict({
                'region': 'region-from-config',
                'storage_provider': 'azure_blobs',
                'key_file': credentials_file.name,
                'bucket_name': 'bucket-from-config',
                'concurrent_transfers': '1',
                'host': 'custom.host.net',
                'port': 123,
            })
            azure_storage = AzureStorage(config)
            self.assertEqual(
                'https://medusa-unit-test.blob.core.custom.host.net:123/',
                azure_storage.azure_blob_service_url
            )

    def test_credentials_with_managed_identity(self):
        credentials_file_content_with_identity_config = """
        {
          "storage_account": "medusa-unit-test",
          "identity_config": {
            "mi_res_id": "<managed_identity>"
          }
        }
        """
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(credentials_file_content_with_identity_config.encode())
            credentials_file.flush()
            config = AttributeDict({
                'region': 'region-from-config',
                'storage_provider': 'azure_blobs',
                'key_file': credentials_file.name,
                'bucket_name': 'bucket-from-config',
                'concurrent_transfers': '1',
                'host': None,
                'port': None,
            })
            azure_storage = AzureStorage(config)
            self.assertTrue(isinstance(azure_storage.credentials, ManagedIdentityCredential))

    def test_credentials_with_blob_url(self):
        credentials_file_content_with_identity_config = """
        {
            "storage_account": "medusa-unit-test",
            "key": "randomString==",
            "blob_url": "https://xxx.blob.host.net/"
        }
        """
        with tempfile.NamedTemporaryFile() as credentials_file:
            credentials_file.write(credentials_file_content_with_identity_config.encode())
            credentials_file.flush()
            config = AttributeDict({
                'region': 'region-from-config',
                'storage_provider': 'azure_blobs',
                'key_file': credentials_file.name,
                'bucket_name': 'bucket-from-config',
                'concurrent_transfers': '1',
                'host': None,
                'port': None,
            })
            azure_storage = AzureStorage(config)

            self.assertEqual(
                'https://xxx.blob.host.net/',
                azure_storage.azure_blob_service_url
            )

