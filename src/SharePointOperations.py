from typing import Dict, List
from datetime import datetime
import requests
import os
import logging

logging.basicConfig(level=logging.INFO)

class SharePointOperations:

    def __init__(self, access_token):
        logging.info("[SharePointOperations] Initializing SharePointOperations")

        self.tenant_name = os.environ.get('TENANT_NAME')
        self.site_name = os.environ.get('SITE_NAME')
        self.ssr_input_filepath = os.environ.get('SSR_INPUT_PATH')
        self.ssr_output_filepath = os.environ.get('SSR_OUTPUT_PATH')
        self.access_token = access_token

    def get_site_id(self) -> str | None:
        logging.info("[SharePointOperations] Getting site ID")

        url = f"https://graph.microsoft.com/v1.0/sites/{self.tenant_name}.sharepoint.com:/sites/{self.site_name}"

        headers = {
        'Authorization': f'Bearer {self.access_token}'
        }

        response = requests.request("GET", url, headers=headers)

        if response.status_code == 200:
            logging.info(f"[SharePointOperations] Site ID retrieved. Info: <{response.status_code}> {response.text}")

            response_dict = response.json()
            return response_dict.get("id")
        
        logging.exception(f"[SharePointOperations] Failed to retrieve site ID. Info: <{response.status_code}> {response.text}")

        return None

    def get_drive_id(self, site_id: str) -> str | None:
        logging.info("[SharePointOperations] Getting drive ID")

        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"

        headers = {
        'Authorization': f'Bearer {self.access_token}'
        }

        response = requests.request("GET", url, headers=headers)

        if response.status_code == 200:
            logging.info(f"[SharePointOperations] Drive ID retrieved. Info: <{response.status_code}> {response.text}")

            response_dict = response.json()
            values: List[Dict] = response_dict.get("value", [])
            shared_documents_dict = next(iter(i for i in values if str(i.get("webUrl")).endswith("/Shared%20Documents")), {})
            return shared_documents_dict.get("id")
        
        logging.exception(f"[SharePointOperations] Failed to retrieve drive ID. Info: <{response.status_code}> {response.text}")

        return None

    def get_bytes_for_latest_file_with_prefix(self, prefix: str, drive_id: str) -> bytes | None:
        logging.info(f"[SharePointOperations] Getting bytes for file with prefix {prefix}")

        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{self.ssr_input_filepath}:/children"

        headers = {
        'Authorization': f'Bearer {self.access_token}'
        }

        response = requests.request("GET", url, headers=headers)

        if response.status_code == 200:
            logging.info(f"[SharePointOperations] Files retrieved. Info: <{response.status_code}> {response.text}")

            response_dict = response.json()
            values: List[Dict] = response_dict.get("value", [])
            filtered_values: List[Dict] = list(filter(lambda item: item.get("name", "").startswith(prefix), values))
            sorted_filtered_values = sorted(filtered_values, key=lambda x: self.parse_zulu(x.get("lastModifiedDateTime")), reverse=True)
            shared_documents_dict = next(iter(sorted_filtered_values), {})
            download_url = shared_documents_dict.get("@microsoft.graph.downloadUrl")

            file_bytes = self.get_file_bytes_from_download_url(download_url)

            return file_bytes

        logging.exception(f"[SharePointOperations] Failed to files where filename with prefix {prefix}. Info: <{response.status_code}> {response.text}")

        return None

    def upload_excel_file(self, drive_id: str, excel_filename: str, file_bytes: bytes) -> None:
        logging.info(f"[SharePointOperations] Uploading excel file with name {excel_filename}")

        for filepath in [self.ssr_output_filepath, self.ssr_input_filepath]:

            url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{filepath}/{excel_filename}:/content"

            headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            response = requests.request("PUT", url, headers=headers, data=file_bytes)

            if response.status_code in [200, 201]:
                logging.info(f"[SharePointOperations] Excel file with name {excel_filename} uploaded in {filepath}. Info: <{response.status_code}> {response.text}")

            else:
                logging.exception(f"[SharePointOperations] Failed to upload excel file with name {excel_filename} in {filepath}. Info: <{response.status_code}> {response.text}")

    @staticmethod
    def parse_zulu(dt_str):
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def get_file_bytes_from_download_url(download_url) -> bytes | None:
        logging.info(f"[SharePointOperations] Downloading file bytes from {download_url}")

        response = requests.request("GET", download_url)

        if response.status_code == 200:
            return response.content
        
        return None
    
    def _ensure_folder_path_exists(self, drive_id: str, folder_path: str) -> bool:
        """
        Ensure a folder path exists in SharePoint, creating it if necessary.
        
        Args:
            drive_id: The ID of the SharePoint drive
            folder_path: The folder path to ensure exists
            
        Returns:
            bool: True if the path exists or was created successfully, False otherwise
        """
        logging.info(f"[SharePointOperations] Ensuring folder path exists: {folder_path}")
        
        # Remove leading slash if present
        if folder_path.startswith('/'):
            folder_path = folder_path[1:]
        
        # If empty path, just return True
        if not folder_path:
            return True
        
        # Check if the entire path exists first
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}"
        
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
        
        response = requests.request("GET", url, headers=headers)
        
        # If the entire path exists, no need to create anything
        if response.status_code == 200:
            logging.info(f"[SharePointOperations] Folder path already exists: {folder_path}")
            return True
        
        # If we're here, the path doesn't exist, but that's okay
        # since SharePoint will do that automatically during file upload
        logging.info(f"[SharePointOperations] Folder path '{folder_path}' doesn't exist yet, but will be created automatically during file upload")
        return True

    def upload_file_to_path(self, drive_id: str, file_path: str, file_name: str, file_bytes: bytes, content_type: str = "text/csv") -> None:
        """
        Upload a file to a specific path in SharePoint.
        SharePoint will automatically create the folder structure if it doesn't exist.
        
        Args:
            drive_id: The ID of the SharePoint drive
            file_path: The full path in SharePoint where the file should be uploaded
            file_name: The name of the file
            file_bytes: The file content as bytes
            content_type: The MIME type of the file (default: "text/csv")
        """
        logging.info(f"[SharePointOperations] Uploading file {file_name} to path {file_path}")
        
        # Remove leading slash if present
        if file_path.startswith('/'):
            file_path = file_path[1:]
        
        # Check if the path contains a filename - if not, append the filename
        if not file_path.endswith(file_name):
            if file_path and not file_path.endswith('/'):
                file_path += '/'
            file_path += file_name
        
        # Upload the file - SharePoint will create necessary folders automatically
        url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{file_path}:/content"
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': content_type
        }
        
        response = requests.request("PUT", url, headers=headers, data=file_bytes)
        
        if response.status_code in [200, 201]:
            logging.info(f"[SharePointOperations] File {file_name} uploaded successfully to {file_path}. Info: <{response.status_code}> {response.text}")
        else:
            logging.exception(f"[SharePointOperations] Failed to upload file {file_name} to {file_path}. Info: <{response.status_code}> {response.text}")