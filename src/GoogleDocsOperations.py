import os
import platform
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.auth import default

from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/documents', 'https://www.googleapis.com/auth/drive']

class GoogleDocsOperations:
    def __init__(self):
        # Check if the operating system is 'Darwin' (macOS)
        if platform.system() == 'Darwin':
            # Path to your service account file
            service_account_file_path = os.path.expanduser('.local/earned.json')
            self.credentials = service_account.Credentials.from_service_account_file(
                service_account_file_path, scopes=SCOPES)
        else:
            # Use the default method for non-macOS operating systems
            self.credentials, _ = default(scopes=SCOPES)
        self.docs_service = build('docs', 'v1', credentials=self.credentials)
        self.drive_service = build('drive', 'v3', credentials=self.credentials)
    
    def download_document(self, url: str, output_path: str):
        pass
    
    def create_document(self, title: str, folder_id: str, template_doc: str = None):
        if template_doc:
            # Duplicate the template document
            copied_file = {'name': title, 'parents': [folder_id]}
            new_doc = self.drive_service.files().copy(
                fileId=template_doc, body=copied_file).execute()
        else:
            # Create a new document
            doc = self.docs_service.documents().create(body={'title': title}).execute()
            # Move document to specified folder
            file_id = doc['documentId']
            # Retrieve the existing parents to remove
            file = self.drive_service.files().get(fileId=file_id,
                                                  fields='parents').execute()
            previous_parents = ",".join(file.get('parents'))
            # Move the file to the new folder
            self.drive_service.files().update(
                fileId=file_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents').execute()
            new_doc = doc
        return new_doc['documentId']
    
    def replace_text_in_document(self, doc_id: str, find_text: str, replace_text: str):
        requests = [
            {
                'replaceAllText': {
                    'containsText': {
                        'text': find_text,
                        'matchCase': True
                    },
                    'replaceText': replace_text,
                }
            }
        ]
        result = self.docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()
        return result
    
    def remove_content_between_tags(self, doc_id: str, start_tag: str = "{START}", end_tag: str = "{END}"):
        """Remove content between specified start and end tags in a Google Doc.

        Args:
            doc_id (str): The ID of the document to modify.
            start_tag (str): The start tag marking the beginning of the content to remove.
            end_tag (str): The end tag marking the end of the content to remove.
        """
        # Fetch the document content
        document = self.docs_service.documents().get(documentId=doc_id).execute()
        content = document.get('body').get('content')

        start_index = None
        end_index = None

        # Iterate through elements in the document to find start and end tags
        for element in content:
            if 'paragraph' in element:
                elements = element.get('paragraph').get('elements')
                for elem in elements:
                    text_run = elem.get('textRun')
                    if text_run:
                        text = text_run.get('content')
                        if start_tag in text:
                            start_index = elem.get('startIndex')
                        elif end_tag in text:
                            end_index = elem.get('endIndex')
        
        # Ensure both start and end tags are found and start comes before end
        if start_index is not None and end_index is not None and start_index < end_index:
            # Prepare a request to delete the content between the tags
            requests = [{
                'deleteContentRange': {
                    'range': {
                        'startIndex': start_index,
                        'endIndex': end_index - 1,  # Adjusted to exclude the end tag itself
                    }
                }
            }]
            # Execute the batchUpdate request
            self.docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()
        else:
            print("Could not find both start and end tags in the correct order.")

# Note: For this code to work, you need to have the GOOGLE_APPLICATION_CREDENTIALS environment variable pointing to a JSON file with your service account key, or pass the path directly to `__init__`.