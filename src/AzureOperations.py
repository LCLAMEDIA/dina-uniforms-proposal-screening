import requests
import os

class AzureOperations:

    def __init__(self):
        self.tenant_id = os.environ.get('TENANT_ID')

    def get_access_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"