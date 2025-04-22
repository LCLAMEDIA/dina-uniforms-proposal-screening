import requests
import os

class AzureOperations:

    def __init__(self):
        self.tenant_id = os.environ.get('TENANT_ID')
        self.client_id = os.environ.get('CLIENT_ID')
        self.client_secret = os.environ.get('CLIENT_SECRET')

    def get_access_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

        headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
        }

        files=[]

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_id": self.client_secret,
            "scope": "https://graph.microsoft.com/.default"
        }

        response = requests.request("POST", url, headers=headers, data=payload, files=files)