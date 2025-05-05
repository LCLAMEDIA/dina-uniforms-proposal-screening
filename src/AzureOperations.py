import requests
import os
import logging

logging.basicConfig(level=logging.INFO)

class AzureOperations:

    def __init__(self):
        logging.info("[AzureOperations] Initializing AzureOperations")
        self.tenant_id = os.environ.get('TENANT_ID')
        self.client_id = os.environ.get('CLIENT_ID')
        self.client_secret = os.environ.get('CLIENT_SECRET')

    def get_access_token(self) -> str | None:
        logging.info("[AzureOperations] Getting access token")

        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

        headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
        }

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default"
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        if response.status_code == 200:
            response_dict = response.json()
            logging.info(f"[AzureOperations] Access token retrieved. Info: <{response.status_code}> {response.text}")
            return response_dict.get("access_token")
        
        logging.exception(f"[AzureOperations] Failed to retrieve access token. Info: <{response.status_code}> {response.text}")

        return None

