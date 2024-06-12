from openai import OpenAI
import logging
import json
import os
import re

class GPTOperations:
    """
    Handles operations with the OpenAI API, including querying ChatGPT and parsing responses.
    """
    def __init__(self, prompts_ops, api_key: str = os.environ.get('OPENAI_KEY')):
        """
        Initializes the GPTOperator with a given API key.
        """
        self.client = OpenAI(api_key=api_key)
        self.prompts_ops = prompts_ops
        
    def query_chatgpt(self, query, model="gpt-4o"):
        """
        Sends a query to ChatGPT and returns the response.
        """
        try:
            completion = self.client.chat.completions.create(
                model=model,
                response_format={ "type": "json_object" },
                messages=[
                    {"role": "system", "content": self.prompts_ops.get_system_prompt()},
                    {"role": "user", "content": query}
                ]
            )
            logging.info(completion.choices[0].message.content)
            return completion.choices[0].message.content
        except Exception as e:
            logging.error(f"[Exception] - {e}")
            return None
         
    def parse_json_response(self, gpt_response):
        """
        Parses a JSON-formatted string from GPT response into a Python object.
        """
        try:
            logging.info(f'[Parse Json] RAW Json response {gpt_response}')
            result = json.loads(gpt_response)
            return result
        except Exception as e:
            logging.info(f"Couldn't parse JSON {e} - {gpt_response}")
            return None
        
    def sanitize_json_string(self, json_string):
        # Regular expression to match control characters except those that are valid in JSON (quotation marks, backslash, and control characters inside a string)
        control_chars_regex = r'[\x00-\x1f\x7f-\x9f]'
        
        # Remove control characters while preserving valid JSON characters
        sanitized_string = re.sub(control_chars_regex, '', json_string)
        cleaned = '{' + sanitized_string.split('{')[1].split('}')[0] + '}'
        
        return cleaned