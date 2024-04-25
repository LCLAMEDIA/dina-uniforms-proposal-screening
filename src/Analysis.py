import json

class Analysis:
    def __init__(self, text: str, prompt: dict, response: dict):
        self.text = text
        self.prompt_name = prompt.get('name')
        self.prompt_obj = prompt
        self.response = response
        #self.analysis_text, self.dot_point_summary = self.parse_response(response)

    def __str__(self):
        return json.dumps({
            "prompt_name": self.prompt_name,
            "response": json.dumps(self.response, indent=4)
        })

    def parse_response(self, response: dict) -> tuple:
        """Parses GPT json into class variables

        Args:
            response (dict): Response from GPT
        """
        return response.get('analysis', 'No Analysis Provided'), response.get('dot_point_summary', [])
