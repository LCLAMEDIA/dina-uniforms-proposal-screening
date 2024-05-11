import json
from typing import Tuple, Dict, Any

class Analysis:
    def __init__(self, text: str, prompt: Dict[str, Any], response: Dict[str, Any]):
        self.text = text
        self.prompt_name = prompt.get('name')
        if not self.prompt_name:
            raise ValueError("Prompt must have a 'name' key.")
        self.prompt_obj = prompt
        self.response = response
        self.analysis_text, self.dot_point_summary = self.parse_response(response)

    def __str__(self) -> str:
        analysis_details = {
            "text": self.text,
            "prompt_name": self.prompt_name,
            "response": json.dumps(self.response, indent=4),
            "analysis_text": self.analysis_text,
            "dot_point_summary": self.dot_point_summary
        }
        return json.dumps(analysis_details, indent=4)

    def parse_response(self, response: Dict[str, Any]) -> Tuple[str, list]:
        """Parses the GPT response into analysis text and dot-point summary.

        Args:
            response (dict): Response from GPT.

        Returns:
            tuple: A tuple containing the analysis text and dot-point summary.
        """
        analysis_text = response.get('analysis', 'No Analysis Provided')
        dot_point_summary = response.get('dot_point_summary', [])
        return analysis_text, dot_point_summary
