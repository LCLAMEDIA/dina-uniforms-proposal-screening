from flask import Flask, request
import logging
import os

from GoogleDocsOperations import GoogleDocsOperations
from NotionOperator import NotionOperator
from VoiceflowOperations import VoiceflowOperations
from ProposalScreeningOperations import ProposalScreeningOperations
from PromptsOperations import PromptsOperations
from GPTOperations import GPTOperations

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


@app.route("/create_report_document", methods=["POST"])
def create_report_document():
    inputs = request.json
    # Create document and return the URL for output
    notion_ops = NotionOperator()
    notion_ops.create_test_page()
    return {}, 200
    pass


@app.route("/analyse_proposal", methods=["POST"])
def analyse_proposal():
    inputs = request.json
    
    #google_docs_ops = GoogleDocsOperations()
    voiceflow_ops = VoiceflowOperations()
    prompts_ops = PromptsOperations()
    gpt_ops = GPTOperations(prompts_ops=prompts_ops)
    notion_ops = NotionOperator()
    proposal_ops = ProposalScreeningOperations(
        inputs.get("proposal_url"),
        google_docs_ops=None,
        voiceflow_ops=voiceflow_ops,
        prompts_ops=prompts_ops,
        gpt_ops=gpt_ops,
        notion_ops=notion_ops
    )
    
    proposal_ops.run(proposal_url=inputs.get('url'))
    
    return {}, 200

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))