from flask import Flask, request
import logging
import os
import threading

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

@app.route("/check_status")

def run_analysis(url: str, page_id: str):
    prompts_ops = PromptsOperations()
    gpt_ops = GPTOperations(prompts_ops=prompts_ops)
    notion_ops = NotionOperator()
    
    proposal_ops = ProposalScreeningOperations(
        proposal_url=url,
        google_docs_ops=None,
        voiceflow_ops=None,
        prompts_ops=prompts_ops,
        gpt_ops=gpt_ops,
        notion_ops=notion_ops,
        page_id=page_id
    )
    
    proposal_ops.run()


@app.route("/analyse_proposal", methods=["POST"])
def analyse_proposal():
    inputs = request.json
    
    notion_ops = NotionOperator()
    page_id, page_url = notion_ops.create_blank_page(inputs.get("title"))
    
    # Start the background task
    thread = threading.Thread(target=run_analysis, args=(inputs.get('url'), page_id))
    thread.start()
    
    return {'url': page_url}, 200
    

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    
