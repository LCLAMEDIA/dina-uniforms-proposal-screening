from flask import Flask, request
import logging
import os
import threading
import uuid

from GoogleDocsOperations import GoogleDocsOperations
from NotionOperator import NotionOperator
from VoiceflowOperations import VoiceflowOperations
from ProposalScreeningOperations import ProposalScreeningOperations
from PromptsOperations import PromptsOperations
from GPTOperations import GPTOperations
from PostgresOperations import PostgresOperations

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


@app.route('/status/<task_id>', methods=['GET'])
def check_status(task_id):
    status_table = PostgresOperations(dbname='postgres',user='automations',password=os.getenv('POSTGRES_PASSWORD'),host='34.116.78.6')
    return status_table.get_status(task_id)

def run_analysis(url: str, page_id: str, run_id: str):
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
    
    status_table = PostgresOperations(dbname='postgres',user='automations',password=os.getenv('POSTGRES_PASSWORD'),host='34.116.78.6')
    status_table.complete_task(run_id)


@app.route("/analyse_proposal", methods=["POST"])
def analyse_proposal():
    inputs = request.json
    
    # Create Run Entry
    status_table = PostgresOperations(dbname='postgres',user='automations',password=os.getenv('POSTGRES_PASSWORD'), host='34.116.78.6')
    id = status_table.initiate_run()
    
    notion_ops = NotionOperator()
    page_id, page_url = notion_ops.create_blank_page(inputs.get("title"))
    
    # Start the background task
    thread = threading.Thread(target=run_analysis, args=(inputs.get('url'), page_id, id))
    thread.start()
    
    return {'url': page_url, 'status_id': id}, 200
    

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
