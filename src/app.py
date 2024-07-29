from flask import Flask, request, jsonify
import logging
import os
import threading
import uuid
import platform
from datetime import datetime, timedelta, timezone
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
import json

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
    try:
        status_table = PostgresOperations(dbname='postgres',user='automations',password=os.getenv('POSTGRES_PASSWORD'),host='34.116.78.6')
        return status_table.get_status(task_id)
    except Exception as e:
        logging.error(f"Error checking status: {str(e)}")
        return jsonify({"error": "Failed to check status"}), 500

def run_analysis(url: str, page_id: str, run_id: str):
    try:
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
    except Exception as e:
        logging.error(f"Error in run_analysis: {str(e)}")
        # You might want to update the task status to 'failed' here

@app.route('/analyse_proposal_backend', methods=["POST"])
def analyse_proposal_backend():
    try:
        inputs = request.json
        run_analysis(inputs.get('url'), page_id=inputs.get('page_id'), run_id=inputs.get('run_id'))
        return {}, 200
    except Exception as e:
        logging.error(f"Error in analyse_proposal_backend: {str(e)}")
        return jsonify({"error": "Failed to analyze proposal"}), 500

@app.route("/analyse_proposal", methods=["POST"])
def analyse_proposal():
    try:
        inputs = request.json
        
        status_table = PostgresOperations(dbname='postgres',user='automations',password=os.getenv('POSTGRES_PASSWORD'), host='34.116.78.6')
        id = status_table.initiate_run()
        
        notion_ops = NotionOperator()
        page_id, page_url = notion_ops.create_blank_page(inputs.get("title"))
        
        url = f'https://dina-uniforms-proposal-screening-xtk56hgj4a-ts.a.run.app/analyse_proposal_backend'
        payload = {
            'url': inputs.get('url'),
            'page_id': page_id,
            'run_id': id
        }
        enqueue_task(url, payload)
        
        return {'url': page_url, 'status_id': id}, 200
    except Exception as e:
        logging.error(f"Error in analyse_proposal: {str(e)}")
        return jsonify({"error": "Failed to initiate proposal analysis"}), 500

def enqueue_task(url, payload):
    try:
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path('dina-uniform-group', 'australia-southeast1', 'proposal-queue')
        task = {
            'http_request': {
                'http_method': tasks_v2.HttpMethod.POST,
                'url': url,
                'headers': {'Content-type': 'application/json'},
                'body': json.dumps(payload).encode()
            },
            'schedule_time': timestamp_pb2.Timestamp().FromDatetime(datetime.now(timezone.utc) + timedelta(seconds=10))
        }
        response = client.create_task(request={"parent": parent, "task": task})
        return response.name
    except Exception as e:
        logging.error(f"Error enqueueing task: {str(e)}")
        raise  # Re-raise the exception to be caught by the calling function

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))