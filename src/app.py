from flask import Flask, request, jsonify, Request, Response
import logging
import os
from datetime import datetime, timedelta, timezone
import base64
import json

from GoogleDocsOperations import GoogleDocsOperations
from NotionOperator import NotionOperator
from DocxOperator import DocxOperator
from VoiceflowOperations import VoiceflowOperations
from ProposalScreeningOperations import ProposalScreeningOperations
from PromptsOperations import PromptsOperations
from GPTOperations import GPTOperations

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

def run_analysis(url: str, page_id: str):
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
    except Exception as e:
        logging.error(f"Error in run_analysis: {str(e)}")

@app.route('/analyse_proposal_backend', methods=["POST"])
def analyse_proposal_backend():
    try:
        inputs = request.json
        run_analysis(inputs.get('url'), page_id=inputs.get('page_id'))
        return {}, 200
    except Exception as e:
        logging.error(f"Error in analyse_proposal_backend: {str(e)}")
        return jsonify({"error": "Failed to analyze proposal"}), 500

@app.route("/analyse_proposal", methods=["POST"])
def analyse_proposal():
    try:
        inputs = request.json
        
        notion_ops = NotionOperator()
        page_id, page_url = notion_ops.create_blank_page(inputs.get("title"))
        
        # Run analysis synchronously
        run_analysis(inputs.get('url'), page_id=page_id)
        
        return {'url': page_url}, 200
    except Exception as e:
        logging.error(f"Error in analyse_proposal: {str(e)}")
        return jsonify({"error": "Failed to initiate proposal analysis"}), 500
    
@app.route("/sharepoint/proposal/analyse", methods=["POST"])
def analyse_proposal_from_sharepoint():
    docx_stream, filename, mimetype = None, None, None
    try:

        file_name = request.headers.get('x-ms-file-name')

        data = request.get_data()
        logging.info(f"Raw data received1: {request.get_data()}")
        logging.info(f"Raw data received2: {request.get_json()}")
        data = json.loads(data)

        if data.get("$content-type") != "application/vnd.openxmlformats-officedocument.wordprocessingml.document" or not data.get("$content"):
            return jsonify({"error": "Invalid content type"}), 422
        
        if not file_name:
            return jsonify({'message': "No selected file"}, 422)
        
        file_bytes = base64.b64decode(data["$content"])    
        
        # Run analysis synchronously
        try:
            prompts_ops = PromptsOperations()
            gpt_ops = GPTOperations(prompts_ops=prompts_ops)
            docx_ops = DocxOperator()
            
            proposal_ops = ProposalScreeningOperations(
                proposal_url=None,
                google_docs_ops=None,
                voiceflow_ops=None,
                prompts_ops=prompts_ops,
                notion_ops=None,
                gpt_ops=gpt_ops,
                page_id=None,
                docx_ops=docx_ops
            )
            
            docx_stream, filename, mimetype = proposal_ops.run_analysis_from_sharepoint(document_bytes=file_bytes, document_filename=file_name)
        except Exception as e:
            import traceback
            logging.error(f"Printing Traceback: {traceback.print_exc()}")
            logging.error(f"Error in run_analysis: {str(e)}")
        
        return Response(
            docx_stream,
            mimetype=mimetype,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        import traceback
        logging.error(f"Printing Traceback: {traceback.print_exc()}")
        logging.error(f"Error in analyse_proposal: {str(e)}")
        return jsonify({"error": "Failed to initiate proposal analysis"}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))