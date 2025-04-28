from flask import Flask, request, jsonify, Response
import logging
import os
from datetime import datetime, timedelta, timezone
import json

from GoogleDocsOperations import GoogleDocsOperations
from NotionOperator import NotionOperator
from OORSharepointProcessor import OORSharePointProcessor
from VoiceflowOperations import VoiceflowOperations
from ProposalScreeningOperations import ProposalScreeningOperations
from PromptsOperations import PromptsOperations
from GPTOperations import GPTOperations
from StockStatusReportOps import StockStatusReportOps

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

@app.route("/sharepoint/ssr/automate", methods=["POST"])
def analyse_proposal_from_sharepoint():
    excel_file_bytes, ssr_filename, mimetype = None, None, None
    try:

        file_name = request.headers.get('x-ms-file-name')
        ssr_folder = request.headers.get('x-ms-file-path')
        content_type = request.headers.get('Content-Type')

        logging.info(f"Attempting to read file: {file_name} of type: {content_type}")

        if content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            response = jsonify({"error": f"Invalid content type for file {file_name} when uploaded in {ssr_folder}"})
            response.status_code = 422
            return response
        
        if not file_name:
            response = jsonify({'message': f"No selected file from fodler: {ssr_folder}"})
            response.status_code = 422
            return response
        
        file_content = request.get_data()
        
        # Run analysis synchronously
        try:
            ssr_ops = StockStatusReportOps(
                exported_file_name=file_name,
                exported_file_bytes=file_content
            )
            

            logging.info(f"Attempting to automate stock status report: {file_name} in directory: {ssr_folder}")
            
            excel_file_bytes, ssr_filename, mimetype, notification_message = ssr_ops.start_automate()

            if not excel_file_bytes:
                logging.warning("Stock Status Report automation unsuccessful")
                response = jsonify({'message': "Stock Status Report automation unsuccessful"})
                response.status_code = 500
                return response

        except Exception as e:
            logging.error(f"Stock Status Report automation failed. Error: {str(e)}")
            response = jsonify({'message': f"Stock Status Report automation failed. Error: {e}"})
            response.status_code = 500
            return response
        
        logging.info(f"Analyse for file: {file_name} of type: {content_type} is success!")
        return Response(
            excel_file_bytes,
            mimetype=mimetype,
            headers={
                "Content-Disposition": f"attachment; filename={ssr_filename}",
                "x-ms-file-name": ssr_filename,
                "x-ms-notification": notification_message
                }
        )
    
    except Exception as e:
        import traceback
        logging.error(f"Printing Traceback: {traceback.print_exc()}")
        logging.error(f"Failed to initialise Stock Status Report automation. Error: {str(e)}")
        response = jsonify({"error": "Failed to initialise Stock Status Report automation. Error: {str(e)}"}) 
        response.status_code = 500
        return response

@app.route("/process_oor", methods=["POST"]) # Using POST, although GET could also work if no input needed
def process_oor_file_endpoint():
    """
    Flask endpoint to trigger the OOR SharePoint file processing.
    """
    logging.info("Received request to process OOR file from SharePoint.")
    try:
        # Instantiate the processor
        # This assumes necessary environment variables (Azure creds, SharePoint paths) are set
        processor = OORSharePointProcessor()

        # Get the latest OOR file bytes
        logging.info("Attempting to fetch the latest OOR file...")
        file_bytes = processor.get_latest_oor_file()

        if not file_bytes:
            logging.error("No OOR file found or error fetching file.")
            return jsonify({"error": "Could not retrieve the latest OOR file from SharePoint."}), 404

        # Process the file
        logging.info("OOR file fetched successfully. Starting processing...")
        stats = processor.process_oor_file(file_bytes)

        if stats and stats.get('success', False):
            logging.info("OOR processing completed successfully.")
            # You could optionally include stats or the summary file path in the response
            summary_path = stats.get('output_files', {}).get('summary', 'N/A')
            return jsonify({
                "message": "OOR processing completed successfully.",
                "summary_file": summary_path,
                "duration_seconds": stats.get('duration')
            }), 200
        else:
            logging.error("OOR processing failed.")
            return jsonify({"error": "OOR processing failed during execution."}), 500

    except Exception as e:
        # Log the full exception details for debugging
        logging.exception(f"An unexpected error occurred during OOR processing: {str(e)}")
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))