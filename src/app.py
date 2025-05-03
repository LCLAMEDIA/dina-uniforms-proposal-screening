from flask import Flask, request, jsonify, Response
import logging
import os
from datetime import datetime, timedelta, timezone
import json

from AzureOperations import AzureOperations
from GoogleDocsOperations import GoogleDocsOperations
from NotionOperator import NotionOperator
from OpenOrdersReporting import OpenOrdersReporting
from SharePointOperations import SharePointOperations
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

@app.route("/stock-status-report/automate", methods=["POST"])
def stock_status_report_automation():
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

# # New endpoint for processing Open Order Reports
# @app.route("/open-orders-report/process", methods=["POST"])
# def process_open_orders_report():
#     try:
#         file_name = request.headers.get('x-ms-file-name')
#         file_path = request.headers.get('x-ms-file-path')
#         content_type = request.headers.get('Content-Type')

#         logging.info(f"[OOR] Received request: file={file_name}, path={file_path}, type={content_type}")

#         # # Validate content type
#         # if content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
#         #     logging.warning(f"[OOR] Invalid content type: {content_type} for file={file_name}")
#         #     response = jsonify({"error": f"Invalid content type for file {file_name}. Excel file required."})
#         #     response.status_code = 422
#         #     return response
        
#         # # Validate file name
#         # if not file_name:
#         #     logging.warning(f"[OOR] No file name provided for path={file_path}")
#         #     response = jsonify({'message': f"No selected file from path: {file_path}"})
#         #     response.status_code = 422
#         #     return response
        
#         logging.info(f"Request headers: {dict(request.headers)}")

#         # Get file content
#         file_content = request.get_data()
#         logging.info(f"[OOR] Received {len(file_content)} bytes for file={file_name}")
        
#         # Initialize and run the Open Orders Report processor
#         oor_ops = OpenOrdersReporting()
#         logging.info(f"[OOR] Starting processing for file={file_name}")
#         result = oor_ops.process_excel_file(
#             excel_file_bytes=file_content,
#             filename=file_name
#         )
        
#         # Return success response with processing statistics
#         response_data = {
#             "message": "Open Orders Report processing completed successfully",
#             "statistics": {
#                 "total_rows_processed": result['total_rows'],
#                 "generic_rows": result['generic_rows'],
#                 "calvary_rows": result['calvary_rows'],
#                 "former_customers_rows": result['filtered_brand_rows'],
#                 "other_rows": result['remaining_rows'],
#                 "output_files": list(result['output_files'].values()),
#                 "processing_time_seconds": result['duration']
#             }
#         }
        
#         logging.info(f"[OOR] Processing successful: {result['total_rows']} rows processed in {result['duration']:.2f}s")
#         logging.info(f"[OOR] Files generated: {', '.join(list(result['output_files'].values()))}")
#         response = jsonify(response_data)
#         logging.info(f"[OOR] Sending response: status=200, data={response_data}")
#         return response, 200
        
#     except Exception as e:
#         import traceback
#         error_trace = traceback.format_exc()
#         logging.error(f"[OOR] Processing failed with exception: {str(e)}")
#         logging.error(f"[OOR] Traceback: {error_trace}")
#         response = jsonify({"error": f"Failed to process Open Orders Report. Error: {str(e)}"})
#         response.status_code = 500
#         logging.error(f"[OOR] Sending error response: status=500")
#         return response

@app.route("/open-orders-report/process", methods=["POST"])
def sharepoint_process_oor():
    try:
        # Get headers
        file_name = request.headers.get('x-ms-file-name')
        file_path = request.headers.get('x-ms-file-path') 
        
        # Get binary data directly
        file_content = request.get_data()
        
        # Log info about the received content
        logging.info(f"[OOR] Process_oor received content: {len(file_content)} bytes")
        
        # Initialize processor
        oor_ops = OpenOrdersReporting()
        result = oor_ops.process_excel_file(
            excel_file_bytes=file_content,
            filename=file_name
        )
        
        # Return success
        return jsonify({"success": True, "stats": result}), 200
        
    except Exception as e:
        logging.error(f"SharePoint OOR processing failed: {str(e)}")
        return jsonify({"error": f"Failed to process: {str(e)}"}), 500


# Simple test endpoint to confirm SharePoint connectivity
@app.route("/open-orders-report/test-connection", methods=["GET"])
def test_sharepoint_connection():
    try:
        # Initialize Azure and SharePoint connections
        azure_ops = AzureOperations()
        access_token = azure_ops.get_access_token()
        
        if not access_token:
            return jsonify({"error": "Failed to obtain Azure access token"}), 500
            
        sharepoint_ops = SharePointOperations(access_token=access_token)
        site_id = sharepoint_ops.get_site_id()
        
        if not site_id:
            return jsonify({"error": "Failed to get SharePoint site ID"}), 500
            
        drive_id = sharepoint_ops.get_drive_id(site_id=site_id)
        
        if not drive_id:
            return jsonify({"error": "Failed to get SharePoint drive ID"}), 500
        
        return jsonify({
            "message": "Successfully connected to SharePoint",
            "site_id": site_id,
            "drive_id": drive_id
        }), 200
        
    except Exception as e:
        logging.error(f"SharePoint connection test failed: {str(e)}")
        return jsonify({"error": f"SharePoint connection test failed: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))