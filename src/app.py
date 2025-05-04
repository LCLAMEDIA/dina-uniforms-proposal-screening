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

@app.route("/open-orders-report/process", methods=["POST"])
def sharepoint_process_oor():
    try:
        # Get headers
        file_name = request.headers.get('x-ms-file-name')
        file_path = request.headers.get('x-ms-file-path')
        content_type = request.headers.get('Content-Type')
        
        # Get binary data directly
        file_content = request.get_data()
        
        # Log info about the received content
        logging.info(f"[OOR] Process_oor received content: {len(file_content)} bytes")
        logging.info(f"[OOR] Content type header: {content_type}")
        
        # Debug first bytes if file is not empty
        if file_content and len(file_content) > 0:
            logging.info(f"[OOR] First 20 bytes (hex): {file_content[:20].hex()}")
            # Excel files start with PK (hex: 504B)
            if file_content[:2] != b'PK':
                logging.warning("[OOR] File does not have Excel/ZIP signature (PK)")
        else:
            return jsonify({"error": "Received empty file or no file content"}), 400
        
        # Initialize processor
        oor_ops = OpenOrdersReporting()
        result = oor_ops.process_excel_file(
            excel_file_bytes=file_content,
            filename=file_name
        )
        
        # Format today's date for display
        today_fmt = datetime.now().strftime("%d-%m-%Y")
        folder_fmt = datetime.now().strftime("%d-%m-%y")
        
        # Build output files section
        output_files_list = []
        output_files_text = ""
        
        for file_type, filename in result.get('output_files', {}).items():
            file_count = 0
            if file_type == 'generic' and result.get('generic_rows'):
                file_count = result.get('generic_rows')
            elif file_type == 'calvary' and result.get('calvary_rows'):
                file_count = result.get('calvary_rows')
            elif file_type == 'former_customers' and result.get('filtered_brand_rows'):
                file_count = result.get('filtered_brand_rows')
            elif file_type == 'others' and result.get('remaining_rows'):
                file_count = result.get('remaining_rows')
            
            file_info = {
                "file_type": file_type.capitalize(),
                "filename": filename,
                "record_count": file_count
            }
            output_files_list.append(file_info)
            output_files_text += f"- {file_type.capitalize()}: {filename} ({file_count} records)\n"
        
        # Create raw text message
        raw_message = f"""
Open Orders Report Processing Complete
Source File: {file_name}
Processed On: {today_fmt}
Processing Time: {round(result.get('duration', 0), 2)} seconds
Total Records: {result.get('total_rows', 0)}

Generated Files:
{output_files_text}
Files saved to: Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Processed/{folder_fmt}/
"""
        
        # Create structured response with both raw text and object data
        response_data = {
            "success": True,
            "raw_message": raw_message.strip(),
            "data": {
                "source_file": file_name,
                "processed_date": today_fmt,
                "processing_time_seconds": round(result.get('duration', 0), 2),
                "total_records": result.get('total_rows', 0),
                "output_location": f"Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Processed/{folder_fmt}/",
                "counts": {
                    "generic": result.get('generic_rows', 0),
                    "calvary": result.get('calvary_rows', 0),
                    "former_customers": result.get('filtered_brand_rows', 0),
                    "others": result.get('remaining_rows', 0)
                },
                "output_files": output_files_list
            }
        }
        
        # Return success response with both formats
        return jsonify(response_data), 200
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logging.error(f"SharePoint OOR processing failed: {str(e)}")
        logging.error(f"Traceback: {error_trace}")
        
        # Format error message in both formats
        raw_error = f"""
Open Orders Report Processing Failed
Error: {str(e)}
Time: {datetime.now().strftime("%d-%m-%Y %H:%M:%S")}

Please contact IT support if this error persists.
"""
        
        error_response = {
            "error": True,
            "raw_message": raw_error.strip(),
            "data": {
                "error_message": str(e),
                "error_time": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            }
        }
        
        return jsonify(error_response), 500

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