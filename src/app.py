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

        not_item_export_file = not file_name.upper().startswith("ITEM EXPORT ALL")
        not_xlsx_file = not file_name.lower().endswith(".xlsx")

        if content_type != "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" or not_item_export_file or not_xlsx_file:
            response = jsonify({"error": f"Invalid file {file_name} uploaded in {ssr_folder}"})
            response.status_code = 422
            logging.error(f"Invalid file {file_name} uploaded in {ssr_folder}")
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
        
        # Initialize processor with configuration
        oor_ops = OpenOrdersReporting()
        
        # Check if configuration was loaded
        if hasattr(oor_ops, 'config_reader'):
            config_loaded = hasattr(oor_ops.config_reader, 'official_brands')
            logging.info(f"[OOR] Configuration loaded: {config_loaded}")
            if config_loaded:
                logging.info(f"[OOR] Using {len(oor_ops.official_brands)} official brands and {len(oor_ops.product_num_mapping)} customer mappings")
        
        # Process the file
        result = oor_ops.process_excel_file(
            excel_file_bytes=file_content,
            filename=file_name
        )
        
        # Check if processing was successful
        if not result.get('success', True):  # Default to True for backward compatibility
            error_type = result.get('error_type', 'unknown_error')
            error_message = result.get('error_message', 'Unknown error occurred')
            logging.error(f"[OOR] Processing failed: {error_type} - {error_message}")
            
            # Format error message based on error type
            user_message = error_message
            if error_type == 'validation_error':
                user_message = f"File validation failed: {error_message}"
            elif error_type == 'empty_data_error':
                user_message = "The Excel file contains no data or only header information."
            elif error_type == 'parser_error':
                user_message = "Unable to parse the Excel file. The file may be corrupted or in an unsupported format."
            
            error_response = {
                "success": False,
                "message": user_message,
                "error_time": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                "error_type": error_type
            }
            
            return jsonify(error_response), 400
        
        # Format today's date for display
        today_fmt = datetime.now().strftime("%d-%m-%Y")
        folder_fmt = datetime.now().strftime("%d-%m-%y")
        
        # Build output files section
        output_files_list = []
        output_files_text = ""
        
        for file_type, filename in result.get('output_files', {}).items():
            file_count = 0
            if file_type in result.get('product_counts', {}):
                file_count = int(result.get('product_counts', {}).get(file_type, 0))
            elif file_type == 'main_or_others' and 'remaining_rows' in result:
                file_count = int(result.get('remaining_rows', 0))
            
            output_files_list.append({
                "file_type": file_type,
                "filename": filename,
                "record_count": file_count
            })
            output_files_text += f"- {filename}: {file_count} records\n"
        
        # Get information about brands
        filtered_brands = []
        if hasattr(oor_ops, 'official_brands'):
            filtered_brands = oor_ops.official_brands
        
        split_brands = []
        if hasattr(oor_ops, 'separate_file_customers'):
            split_brands = oor_ops.separate_file_customers
        
        # Create concise text message
        raw_message = f"""OOR Processing Complete
File: {file_name}
Date: {today_fmt}
Time: {round(float(result.get('duration', 0)), 2)}s
Records: {int(result.get('total_rows', 0))}
Duplicates Removed: {int(result.get('duplicate_rows_removed_by_customer_logic', 0))}
Filtered Brands: {int(result.get('filtered_brand_rows', 0))}

Files:
{output_files_text}
Location: OOR/Processed/{folder_fmt}/"""
        
        # Create concise structured response
        response_data = {
            "success": True,
            "message": raw_message.strip(),
            "data": {
                "file": file_name,
                "date": today_fmt,
                "processing_time": round(float(result.get('duration', 0)), 2),
                "stats": {
                    "total": int(result.get('total_rows', 0)),
                    "duplicates_removed": int(result.get('duplicate_rows_removed_by_customer_logic', 0)),
                    "filtered_brands_count": int(result.get('filtered_brand_rows', 0)),
                    "remaining": int(result.get('remaining_rows', 0))
                },
                "filtered_brands": filtered_brands,
                "split_brands": split_brands,
                "files": output_files_list,
                "location": f"Operations & Knowledge Base/1. Automations/OPEN ORDER REPORTING (OOR)/Processed/{folder_fmt}/"
            }
        }
        
        # Return success response
        return jsonify(response_data), 200
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logging.error(f"SharePoint OOR processing failed: {str(e)}")
        logging.error(f"Traceback: {error_trace}")
        
        # Format error message
        error_response = {
            "success": False,
            "message": f"Error: {str(e)}",
            "error_time": datetime.now().strftime("%d-%m-%Y %H:%M:%S")
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