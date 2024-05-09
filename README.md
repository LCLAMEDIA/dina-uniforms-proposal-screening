# Dina Unifomrs Proposal Screening

## Description
This program is the property of LCLA Media. All rights reserved. The Dina Uniform Proposal Screening tool is designed to streamline the process of managing and analyzing uniform manufacturing proposals. By automating the evaluation and summarization of proposal documents, this tool significantly reduces manual effort and enhances the accuracy and efficiency of proposal screening.

Developed to support professionals in the uniform manufacturing sector, this tool assists in tracking, evaluating, and reporting on the critical aspects of proposals such as key focus areas, important dates, and cost details. It integrates seamlessly with various document management systems and platforms, providing real-time insights and analytics.

## Legal Notice
This software is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement. In no event shall the authors or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software.

This program is proprietary software; redistribution and/or modification are not permitted without the express permission of LCLA Media.


You can modify the disclaimer to include a note about potential additional charges resulting from overuse during testing. Here’s how you can phrase it:

---

### Disclaimer for Testing
If you are testing the Dina Uniform Proposal Screening tool, please be aware that there is a limit set on the number of queries that can be sent to GPT-4. This limit is designed to manage resource utilization and ensure efficient operation of the tool under typical usage conditions.

We strongly recommend that testers and developers take this limit into consideration when performing extensive testing or developing features that require repeated interactions with GPT-4. Exceeding these limits may not only result in temporary suspension of GPT-4 service access but could also incur additional charges due to excessive usage.

For detailed information on the specific limits or to discuss increasing your query capacity for development purposes, please contact the system administrator or the person responsible for managing your API keys.

---

## **What this tool does**

The Dina Uniform Proposal Screening tool processes proposal documents from companies seeking a new uniform manufacturer, analyzing these based on client requirements. It:

- Summarizes the focal points of the proposal.
- Provides insights into key items, dates, and costs.


## Installation and Setup

### Prerequisites
Before you begin, ensure you have the latest version of Python installed on your system. Python 3.6 or higher is required. You can download Python from [https://www.python.org/downloads/](https://www.python.org/downloads/).

### Setting up a Virtual Environment
It's recommended to use a virtual environment for Python projects. This helps to keep dependencies required by different projects separate. To set up a virtual environment, follow these steps:

1. Install the virtual environment package:
```sh
pip install virtualenv
```

2. Navigate to your project directory and create a virtual environment:
```sh
virtualenv venv
```

3. Activate the virtual environment:
   - On Windows:
   ```sh
   .\venv\Scripts\activate
   ```
   - On macOS and Linux:
   ```sh
   source venv/bin/activate
   ```
---

After activating the virtual environment, install the project dependencies by running:

```sh
pip install -r src/requirements.txt
```

This command reads the `requirements.txt` file in your project directory, installing all the necessary Python packages listed there. Ensure you have a `requirements.txt` file that lists all dependencies.


Here's how to properly set up and run the described Flask application, which includes operations for proposal screening and managing the status of tasks:

### Prerequisites
Before running the application, you need to have several API keys and passwords, as well as set up a proper environment:

1. **API Keys and Passwords**:
   - `OPENAI_KEY`: Used for operations involving OpenAI services.
   - `NOTION_KEY`: Needed for operations involving Notion API.
   - `Postgres Password`: Essential for accessing your PostgreSQL database.

   To obtain these keys, please consult with Liam Armitage.

2. **Environment Setup**:
   - Ensure you have a Python virtual environment set up and all the required modules installed.
   - Navigate into your project's source directory by running:
     ```bash
     cd src
     ```

### Running the Application
After setting up your environment and navigating to the correct directory, start the Flask application by running:
```bash
python app.py
```

### Flask Application Routes and Functions Explanation
The application includes several routes and functions designed for different operations:

1. **Status Check**:
   - Route: `@app.route('/status/<task_id>', methods=['GET'])`
   - Functionality: Retrieves the status of a given task using its `task_id` from a PostgreSQL database.
   - Utilizes a `PostgresOperations` class to handle database operations.

2. **Proposal Analysis**:
   - Route: `@app.route('/analyse_proposal_backend', methods=['POST'])`
   - Triggered internally to handle analysis operations in the background.
   - Calls the `run_analysis` function with data from a POST request to process a proposal document.

3. **Initiating Proposal Analysis**:
   - Route: `@app.route("/analyse_proposal", methods=["POST"])`
   - Initiates the analysis of a proposal.
   - It creates a new page in Notion for the proposal, starts a database entry for the run, and enqueues the task for background processing.

4. **Task Enqueueing**:
   - `enqueue_task` function sends tasks to Google Cloud Tasks for background processing.
   - Specifies the endpoint, payload, and timing for the task execution.

### Testing the Application with Flask
To test the Flask application locally:

1. **Ensure Flask is Installed**:
   Ensure Flask is installed in your environment:
   ```bash
   pip install Flask
   ```

2. **Run the Flask Application**:
   Use the following command to start the Flask server:
   ```bash
   flask run
   ```
   or
   ```bash
   python app.py
   ```
   This command starts the application with debugging enabled and makes the server accessible on your local network.

3. **Testing Routes**:
   You can test the routes using tools like Postman or cURL. For example, to check the status of a task:
   ```bash
   curl http://localhost:5000/status/1234
   ```
   Replace `1234` with the actual task ID you wish to query.


To test the routes that handle POST requests in your Flask application, such as `/analyse_proposal` and `/analyse_proposal_backend`, you'll need to understand how to structure the body of the POST request. This body should be formatted as a JSON object containing the necessary keys and values expected by the functions. Below, I’ll explain the required structure for these requests, and how to use `curl` to send a POST request for testing purposes.

### Key Functions and Expected Request Body

1. **`analyse_proposal`**:
   - **Description**: Initiates the proposal analysis process by creating a Notion page and enqueuing a background task for detailed analysis.
   - **Expected Body**:
     - `title`: The title for the new Notion page that will be created for this proposal.
     - `url`: The URL of the proposal document to be analyzed.
   - **Example JSON Body**:
     ```json
     {
       "title": "New Uniform Proposal",
       "url": "https://example.com/proposal.docx"
     }
     ```

2. **`analyse_proposal_backend`**:
   - **Description**: Processes the analysis of the proposal in the background.
   - **Expected Body**:
     - `url`: The URL of the proposal document to analyze.
     - `page_id`: The ID of the Notion page where the analysis will be stored.
     - `run_id`: A unique identifier for this run of the analysis.
   - **Example JSON Body**:
     ```json
     {
       "url": "https://example.com/proposal.docx",
       "page_id": "abcdef123456",
       "run_id": "1234567890"
     }
     ```

### Sending POST Requests Using `curl`

To send these requests using `curl`, you can use the following commands, making sure to replace the placeholder values with actual data relevant to your application:

1. **Testing `/analyse_proposal`**:
   ```bash
   curl -X POST http://localhost:5000/analyse_proposal \
   -H "Content-Type: application/json" \
   -d '{"title": "New Uniform Proposal", "url": "https://example.com/proposal.docx"}'
   ```

2. **Testing `/analyse_proposal_backend`**:
   ```bash
   curl -X POST http://localhost:5000/analyse_proposal_backend \
   -H "Content-Type: application/json" \
   -d '{"url": "https://example.com/proposal.docx", "page_id": "abcdef123456", "run_id": "1234567890"}'
   ```

These `curl` commands:
- Use the `-X POST` flag to specify that a POST request is being made.
- Include `-H "Content-Type: application/json"` to set the header indicating that the body of the request is in JSON format.
- Use `-d` to provide the data (body) of the request, formatted as a JSON string.

By using these commands, you can effectively test your Flask routes to ensure they are receiving and processing data as expected.