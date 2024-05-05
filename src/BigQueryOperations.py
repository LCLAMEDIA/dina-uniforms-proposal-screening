from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import uuid
from datetime import datetime, timezone

class BigQueryOperations:
    def __init__(self, project_id, dataset_id, table_id):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)


    
    def set_status(self, task_id, status, progress, start_time=None, end_time=None):
        """
        Update or insert the status of a task.
        """
        rows_to_insert = [{
            "task_id": task_id,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "progress": progress
        }]

        errors = self.client.insert_rows_json(self.table_ref, rows_to_insert)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
            return False
        return True

    def get_status(self, task_id):
        """
        Retrieve the status of a task by task ID.
        """
        query = f"""
            SELECT task_id, status, start_time, end_time, progress
            FROM `{self.client.project}.{self.dataset_id}.{self.table_id}`
            WHERE task_id = @task_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("task_id", "STRING", task_id)
            ]
        )
        query_job = self.client.query(query, job_config=job_config)
        results = query_job.result()

        status_info = None
        for row in results:
            status_info = {
                "task_id": row.task_id,
                "status": row.status,
                "start_time": row.start_time,
                "end_time": row.end_time,
                "progress": row.progress
            }
        return status_info
    
# a = BigQueryOperations('dina-uniform-group','status','task_status')

# a.set_status(task_id=uuid.uuid4(), status='In Progress', start_time=datetime.now(timezone.utc).isoformat(), progess=0.1)