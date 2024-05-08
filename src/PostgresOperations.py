import psycopg2
from psycopg2 import sql
import uuid
from datetime import datetime
import os

class PostgresOperations:
    def __init__(self, dbname, user, password, host='localhost', port=5432):
        self.connection = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self.cursor = self.connection.cursor()

    def create_table_if_not_exists(self):
        """
        Create the task_status table if it does not already exist.
        """
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_status (
                task_id VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50),
                progress FLOAT,
                start_time TIMESTAMP,
                end_time TIMESTAMP
            );
        """)
        self.connection.commit()

    def initiate_run(self):
        """
        Instantiate a new run by generating a UUID, setting status to 'In Progress',
        start time to now, and progress to 0.01.
        """
        task_id = str(uuid.uuid4())
        status = 'In Progress'
        progress = 0.1
        start_time = datetime.now()

        self.set_status(task_id, status, progress, start_time=start_time)

        return task_id

    def set_status(self, task_id, status, progress, start_time=None, end_time=None):
        """
        Insert or update a task's status.
        """
        query = sql.SQL("""
        INSERT INTO task_status (task_id, status, progress, start_time, end_time)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (task_id) DO UPDATE SET
        status = EXCLUDED.status,
        progress = EXCLUDED.progress,
        start_time = EXCLUDED.start_time,
        end_time = EXCLUDED.end_time;
        """)
        self.cursor.execute(query, (task_id, status, progress, start_time, end_time))
        self.connection.commit()

    def get_status(self, task_id):
        """
        Fetch the status and progress of a task by task_id and return as a dictionary.
        """
        query = "SELECT status, progress FROM task_status WHERE task_id = %s;"
        self.cursor.execute(query, (task_id,))
        result = self.cursor.fetchone()
        if result:
            status, progress = result
            return {"task_id": task_id, "status": status, "progress": progress}
        else:
            return {"task_id": task_id, "status": "Not Found", "progress": 0.0}


    def complete_task(self, task_id):
        """
        Finalize the task by setting its status to 'Done', progress to 100, and end_time to current timestamp.
        """
        query = sql.SQL("""
        UPDATE task_status SET status = 'Done', progress = 1.00, end_time = CURRENT_TIMESTAMP
        WHERE task_id = %s;
        """)
        self.cursor.execute(query, (task_id,))
        self.connection.commit()

    def __del__(self):
        self.cursor.close()
        self.connection.close()
