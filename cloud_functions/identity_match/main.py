import os
import json
import base64
from google.cloud import bigquery
from google.cloud import error_reporting

def identity_match(event, context):
    """Triggered by Pub/Sub message from GA4 export log sink."""
    
    # Get environment variables
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    region = os.environ.get('REGION', 'us-central1')
    
    bq_client = bigquery.Client()
    error_client = error_reporting.Client()
    
    try:
        # Parse the Pub/Sub message if needed
        latest_update_date = 'NULL'
        if 'data' in event:
            try:
                message = base64.b64decode(event['data']).decode('utf-8')
                data = json.loads(message)
                latest_update_date = data.get('latest_update_date', 'NULL')
            except:
                # If message parsing fails, use default
                pass
        
        # Call the stored procedure
        if latest_update_date == 'NULL':
            proc_query = f"""
            CALL `{project_id}.{dataset_id}.update_identity_match`(NULL)
            """
        else:
            proc_query = f"""
            CALL `{project_id}.{dataset_id}.update_identity_match`('{latest_update_date}')
            """
        
        job = bq_client.query(proc_query, location=region)
        job.result()  # Wait for the job to complete
        
        print(f"Job {job.job_id} completed successfully")
        return f"Job: {job.job_id} has completed"
        
    except Exception as e:
        error_client.report_exception()
        print(f"Error: {str(e)}")
        raise