import os
from google.cloud import bigquery
import functions_framework

@functions_framework.cloud_event
def identity_match(cloud_event):
    """Triggered by Pub/Sub message from GA4 export log sink."""
    
    # Get environment variables
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('DATASET_ID')
    
    print(f"Function triggered - Project: {project_id}, Dataset: {dataset_id}")
    
    client = bigquery.Client()
    
    query = f"CALL `{project_id}.{dataset_id}.update_identity_match`()"
    
    try:
        print(f"Executing: {query}")
        job = client.query(query)
        job.result()
        
        print(f"Job {job.job_id} completed successfully")
        return {'status': 'success', 'job_id': job.job_id}
        
    except Exception as e:
        print(f"Error: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return {'error': str(e)}, 500