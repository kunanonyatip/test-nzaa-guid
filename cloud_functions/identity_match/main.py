from google.cloud import bigquery
from google.cloud import error_reporting

def identity_match(event, context):
    """Triggered by Pub/Sub message from GA4 export log sink."""
    
    bq_client = bigquery.Client()
    error_client = error_reporting.Client()
    
    try:
        # Call the stored procedure with parameter
        proc_query = f"""
        CALL `${project_id}.${dataset_id}.update_identity_match`(${latest_update_date})
        """
        
        job = bq_client.query(proc_query, location="${region}")
        job.result()
        
        return "Job: {} has completed".format(job.job_id)
        
    except:
        error_client.report_exception()
        return "Error: {}".format(job.exception(100))