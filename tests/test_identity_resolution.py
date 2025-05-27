import pytest
from google.cloud import bigquery
from datetime import datetime, timedelta

class TestIdentityResolution:
    
    @pytest.fixture
    def bq_client(self):
        return bigquery.Client()
    
    def test_initial_load(self, bq_client):
        """Test initial data load"""
        # Run procedure with historical date
        query = """
        CALL `{project-id}.identity_resolution_staging.update_identity_match`('20250521')
        """
        job = bq_client.query(query)
        job.result()
        
        # Verify data loaded
        count_query = """
        SELECT COUNT(*) as cnt 
        FROM `{project-id}.identity_resolution_staging.identity_match`
        """
        results = list(bq_client.query(count_query))
        assert results[0].cnt > 0
    
    def test_incremental_update(self, bq_client):
        """Test incremental updates"""
        # Get current count
        before_query = """
        SELECT COUNT(*) as cnt 
        FROM `{project-id}.identity_resolution_staging.identity_match`
        WHERE updated_date IS NOT NULL
        """
        before_count = list(bq_client.query(before_query))[0].cnt
        
        # Run update
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        update_query = f"""
        CALL `{project-id}.identity_resolution_staging.update_identity_match`('{yesterday}')
        """
        job = bq_client.query(update_query)
        job.result()
        
        # Verify updates
        after_count = list(bq_client.query(before_query))[0].cnt
        assert after_count >= before_count
    
    def test_alternate_ids(self, bq_client):
        """Test alternate identity matching"""
        query = """
        SELECT 
          alternate_id_type,
          COUNT(*) as cnt
        FROM `{project-id}.identity_resolution_staging.alternate_identity_match`
        GROUP BY alternate_id_type
        """
        results = bq_client.query(query)
        
        expected_types = {'floodlight_id', 'gads_id', 'fb_id', 'tiktok_id'}
        actual_types = {row.alternate_id_type for row in results}
        
        assert expected_types.issubset(actual_types)