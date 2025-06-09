import os
import pytest
import json
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound
import time


class TestIdentityResolution:
    """Test suite for identity resolution system."""
    
    @pytest.fixture(scope="class")
    def bq_client(self):
        """Create BigQuery client."""
        return bigquery.Client()
    
    @pytest.fixture(scope="class")
    def pubsub_client(self):
        """Create Pub/Sub client."""
        return pubsub_v1.PublisherClient()
    
    @pytest.fixture(scope="class")
    def test_config(self):
        """Test configuration."""
        return {
            'project_id': os.environ.get('PROJECT_ID', 'em-identity-graph'),
            'dataset_id': os.environ.get('DATASET_ID', 'identity_resolution_staging'),
            'ga4_project': os.environ.get('GA4_PROJECT', 'em-sandbox-455802'),
            'ga4_dataset': os.environ.get('GA4_DATASET', 'analytics_XXXXXXXXX'),
            'topic_name': os.environ.get('TOPIC_NAME', 'ga4-export-identity-resolution-staging'),
            'function_name': os.environ.get('FUNCTION_NAME', 'identity-match-staging')
        }
    
    @pytest.fixture
    def test_data(self):
        """Generate test data for each test."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return {
            'test_id': f"test_{timestamp}",
            'hashed_email': f"test_hash_{timestamp}",
            'ga_id': f"test_ga_{timestamp}",
            'fb_id': f"FB_TEST_{timestamp}",
            'tiktok_id': f"TT_TEST_{timestamp}",
            'reddit_id': f"RD_TEST_{timestamp}"
        }
    
    def test_dataset_exists(self, bq_client, test_config):
        """Test that BigQuery dataset exists."""
        dataset_ref = f"{test_config['project_id']}.{test_config['dataset_id']}"
        
        try:
            dataset = bq_client.get_dataset(dataset_ref)
            assert dataset.dataset_id == test_config['dataset_id']
        except NotFound:
            pytest.fail(f"Dataset {dataset_ref} not found")
    
    def test_identity_match_table_schema(self, bq_client, test_config):
        """Test identity_match table schema is correct."""
        table_ref = f"{test_config['project_id']}.{test_config['dataset_id']}.identity_match"
        table = bq_client.get_table(table_ref)
        
        # Check required fields
        field_names = {field.name for field in table.schema}
        required_fields = {'id', 'hashed_email', 'ga_id', 'created_date', 'updated_date'}
        missing_fields = required_fields - field_names
        assert not missing_fields, f"Missing required fields: {missing_fields}"
        
        # Check field types
        field_types = {field.name: field.field_type for field in table.schema}
        assert field_types['id'] == 'STRING'
        assert field_types['hashed_email'] == 'STRING'
        assert field_types['ga_id'] == 'STRING'
        assert field_types['created_date'] == 'DATETIME'
        assert field_types['updated_date'] == 'DATETIME'
        
        # Check field modes
        field_modes = {field.name: field.mode for field in table.schema}
        assert field_modes['id'] == 'REQUIRED'
        assert field_modes['hashed_email'] == 'REQUIRED'
        assert field_modes['ga_id'] == 'REQUIRED'
        assert field_modes['created_date'] == 'REQUIRED'
        assert field_modes['updated_date'] == 'REPEATED'
    
    def test_alternate_identity_match_table_schema(self, bq_client, test_config):
        """Test alternate_identity_match table schema is correct."""
        table_ref = f"{test_config['project_id']}.{test_config['dataset_id']}.alternate_identity_match"
        table = bq_client.get_table(table_ref)
        
        # Check required fields
        field_names = {field.name for field in table.schema}
        required_fields = {'hashed_email', 'alternate_id', 'alternate_id_type', 'updated_date'}
        missing_fields = required_fields - field_names
        assert not missing_fields, f"Missing required fields: {missing_fields}"
        
        # Check field modes
        field_modes = {field.name: field.mode for field in table.schema}
        assert field_modes['hashed_email'] == 'REQUIRED'
        assert field_modes['alternate_id'] == 'REPEATED'
        assert field_modes['alternate_id_type'] == 'REQUIRED'
        assert field_modes['updated_date'] == 'REPEATED'
    
    def test_stored_procedure_exists(self, bq_client, test_config):
        """Test that stored procedure exists and has correct signature."""
        query = f"""
        SELECT 
            routine_name,
            routine_type,
            language
        FROM `{test_config['project_id']}.{test_config['dataset_id']}.INFORMATION_SCHEMA.ROUTINES`
        WHERE routine_name = 'update_identity_match'
        """
        
        results = list(bq_client.query(query))
        assert len(results) == 1
        assert results[0].routine_name == 'update_identity_match'
        assert results[0].routine_type == 'PROCEDURE'
        assert results[0].language == 'SQL'
    
    def test_insert_identity_match_record(self, bq_client, test_config, test_data):
        """Test inserting a record into identity_match table."""
        query = f"""
        INSERT INTO `{test_config['project_id']}.{test_config['dataset_id']}.identity_match`
        (id, hashed_email, ga_id, created_date, updated_date)
        VALUES
        (GENERATE_UUID(), '{test_data['hashed_email']}', '{test_data['ga_id']}', 
         CURRENT_DATETIME(), [CURRENT_DATETIME()])
        """
        
        job = bq_client.query(query)
        job.result()  # Wait for completion
        
        # Verify insertion
        verify_query = f"""
        SELECT COUNT(*) as count
        FROM `{test_config['project_id']}.{test_config['dataset_id']}.identity_match`
        WHERE hashed_email = '{test_data['hashed_email']}'
        """
        
        results = list(bq_client.query(verify_query))
        assert results[0].count == 1
        
        # Cleanup
        cleanup_query = f"""
        DELETE FROM `{test_config['project_id']}.{test_config['dataset_id']}.identity_match`
        WHERE hashed_email = '{test_data['hashed_email']}'
        """
        bq_client.query(cleanup_query).result()
    
    def test_insert_alternate_identity_record(self, bq_client, test_config, test_data):
        """Test inserting a record into alternate_identity_match table."""
        query = f"""
        INSERT INTO `{test_config['project_id']}.{test_config['dataset_id']}.alternate_identity_match`
        (hashed_email, alternate_id, alternate_id_type, updated_date)
        VALUES
        ('{test_data['hashed_email']}', 
         ['{test_data['fb_id']}', '{test_data['fb_id']}_2'], 
         'fb_id', 
         [CURRENT_DATETIME()])
        """
        
        job = bq_client.query(query)
        job.result()
        
        # Verify insertion
        verify_query = f"""
        SELECT 
            hashed_email,
            ARRAY_LENGTH(alternate_id) as id_count,
            alternate_id_type
        FROM `{test_config['project_id']}.{test_config['dataset_id']}.alternate_identity_match`
        WHERE hashed_email = '{test_data['hashed_email']}'
        """
        
        results = list(bq_client.query(verify_query))
        assert len(results) == 1
        assert results[0].id_count == 2
        assert results[0].alternate_id_type == 'fb_id'
        
        # Cleanup
        cleanup_query = f"""
        DELETE FROM `{test_config['project_id']}.{test_config['dataset_id']}.alternate_identity_match`
        WHERE hashed_email = '{test_data['hashed_email']}'
        """
        bq_client.query(cleanup_query).result()
    
    def test_stored_procedure_execution(self, bq_client, test_config):
        """Test executing the stored procedure."""
        # Execute with NULL parameter
        query = f"""
        CALL `{test_config['project_id']}.{test_config['dataset_id']}.update_identity_match`(NULL)
        """
        
        job = bq_client.query(query)
        results = job.result()
        
        # Check results
        result_list = list(results)
        assert len(result_list) > 0
        
        # Verify result contains expected fields
        first_row = result_list[0]
        assert 'status' in dict(first_row)
        assert 'Processing completed' in dict(first_row)['status']
    
    def test_pubsub_topic_exists(self, pubsub_client, test_config):
        """Test that Pub/Sub topic exists."""
        topic_path = pubsub_client.topic_path(test_config['project_id'], test_config['topic_name'])
        
        try:
            # Try to get the topic
            from google.cloud import pubsub_v1
            publisher = pubsub_v1.PublisherClient()
            topic = publisher.get_topic(request={"topic": topic_path})
            assert topic.name == topic_path
        except Exception as e:
            pytest.fail(f"Pub/Sub topic not found: {e}")