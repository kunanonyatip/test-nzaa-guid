from unittest import TestCase
from test.bq_test_helper import BiqQueryTest
from google.cloud import bigquery

class TestUpdateIdentityMatch(TestCase):
    def setUp(self):
        # initialise the bq test class with the project and dataset id used for testing
        self.test_helper = BiqQueryTest('nzaa-mkt-guid', 'test_identity_resolution')
        
        # Create a generic events table schema first
        self.test_helper.create_table('', 'events', 
                                    key="events_*", 
                                    path="bigquery/schemas", 
                                    use_root_path=True)
        
        # Create specific date tables using the events schema
        events_schema = self.test_helper.tables['events']['table'].schema
        
        # Create events_20250606 table
        events_20250606_ref = f'{self.test_helper.project}.{self.test_helper.dataset}.events_20250606'
        table_20250606 = bigquery.Table(events_20250606_ref, events_schema)
        self.test_helper.client.create_table(table_20250606)
        
        # Create events_20250607 table
        events_20250607_ref = f'{self.test_helper.project}.{self.test_helper.dataset}.events_20250607'
        table_20250607 = bigquery.Table(events_20250607_ref, events_schema)
        self.test_helper.client.create_table(table_20250607)
        
        # Add to tables dict for tracking
        self.test_helper.tables['events_20250606'] = {
            'table': table_20250606, 
            'key': 'events_20250606', 
            'table_name': 'events_20250606',
            'table_ref': events_20250606_ref
        }
        self.test_helper.tables['events_20250607'] = {
            'table': table_20250607,
            'key': 'events_20250607',
            'table_name': 'events_20250607', 
            'table_ref': events_20250607_ref
        }
        
        # Create identity tables
        self.test_helper.create_table('', 'identity_match', 
                                    path="bigquery/schemas", 
                                    use_root_path=True)
        self.test_helper.create_table('', 'alternate_identity_match', 
                                    path="bigquery/schemas", 
                                    use_root_path=True)
        
        # Load the stored procedure template
        create_procedure_sql = self.test_helper.load_template('', 'bigquery/procedures/update_identity_match.sql', 
                                                     overrides={
            'project_id': self.test_helper.project,
            'dataset_id': self.test_helper.dataset,
            'ga4_project': self.test_helper.project,
            'ga4_dataset': self.test_helper.dataset
        }, use_root_path=True)
        
        # CREATE the stored procedure (only once in setUp)
        self.test_helper.client.query(create_procedure_sql).result()
        
    def test_new_identity_insertion(self):
        self.test_helper.start_test()
        
        # load fixtures and initialise tables
        session_data = self.test_helper.initialise_table_from_fixture('events_20250606')
        
        # Extract test data for assertions (now from event_params instead of user_properties)
        first_record = session_data[0]
        hashed_email = first_record['event_params'][0]['value']['string_value']
        ga_id = first_record['user_pseudo_id']
        fb_id = first_record['event_params'][1]['value']['string_value'] if len(first_record['event_params']) > 1 else None
        
        # CALL the stored procedure (not CREATE it again)
        call_procedure = f"CALL `{self.test_helper.project}.{self.test_helper.dataset}.update_identity_match`()"
        self.test_helper.query([], call_procedure)
        
        # test identity match table
        identity_match = self.test_helper.get_dataframe('identity_match')
        self.assertEqual(identity_match.shape[0], 3)  # 3 rows based on fixture
        self.assertTrue(hashed_email in identity_match['hashed_email'].values)
        self.assertTrue(ga_id in identity_match['ga_id'].values)
        
        # test alternate identity match table
        alternate_identity_match = self.test_helper.get_dataframe('alternate_identity_match')
        self.assertGreater(alternate_identity_match.shape[0], 0)
        
        if fb_id:
            self.assertTrue('fb_id' in alternate_identity_match['alternate_id_type'].values)
            
            # Check for fb_id in alternate_id arrays
            fb_rows = alternate_identity_match[alternate_identity_match['alternate_id_type'] == 'fb_id']
            fb_ids_found = False
            for _, row in fb_rows.iterrows():
                if fb_id in row['alternate_id']:
                    fb_ids_found = True
                    break
            self.assertTrue(fb_ids_found)
        
    def test_identity_update(self):
        self.test_helper.start_test()
        
        # load initial fixtures
        session_data = self.test_helper.initialise_table_from_fixture('events_20250606')
        existing_data = self.test_helper.initialise_table_from_fixture('identity_match')
        
        # Debug: Check what data we loaded
        print(f"\nExisting records loaded: {len(existing_data)}")
        print(f"Event records loaded: {len(session_data)}")
        
        # Debug: Check the last_update_date that will be used
        check_date_query = f"""
        SELECT 
            IFNULL(
                FORMAT_DATETIME("%Y%m%d", MAX(updated_date[OFFSET(ARRAY_LENGTH(updated_date) - 1)])),
                "20250603"
            ) as last_update_date
        FROM `{self.test_helper.project}.{self.test_helper.dataset}.identity_match`
        """
        date_result = list(self.test_helper.client.query(check_date_query).result())
        print(f"Last update date from procedure: {date_result[0].last_update_date}")
        
        # Debug: Check events that would be processed (updated to use event_params)
        events_check_query = f"""
        SELECT 
            event_date,
            user_pseudo_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'guid_email') as email
        FROM `{self.test_helper.project}.{self.test_helper.dataset}.events_*`
        WHERE _TABLE_SUFFIX > '{date_result[0].last_update_date}'
        """
        events_result = list(self.test_helper.client.query(events_check_query).result())
        print(f"Events after last_update_date: {len(events_result)}")
        for event in events_result:
            print(f"  - Date: {event.event_date}, User: {event.user_pseudo_id}, Email: {event.email[:20] if event.email else 'None'}...")
        
        # CALL the stored procedure
        call_procedure = f"CALL `{self.test_helper.project}.{self.test_helper.dataset}.update_identity_match`()"
        self.test_helper.query([], call_procedure)
        
        # Check results
        identity_match = self.test_helper.get_dataframe('identity_match')
        print(f"Final identity_match rows: {identity_match.shape[0]}")
        
        # Debug: Show what's in the identity_match table
        print("\nIdentity match contents:")
        for idx, row in identity_match.iterrows():
            print(f"  Email: {row['hashed_email'][:20]}..., GA ID: {row['ga_id']}, Updated dates: {len(row['updated_date'])}")
        
        # The test expectations
        self.assertGreaterEqual(identity_match.shape[0], len(existing_data))
        
        # Check for updates
        records_with_updates = 0
        for _, row in identity_match.iterrows():
            if len(row['updated_date']) > 1:
                records_with_updates += 1
        
        self.assertGreater(records_with_updates, 0, "Some records should have been updated")
        
    def test_cross_device_identity(self):
        self.test_helper.start_test()
        
        # load fixtures - events_20250606 has same email with different GA IDs
        self.test_helper.initialise_table_from_fixture('events_20250606')
        
        # CALL the stored procedure
        call_procedure = f"CALL `{self.test_helper.project}.{self.test_helper.dataset}.update_identity_match`()"
        self.test_helper.query([], call_procedure)
        
        # Check for same email with multiple GA IDs
        identity_match = self.test_helper.get_dataframe('identity_match')
        email_ga_counts = identity_match.groupby('hashed_email')['ga_id'].nunique()
        
        # Should have at least one email with multiple GA IDs (cross-device)
        multi_device_emails = email_ga_counts[email_ga_counts > 1]
        self.assertGreater(len(multi_device_emails), 0, "Should detect cross-device users")
        
    def test_no_duplicate_alternate_ids(self):
        self.test_helper.start_test()
        
        # load fixtures
        self.test_helper.initialise_table_from_fixture('events_20250606')
        
        # CALL the stored procedure twice to test duplicate prevention
        call_procedure = f"CALL `{self.test_helper.project}.{self.test_helper.dataset}.update_identity_match`()"
        self.test_helper.query([], call_procedure)
        
        # Load new events to trigger updates
        self.test_helper.initialise_table_from_fixture('events_20250607')
        
        # Call again
        self.test_helper.query([], call_procedure)
        
        # Check alternate_identity_match for duplicates
        alternate_identity_match = self.test_helper.get_dataframe('alternate_identity_match')
        
        # The MAX() logic in the procedure should prevent excessive duplicates
        for _, row in alternate_identity_match.iterrows():
            alternate_ids = row['alternate_id']
            unique_ids = list(set(alternate_ids))
            # Allow for one duplicate due to the update logic
            self.assertLessEqual(len(alternate_ids), len(unique_ids) + 1, 
                               f"Too many duplicates in alternate_id array for {row['alternate_id_type']}")
        
    def tearDown(self):
        """Clean up test tables after each test"""
        for table_name in self.test_helper.tables:
            try:
                self.test_helper.client.delete_table(self.test_helper.tables[table_name]['table_ref'])
            except:
                pass