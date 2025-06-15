import os
import json
import uuid
import re
from string import Template
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class BiqQueryTest:

    def __init__(self, project=None, dataset=None):
        self.project = os.getenv('TEST_PROJECT_ID') or project
        self.dataset = os.getenv('TEST_DATASET') or dataset
        self.client = bigquery.Client(project=self.project)
        self.tables = {}  # dictionary to keep track of tables created for this test

        # create the test dataset if doesn't exist
        self.client.create_dataset(self.dataset, exists_ok=True)

        # clean up old test tables
        old_tables = self.client.list_tables(self.dataset)
        for table in old_tables:
            self.client.delete_table(table)

    def create_table(self, module, name, key=None, path="bigquery_schemas", use_root_path=False):
        """
        Creates a table from schema
        
        Args:
            module: Module name (used for path construction)
            name: Table name
            key: Key for table substitution (e.g., 'events_*')
            path: Path to schemas relative to src/{module}/ or root if use_root_path=True
            use_root_path: If True, path is relative to project root, not src/{module}/
        """
        table_name = key or name
        
        # Determine schema path
        if use_root_path:
            # Use path directly from project root
            schema_path = f'{ROOT_DIR}/{path}/{name}_schema.json'
            
        # Check if schema exists, if not try without _schema suffix
        if not os.path.exists(schema_path):
            alt_schema_path = schema_path.replace('_schema.json', '.json')
            if os.path.exists(alt_schema_path):
                schema_path = alt_schema_path
        
        schema = self.client.schema_from_json(schema_path)
        table_ref = f'{self.project}.{self.dataset}.{name}'
        table = bigquery.Table(table_ref, schema)
        self.client.create_table(table)
        self.tables[name] = {'table': table, 'key': key or name, 'table_name': table_name, 'table_ref': table_ref}
        return self.tables[name]

    # initialise table with json structured data
    def initialise_table(self, name, data=[]):
        if self.tables[name]:
            values = list(str(self.json_to_insert_values(row)) for row in data)
            insert_template = Template('INSERT `${table_ref}` (${field_names}) VALUES ${values}')
            insert_sql = insert_template.substitute({
                'table_ref': self.tables[name]["table_ref"],
                'field_names': ", ".join(data[0].keys()),
                'values': re.sub(r"'_|_'", "", ", ".join(values))  # '_ and _' patterns are removed
            })
            insert_sql = re.sub(r",\)", ")", insert_sql)  # strip out comma's after arrays
            insert_sql = re.sub(r"\\'", "'", insert_sql)  # remove escaped commas
            job = self.client.query(insert_sql)
            results = job.result()
            return results
        else:
            return False

    # private method to support json_to_insert_values
    def __handle_value_type(self, value):
        if isinstance(value, dict):
            return self.json_to_insert_values(value)
        if isinstance(value, list):
            return list(map(self.json_to_insert_values, value))
        return value

    # converts dictionary of values to sql insert values syntax
    def json_to_insert_values(self, data):
        return tuple(map(self.__handle_value_type, [data] if isinstance(data, str) else data.values()))

    # generate a query from a template
    def load_template(self, module, filename, overrides={}, use_root_path=False):
        """
        Load a template file
        
        Args:
            module: Module name (used for path construction if not use_root_path)
            filename: File name or path relative to root if use_root_path
            overrides: Dictionary of template variables to replace
            use_root_path: If True, filename is relative to project root
        """
        if use_root_path:
            template_path = f'{ROOT_DIR}/{filename}'
        else:
            template_path = f'{ROOT_DIR}/src/{module}/{filename}' if module else f'{ROOT_DIR}/{filename}'
            
        with open(template_path, 'r') as f:
            template = Template(f.read())
            return template.safe_substitute({
                'project_id': self.project,
                'dataset_id': self.dataset
            } | overrides)

    # start test empties the test tables
    def start_test(self):
        for table in self.tables:
            job = self.client.query(f'TRUNCATE TABLE `{self.tables[table]["table_ref"]}`')
            job.result()

    # runs a bigquery sql query
    def query(self, args, sql):
        # adjust sql with test table names
        for name in self.tables:
            sql = sql.replace(f'.{self.tables[name]["key"]}', f'.{self.tables[name]["table_name"]}')

        # prep args
        args = '; '.join(list(map(lambda arg: f'DECLARE {arg["name"]} {arg["type"]} DEFAULT {arg["value"]}', args)))
        x = f'{args}; {sql}'
        job = self.client.query(x)
        results = job.result()
        return results

    # return data from named table
    def get_dataframe(self, name):
        job = self.client.query(f'SELECT * FROM `{self.project}.{self.dataset}.{self.tables[name]["table_name"]}`;')
        return job.to_dataframe()

    # return data from named table
    def get_table_data(self, name):
        job = self.client.query(f'SELECT * FROM `{self.project}.{self.dataset}.{self.tables[name]["table_name"]}`;')
        return list(job.result())

    # get a column from table data
    def get_column(self, table_data, column):
        return [row[column] for row in table_data]

    # find a record in an array structure column
    def find_in_array(self, table_data, field, column, value=''):
        flatten = [item for sublist in self.get_column(table_data, field) for item in sublist if item[column] == value]
        return flatten.pop() if flatten else False

    # find a record in an array structure column
    def find_all_in_array(self, table_data, field, column):
        flatten = [item[column] for sublist in self.get_column(table_data, field) for item in sublist]
        return flatten.pop() if flatten else False

    # load a test sql fixture
    def load_sql_fixture(self, fixture_name, params=None, overrides=None):
        if overrides is None:
            overrides = {}
        if params is None:
            params = {}
        with open(f'{ROOT_DIR}/test/fixtures/{fixture_name}.sql', 'r') as f:
            template = Template(f.read())
            return template.safe_substitute({'table_ref': self.tables[fixture_name]['table_ref']} | params)

    # load a json file fixture template and apply parameters
    # override any properties before returning
    def load_fixture(self, fixture_name, params=None, overrides=None, add=None):
        if overrides is None:
            overrides = {}
        if params is None:
            params = {}
        with open(f'{ROOT_DIR}/test/fixtures/{fixture_name}.json', 'r') as f:
            template = Template(f.read())
            fixture = json.loads(template.safe_substitute(params))
            if isinstance(fixture, list):
                fixture[0] = fixture[0] | overrides
                if isinstance(add, list):
                    fixture = fixture + add
                if isinstance(add, dict):
                    fixture.append(add)
            if isinstance(fixture, dict):
                fixture = [fixture | overrides]
                if isinstance(add, list):
                    fixture = [fixture] + add
                if isinstance(add, dict):
                    fixture = [fixture, add]

            return fixture

    # initialise table from a fixture
    def initialise_table_from_fixture(self, name, fixture=None, params=None, overrides=None, add=None):
        fixture_data = self.load_fixture(fixture or name, params, overrides, add)
        self.initialise_table(name, fixture_data)
        return fixture_data