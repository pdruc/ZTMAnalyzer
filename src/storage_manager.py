import sys
import time
import configparser
from azure.cosmosdb.table.tableservice import TableService

cnf = configparser.ConfigParser()
cnf.read('configuration.cfg')


def print_uploading_state(num_last_entity, num_entities, table_name):
    sys.stdout.write(f'\r{num_last_entity} from {num_entities} entities uploaded to a table named {table_name}.')


def print_successful_upload_state(num_entities, table_name, elapsed_time):
    print(f'\r{num_entities} entities uploaded to a table named {table_name} in {elapsed_time:.2f} seconds.')


class StorageManager:
    def __init__(self, table_name=None):
        self.azure_storage_name = cnf.get('credentials', 'azure_storage_name')
        self.azure_storage_key = cnf.get('credentials', 'azure_storage_key')
        self.table_service = TableService(account_name=self.azure_storage_name, account_key=self.azure_storage_key)
        self.table_name = table_name if table_name is not None else cnf.get('resources', 'table_name')

    def create_table(self):
        self.table_service.create_table(self.table_name)

    def upload_data(self, entities):
        # Count records to upload
        num_entities = len(entities)

        # Upload record by record and print info
        time_start = time.time()
        for i, entity in enumerate(entities):
            self.table_service.insert_or_replace_entity(self.table_name, entity)
            print_uploading_state(i + 1, num_entities, self.table_name)
        print_successful_upload_state(num_entities, self.table_name, time.time() - time_start)

    def query_entities(self, query_filter=None, query_selector=None):
        return self.table_service.query_entities(self.table_name, filter=query_filter, select=query_selector)


if __name__ == '__main__':
    pass
