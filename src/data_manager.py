import ast
import configparser
import pandas as pd

cnf = configparser.ConfigParser()
cnf.read('configuration.cfg')

COL_TIME = cnf.get('namespace', 'col_time')
COL_LINE = cnf.get('namespace', 'col_line')
COL_VEHICLE_NUMBER = cnf.get('namespace', 'col_vehicle_number')
COL_LATITUDE = cnf.get('namespace', 'col_latitude')
COL_LONGITUDE = cnf.get('namespace', 'col_longitude')
COL_BRIGADE = cnf.get('namespace', 'col_brigade')
COL_PARTITION_KEY = cnf.get('namespace', 'col_partition_key')
COL_ROW_KEY = cnf.get('namespace', 'col_row_key')


def print_when_df_conversion_failed():
    print(f'Conversion of the response content to DataFrame failed.')


class DataManager:
    def __init__(self):
        self.data = None

    @staticmethod
    def parse_api_response(response):
        # Convert response content into DataFrame
        try:
            return pd.DataFrame(ast.literal_eval(response.content.decode('utf8').strip())['result'])
        except SyntaxError:
            print_when_df_conversion_failed()

    @staticmethod
    def parse_data_into_entities(data):
        # Initialize entities
        entities = list()

        # Convert DataFrame to dict and rename some keys
        for record in data.to_dict(orient='records'):
            record[COL_PARTITION_KEY] = record.pop(COL_VEHICLE_NUMBER)
            record[COL_ROW_KEY] = record.pop(COL_TIME)
            entities.append(record)

        return entities

    @staticmethod
    def parse_entities_into_data(entities):
        # Initialize list of records
        records = list()

        # Extract each record as a dict and rename some keys
        for entity in list(entities):
            record = {k: v for k, v in list(entity.items())}
            record[COL_VEHICLE_NUMBER] = record.pop(COL_PARTITION_KEY)
            record[COL_TIME] = record.pop(COL_ROW_KEY)
            record.pop('Timestamp')
            record.pop('etag')
            records.append(record)

        # Convert to DataFrame and return
        return pd.DataFrame(records)


if __name__ == '__main__':
    pass