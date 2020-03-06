from src.api_client import APIClient
from src.storage_manager import StorageManager
from src.data_manager import DataManager

if __name__ == '__main__':
    api_client = APIClient(bus_or_tram='bus')
    storage_manager = StorageManager()
    data_manager = DataManager()

    api_client.get_data()
    df = data_manager.parse_api_response(api_client.response)
    storage_manager.upload_data(data_manager.parse_data_into_entities(df))