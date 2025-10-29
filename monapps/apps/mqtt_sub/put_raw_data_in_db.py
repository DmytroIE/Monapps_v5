from services.raw_data_processor import RawDataProcessor


def put_raw_data_in_db(dev_ui, payload: dict):
    raw_data_processor = RawDataProcessor(dev_ui, payload)
    raw_data_processor.execute()
