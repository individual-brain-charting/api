import utils as ibc

ibc.authenticate()

db = ibc.get_data_info(data_type="statistic_map")

filtered_db = ibc.filter_data(db, subject_list=["sub-08"], task_list=["Discount"])

dowloaded_db = ibc.download_data(filtered_db, download_dir="ibc_data", organise_by="session")