from ibc_api import utils as ibc

# Fetch info on all available files
# Load as a pandas dataframe and save as ibc_data/available_{data_type}.csv
db = ibc.get_info(data_type="volume_maps")

# Keep statistic maps for sub-08, for task-Discount
filtered_db = ibc.filter_data(db, subject_list=["08"], task_list=["Lec1"])

# Download all statistic maps for sub-08, task-Lec1
# Also creates ibc_data/downloaded_volume_maps.csv
# which contains local file paths and time of download
downloaded_db = ibc.download_data(filtered_db, n_jobs=2)
