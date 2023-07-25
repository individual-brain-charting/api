import ibc_api.utils as ibc

# Returns a link that takes you to EBRAINS login page and stores an access token locally
ibc.authenticate()

# Fetch info on all available files
# Load as a pandas dataframe and save as ibc_data/available_{data_type}.csv 
db = ibc.get_info(data_type="volume_maps")

# Keep statistic maps for sub-08, for task-Discount
filtered_db = ibc.filter_data(db, subject_list=["sub-08"], task_list=["Discount"])

# Download all statistic maps for sub-08, task-Discount 
# Saved under ibc_data/resulting_smooth_maps/sub-08/task-Discount
# Also creates ibc_data/downloaded_volume_maps.csv 
# which contains downloaded file paths and time of download
downloaded_db = ibc.download_data(filtered_db, organise_by='task')