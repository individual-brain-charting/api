import ibc_api.utils as ibc

# Fetch info on all available files
# Load as a pandas dataframe and save as ibc_data/available_{data_type}.csv 
db = ibc.get_info(data_type="volume_maps")

# Keep statistic maps for sub-08, for task-Discount
filtered_db = ibc.filter_data(db, subject_list=[8], task_list=["Discount"])

# Authenticate with EBRAINS before downloading
# Returns a link that takes you to EBRAINS login page and stores an access token locally
ibc.authenticate()

# Download all statistic maps for sub-08, task-Discount 
# Also creates ibc_data/downloaded_volume_maps.csv 
# which contains local file paths and time of download
downloaded_db = ibc.download_data(filtered_db)