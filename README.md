# api
API to fetch publicly available IBC data 

# Install
Clone this repo, switch to the cloned repo and install the package with pip (needs latest pip version)
```bash
git clone git@github.com:individual-brain-charting/api.git
cd api
pip install --upgrade pip
pip install -e .
```

# Usage
* The first step to access IBC data would be to register with EBRAINS here: https://ebrains.eu/register/

* Once you have an EBRAINS account, you're good to go

* A minimal example usage is given in `example.py` and below:
```python
import ibc_api.utils as ibc

# Returns a link that takes you to EBRAINS login page and stores an access token locally
ibc.authenticate()

# Fetch info on all available files
# Load as a pandas dataframe and save as ibc_data/available_statistic_map.csv 
db = ibc.get_info(data_type="statistic_map")

# Keep statistic maps for sub-08, for task-Discount
filtered_db = ibc.filter_data(db, subject_list=["sub-08"], task_list=["Discount"])

# Download all statistic maps for sub-08, task-Discount 
# Saved under ibc_data/resulting_smooth_maps/sub-08/task-Discount
# Also create ibc_data/downloaded_statistic_map.csv 
# This contains downloaded file paths and time of download
downloaded_db = ibc.download_data(filtered_db, organise_by='task')
```
