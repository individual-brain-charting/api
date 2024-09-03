# api
API to fetch publicly available IBC data 

# Install
Install this package as follows:
```bash
pip install git+https://github.com/individual-brain-charting/api.git#egg=ibc_api
```

# Usage
* The first step to access IBC data would be to register with EBRAINS here: https://ebrains.eu/register/

* Once you have an EBRAINS account, you're good to go

* A minimal example usage is given in `example.py`:
```python
import ibc_api.utils as ibc

# Fetch info on all available files
# Load as a pandas dataframe and save as ibc_data/available_{data_type}.csv 
db = ibc.get_info(data_type="volume_maps")

# Keep statistic maps for sub-08, for task-Discount
filtered_db = ibc.filter_data(db, subject_list=["08"], task_list=["Lec1"])

# Download all statistic maps for sub-08, task-Lec1 using 2 parallel jobs
# Also creates ibc_data/downloaded_volume_maps.csv 
# which contains local file paths and time of download
downloaded_db = ibc.download_data(filtered_db, n_jobs=2)
```
# Note
Since this API is under active development, make sure to update it regularly
```bash
pip install -U git+https://github.com/individual-brain-charting/api.git#egg=ibc_api
```