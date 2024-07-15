"""API to fetch IBC data from EBRAINS via Human Data Gateway using siibra. 
"""

# %$
import siibra
from siibra.retrieval.repositories import EbrainsHdgConnector
from siibra.retrieval.requests import EbrainsRequest, SiibraHttpRequestError
import os
from tqdm import tqdm
import nibabel
from siibra.retrieval.cache import CACHE
import pandas as pd
from datetime import datetime

# from . import metadata as md
import metadata as md
import json
import numpy as np
from joblib import Parallel, delayed
import pdb

# clear cache
CACHE.clear()

# dataset ids on ebrains
METADATA = md.fetch_metadata()

# all subjects in IBC dataset
SUBJECTS = md.SUBJECTS

# token root directory
TOKEN_ROOT = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(TOKEN_ROOT, exist_ok=True)


def _authenticate(token_dir=TOKEN_ROOT):
    """This function authenticates you to EBRAINS. It would return a link that
    would prompt you to login or create an EBRAINS account. Read more about
    registering for EBRAINS here: https://ebrains.eu/register.
    Once authenticated, it would store an access token locally in a token file and
    use that token to connect to EBRAINS in the future. If the token expires, it
    would prompt you to login again.
    """

    # read the token file
    token_file = os.path.join(token_dir, "token")
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            token = f.read()
            # set the token
            siibra.set_ebrains_token(token)
    else:
        siibra.fetch_ebrains_token()
        token = EbrainsRequest._KG_API_TOKEN
        # save the token
        with open(token_file, "w") as f:
            f.write(token)

    f.close()

    return token_file


def _connect_ebrains(data_type="volume_maps", metadata=METADATA, version=None):
    """Connect to given IBC dataset on EBRAINS via Human Data Gateway.

    Parameters
    ----------
    data_type : str, optional
        dataset to fetch, by default "statistic_map", can be one of
        ["volume_maps", "surface_maps", "preprocessed", "raw]

    metadata : dict, optional
        dictionary object containing version info, dataset ids etc, by default
        METADATA

    version : int, optional
        version of the dataset to select, starts from 1, by default None

    Returns
    -------
    EbrainsHdgConnector
        connector to the dataset
    """
    # get the dataset id
    dataset = md.select_dataset(data_type, metadata, version)
    dataset_id = dataset["id"]

    # authenticate with ebrains
    token_file = _authenticate()

    try:
        return EbrainsHdgConnector(dataset_id)
    except AttributeError:
        raise ValueError(
            f"Unable to fetch dataset {data_type}, version {version} from EBRAINS."
        )
    except SiibraHttpRequestError:
        print("Saved token is invalid. Fetching a new token.")
        # delete the token file
        os.remove(token_file)
        # try connecting again
        return _connect_ebrains(data_type, metadata, version)


def _create_root_dir(dir_path=None):
    """Create a root directory ibc_data to store all downloaded data.

    Parameters
    ----------
    dir_path : str or None, optional
        path upto the root directory, by default None, if None creates
        ibc_data in the current directory

    Returns
    -------
    str
        path to the root directory
    """

    if dir_path is None:
        dir_path = "ibc_data"
    else:
        dir_path = os.path.join(dir_path, "ibc_data")
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

    return dir_path


def download_gm_mask(resolution=1.5, save_to=None):
    """Download the grey matter mask

    Parameters
    ----------
    resolution : float, optional
        resolution of the mask, by default 1.5
    save_to : str, optional
        where to save the mask, by default None

    Returns
    -------
    save_as : str
        path to the downloaded mask
    """

    if resolution == 1.5:
        mask = "gm_mask_1_5mm.nii.gz"
    else:
        mask = "gm_mask_3mm.nii.gz"

    remote_file = "https://api.github.com/repos/individual-brain-charting/public_analysis_code/contents/ibc_data"
    remote_file = os.path.join(remote_file, mask)
    # save the database file
    save_to = _create_root_dir(save_to)
    save_as = os.path.join(save_to, mask)

    # use curl with github api to download the file
    os.system(
        f"curl -s -S -L -H 'Accept: application/vnd.github.v4.raw' -H 'X-GitHub-Api-Version: 2022-11-28' {remote_file} -o '{save_as}'"
    )

    return save_as


def get_info(data_type="volume_maps", save_to=None, metadata=METADATA):
    """Fetch a csv file describing each file in a given IBC dataset on EBRAINS.

    Parameters
    ----------
    data_type : str, optional
        dataset to fetch, by default "volume_maps", one of
        ["volume_maps", "surface_maps", "raw", "preprocessed"]
    save_as : str or None, optional
        filename to save this csv as, by default None, if None saves as
        "ibc_data/available_{data_type}.csv"

    Returns
    -------
    pandas.DataFrame
        dataframe with information about each file in the dataset
    """
    # file with all information about the dataset
    db_file = md.fetch_dataset_db(data_type, metadata)
    # load the file as dataframe
    # convert subject, session and run to string to avoid losing leading zeros
    db = pd.read_csv(
        db_file, converters={"subject": str, "session": str, "run": str}
    )
    db.drop(columns=["Unnamed: 0"], inplace=True, errors="ignore")
    # save the database file
    save_to = _create_root_dir(save_to)
    save_as = os.path.join(save_to, f"available_{data_type}.csv")
    db.to_csv(save_as)
    return db


def filter_data(db, subject_list=SUBJECTS, task_list=False):
    """Filter the dataframe to only include certain subjects and tasks.

    Parameters
    ----------
    db : pandas.DataFrame
        dataframe with information about all files in the dataset
    subject_list : list, optional
        list of subjects to keep, by default SUBJECTS, SUBJECTS contains all
        subjects from ["01", "02", "04",...,"15"]
    task_list : list or bool, optional
        list of tasks to keep, by default False

    Returns
    -------
    pandas.DataFrame
        dataframe with information about files corresponding to only include
        given subjects and tasks
    """
    # filter the database on subject
    filtered_db = db[db["subject"].isin(subject_list)]
    # filter the database on task if specified
    if task_list:
        filtered_db = filtered_db[filtered_db["task"].isin(task_list)]
    length = len(filtered_db)
    if length == 0:
        raise ValueError(
            f"No files found for subjects {subject_list} and tasks {task_list}."
        )
    else:
        print(
            f"Found {length} files for subjects {subject_list} and tasks {task_list}."
        )
    return filtered_db


def get_file_paths(db, metadata=METADATA):
    """Get the remote and local file paths for each file in a (filtered) dataframe.

    Parameters
    ----------
    db : pandas.DataFrame
        dataframe with information about files in the dataset, ideally a subset
        of the full dataset

    Returns
    -------
    filenames, list
        lists of file paths for each file in the input dataframe. First list is the remote file paths and second list is the local file paths
    """
    # get the data type from the db
    data_type = db["dataset"].unique()
    # only fetching one data type at a time for now
    assert len(data_type) == 1
    data_type = data_type[0]
    # get the file names
    file_names = db["path"].tolist()
    # update file names to be relative to the dataset
    remote_file_names = []
    local_file_names = []
    remote_root_dir = md.select_dataset(data_type, metadata)["root"]
    local_root_dir = data_type
    for file in file_names:
        # put the file path together
        # always use "/" as the separator for remote file paths
        remote_file = remote_root_dir + "/" + file
        # but separator on local machine could be different
        # get all parts of the file path
        all_file_parts = file.split("/")
        # put the file path together using the local separator
        local_file = os.path.join(local_root_dir, *all_file_parts)
        remote_file_names.append(remote_file)
        local_file_names.append(local_file)

    return remote_file_names, local_file_names


def _update_local_db(db_file, files_data):
    """Update the local database of downloaded files.

    Parameters
    ----------
    db_file : str
        path to the local database file
     files_data : list of tuples
        list of tuples where each tuple contains (file_name, file_time)

    Returns
    -------
    pandas.DataFrame
        updated local database
    """

    file_names = [file_data[0] for file_data in files_data]
    file_times = [file_data[1] for file_data in files_data]

    if not os.path.exists(db_file):
        # create a new database
        db = pd.DataFrame(
            {"local_path": file_names, "downloaded_on": file_times}
        )
    else:
        # load the database
        db = pd.read_csv(db_file, index_col=False)
        new_db = pd.DataFrame(
            {"local_path": file_names, "downloaded_on": file_times}
        )
        # update the database
        db = pd.concat([db, new_db])
    db.reset_index(drop=True, inplace=True)
    # save the database
    db.to_csv(db_file, index=False)

    return db


def _write_file(file, data):
    """Write data to a file.

    Parameters
    ----------
    file : str
        path to the file to write to
    data : data fetched from ebrains
        data to write to the file
    """
    # check file type and write accordingly
    if type(data) == nibabel.nifti1.Nifti1Image:
        nibabel.save(data, file)
    elif type(data) == nibabel.gifti.gifti.GiftiImage:
        nibabel.save(data, file, mode="compat")
    elif type(data) == pd.core.frame.DataFrame:
        if file.endswith(".csv"):
            data.to_csv(file, index=False)
        elif file.endswith(".tsv"):
            data.to_csv(file, index=False, sep="\t")
        else:
            raise ValueError(
                f"File type not supported for {file}. Only .csv and .tsv are supported."
            )
    elif type(data) == dict:
        with open(file, "w") as f:
            json.dump(data, f)
    elif type(data) == bytes:
        if file.endswith(".bvec") or file.endswith(".bval"):
            with open(file, "wb") as f:
                f.write(data)
            f.close()
        else:
            raise ValueError(
                f"Don't know how to save file {file} of type {type(data)}"
            )
    else:
        raise ValueError(
            f"Don't know how to save file {file} of type {type(data)}"
        )

    return file


def _download_file(src_file, dst_file, connector):
    """Download a file from ebrains.

    Parameters
    ----------
    src_file : str
        path to the file on ebrains
    dst_file : str
        path to save the file to locally
    connector : EbrainsHdgConnector
        connector to the IBC dataset on ebrains

    Returns
    -------
    str, datetime
        path to the downloaded file and time at which it was downloaded
    """
    if not os.path.exists(dst_file):
        # load the file from ebrains
        src_data = connector.get(src_file)
        # make sure the directory exists
        dst_file_dir = os.path.split(dst_file)[0]
        os.makedirs(dst_file_dir, exist_ok=True)
        # save the file locally
        dst_file = _write_file(dst_file, src_data)
        download_time = datetime.now()
        return dst_file, download_time
    else:
        print(f"File {dst_file} already exists, skipping download.")
        return dst_file, None


def download_data(db, save_to=None):
    """Download the files in a (filtered) dataframe.

    Parameters
    ----------
    db : pandas.DataFrame
        dataframe with information about files in the dataset, ideally a subset
        of the full dataset
    save_to : str, optional
        where to save the data, by default None, in which case the data is
        saved in a directory called "ibc_data" in the current working directory

    Returns
    -------
    pandas.DataFrame
        dataframe with downloaded file names and times from the dataset
    """
    # make sure the dataframe is not empty
    db_length = len(db)
    if db_length == 0:
        raise ValueError(
            f"The input dataframe is empty. Please make sure that it atleast has columns 'dataset' and 'path' and a row containing appropriate values corresponding to those columns."
        )
    else:
        print(f"Found {db_length} files to download.")
    # make sure the dataframe has dataset and path columns
    db_columns = db.columns.tolist()
    if "dataset" not in db_columns or "path" not in db_columns:
        raise ValueError(
            f"The input dataframe should have columns 'dataset' and 'path' and a row containing appropriate values corresponding to those columns."
        )

    # get data type from db
    data_type = db["dataset"].unique()[0]
    # connect to ebrains dataset
    connector = _connect_ebrains(data_type)
    # get the file names as they are on ebrains
    src_file_names, dst_file_names = get_file_paths(db)
    # set the save directory
    save_to = _create_root_dir(save_to)
    # track downloaded file names and times
    local_db_file = os.path.join(save_to, f"downloaded_{data_type}.csv")

    """
    def download_and_update(src_file, dst_file, connector):
        if not os.path.exists(dst_file):
            #dst_file_path = os.path.join(save_to, dst_file)
            file_name = _download_file(src_file, dst_file, connector)
            file_time = datetime.now()
    """

    # download the files
    results = Parallel(n_jobs=4, backend="threading")(
        delayed(_download_file)(src_file, dst_file, connector)
        for src_file, dst_file in tqdm(
            zip(src_file_names, dst_file_names),
            position=1,
            leave=True,
            total=db_length,
            desc="Overall Progress: ",
            colour="green",
        )
    )

    # filter out results with None as download time to save a clean database
    results = [result for result in results if result[1] is not None]
    local_db = _update_local_db(local_db_file, results)

    CACHE.run_maintenance()

    print(
        f"Downloaded requested files from IBC {data_type} dataset. See "
        f"{local_db_file} for details."
    )

    return local_db
