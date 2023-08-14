"""API to fetch IBC data from EBRAINS via Human Data Gateway using siibra. 
"""

import siibra
from siibra.retrieval.repositories import EbrainsHdgConnector
import os
from tqdm import tqdm
import nibabel
from siibra.retrieval.cache import CACHE
import pandas as pd
from datetime import datetime
from . import metadata as md
import json

# clear cache
CACHE.run_maintenance()

# dataset ids on ebrains
METADATA = md.fetch_metadata()

# all subjects in IBC dataset
SUBJECTS = md.SUBJECTS


def authenticate():
    """This function authenticates you to EBRAINS. It would return a link that
    would prompt you to login or create an EBRAINS account. Read more about
    registering for EBRAINS here: https://ebrains.eu/register"""
    siibra.fetch_ebrains_token()


def _connect_ebrains(data_type="volume_maps", metadata=METADATA):
    """Connect to given IBC dataset on EBRAINS via Human Data Gateway.

    Parameters
    ----------
    data_type : str, optional
        dataset to fetch, by default "statistic_map", can be one of
        ["volume_maps", "surface_maps", "preprocessed", "raw]

    Returns
    -------
    EbrainsHdgConnector
        connector to the dataset
    """
    # get the dataset id
    dataset = md.select_dataset(data_type, metadata)
    dataset_id = dataset["id"]

    return EbrainsHdgConnector(dataset_id)


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
    # connect to ebrains dataset
    connector = _connect_ebrains(data_type)
    # file with all information about the dataset
    db_file = md.fetch_dataset_db(data_type, metadata)
    # load the file as dataframe
    db = pd.read_csv(db_file)
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
        subjects from 1 to 15
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
    list, list
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


def _update_local_db(db_file, file_names, file_times):
    """Update the local database of downloaded files.

    Parameters
    ----------
    db_file : str
        path to the local database file
    file_names : str or list
        path to the downloaded file(s)
    file_times : str or list
        time at which the file(s) were downloaded

    Returns
    -------
    pandas.DataFrame
        updated local database
    """

    if type(file_names) is str:
        file_names = [file_names]
        file_times = [file_times]

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
    else:
        raise ValueError(
            f"Don't know how to save file {file}" f" of type {type(data)}"
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
        return dst_file
    else:
        print(f"File {dst_file} already exists, skipping download.")

        return []


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
    # download the files
    for src_file, dst_file in tqdm(zip(src_file_names, dst_file_names)):
        # final file path to save the data
        dst_file = os.path.join(save_to, dst_file)
        file_name = _download_file(src_file, dst_file, connector)
        file_time = datetime.now()
        local_db = _update_local_db(local_db_file, file_name, file_time)
        # keep cache < 2GiB, delete oldest files first
        CACHE.run_maintenance()
    print(
        f"Downloaded requested files from IBC {data_type} dataset. See "
        f"{local_db_file} for details."
    )

    return local_db
