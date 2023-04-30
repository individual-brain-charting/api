"""Functions to fetch IBC data from Ebrains via Human Data Gateway using siibra. 
#TODO add other data sources: neurovault, openneuro"""

import siibra
from siibra.retrieval.repositories import EbrainsHdgConnector
import os
from tqdm import tqdm
import nibabel
from siibra.retrieval.cache import CACHE
import pandas as pd
from io import BytesIO
from datetime import datetime

# clear cache
CACHE.clear()

# dataset ids on ebrains
DATASET_ID = {
    "statistic_map": "07ab1665-73b0-40c5-800e-557bc319109d",
    "preproc": "3ca4f5a1-647b-4829-8107-588a699763c1",
    "raw": "8ddf749f-fb1d-4d16-acc3-fbde91b90e24",
}

# path to csv file with information about all statistic maps on Ebrains
STAT_MAPS_DB = "resulting_smooth_maps/ibc_neurovault.csv"

# all subjects in IBC dataset
SUBJECTS = [
    "sub-%02d" % i for i in [1, 2, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15]
]


def authenticate():
    """This function authenticates you to Ebrains. It would return a link that
    would prompt you to login or create an Ebrains account. Read more about
    registering for Ebrains here: https://ebrains.eu/register"""
    siibra.fetch_ebrains_token()


def _connect_ebrains(data_type="statistic_map"):
    """Connect to given IBC dataset on Ebrains via Human Data Gateway.

    Parameters
    ----------
    data_type : str, optional
        dataset to fetch, by default "statistic_map", TODO one of
        ["statistic_map", "raw", "preprocessed", "surface_map"]

    Returns
    -------
    EbrainsHdgConnector
        connector to the dataset
    """
    if data_type == "statistic_map":
        # dataset id on ebrains
        dataset_id = "07ab1665-73b0-40c5-800e-557bc319109d"
    # TODO add other data types: raw, preprocessed, etc.
    else:
        return ValueError(f"Unknown data type: {data_type}")

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
    if not os.path.exists(dir_path):
        return ValueError(f"Directory {dir_path} does not exist.")
    dir_path = os.path.join(dir_path, "ibc_data")
    os.mkdir(dir_path)

    return dir_path


def get_info(data_type="statistic_map", save_to=None):
    """Fetch a csv file describing each file in a given IBC dataset on Ebrains.

    Parameters
    ----------
    data_type : str, optional
        dataset to fetch, by default "statistic_map", TODO one of
        ["statistic_map", "raw", "preprocessed"]
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
    if data_type == "statistic_map":
        # file with all information about the dataset
        db_file = STAT_MAPS_DB
        # get the file
        db = connector.get(
            db_file,
            decode_func=lambda b: pd.read_csv(BytesIO(b), delimiter=","),
        )
        db.drop(columns=["Unnamed: 0"], inplace=True)
    # TODO add other data types: raw, preprocessed, etc.
    else:
        return ValueError(f"Unknown data type: {data_type}")
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
        subjects from sub-01 to sub-15
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


def get_file_paths(db):
    """Get the file paths for each file in a (filtered) dataframe.

    Parameters
    ----------
    db : pandas.DataFrame
        dataframe with information about files in the dataset, ideally a subset
        of the full dataset

    Returns
    -------
    list
        list of file paths for each file in the input dataframe
    """
    # get the data type from the db
    data_type = db["image_type"].unique()
    # only fetching one data type at a time for now
    assert len(data_type) == 1
    data_type = data_type[0]
    # get the file names
    _file_names = db["path"].tolist()
    # update file names to be relative to the dataset
    file_names = []
    for file in _file_names:
        if data_type == "statistic_map":
            root_dir = "resulting_smooth_maps"
        # TODO add other data types: raw, preprocessed, etc.
        else:
            return ValueError(f"Unknown data type: {data_type}")
        # get the subject and session
        sub_ses = file.split("_")[:2]
        # put the file path together
        file = os.path.join(root_dir, *sub_ses, file)
        file_names.append(file)

    return file_names


def _construct_dir(download_dir, file, organise_by="session"):
    """Construct the directory structure to save a file in.

    Parameters
    ----------
    download_dir : str
        top most directory to save the file in
    file : str
        file path as on ebrains
    organise_by : str, optional
        whether to organise files under separate task or session folders,
        by default "session", could be one of ["session", "task"]

    Returns
    -------
    str, str
        head and base of the file path to save the file in
    """
    file_head, file_base = os.path.split(file)
    if organise_by == "session":
        file_head = os.path.join(download_dir, file_head)
    elif organise_by == "task":
        root_dir = file.split(os.sep)[0]
        sub_name = file_base.split("_")[0]
        task_name = file_base.split("_")[2]
        file_head = os.path.join(download_dir, root_dir, sub_name, task_name)
    if not os.path.exists(file_head):
        os.makedirs(file_head)

    return file_head, file_base


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
        # update the database
        db = pd.concat(
            [
                db,
                pd.DataFrame(
                    {"local_path": file_names, "downloaded_on": file_times}
                ),
            ]
        )
    # save the database
    db.to_csv(db_file, index=False)

    return db


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
        if type(src_data) is nibabel.nifti1.Nifti1Image:
            src_data.to_filename(dst_file)
        # TODO add other data like json, etc.
        else:
            return ValueError(
                f"Don't know how to save file {src_file}"
                f" of type {type(src_data)}"
            )

        return dst_file, datetime.now()

    else:
        print(f"File {dst_file} already exists, skipping download.")

        return [], []


def download_data(db, save_to=None, organise_by="session"):
    """Download the files in a (filtered) dataframe.

    Parameters
    ----------
    db : pandas.DataFrame
        dataframe with information about files in the dataset, ideally a subset
        of the full dataset
    save_to : str, optional
        where to save the data, by default None, in which case the data is
        saved in a directory called "ibc_data" in the current working directory
    organise_by : str, optional
        whether to organise files under separate task or session folders,
        by default "session", could be one of ["session", "task"]

    Returns
    -------
    pandas.DataFrame
        dataframe with downloaded file names and times from the dataset
    """
    # get data type from db
    data_type = db["image_type"].unique()[0]
    # connect to ebrains dataset
    connector = _connect_ebrains(data_type)
    # get the file names as they are on ebrains
    src_file_names = get_file_paths(db)

    # set the save directory
    save_to = _create_root_dir(save_to)

    # track downloaded file names and times
    local_db_file = os.path.join(save_to, f"downloaded_{data_type}.csv")
    # download the files
    for src_file in tqdm(src_file_names):
        # construct the directory structure as required by the user
        # either by session or by task
        dst_file_head, dst_file_base = _construct_dir(
            save_to, src_file, organise_by
        )
        # file path to save the data
        dst_file = os.path.join(dst_file_head, dst_file_base)

        file_name, file_time = _download_file(src_file, dst_file, connector)
        local_db = _update_local_db(local_db_file, file_name, file_time)

        # keep cache < 2GiB, delete oldest files first
        CACHE.run_maintenance()

    print(
        f"Downloaded requested files from IBC {data_type} dataset. See "
        f"{local_db_file} for details."
    )

    return local_db
