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
    db["image_type"] = [data_type for _ in range(len(db))]
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


def get_file_paths(db, metadata=METADATA):
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
    root_dir = md.select_dataset(data_type, metadata)["root"]
    for file in _file_names:
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
    db.reset_index(drop=True, inplace=True)
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
        print(src_file)
        # load the file from ebrains
        src_data = connector.get(src_file)
        # print(type(src_data))
        if type(src_data) in [
            nibabel.nifti1.Nifti1Image,
            nibabel.gifti.gifti.GiftiImage,
        ]:
            nibabel.save(src_data, dst_file, mode="compat")
            return dst_file
        else:
            print("not a nifti file")
            return ValueError(
                f"Don't know how to save file {src_file}"
                f" of type {type(src_data)}"
            )
    else:
        print(f"File {dst_file} already exists, skipping download.")

        return []


def download_data(db, save_to=None, organise_by="session", metadata=METADATA):
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
