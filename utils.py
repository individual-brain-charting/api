"""Functions to fetch IBC data from Ebrains via Human Data Gateway using siibra. #TODO add other data sources: neurovault, openneuro"""

import siibra
from siibra.retrieval.repositories import EbrainsHdgConnector
import os
from tqdm import tqdm
import nibabel
from siibra.retrieval.cache import CACHE

# keep cache < 2GiB, delete oldest files first
CACHE.clear()

# dataset ids on ebrains
DATASET_ID = {"statistic_map": "07ab1665-73b0-40c5-800e-557bc319109d", "preproc": "3ca4f5a1-647b-4829-8107-588a699763c1", "raw": "8ddf749f-fb1d-4d16-acc3-fbde91b90e24"}

# path to csv file with information about all statistic maps on Ebrains
STAT_MAPS_DB = "resulting_smooth_maps/ibc_neurovault.csv"

# all subjects in IBC dataset
SUBJECTS = ['sub-%02d' % i for i in
            [1, 2, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15]]

def authenticate():
    """Authenticate with ebrains."""
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
        db = connector.get(db_file)
        db.drop(columns=["Unnamed: 0"], inplace=True)
    # TODO add other data types: raw, preprocessed, etc.
    else:
        return ValueError(f"Unknown data type: {data_type}")
    # save the database file
    if save_to is None:
        save_to = "ibc_data"
    if not os.path.exists(save_to):
        os.makedirs(save_to)
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
        task_name = file_base.split("_")[3]
        file_head = os.path.join(download_dir, root_dir, sub_name, task_name)
    if not os.path.exists(file_head):
        os.makedirs(file_head)

    return file_head, file_base


def download_data(db, download_dir="ibc_data", organise_by="session"):
    """Download the files in a (filtered) dataframe.

    Parameters
    ----------
    db : pandas.DataFrame
        dataframe with information about files in the dataset, ideally a subset
        of the full dataset
    download_dir : str, optional
        where to save the data, by default "ibc_data"
    organise_by : str, optional
        whether to organise files under separate task or session folders,
        by default "session", could be one of ["session", "task"]

    Returns
    -------
    pandas.DataFrame
        dataframe with information about files downloaded from the dataset
    """
    # get data type from db
    data_type = db["image_type"].unique()[0]
    # connect to ebrains dataset
    connector = _connect_ebrains(data_type)
    # get the file names as they are on ebrains
    src_file_names = get_file_paths(db)
    # save file names as saved locally
    dst_file_names = []
    # download the files
    for src_file in tqdm(src_file_names):
        # construct the directory structure as required by the user
        # either by session or by task
        dst_file_head, dst_file_base = _construct_dir(
            download_dir, src_file, organise_by
        )
        # file path to save the data
        dst_file = os.path.join(dst_file_head, dst_file_base)
        if not os.path.exists(dst_file):
            # load the file from ebrains
            src_data = connector.get(src_file)
            if type(src_data) is nibabel.nifti1.Nifti1Image:
                src_data.to_filename(dst_file)
            # TODO add other data like json, etc.
            else:
                return ValueError(
                    f"Don't know how to save data of type {type(src_data)}"
                )
        else:
            print(f"File {dst_file} already exists, skipping download.")
        dst_file_names.append(dst_file)
    db["local_path"] = dst_file_names
    db.to_csv(os.path.join(download_dir, f"downloaded_{data_type}.csv"))

    print(f"Downloaded following {data_type}s from IBC data:\n{db}")

    return db
