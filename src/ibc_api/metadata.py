"""Functions to fetch metadata about the available IBC datasets."""

import json
import os

REMOTE_ROOT = (
    "https://api.github.com/repos/individual-brain-charting/api/contents/src/ibc_api/data"
)

LOCAL_ROOT = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(LOCAL_ROOT, exist_ok=True)

SUBJECTS = [1, 2, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15]


def _load_json(data_file):
    """Read a given json file

    Parameters
    ----------
    data_file : str
        path to json file to read

    Returns
    -------
    dict
        json file loaded as a dictionary
    """
    with open(data_file, "r") as f:
        data = json.load(f)

    return data


def select_dataset(data_type, metadata=None, version=None):
    """Select metadata of the requested dataset

    Parameters
    ----------
    data_type : str
        what dataset to select, could be one of 'volume_maps', 'surface_maps', 'preprocessed', 'raw'
    metadata : dict, optional
        dictionary object containing version info, dataset ids etc, by default None
    version : int, optional
        version of the dataset to select, starts from 1, by default None

    Returns
    -------
    dict
        the metadata of latest version of the requested dataset

    Raises
    ------
    KeyError
        if the requested dataset is not found in the metadata
    """
    if metadata is None:
        metadata = fetch_metadata()
    try:
        dataset = metadata[data_type]
    except KeyError:
        raise KeyError(
            f"Dataset type {data_type} not found in IBC collection."
        )
    if version is not None:
        version = version - 1
        try:
            dataset = dataset[version]
        except IndexError:
            raise IndexError(
                f"Version {version + 1} of {data_type} dataset does not exist."
            )
        assert dataset["version"] == version + 1
    else:
        latest_version = _find_latest_version(dataset)
        dataset = dataset[latest_version]
    return dataset


def _find_latest_version(dataset):
    """Find the latest version of the dataset

    Parameters
    ----------
    dataset : list of dicts
        value of one of the datasets in the metadata, probably with multiple versions in a list

    Returns
    -------
    int
        index of the latest version of the dataset
    """
    latest_version_index = 0
    latest_version = 0
    for i, data in enumerate(dataset):
        if data["version"] > latest_version:
            latest_version = data["version"]
            latest_version_index = i

    return latest_version_index


def fetch_remote_file(
    file,
    remote_root=REMOTE_ROOT,
    local_root=LOCAL_ROOT,
):
    """Fetch a file from the IBC docs repo

    Parameters
    ----------
    file : str
        name of the file to fetch
    remote_root : str, optional
        root link to wherever the file is stores, by default REMOTE_ROOT
    local_root : str, optional
        location to write the fetched file, by default LOCAL_ROOT

    Returns
    -------
    str
        full path of the fetched file
    """
    # Link to the json file on the IBC docs repo
    remote_file = f"{remote_root}/{file}"
    # save the file locally
    save_as = os.path.join(local_root, file)
    # use curl with github api to download the file
    os.system(
        f"curl -s -S -L -H 'Accept: application/vnd.github.v4.raw' -H 'X-GitHub-Api-Version: 2022-11-28' {remote_file} -o '{save_as}'"
    )

    # Return the data
    return save_as


def fetch_metadata(file="datasets.json"):
    """Fetch the metadata file from the IBC docs repo

    Parameters
    ----------
    file : str, optional
        name of the file, by default "datasets.json"

    Returns
    -------
    dict
        json file loaded as a dictionary
    """
    # Fetch the datasets.json file
    data_file = fetch_remote_file(file)

    # Load the data as a dictionary
    return _load_json(data_file)


def fetch_dataset_db(data_type, metadata=None):
    """Fetch csv containing file-by-file information about the requested dataset.

    Parameters
    ----------
    data_type : str
        what dataset to select, could be one of 'volume_maps', 'surface_maps', 'preprocessed', 'raw'
    metadata : dict, optional
        dictionary object containing version info, dataset ids etc, by default None
    Returns
    -------
    str
        full path of the fetched file csv file
    """
    dataset = select_dataset(data_type, metadata)

    return fetch_remote_file(dataset["db_file"])
