"""Functions to fetch metadata about the available IBC datasets."""

import json
import os

REMOTE_ROOT = (
    "https://api.github.com/repos/individual-brain-charting/docs/contents"
)

LOCAL_ROOT = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(LOCAL_ROOT, exist_ok=True)


def _load_json(data_file):
    """Read a given json file"""
    with open(data_file, "r") as f:
        data = json.load(f)

    return data


def ibc_subjects():
    return [
        f"sub-{sub:02d}"
        for sub in [1, 2, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15]
    ]


def select_dataset(data_type, metadata=None):
    """Select metadata of the requested dataset"""
    if metadata is None:
        metadata = fetch_metadata()
    try:
        dataset = metadata[data_type]
    except KeyError:
        raise KeyError(
            f"Dataset type {data_type} not found in IBC collection."
        )
    latest_version = find_latest_version(dataset)
    dataset = dataset[latest_version]
    return dataset


def find_latest_version(dataset):
    """Find the latest version of the dataset"""

    latest_version_index = 0
    latest_version = 0
    for i, data in enumerate(dataset):
        if data["version"] > latest_version:
            latest_version = data["version"]
            latest_version_index = i

    return latest_version_index


def fetch_remote_file(
    file="datasets.json",
    remote_root=REMOTE_ROOT,
    local_root=LOCAL_ROOT,
):
    """Fetch a file from the IBC docs repo"""

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
    # Fetch the datasets.json file
    data_file = fetch_remote_file(file)

    # Load the data as a dictionary
    return _load_json(data_file)


def fetch_dataset_db(data_type, metadata=None):
    """Fetch csv containing file-by-file information about the requested dataset."""

    dataset = select_dataset(metadata, data_type)

    return fetch_remote_file(dataset["db_file"])
