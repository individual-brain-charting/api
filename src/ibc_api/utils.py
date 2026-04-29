"""API to fetch IBC data from EBRAINS via Human Data Gateway using siibra."""

# %$
import gzip
import json
import os
from datetime import datetime

import nibabel
import pandas as pd
import siibra
from ebrains_drive import BucketApiClient
from joblib import Memory
from siibra.retrieval.cache import CACHE
from siibra.retrieval.requests import EbrainsRequest
from tqdm import tqdm

from . import metadata as md

# enable device flow for authentication
os.environ["SIIBRA_ENABLE_DEVICE_FLOW"] = "1"

# clear cache
CACHE.clear()

# dataset ids on ebrains
METADATA = md.fetch_metadata()

# all subjects in IBC dataset
SUBJECTS = md.SUBJECTS

# token root directory
TOKEN_ROOT = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(TOKEN_ROOT, exist_ok=True)

# memory cache
joblib_cache_dir = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(joblib_cache_dir, exist_ok=True)
memory = Memory(joblib_cache_dir, verbose=0)


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
            EbrainsRequest.set_token(token)
    else:
        EbrainsRequest.fetch_token()
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
    bucket: siibra.retrieval.buckets.DatasetBucket
        a bucket object corresponding to the requested dataset
    """
    # get the dataset id
    dataset = md.select_dataset(data_type, metadata, version)
    dataset_id = dataset["id"]
    # fetch token and connect to ebrains
    EbrainsRequest.fetch_token()
    # use the token to create a client and get a data bucket
    client = BucketApiClient(token=EbrainsRequest._KG_API_TOKEN)
    bucket = client.buckets.get_dataset(dataset_id, request_access=True)

    return bucket


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


def _is_empty_db(db):
    return db is None or db.empty or len(db) == 0


def get_info(
    data_type="volume_maps", save_to=None, metadata=METADATA, version=None
):
    """Fetch a csv file describing each file in a given IBC dataset on EBRAINS.

    Parameters
    ----------
    data_type : str, optional
        dataset to fetch, by default "volume_maps", one of
        ["volume_maps", "surface_maps", "raw", "preprocessed"]
    save_as : str or None, optional
        filename to save this csv as, by default None, if None saves as
        "ibc_data/available_{data_type}.csv"
    metadata : dict, optional
        dictionary object containing version info, dataset ids etc, by default
        METADATA
    version : int or None, optional
        version of the dataset to select, starts from 1, by default None, if
        None selects the latest version

    Returns
    -------
    pandas.DataFrame
        dataframe with information about each file in the dataset
    """
    datasets = metadata[data_type]
    latest_idx = md._find_latest_version(datasets)

    # if version is specified, only try that version
    # otherwise try from latest to oldest
    if version is not None:
        # find the index corresponding to the requested version
        version_indices = [
            i for i, d in enumerate(datasets) if d["version"] == version
        ]
        if not version_indices:
            raise ValueError(
                f"Version {version} not found for dataset {data_type}."
            )
        version_range = version_indices  # only one version to try
    else:
        version_range = range(latest_idx, -1, -1)

    last_exception = None
    db = None

    # Try from latest version → older versions
    for version_idx in version_range:
        # fetch the information corresponding to this version
        dataset = datasets[version_idx]
        db_file = md.fetch_remote_file(dataset["db_file"])
        # load the file as dataframe
        # convert subject, session and run to string to avoid losing
        # leading zeros
        db = pd.read_csv(
            db_file,
            converters={"subject": str, "session": str, "run": str},
        )
        db.drop(columns=["Unnamed: 0"], inplace=True, errors="ignore")

        if not _is_empty_db(db):
            print(
                f"Fetched database for {data_type}, version {dataset['version']}."
            )
            break
        else:
            last_exception = ValueError(
                f"No versions found for dataset {data_type}, version {dataset['version']}."
            )
            print(
                f"Failed to fetch database for {data_type}, version {dataset['version']}."
                "Trying older version..."
            )

    # If all versions failed, raise the last exception
    if _is_empty_db(db):
        raise (
            last_exception
            if last_exception
            else ValueError(f"No versions found for dataset {data_type}.")
        )

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


def get_file_paths(db, metadata=METADATA, save_to_dir=None):
    """Get the remote and local file paths for each file in a (filtered) dataframe.

    Parameters
    ----------
    db : pandas.DataFrame
        dataframe with information about files in the dataset, ideally a subset
        of the full dataset

    Returns
    -------
    filenames, list
        lists of file paths for each file in the input dataframe. First list
        is the remote file paths and second list is the local file paths
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
    if save_to_dir == None:
        local_root_dir = data_type
    else:
        local_root_dir = os.path.join(save_to_dir, data_type)
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


def _update_local_db(db_file, downloaded_file, download_time):
    """Update the local database of downloaded files.

    Parameters
    ----------
    db_file : str
        path to the local database file
    downloaded_file : str
        path to the downloaded file
    download_time : datetime
        time at which the file was downloaded

    Returns
    -------
    pandas.DataFrame
        updated local database
    """
    if not os.path.exists(db_file):
        # create a new database
        db = pd.DataFrame(columns=["local_path", "downloaded_on"])
    else:
        try:
            # load the database
            db = pd.read_csv(db_file, index_col=False)
        except (
            pd.errors.EmptyDataError,
            pd.errors.ParserError,
            FileNotFoundError,
        ):
            tqdm.write("Empty database file. Creating a new one.")
            db = pd.DataFrame(columns=["local_path", "downloaded_on"])

    downloaded_db = pd.DataFrame(
        {"local_path": [downloaded_file], "downloaded_on": [download_time]}
    )
    db = pd.concat([db, downloaded_db], ignore_index=True)
    db.to_csv(db_file, index=False)

    return db


def _write_file(src_file, dst_file, data):
    """Write data to a file.

    Parameters
    ----------
    src_file : str
       path to the source file on ebrains
    dst_file : str
        path to save the file to locally
    data : bytes
        data to write to the file

    Returns
    -------
    dts_file: str
        path to the written

    Raises
    ------
    ValueError
        if the file type is not supported
    nibabel.filebasedimages.ImageFileError
        if the image data is corrupt or cannot be read
    """
    try:
        if src_file.endswith(".nii.gz") or src_file.endswith(".nii"):
            img = nibabel.Nifti1Image.from_bytes(gzip.decompress(data))
            nibabel.save(img, dst_file)
        elif src_file.endswith(".gii"):
            img = nibabel.gifti.gifti.GiftiImage.from_bytes(data)
            nibabel.save(img, dst_file, mode="compat")
        elif src_file.endswith(".csv") or src_file.endswith(".tsv"):
            # read the data as a dataframe and save it
            df = pd.read_csv(io.StringIO(data.decode("utf-8")))
            if dst_file.endswith(".csv"):
                df.to_csv(dst_file, index=False)
            elif dst_file.endswith(".tsv"):
                df.to_csv(dst_file, index=False, sep="\t")
            else:
                raise ValueError(
                    f"File type not supported for {dst_file}. Only .csv and .tsv are supported."
                )
        elif src_file.endswith(".json"):
            # read the data as a dictionary and save it
            dict_data = json.loads(data.decode("utf-8"))
            with open(dst_file, "w") as f:
                json.dump(dict_data, f)
        elif src_file.endswith(".bvec") or src_file.endswith(".bval"):
            with open(dst_file, "wb") as f:
                f.write(data)
            f.close()
        else:
            raise ValueError(
                f"Don't know how to save file {dst_file} of type {type(data)}"
            )
    except gzip.BadGzipFile as e:
        raise gzip.BadGzipFile(f"Failed to decompress {src_file}: {e}") from e
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"Failed to decode {src_file}: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error writing {dst_file}: {e}") from e

    return dst_file


def _download_file(src_file, dst_file, connector):
    """Download a file from ebrains.

    Parameters
    ----------
    src_file : str
        path to the file on ebrains
    dst_file : str
        path to save the file to locally
    connector : Ebrains connector object
        connector to the IBC dataset on ebrains

    Returns
    -------
    str
        path to the downloaded file

    Raises
    ------
    RuntimeError
        if the file cannot be downloaded due to an unexpected error
    OSError
        if the destination directory cannot be created
    """
    if os.path.exists(dst_file):
        tqdm.write(f"File {dst_file} already exists, skipping download.")
        return dst_file

    try:
        f = connector.get_file(src_file)
    except Exception as e:
        raise RuntimeError(
            f"Failed to fetch {src_file} from EBRAINS: {e}"
        ) from e

    try:
        # make sure the directory exists
        dst_file_dir = os.path.split(dst_file)[0]
        os.makedirs(dst_file_dir, exist_ok=True)
    except OSError as e:
        raise OSError(f"Failed to create directory {dst_file_dir}: {e}") from e

    # save the file locally
    dst_file = _write_file(src_file, dst_file, f.get_content())
    return dst_file


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

    Raises
    ------
    ValueError
        if the input dataframe is empty or missing required columns
    RuntimeError
        if a file fails to download after retries
    """
    # make sure the dataframe is not empty
    db_length = len(db)
    if db_length == 0:
        raise ValueError(
            "The input dataframe is empty. Please make sure that it at least "
            "has columns 'dataset' and 'path' with appropriate values."
        )
    else:
        print(f"Found {db_length} files to download.")
    # make sure the dataframe has dataset and path columns
    db_columns = db.columns.tolist()
    if "dataset" not in db_columns or "path" not in db_columns:
        raise ValueError(
            "The input dataframe must have columns 'dataset' and 'path'."
        )

    # get data type from db
    data_type = db["dataset"].unique()[0]
    # connect to ebrains dataset
    print("... Fetching token and connecting to EBRAINS ...")
    connector = _connect_ebrains(data_type)

    # set the save directory
    save_to = _create_root_dir(save_to)
    # file to track downloaded file names and times
    local_db_file = os.path.join(save_to, f"downloaded_{data_type}.csv")
    # get the file names as they are on ebrains
    src_file_names, dst_file_names = get_file_paths(db, save_to_dir=save_to)

    # download finally
    print(f"\n...Starting download of {len(src_file_names)} files...")

    failed_files = []
    download_details = None

    with tqdm(
        zip(src_file_names, dst_file_names),
        total=len(src_file_names),
        desc=f"Downloading {data_type}",
        unit="file",
    ) as pbar:
        for src_file, dst_file in pbar:
            try:
                file_name = _download_file(src_file, dst_file, connector)
                download_details = _update_local_db(
                    local_db_file, file_name, datetime.now()
                )
            except (RuntimeError, OSError, ValueError) as e:
                tqdm.write(f"ERROR: skipping {src_file}: {e}")
                failed_files.append(src_file)

    if failed_files:
        tqdm.write(
            f"\nWarning: {len(failed_files)}/{len(src_file_names)} files failed to download:\n"
            + "\n".join(failed_files)
        )

    tqdm.write(
        f"Downloaded requested files from IBC {data_type} dataset. "
        f"See {local_db_file} for details.\n"
    )

    # clean up the cache
    CACHE.clear()
    memory.clear()

    return download_details
