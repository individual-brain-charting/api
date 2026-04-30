"""Script to create the database from the raw, preprocessed, volume maps and surface maps
data on EBRAINS."""

import argparse
import os

import ibc_api.utils as ibc
import pandas as pd
from bids.layout import parse_file_entities
from ebrains_drive import BucketApiClient
from ibc_api.metadata import select_dataset
from siibra.retrieval.requests import EbrainsRequest
from tqdm import tqdm

datasets = ["raw", "preprocessed", "volume_maps", "surface_maps"]


def main(dataset_types):
    """Main function to create the database containing the available data in
    each Ebrains collection.
    Parameters
    ----------
    dataset_types : list of str
        list of dataset types to process, could be one or more of
        'volume_maps', 'surface_maps', 'preprocessed', 'raw'
    """
    EbrainsRequest.fetch_token()
    client = BucketApiClient(token=EbrainsRequest._KG_API_TOKEN)

    for dataset_type in tqdm(dataset_types, desc="Dataset types"):
        for version in tqdm(
            range(1, 6), desc=f"{dataset_type} versions", leave=False
        ):
            # Get EBRAINS metadata about the dataset
            try:
                dataset = select_dataset(dataset_type, version=version)
            except (ValueError, IndexError) as error:
                tqdm.write(str(error))
                tqdm.write(
                    f"skipping dataset {dataset_type}, version {version}"
                )
                continue

            dataset_id = dataset["id"]
            bucket = client.buckets.get_dataset(
                dataset_id, request_access=True
            )

            if bucket is None:
                tqdm.write(f"dataset {dataset_id} not found, skipping")
                continue

            rows = []
            for item in tqdm(
                bucket.ls(),
                desc=f"Files in {dataset_type} v{version}",
                leave=False,
            ):
                # parse filenames using pybids to get all the entities
                bids_entity = parse_file_entities(
                    item.name,
                    include_unmatched=True,
                    config=os.path.join(
                        os.path.dirname(__file__), "ibc_config.json"
                    ),
                )
                path = "/".join(item.name.split("/")[1:])
                root_dir_series = item.name.split("/")[0]
                row = {
                    **bids_entity,
                    "megabytes": item.bytes / (1024**2),
                    "dataset": dataset_type,
                    "path": path,
                    "root_series": root_dir_series,
                }
                rows.append(row)

            df = pd.DataFrame(rows)

            # separate surface maps and volume maps in different csv files
            if dataset_type == "surface_maps":
                mask = (
                    df["root_series"] == "resulting_smooth_maps_surface"
                ) & df["extension"].isin([".gii", ".json"])
                df = df[mask]
            # there are some files with .gii extension in the volume maps folder
            # filtering them out
            elif dataset_type == "volume_maps":
                mask = (df["root_series"] == "resulting_smooth_maps") & df[
                    "extension"
                ].isin([".nii.gz", ".json"])
                df = df[mask]

            bids_df = df.drop(columns=["root_series"])
            # create a csv file with the bids entities
            csv_file = os.path.join(
                os.path.dirname(__file__),
                "..",
                "data",
                f"{dataset_type}_v{version}.csv",
            )
            bids_df.to_csv(csv_file)
            tqdm.write(f"{csv_file} created!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset_type",
        choices=datasets,
        nargs="+",  # allows one or more values
        default=datasets,
        help="Dataset type(s) to process. Defaults to all.",
    )
    args = parser.parse_args()
    main(args.dataset_type)
