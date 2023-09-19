"""Script to create the database from the raw, preprocessed, volume maps and surface maps
 data on EBRAINS."""

# import libraries
import pandas as pd
import os
import ibc_api.utils as ibc
from bids.layout import parse_file_entities

datasets = ["raw", "preprocessed", "volume_maps", "surface_maps"]

ibc.authenticate()
for dataset in datasets:
    for version in range(1, 4):
        # Get EBRAINS metadata about the dataset
        try:
            ebrains_data = ibc._connect_ebrains(dataset, version=version)
        except (ValueError, IndexError) as error:
            print(f"skipping dataset {dataset}, version {version}")
            continue
        # Get the file names and other info as dataframes
        ebrains_df = pd.DataFrame(ebrains_data.__dict__["_files"])
        filenames = ebrains_df["name"].tolist()
        # parse filenames using pybids to get all the entities
        bids_entities = []
        for file in filenames:
            bids_entity = parse_file_entities(
                file, include_unmatched=True, config="ibc_conifg.json"
            )
            bids_entities.append(bids_entity)
        # convert the list of dictionaries with bids entities to a dataframe
        bids_df = pd.DataFrame(bids_entities)
        # remove rows with empty path
        bids_df = bids_df.dropna(how="all")
        # add a column with the file sizes in MB
        bids_df["megabytes"] = ebrains_df["bytes"].astype(int).div(1024**2)
        # add a column with the dataset name
        bids_df["dataset"] = [dataset] * len(bids_df)
        root_dir = ebrains_df["name"].str.split("/").str[0]
        # add a column with the file path without the root directory
        path = ebrains_df["name"].str.split("/").str[1:].str.join("/")
        bids_df["path"] = path
        # separate surface maps and volume maps in different csv files
        if dataset == "surface_maps":
            mask = (root_dir == "resulting_smooth_maps_surface") & bids_df[
                "extension"
            ].isin([".gii", ".json"])
            bids_df = bids_df[mask]
        # there are some files with .gii extension in the volume maps folder
        # filtering them out
        elif dataset == "volume_maps":
            mask = (root_dir == "resulting_smooth_maps") & bids_df[
                "extension"
            ].isin([".nii.gz", ".json"])
            bids_df = bids_df[mask]
        bids_df = bids_df.reset_index(drop=True)
        # create a csv file with the bids entities
        csv_file = os.path.join("..", "data", f"{dataset}_v{version}.csv")
        bids_df.to_csv(csv_file)
        print(f"{csv_file} created!")
