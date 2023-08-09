# %%
import pandas as pd
import ibc_api.utils as ibc

ibc.authenticate()
# %%
raw = ibc._connect_ebrains("raw")
preproc = ibc._connect_ebrains("preprocessed")

# %%
raw_df = pd.DataFrame(raw.__dict__["_files"])
preproc_df = pd.DataFrame(preproc.__dict__["_files"])

# %%
raw_df.to_csv("raw.csv")
preproc_df.to_csv("preproc.csv")

# %%
raw_df.head()
preproc_df.head()

# %%
raw_df.columns
raw_df["name"]

# To follow previous db structure, rename name to path
raw_df.rename(columns={'name': 'path'}, inplace=True)


# Get the file extension column, either gz, tsv, bval, bvec or json
raw_df["file_ext"] = (
    raw_df["path"].str.split("/").str[-1].str.split(".").str[-1]
)
raw_df["file_ext"].unique()
raw_df["file_ext"].value_counts()

# Helper column to know when the file is a NIFTI, maybe not needed since all
# gz files are NIFTI
raw_df["isNII"] = (
    raw_df[raw_df["file_ext"] == "gz"]["path"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[-2]
)
raw_df.loc[(raw_df["isNII"] == 'nii'), "isNII"] = "NIFTI"


# Get the subject column from the path
raw_df["subject"] = (
    raw_df[raw_df["file_ext"].isin(["gz","tsv","bval","bvec"])]["path"]
    .str.split("/")
    .str[-1]
    .str.split("_")
    .str[0]
)


# Get the session column from the path
raw_df["session"] = (
    raw_df[raw_df["file_ext"].isin(["gz","tsv","bval","bvec"])]["path"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[0]
    .str.split("_")
    .str[1]
)


# Get the modality column from the path (bold, sbref, fmap, doc, etc)
raw_df["modality"] = (
    raw_df.apply(lambda row: 'doc' if row["file_ext"] == "json" else 
                 row["path"].split("/")[-1].split(".")[0].split("_")[-1],
                 axis=1)
)
# Decided to categorize participants.tsv and json files as doc, TBD!
raw_df.loc[raw_df["modality"] == 
           'participants', "modality"] = "doc"
# Decided to rename 'epi' files modality as fmap, think it's more accurate
raw_df.loc[(raw_df["modality"] == 
            'epi') &   (raw_df["file_ext"] == 'gz'), "modality"] = "fmap"
raw_df["modality"].value_counts()


# Get the task column from the path and modality 
raw_df["task"] = (
    raw_df[raw_df["modality"].isin(["bold", "sbref","events"])]["path"]
    .str.split("task-")
    .str[-1]
    .str.split("_")
    .str[0]
)
# Get the task column for task-reference json files
task_ref = (raw_df["path"].str.contains("json") & 
            raw_df["path"].str.contains("task"))
raw_df.loc[task_ref, "task"] = (
    raw_df.loc[task_ref, "path"]
    .str.split("task-")
    .str[-1]
    .str.split("_")
    .str[0]
)

# Get the file size in MB
raw_df["MB"] = raw_df["bytes"].astype(int).div(1024**2)


# ----------- Preproc data ----------------

# To follow previous db structure, rename name to path
preproc_df.rename(columns={'name': 'path'}, inplace=True)


# Get the file extension column, either gz, tsv, bval, bvec or json
preproc_df["file_ext"] = (
    preproc_df["path"].str.split("/").str[-1].str.split(".").str[-1]
)
preproc_df["file_ext"].unique()
preproc_df["file_ext"].value_counts()


# Get the subject column from the path
preproc_df["subject"] = (
    preproc_df[preproc_df["file_ext"].isin(["gz","tsv"])]["path"]
    .str.split("/")
    .str[-1]
    .str.split("_")
    .str[0]
)


# Get the session column from the path
preproc_df["session"] = (
    preproc_df[preproc_df["file_ext"].isin(["gz","tsv"])]["path"]
    .str.split("/")
    .str[-1]
    .str.split("_")
    .str[1]
)


# Get the modality column from the path (bold, sbref, fmap, doc, etc)
preproc_df["modality"] = (
    preproc_df["path"].str.split("/").str[-1]
    .str.split(".").str[0]
    .str.split("_").str[-1]
)
preproc_df["modality"].value_counts()