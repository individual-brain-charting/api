# %%
import ibc_api.utils as ibc

ibc.authenticate()
# %%
raw = ibc._connect_ebrains("raw")
preproc = ibc._connect_ebrains("preprocessed")

# %%
import pandas as pd

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
raw_df.rename(columns={'name': 'path'}, inplace=True)

raw_df["path"].str.split("/").str[-1].str.split(".").str[-1]
raw_df["file_ext"] = (
    raw_df["path"].str.split("/").str[-1].str.split(".").str[-1]
)
raw_df["file_ext"].unique()
raw_df["file_ext"].value_counts()
raw_df[raw_df["file_ext"] == "gz"]["path"].str.split("/").str[-1].str.split(
    "."
).str[-2].value_counts()
raw_df["isNII"] = (
    raw_df[raw_df["file_ext"] == "gz"]["path"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[-2]
)
raw_df.loc[(raw_df["isNII"] == 'nii'), "isNII"] = "NIFTI"


raw_df[raw_df["file_ext"] == "gz"]["path"].str.split("/").str[-1].str.split(
    "_"
).str[0]

raw_df["subject"] = (
    raw_df[raw_df["file_ext"].isin(["gz","tsv","bval","bvec"])]["path"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[0]
    .str.split("_")
    .str[0]
)
raw_df["session"] = (
    raw_df[raw_df["file_ext"].isin(["gz","tsv","bval","bvec"])]["path"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[0]
    .str.split("_")
    .str[1]
)

raw_df["modality"] = (
    raw_df.apply(lambda row: 'doc' if row["file_ext"] == "json" else 
                 row["path"].split("/")[-1].split(".")[0].split("_")[-1],
                 axis=1)
)
raw_df.loc[raw_df["modality"] == 
           'participants', "modality"] = "doc"
raw_df.loc[(raw_df["modality"] == 
            'epi') &   (raw_df["file_ext"] == 'gz'), "modality"] = "fmap"
raw_df["modality"].value_counts()

raw_df["task"] = (
    raw_df[raw_df["modality"].isin(["bold", "sbref","events"])]["path"]
    .str.split("task-")
    .str[-1]
    .str.split("_")
    .str[0]
)
raw_df["task"].value_counts()

raw_df["MB"] = raw_df["bytes"].astype(int).div(1024**2)


