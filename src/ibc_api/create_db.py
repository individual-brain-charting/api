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
raw_df["name"].str.split("/").str[-1].str.split(".").str[-1]
raw_df["file_ext"] = (
    raw_df["name"].str.split("/").str[-1].str.split(".").str[-1]
)
raw_df["file_ext"].unique()
raw_df["file_ext"].value_counts()
raw_df[raw_df["file_ext"] == "gz"]["name"].str.split("/").str[-1].str.split(
    "."
).str[-2].value_counts()
raw_df["tmp"] = (
    raw_df[raw_df["file_ext"] == "gz"]["name"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[-2]
)
raw_df[raw_df["tmp"] == "nii"]["file_type"] = "NIFTI"
raw_df[raw_df["file_ext"] == "gz"]["name"].str.split("/").str[-1].str.split(
    "_"
).str[0]
raw_df["."] = (
    raw_df[raw_df["file_ext"] == "gz"]["name"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[0]
    .str.split("_")
    .str[-1]
)
raw_df["subject"] = (
    raw_df[raw_df["file_ext"] == "gz"]["name"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[0]
    .str.split("_")
    .str[0]
)
raw_df["session"] = (
    raw_df[raw_df["file_ext"] == "gz"]["name"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[0]
    .str.split("_")
    .str[1]
)
raw_df["modality"].value_counts()
raw_df["task"] = (
    raw_df[raw_df["modality"].isin(["bold", "sbref"])]["name"]
    .str.split("/")
    .str[-1]
    .str.split(".")
    .str[0]
    .str.split("_")
    .str[3]
)
raw_df["MB"] = raw_df["bytes"].div(1024**2)
