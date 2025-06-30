"""Processing of the Tranzition Zero Solar Asset Mapper (TZ-SAM) dataset."""
# %%

import geopandas as gpd
import pandas as pd

# %%

raw_df = gpd.read_file("resources/automatic/tz/sam.gpkg")
raw_df.head()

# %%
