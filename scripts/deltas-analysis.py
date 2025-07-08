# load packages 
import os
import glob
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import geopandas as gpd
import pyproj
import shapely.geometry
from matplotlib.ticker import FormatStrFormatter
import matplotlib as mpl
import numpy as np
import xarray as xr
import rioxarray as rxr
from geocube.api.core import make_geocube
import xesmf as xe
import cartopy.crs as ccrs
import contextily as ctx

# imports for rasterizing 
import geopandas as gpd
import rasterio
from rasterio import features
from rasterio.enums import MergeAlg
from rasterio.plot import show
import xwrf

# dask
import dask
from dask.distributed import Client, LocalCluster

def process_delta(ds_control, ds_target, vars_subset, clip_gdf, timezone_shift=-7):
    control = ds_control[vars_subset]
    target = ds_target[vars_subset]
    delta = target - control
    delta = delta.compute()
    # assign crs
    wrf_crs = ds_control.wrf_projection.item()
    delta = delta.rio.write_crs(wrf_crs)
    # clip by urban LA County pixels
    delta_clipped = delta.rio.clip(clip_gdf.geometry.values, clip_gdf.crs)
    # remove first two days (spin-up) and last timestep (extra timestep)
    delta_clipped = delta_clipped.isel(Time=slice(48, -1))
    # create local hour variable
    # delta_clipped['hour'] = (delta_clipped['Time'].dt.hour - 7)%24
    delta_clipped = delta_clipped.assign_coords(hour=(delta_clipped['Time'].dt.hour+timezone_shift)%24)
    return delta_clipped


def main():
    ### ===== import boundaries ===== ###
    # import US Census Urban Area vector file
    home_path = Path('/home1/kojoseph/anthropogenic-heat-la-20230205')
    data_path = home_path / 'data'
    filepath = data_path / 'boundaries/2016_urban_area.geojson'
    urban = gpd.read_file(filepath)
    # import la county boundary
    filename = data_path / 'boundaries/la_county_bound_simplified.gpkg'
    la_county = gpd.read_file(filename)
    # urban areas in la county
    urban = urban.to_crs(la_county.crs)
    urban_la_county = urban.overlay(la_county)
    ### =============================== ###

    ### ===== set up case combinations ===== ###
    #TO-DO
    ### =============================== ###

    ### ===== Loop through case pairs ===== ###
        ### ===== Calculate Deltas ===== ###
        #TO-DO
        ### =============================== ###

        ### ===== Diurnal Line Plots ===== ###
        #TO-DO
        ### =============================== ###
        
        ### ===== Delta Map Animations ===== ###
        #TO-DO
        ### =============================== ###

    ### =============================== ###


if __name__ == "__main__":
    main()



