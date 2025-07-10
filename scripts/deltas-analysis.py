# load packages 
import yaml
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

def process_delta(control_path, target_path, wrf_vars, clip_gdf, month):
    print('inside process_delta()')
    if (month >= 3) and (month < 11): # PDT (~March - Nov)
        timezone_shift =-7
    else: # PST
        timezone_shift=-8
    # get control data
    filelist_control = glob.glob(os.path.join(control_path, 'wrfout_d02*'))
    ds_control = xr.open_mfdataset(filelist_control, 
                        engine="netcdf4",
                        concat_dim="Time",
                        combine="nested").xwrf.postprocess()
    ds_control = ds_control.sortby('Time')
    # get target data
    filelist_target = glob.glob(os.path.join(target_path, 'wrfout_d02*'))
    ds_target = xr.open_mfdataset(filelist_target, 
                        engine="netcdf4",
                        concat_dim="Time",
                        combine="nested").xwrf.postprocess()
    ds_target = ds_target.sortby('Time')
    # calculate wind speed if necessary
    if 'WS' in wrf_vars:
        calc_ws(ds_control)
        calc_ws(ds_target)

    # subset ds
    control = ds_control[wrf_vars]
    target = ds_target[wrf_vars]
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
    delta_clipped = delta_clipped.assign_coords(hour=(delta_clipped['Time'].dt.hour+timezone_shift)%24)
    print("SUCCESS!!!")
    return delta_clipped

def apply_time_encoding(ds, reference):
    if 'Time' in ds.coords:
        encoding = {
            'Time': {
                'units': f'hours since {reference}',
                'calendar': 'gregorian',
                'dtype': 'float64'
            }
        }
        return ds, encoding
    return ds, {}

def calculate_deltas(months, control_target_pairs, wrf_dir_map, wrf_dir, wrf_vars, boundary):
    deltas = {}
    for month in months:
        for pair in control_target_pairs:
            # get paths to wrf directories 
            print(f'---- month: {month} ----')
            print(f'---- pairs: {pair} ----')
            control_dir = wrf_dir_map[str(month).zfill(2)][pair[0]]
            target_dir = wrf_dir_map[str(month).zfill(2)][pair[1]]
            control_path = os.path.join(wrf_dir, control_dir)
            target_path = os.path.join(wrf_dir, target_dir)
            delta_clipped = process_delta(control_path, target_path, wrf_vars, boundary, month)
            # Remove XTIME coordinate to avoid issues with saving as netcdf
            if 'XTIME' in delta_clipped.coords:
                delta_clipped = delta_clipped.drop_vars('XTIME')
            # # apply time encoding to avoid saving bug issues
            # common_time_reference = '1900-01-01 00:00:00'
            # ds, encoding = apply_time_encoding(delta_clipped, common_time_reference)
            print(delta_clipped)
            delta_key = f'{pair[0]}-{pair[1]}-{month:02d}'
            delta_filename = f'{delta_key}.nc'
            delta_savepath = os.path.join(deltas_dir, delta_filename)
            delta_clipped.to_netcdf(delta_savepath)
            deltas[delta_key] = delta_clipped
    return deltas

def load_deltas(deltas_dir):
    deltas = {}
    for month in months:
        for pair in control_target_pairs:
            delta_key = f'{pair[0]}-{pair[1]}-{month:02d}'
            delta_filename = f'{delta_key}.nc'
            delta_path = os.path.join(deltas_dir, delta_filename)
            ds = xr.load_dataset(delta_path)
            deltas[delta_key] = ds
    return deltas

def diurnal_line_plot(ds_by_season, control_name, target_name, v, plot_dir, labels, vis_settings):
    # initiate plot
    fig, ax = plt.subplots(figsize=(6,4))
    hrs = range(24)
    ymin, ymax = np.inf, -np.inf
    for season in ['summer', 'winter']:
        print('TEST PRINT!!!')
        print(season)
        ds = ds_by_season[season]
        mean = ds[v].mean(dim=['y','x'], skipna=True)
        mean_diurnal = mean.groupby('hour').mean('Time')
        std_diurnal = mean.groupby('hour').std('Time')
        n_sample = len(mean[mean['hour']==0])
        err_diurnal = 1.96*(std_diurnal/(n_sample)**(1/2))
        ci_lower = mean_diurnal - err_diurnal
        ci_upper = mean_diurnal + err_diurnal
        # add to plot
        ax.axhline(y=0, color=vis_settings['axhline_color'], linestyle=vis_settings['axhline_linestyle'], linewidth=vis_settings['axhline_linewidth'], alpha=vis_settings['axhline_alpha'])
        ax.plot(hrs, mean_diurnal, vis_settings[season]['line_color'],label=season, linewidth=vis_settings[season]['line_linewidth'], alpha=vis_settings[season]['line_alpha'])
        ax.fill_between(hrs, ci_upper, ci_lower, alpha=vis_settings[season]['fill_alpha'], edgecolor=vis_settings[season]['fill_edgecolor'], facecolor=vis_settings[season]['fill_facecolor'], linewidth=vis_settings[season]['fill_linewidth'], antialiased=True)
        # Update ymin, ymax
        if ci_lower.min() < ymin: 
            ymin = ci_lower.min()
        if ci_upper.max() > ymax:
            ymax = ci_upper.max()
    ax.xaxis.set_ticks(np.arange(0,24,3))
    ax.grid(linewidth=vis_settings['grid_linewidth'], alpha=vis_settings['grid_alpha'], linestyle=vis_settings['grid_linestyle'])
    ax.set_xlabel('Hour of Day')
    ax.set_ylabel(labels[v])
    ax.legend()
    ax.set_xlim(min(hrs), max(hrs))
    ax.set_ylim(ymin - 0.05*abs(ymin), ymax + 0.05*abs(ymax)) # min, max with 5% buffer
    ax.annotate('error = 95% CI', xy=(0.5, 0.9), xycoords='axes fraction', 
                fontsize=10, horizontalalignment='center', verticalalignment='bottom')
    save_dir = os.path.join(plot_dir, 'line_plots')
    os.makedirs(save_dir, exist_ok=True)
    filename = f'diurnal-lineplot-{v}-{control_name}-{target_name}.png'
    plt.savefig(os.path.join(save_dir, filename), dpi=300)

def create_diurnal_line_plots(ds_dict, vars, pairs, season_map, save_dir, labels, vis_settings):
    for p in pairs:
        for v in vars:
            print(f'plotting for {p} and {v}...')
            ds_by_season = {}
            control_name, target_name = p[0], p[1]
            month_summer = str(season_map['summer']).zfill(2)
            month_winter = str(season_map['winter']).zfill(2)
            ds_by_season['summer'] = ds_dict[f'{control_name}-{target_name}-{month_summer}']
            ds_by_season['winter'] = ds_dict[f'{control_name}-{target_name}-{month_winter}']
            diurnal_line_plot(ds_by_season, control_name, target_name, v, plot_dir, labels, vis_settings)
            print(f'Success! Plot for {p} and {v} has been saved!')

def histogram():
    pass

def create_histograms():
    pass


def create_diurnal_animation():
    pass

def calc_ws(ds):
    ds['WS'] = np.sqrt(ds['U10']**2 + ds['V10']**2)
    ds['WS'].attrs['long_name'] = 'Wind Speed'
    ds['WS'].attrs['units'] = 'm/s'
    ds['WS'].attrs['description'] = 'Magnitude of wind velocity calculated from u and v components'

def get_plot_labels():
    labels = {}
    degree_sign = u'\N{DEGREE SIGN}'
    labels['T2'] = r'$\mathrm{{\Delta}T_{air}}$ (' + degree_sign + 'C)'
    labels['TC_URB'] = r'$\mathrm{{\Delta}T_{canopy}}$ (' + degree_sign + 'C)'
    labels['AHF'] = r'Anthropogenic heat flux ($\mathrm{W\/m^{-2}}$)'
    labels['WS'] = r'$\mathrm{{\Delta}}$(Wind Speed) ($\mathrm{m\/s^{-1}}$)'
    labels['PBLH'] = r'$\mathrm{{\Delta}}$PBLH (m)'
    return labels

def main():
    # Option 1: set variables/paths from config yaml file
    with open("./config.yaml", "r") as f:
        config = yaml.safe_load(f)
    # Option 2: Set all keys as variables (use with caution)
    for key, value in config.items():
        globals()[key] = value

    # Print all variables and their types
    print("=== YAML Variables and Types ===")
    for key, value in config.items():
        if isinstance(value, (dict, list)):
            print(f"{key}: {type(value).__name__}")
            print(f"  Value: {value}")
        else:
            print(f"{key}: {type(value).__name__} = {value}")
        print("-" * 50)
    
    # Set plot labels
    labels = get_plot_labels()
    
    ### ===== Calculate and set urban boundary ===== ###
    urban = gpd.read_file(urban_area_file)
    la_county_boundary = gpd.read_file(la_county_boundary_file)
    urban = urban.to_crs(la_county_boundary.crs)
    urban_la_county = urban.overlay(la_county_boundary)
    ### =============================== ###

    ### ===== Calculate Deltas ===== ###
    # get list of expected delta files
    expected_delta_filelist = []
    for month in months:
        for pair in control_target_pairs:
            delta_filename = f'{pair[0]}-{pair[1]}-{month:02d}.nc'
            delta_savepath = os.path.join(deltas_dir, delta_filename)
            expected_delta_filelist.append(delta_savepath)
    # check that expected files are in directory
    paths_in_deltas_dir = [os.path.join(deltas_dir, f) for f in os.listdir(deltas_dir) if os.path.isfile(os.path.join(deltas_dir, f))]
    print(paths_in_deltas_dir)
    set1 = set(expected_delta_filelist)
    set2 = set(paths_in_deltas_dir)
    print(f'set 1: {set1}')
    print(f'set 2: {set2}')
    # if already exist, just proceed
    if set1 == set2:
        print("Deltas directory already contains necessary files. No need to re-calculate!")
        deltas = load_deltas(deltas_dir)
    # if does not exist, need to calculate (or re-calculate)
    else:
        print(f"In set1 but not set2: {set1 - set2}")
        print(f"In set2 but not set1: {set2 - set1}")
        print("Deltas directory does not contain all necessary files. Re-calculating deltas....")
        deltas = calculate_deltas(months, control_target_pairs, wrf_dir_map, wrf_dir, wrf_vars, urban_la_county)
    ### ============================== ###

    ### ===== Diurnal Line Plots ===== ###
    create_diurnal_line_plots(deltas, wrf_vars, control_target_pairs, season_map, plot_dir, labels, line_plot_vis_settings)
    ### =============================== ###

    ### ===== Histograms ===== ###
    create_histograms(deltas, wrf_vars, control_target_pairs, season_map, plot_dir, labels, line_plot_vis_settings)
    ### =============================== ###

    ### ===== Diurnal Animations ===== ###
    #TO-DO
    ### =============================== ###

if __name__ == "__main__":
    main()



