#!/usr/bin/env python
# coding: utf-8

# # Generate zarr store from netCDF dataset

## import required packages
import numpy as np
import xarray as xr
import s3fs
import zarr
import argparse
from pathlib import Path
import os
import netCDF4 as nc4
import boto3
import fsspec
import time
import pandas as pd
from dask.distributed import Client
import dask.array
import uuid as uuid
from datetime import datetime, timedelta
from contextlib import contextmanager
import argparse
import sys


# initialize dask
# client = Client(n_workers=32)


## Define functions


@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout

def time_it(func):
    """
    Decorator that reports the execution time.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Capture the start time
        result = func(*args, **kwargs)  # Execute the function
        end_time = time.time()  # Capture the end time
        print(f"{func.__name__} took {end_time-start_time:.4f} seconds to execute")
        return result
    return wrapper


def rechunk_ds(ds, time_chunk=1):
    for dv in ds.data_vars:
        print('... rechunking ', dv)

        # 3D fields need chunking in space
        if len(ds[dv].dims) == 4:
            if 'k' in ds[dv].dims:
                if 'i_g' in ds[dv].dims:
                    ds[dv] = ds[dv].chunk({'time':time_chunk, 'k':15, 'j':270, 'i_g':450})
                elif 'j_g' in ds[dv].dims:
                    ds[dv] = ds[dv].chunk({'time':time_chunk, 'k':15, 'j_g':270, 'i':450})
                else:
                    ds[dv] = ds[dv].chunk({'time':time_chunk, 'k':15, 'j':270, 'i':450})
            if 'k_l' in ds[dv].dims:
                ds[dv] = ds[dv].chunk({'time':time_chunk, 'k_l':15, 'j':270, 'i':450})
        else:
            # 2D fields don't need rechunking in space
            if 'i_g' in ds[dv].dims:
                ds[dv] = ds[dv].chunk({'time':time_chunk, 'j':1080, 'i_g':1800})
            elif 'j_g' in ds[dv].dims:
                ds[dv] = ds[dv].chunk({'time':time_chunk, 'j_g':1080, 'i':1800})
            else:
                # print(f"... dims 'time' 'j' and 'i' found in {ds[dv].dims}")
                ds[dv] = ds[dv].chunk({'time':time_chunk, 'j':1080, 'i':1800})
    return ds


def reorder_dims(xr_dataset):
    ## specify order of dims

    # if 3D
    if 'k' in list(xr_dataset.dims):
        #tmp = xr_dataset[["time","j","i","k","j_g","i_g","k_u","k_l","k_p1","nv","nb"]] 
        if 'i_g' in xr_dataset.dims:
            tmp = xr_dataset[["time","k","j","i_g","k_u","k_l","k_p1","nv"]]
        elif 'j_g' in xr_dataset.dims:
            tmp = xr_dataset[["time","k","j_g","i","k_u","k_l","k_p1","nv"]]
        else:
            tmp = xr_dataset[["time","k","j","i","k_u","k_l","k_p1","nv"]]
    # if 2D
    else:
        if 'i_g' in xr_dataset.dims:
            tmp = xr_dataset[["time","j","i_g","nv"]]
        elif 'j_g' in xr_dataset.dims:
            tmp = xr_dataset[["time","j_g","i","nv"]]
        else:
            tmp = xr_dataset[["time","j","i","nv"]]

    tmp = tmp.drop_indexes(["nv"]).reset_coords(["nv"], drop=True)
    

    ## reassign dataset to new dims
    xr_ds_ordered = tmp.assign(xr_dataset)
    
    return xr_ds_ordered


def create_encoding(ecco_ds, output_array_precision = np.float32):
    
    # Create NetCDF encoding directives
    # ---------------------------------------------
    # print('\n... creating variable encodings')
    # ... data variable encoding directives
    
    # Define fill values for NaN
    if output_array_precision == np.float32:
        netcdf_fill_value = nc4.default_fillvals['f4']

    elif output_array_precision == np.float64:
        netcdf_fill_value = nc4.default_fillvals['f8']
    
    dv_encoding = dict()
    for dv in ecco_ds.data_vars:
        dv_encoding[dv] =  {'compressor': zarr.Blosc(cname="zlib", clevel=5, shuffle=False)}

    # ... coordinate encoding directives
    coord_encoding = dict()
    
    for coord in ecco_ds.coords:
        # set default no fill value for coordinate
        if output_array_precision == np.float32:
            coord_encoding[coord] = {'_FillValue':None, 'dtype':'float32'}
        elif output_array_precision == np.float64:
            coord_encoding[coord] = {'_FillValue':None, 'dtype':'float64'}

        # force 64 bit ints to be 32 bit ints
        if (ecco_ds[coord].values.dtype == np.int32) or \
           (ecco_ds[coord].values.dtype == np.int64) :
            coord_encoding[coord]['dtype'] ='int32'

        # fix encoding of time
        if coord == 'time' or coord == 'time_bnds':
            coord_encoding[coord]['dtype'] ='int32'

            if 'units' in ecco_ds[coord].attrs:
                # apply units as encoding for time
                coord_encoding[coord]['units'] = ecco_ds[coord].attrs['units']
                # delete from the attributes list
                del ecco_ds[coord].attrs['units']

        elif coord == 'time_step':
            coord_encoding[coord]['dtype'] ='int32'

    # ... combined data variable and coordinate encoding directives
    encoding = {**dv_encoding, **coord_encoding}

    return encoding


def update_metadata(ds):
    """
    Update global attributes to reflect entire dataset
    """
    
    ds.attrs['product_name'] = 'SASSIE_ECCO_L4_OCN_AVGDAILY_V1R1'
    ds.attrs['id'] = '10.5067/SASSIE-ECCO-V1R1'
    ds.attrs['uuid'] = str(uuid.uuid1())
    ds.attrs['time_coverage_start'] = '2014-01-15T00:00:00'
    ds.attrs['time_coverage_end'] = '2021-02-08T00:00:00'

    ## add current time and date
    current_time = datetime.now().isoformat()[0:19]
    ds.attrs['date_created'] = current_time
    ds.attrs['date_modified'] = current_time
    ds.attrs['date_metadata_modified'] = current_time
    ds.attrs['date_issued'] = current_time
    
    ## remove some attributes we don't need
    # attributes_to_remove = ['product_time_coverage_start', 'product_time_coverage_end',\
    #                         'geospatial_lat_resolution', 'geospatial_lon_resolution']
    # for attr in attributes_to_remove:
    #     ds.attrs.pop(attr, None)
        
    return ds


@time_it
def initialize_zarr_store(var_name, nc_ec2_dir_path, ec2_store):
    """
    Uses one netCDF from the dataset to set up the structure/shape of a blank zarr store which will be filled in later.

    Args:
        var_name (str): variable name (e.g., 'THETA')
        nc_ec2_dir_path (Path): path to the directory where netcdfs are stored on the local ec2.
        ec2_store (str): location where local ec2 zarr store will be saved (e.g., '/nvme_data2/zarr_tmp/THETA_AVG_DAILY.ZARR')
        
    Returns:
        None
    """

    ## open the first file from the netcdf directory ---------------------
    print(f"... opening first netCDF file to create a template.\n")
    start_nc = np.sort(list(nc_ec2_dir_path.glob('*.nc')))[0]
    var_ds = xr.open_dataset(start_nc)

    ## create empty dataset ----------------------------------------------
    # define the start/end dates for the complete dataset
    sassie_start_date = np.datetime64('2014-01-15T12:00:00.000000000')
    sassie_end_date = np.datetime64('2021-02-07T12:00:00.000000000')
    total_num_days = int((sassie_end_date - sassie_start_date)/1e9/86400 + 1)
    
    sassie_start_date_h = sassie_start_date.astype('datetime64[h]')
    sassie_start_date_d = sassie_start_date.astype('datetime64[D]')
    
    print(f"total number of days in the dataset: {total_num_days}")

    # create DataArray of all time coords
    sassie_time_delta = np.timedelta64(1,'D')
    sassie_days = np.array([sassie_start_date_h + sassie_time_delta * n for n in range(total_num_days)])
    sassie_days=sassie_days.astype('datetime64[ns]')

    # define DataArray for time bounds
    sassie_tb_0 = np.array([sassie_start_date_d + sassie_time_delta * n for n in range(total_num_days)])
    sassie_tb_0=sassie_tb_0.astype('datetime64[ns]')
    
    sassie_tb_1 = np.array([sassie_start_date_d + sassie_time_delta * (n+1) for n in range(total_num_days)])
    sassie_tb_1=sassie_tb_1.astype('datetime64[ns]')
    
    sassie_tb = np.vstack([sassie_tb_0, sassie_tb_1])
    sassie_tb = sassie_tb.T

    # create empty array with the shape we want
    # 3D
    if len(var_ds[var_name].dims) == 4:
        de = dask.array.empty([total_num_days, 90, 1080, 1800], chunks=[1,15,270,450])
        
        # convert empty array into DataArray with proper time index
        if 'i_g' in var_ds[var_name].dims:
            tt = xr.DataArray(de, dims=['time','k','j','i_g'], coords={'time':sassie_days})
        elif 'j_g' in var_ds[var_name].dims:
            tt = xr.DataArray(de, dims=['time','k','j_g','i'], coords={'time':sassie_days})
        else:
            if 'k_l' in var_ds[var_name].dims:
                tt = xr.DataArray(de, dims=['time','k_l','j','i'], coords={'time':sassie_days})
            else:
                tt = xr.DataArray(de, dims=['time','k','j','i'], coords={'time':sassie_days})
    
    # 2D
    else:
        de = dask.array.empty([total_num_days, 1080, 1800], chunks=[1,270,450])
        
        # convert empty array into DataArray with proper time index
        if 'i_g' in var_ds[var_name].dims:
            tt = xr.DataArray(de, dims=['time','j','i_g'], coords={'time':sassie_days})
        elif 'j_g' in var_ds[var_name].dims:
            tt = xr.DataArray(de, dims=['time','j_g','i'], coords={'time':sassie_days})
        else:
            tt = xr.DataArray(de, dims=['time','j','i'], coords={'time':sassie_days})

    tt.name = var_name
    tt.attrs = var_ds[var_name].attrs

    # then drop variable from the original dataset and swap it with the new 'empty' DataArray
    tmp_ds = var_ds.drop_vars([var_name, 'time', 'time_bnds'])
    tmp_ds[var_name] = tt

    # add time bnds to the new dataset
    time_bnds_ds = xr.Dataset({'time_bnds': (['time','nv'], sassie_tb)}, coords={'time':tmp_ds.time})
    
    tmp_template = xr.merge([tmp_ds, time_bnds_ds])
    tmp_template = tmp_template.set_coords(['time_bnds'])
    
    tmp_template['time'].attrs = var_ds['time'].attrs
    tmp_template['time_bnds'].attrs = var_ds['time_bnds'].attrs
    
    # reorder dims
    tmp_template_ds = reorder_dims(tmp_template)

    print(f"... empty dataset {tmp_template_ds} created.")

    ## Write the template Zarr dataset -------------------------------

    # update global metadata
    tmp_template_ds = update_metadata(tmp_template_ds)
    
    # create encoding
    enc = create_encoding(tmp_template_ds, output_array_precision = np.float32)
    
    # write to s3 zarr store
    start_time = time.time()
    # once again double checking to make sure it doesn't already exist
    isExist = os.path.exists(ec2_store)
    if isExist:
        print(f">> zarr store {ec2_store} already exists. Proceeding with appending files.")
    else:
        print(f"... saving zarr store {ec2_store}")
        tmp_template_ds.to_zarr(ec2_store, compute=False, mode="w", encoding=enc)
        
    print(f'total time to save template dataset to {ec2_store} : {(time.time() - start_time):.2f}\n')


@time_it
def save_nc_set_to_zarr_store(var_name, nc_ec2_dir_path, files_to_process, ec2_store):
    """
    Opens netCDF files and saves them to a zarr store.

    Args:
        var_name (str): variable name (e.g., 'THETA')
        nc_ec2_dir_path (Path): path to the directory where netcdfs are stored on the local ec2.
        files_to_process (str): list of two numbers (start and end) for the range of netcdf files to process or [-1] for all files.
        ec2_store (str): location of local ec2 zarr store (e.g., '/nvme_data2/zarr_tmp/THETA_AVG_DAILY.ZARR')
        
    Returns:
        None
    """
    # list of all nc files in ec2 directory
    nc_file_list = np.sort(list(nc_ec2_dir_path.glob('*.nc')))
    print(f"... found {len(nc_file_list)} files in {str(nc_ec2_dir_path)}\n")
    
    ## specify start and end indices or process all files ------------------------   
    if len(files_to_process) == 2: # two numbers indicates a range (two indices)
        try:
            nc_select = nc_file_list[files_to_process[0]:files_to_process[1]]
            print(f'... first file to process : {nc_select[0]}')
            print(f'... last file to process  : {nc_select[-1]}\n')
            print(f'... found {len(nc_select)} files in nc_select')
            
            # # define range of files that will be processed in batches of 10 files
            # slice_start = np.arange(files_to_process[0],files_to_process[1],10)
            # slice_end = np.arange(files_to_process[0]+10,files_to_process[1]+10,10)

            # define range of files that will be processed in batches of 10 files
            # slice_start = np.arange(0,len(nc_select),10)
            # slice_end = np.arange(10,len(nc_select)+10,10)

            slice_start = np.arange(0,len(nc_select),5)
            slice_end = np.arange(5,len(nc_select)+5,5)
        except:
            print("No files exist in this dataset from selected files in files_to_process.")
            sys.exit(1)
    
    elif len(files_to_process) == 1 and files_to_process[0] == -1: # process all files
        nc_select = nc_file_list
        print(f'... first file to process : {nc_select[0]}')
        print(f'... last file to process  : {nc_select[-1]}\n')

        # define range of files that will be processed in batches of 10 files
        # slice_start = np.arange(0,len(nc_select),10)
        # slice_end = np.arange(10,len(nc_select)+10,10)

        slice_start = np.arange(0,len(nc_select),5)
        slice_end = np.arange(5,len(nc_select)+5,5)
    
    else:
        print("ERROR: invalid entry for `files_to_process` argument")
        sys.exit(1)
        
    # define each slice of 10 netCDFs that we will loop through
    slices = np.vstack([slice_start,slice_end]).T
    
    ## loop through each stack of 10 netCDFs ------------------------------------
    
    for nc_slice in slices:
        # select slice from subset of data
        nc_set_tmp = nc_select[nc_slice[0]:nc_slice[1]]
        
        if len(nc_set_tmp) > 0:
            print(f"... processing {len(nc_set_tmp)} existing files contained in the {nc_slice} slice from the full {var_name} dataset.\n")
            
            # see whether there are any missing days
            days = []
            for nc in nc_set_tmp:
                nc_str = str(nc)
                days.append(nc_str.split("/")[-1].split("_")[-6])
                days_dt64 = np.array(days, dtype='datetime64[D]')
            
            if np.all(np.diff(days_dt64).astype('int') == 1):
                print(">> No missing days. Slice of netCDFs will be opened as a group and then saved to the zarr store.\n")
                
                start_time = time.time()
                # open all files at once and save to zarr store
                print(f"... opening {len(nc_set_tmp)} files")
                # tmp = xr.open_mfdataset(nc_set_tmp, parallel=True)
                tmp = xr.open_mfdataset(nc_set_tmp)
                # rechunk the dataset
                ds_slice = rechunk_ds(tmp, time_chunk=1)
                # drop unneeded vars
                if len(ds_slice[var_name].dims) == 4:
                    ds_slice_drop = ds_slice.drop_vars(['i', 'j', 'k', 'k_l', 'k_p1', 'k_u', 'XC', 'YC', 'Z', 'Zu', 'Zl', 'Zp1'])
                else:
                    ds_slice_drop = ds_slice.drop_vars(['i', 'j', 'XC', 'YC'])
                # save to zarr store
                print(f"... saving {len(nc_set_tmp)} files to zarr store")
                try:
                    ds_slice_drop.to_zarr(ec2_store, mode="r+", region='auto')
                except ValueError:
                    print("ValueError raised: The auto-detected region of coordinate 'time' for writing new data to the original store had non-contiguous indices. Proceeding to process each file individually.")
                    for nc in nc_set_tmp:
                        start_time = time.time()
                        # open one netcdf
                        print(f"... opening 1 file: {nc}")
                        tmp = xr.open_dataset(nc)
                        # rechunk
                        ds_slice = rechunk_ds(tmp, time_chunk=1)
                        # drop unneeded vars
                        if len(ds_slice[var_name].dims) == 4:
                            ds_slice_drop = ds_slice.drop_vars(['i', 'j', 'k', 'k_l', 'k_p1', 'k_u', 'XC', 'YC', 'Z', 'Zu', 'Zl', 'Zp1'])
                        else:
                            ds_slice_drop = ds_slice.drop_vars(['i', 'j', 'XC', 'YC'])
                        # save to zarr store
                        print(f"... saving 1 file to zarr store")
                        ds_slice_drop.to_zarr(ec2_store, mode="r+", region='auto')
                    
                print(f'total time to save {len(nc_set_tmp)} files ({str(days_dt64[0])} to {str(days_dt64[-1])}) to {ec2_store}: {(time.time() - start_time):.2f} seconds\n')
            
            else:
                print(">> One or more days are missing. Each netCDF will be opened and saved to the zarr store individually.\n")
                # open each netcdf and save it to the zarr store, one by one
                for nc in nc_set_tmp:
                    start_time = time.time()
                    # open one netcdf
                    print(f"... opening 1 file: {nc}")
                    tmp = xr.open_dataset(nc)
                    # rechunk
                    ds_slice = rechunk_ds(tmp, time_chunk=1)
                    # drop unneeded vars
                    if len(ds_slice[var_name].dims) == 4:
                        ds_slice_drop = ds_slice.drop_vars(['i', 'j', 'k', 'k_l', 'k_p1', 'k_u', 'XC', 'YC', 'Z', 'Zu', 'Zl', 'Zp1'])
                    else:
                        ds_slice_drop = ds_slice.drop_vars(['i', 'j', 'XC', 'YC'])
                    # save to zarr store
                    print(f"... saving 1 file to zarr store")
                    ds_slice_drop.to_zarr(ec2_store, mode="r+", region='auto')
                    
                    print(f'total time to save {nc} to {ec2_store}: {(time.time() - start_time):.2f} seconds\n')
        else:
            print(f"... no existing files to process in the {nc_slice} slice from the full {var_name} dataset.\n")

@time_it
def push_zarr_to_s3(var_name, ec2_store, zarr_s3_bucket):
    """
    Pushes the zarr store from a directory on the ec2 to an S3 bucket.

    Args:
        var_name (str): variable name
        ec2_store (str): The root directory containing the zarr store on the EC2 instance (e.g., '/nvme_data2/zarr_tmp/THETA_AVG_DAILY.ZARR').
        zarr_s3_bucket (str): The name of the S3 bucket where the files will be pushed (e.g., 's3://podaac-dev-sassie/ECCO_model/N1/V1/HH/ZARR')

    Returns:
        None
    """
 
    ## push zarr dir to s3 bucket
    mybucket = zarr_s3_bucket + var_name + "_AVG_DAILY.ZARR"

    print(f'\n>> pushing zarr store in {ec2_store} to s3 bucket : {mybucket}')
    print(f'... looking for zarr store in {ec2_store}')

    isExist = os.path.exists(ec2_store)
    if isExist:
        # aws s3 cp overwrites existing files whereas sync will not; using cp here to ensure the most up-to-date version is being saved
        cmd=f"aws s3 cp {ec2_store}/ {mybucket}/ --recursive --no-progress > /dev/null 2>&1"
        print(f'... aws command: {cmd}')
        with suppress_stdout():
           os.system(cmd)
    else:
        print("... nothing to upload!") 


@time_it
def download_nc_ds_from_s3(var_name, ec2_nc_dir, nc_s3_bucket):
    """
    Pull dataset of netcdfs from s3 bucket to local ec2.
    
    Args:
        var_name (str): Variable name.
        ec2_nc_dir (str): Scratch directory where netcdfs will be stored on local ec2.
        nc_s3_bucket: The name of the s3 bucket where the netcdfs are stored (e.g., 's3://podaac-dev-sassie/ECCO_model/N1/V1/HH/NETCDF')
    
    Returns:
        None
    """
    ## pull netcdfs to local ec2
    mybucket = nc_s3_bucket + var_name + "_AVG_DAILY"

    print(f'\n>> downloading netcdfs for {var_name} from s3 bucket {mybucket} to ec2 {ec2_nc_dir}.')

    cmd=f"aws s3 sync {mybucket}/ {ec2_nc_dir}/ --no-progress"
    print(f'... aws command: {cmd}')
    with suppress_stdout():
        os.system(cmd)

    print(f"... download from s3 to ec2 complete. Proceeding with conversion to zarr store.\n")


########### Create final routine to process files ########### 

def convert_nc_to_zarr_store(var_name, files_to_process, ec2_scratch_dir, nc_s3_bucket, zarr_s3_bucket, keep_local_nc_files, keep_local_zarr_store, push_zarr_to_s3):
    """
    Downloads SASSIE-ECCO netcdf dataset from s3 bucket, then creates zarr store and pushes it to the cloud.
    
    Args:
        var_name (str): variable name (e.g., 'THETA')
        files_to_process (str): list of two numbers (start and end) for the range of netcdf files to process or [-1] for all files.
        ec2_scratch_dir (str): location of local ec2 scratch directory (e.g., '/nvme_data2')
        nc_s3_bucket (str): The name of the S3 bucket where the netcdf files are located (e.g., 's3://podaac-dev-sassie/ECCO_model/N1/V1/HH/NETCDF')
        zarr_s3_bucket (str): The name of the S3 bucket where the files will be pushed (e.g., 's3://podaac-dev-sassie/ECCO_model/N1/V1/HH/ZARR')
        keep_local_nc_files (bool): Boolean (True/False) indicating whether to keep local netcdf files or delete.
        keep_local_zarr_store (bool): Boolean (True/False) indicating whether to keep local zarr store or delete.
        push_zarr_to_s3 (bool): Boolean (True/False) indicating whether to push the local ec2 zarr store to the zarr_s3_bucket.
        
    Returns:
        None
    """

    ## download dataset from s3 to local ec2 scratch directory -------------------------------
    # create directories
    ec2_nc_dir = f"{ec2_scratch_dir}/nc_tmp/{var_name}_AVG_DAILY"
    ec2_store = f"{ec2_scratch_dir}/zarr_tmp/{var_name}_AVG_DAILY.ZARR"

    nc_ec2_dir_path = Path(ec2_nc_dir)

    # download from s3 if it doesn't already exist on local ec2
    isExist = os.path.exists(ec2_nc_dir)
    if isExist:
        print(f">> Not downloading from s3 bucket since nc_tmp directory {ec2_nc_dir} already exists.")
    else:
        print(f">> Creating nc_tmp directory {ec2_nc_dir}.")
        download_nc_ds_from_s3(var_name, ec2_nc_dir, nc_s3_bucket)
    
    ## create zarr store if it is not already established -------------------------------------
    # Check whether the specified path exists or not
    isExist = os.path.exists(ec2_store)
    if isExist:
        print(f">> zarr store {ec2_store} already exists. Proceeding with appending files.\n")
    else:
        print(f"\n... initializing zarr store {ec2_store}...")
        initialize_zarr_store(var_name, nc_ec2_dir_path, ec2_store)
    
    ## fill zarr store with data from selected files defined by `files_to_process` -----------
    save_nc_set_to_zarr_store(var_name, nc_ec2_dir_path, files_to_process, ec2_store)

    ## push zarr store to s3 bucket
    if push_zarr_to_s3:
        push_zarr_to_s3(var_name, ec2_store, zarr_s3_bucket)
    else:
        print("... not pushing zarr store to s3 bucket")

    ## delete local nc files on ec2
    if keep_local_nc_files:
        print('... keeping local nc files')
    else:
        ## remove tmp zarr var directory and all of its contents
        print(f"... removing tmp nc files {ec2_nc_dir}")
        os.system(f"rm -rf {ec2_nc_dir}")
    
    ## delete local zarr store on ec2
    if keep_local_zarr_store:
        print('... keeping local zarr store')
    else:
        ## remove tmp zarr var directory and all of its contents
        print(f"... removing tmp zarr dir {ec2_store}")
        os.system(f"rm -rf {ec2_store}")

    print("\n* * * * * * * * NETCDF TO ZARR PROCESSING COMPLETE * * * * * * * *\n")




if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    
    parser.add_argument("-v", "--var_name", action="store",
                        help="The variable to be processed (e.g., SALT).",
                        dest="var_name", type=str, required=True)

    parser.add_argument("-p", "--files_to_process", action="store",
                        help="String specifying whether to process all files (-1) or range (start end) from netcdf to zarr.",
                        dest="files_to_process", nargs="*", type=int, required=False, default = [-1])

    parser.add_argument("-l", "--ec2_scratch_dir", action="store",   
                        help="The local scratch directory on the ec2 instance where files will be stored temporarily.", 
                        dest="ec2_scratch_dir", type=str, required=True)
 
    parser.add_argument("-f", "--nc_s3_bucket", action="store",
                        help="The s3 bucket name where all netCDF data files are stored (e.g. s3://podaac-dev-sassie/ECCO_model/N1/V1/HH/NETCDF/).", 
                        dest="nc_s3_bucket", type=str, required=True)

    parser.add_argument("-d", "--zarr_s3_bucket", action="store",
                        help="The destination s3 bucket where processed zarr stores will be saved (e.g., s3://podaac-dev-sassie/ECCO_model/N1/V1/HH/ZARR/).", 
                        dest="zarr_s3_bucket", type=str, required=True)

    parser.add_argument("--keep_local_nc_files", action="store_true",
                        help="Boolean to indicate whether to keep local files on ec2 instance after processing.")
    
    parser.add_argument("--keep_local_zarr_store", action="store_true",
                        help="Boolean to indicate whether to keep local files on ec2 instance after processing.")
    
    parser.add_argument("--push_zarr_to_s3", action="store_true",
                        help="Boolean to indicate whether to push the local ec2 zarr store to the zarr_s3_bucket.")
    
    args = parser.parse_args()

    var_name = args.var_name
    files_to_process = args.files_to_process
    ec2_scratch_dir = args.ec2_scratch_dir
    nc_s3_bucket = args.nc_s3_bucket
    zarr_s3_bucket = args.zarr_s3_bucket
    keep_local_nc_files = args.keep_local_nc_files
    keep_local_zarr_store = args.keep_local_zarr_store
    push_zarr_to_s3 = args.push_zarr_to_s3

    print('WELCOME TO THE NETCDF TO ZARR CONVERTER')
    print('----------------------------------------------')
    print('\nARGUMENTS: ')
    print('var_name ', var_name)
    print('files_to_process ', files_to_process)
    print('ec2_scratch_dir ', ec2_scratch_dir)
    print('nc_s3_bucket ', nc_s3_bucket)
    print('zarr_s3_bucket ', zarr_s3_bucket)
    print('keep_local_nc_files ', keep_local_nc_files)
    print('keep_local_zarr_store ', keep_local_zarr_store)
    print('push_zarr_to_s3 ', push_zarr_to_s3)

    print('\n>>>> BEGIN EXECUTION')
    convert_nc_to_zarr_store(var_name, files_to_process, ec2_scratch_dir, nc_s3_bucket,\
                             zarr_s3_bucket, keep_local_nc_files, keep_local_zarr_store, push_zarr_to_s3)
    print('>>>> END EXECUTION\n')

