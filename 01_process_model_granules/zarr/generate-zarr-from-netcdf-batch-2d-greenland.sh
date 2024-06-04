#!/bin/bash

# this script converts 2D Greenland SASSIE-ECCO model netCDF datasets to zarr and pushes them to the cloud.
# you must pass one argument: the disk number (or nvme storage number)

# log files are generated to keep track of progress and any errors produced during conversion.

# there are ~2600 files in a full netCDF dataset

# specify datasets you want to convert
declare -a arr=("PHIBOT" "sIceLoad" "KPPhbl" "EXFaqh" "EXFatemp" "EXFempmr" "EXFevap" "EXFpreci" "EXFroff" "EXFqnet" "EXFhl" "EXFhs")

## now loop through the above array
for x in "${arr[@]}"
## first create the zarr store (empty dataset that we will then fill)
do

  for i in `seq 0 0`
  do
    sn=$((i))
    en=$((sn+1))
    fsn=$(printf "%0*d" 4 $sn)
    fen=$(printf "%0*d" 4 $en)
    
    # disk needs to be the same nvme for the whole conversion process because the zarr store is located in one place
    disk=$1
    
    echo $sn $en $fsn $fen $disk

    # this command intentionally does not push zarr to s3 bucket
    cmd="python create-zarr-from-netcdf-greenland.py --var_name $x --files_to_process $sn $en --ec2_scratch_dir /nvme_data${disk} --nc_s3_bucket s3://ecco-processed-data/SASSIE/N1/GREENLAND/NETCDF/ --zarr_s3_bucket s3://ecco-processed-data/SASSIE/N1/GREENLAND/ZARR/ --keep_local_nc_files --keep_local_zarr_store 1> ${x}_${fsn}_${fen}.log 2> ${x}_${fsn}_${fen}.err.log &"
  
    echo $cmd
    eval $cmd
  done
  
  # wait for this zarr store to be established before proceeding to write to it
  wait

## then write to zarr store with data from netcdfs in batches of 100
## this code does NOT push the local ec2 zarr store to the s3 bucket
  for i in `seq 0 25`
  do
    sn=$((i*100))
    en=$((sn+100))
    fsn=$(printf "%0*d" 4 $sn)
    fen=$(printf "%0*d" 4 $en)

    echo $sn $en $fsn $fen $disk
    
    # this command intentionally does not push zarr to s3 bucket but keeps all local files
    cmd="python create-zarr-from-netcdf-greenland.py --var_name $x --files_to_process $sn $en --ec2_scratch_dir /nvme_data${disk} --nc_s3_bucket s3://ecco-processed-data/SASSIE/N1/GREENLAND/NETCDF/ --zarr_s3_bucket s3://ecco-processed-data/SASSIE/N1/GREENLAND/ZARR/ --keep_local_nc_files --keep_local_zarr_store 1> ${x}_${fsn}_${fen}.log 2> ${x}_${fsn}_${fen}.err.log &"
  
    echo $cmd
    eval $cmd

  done

  # If wait is called without any arguments, it waits for all currently active child processes to complete.
  wait

  # Once all data is written to local ec2 zarr store, push store to s3 bucket
  printf "\n%s\n" ">>> pushing zarr store to s3 bucket."
  
  cmd="aws s3 sync /nvme_data${disk}/zarr_tmp/${x}_AVG_DAILY.ZARR/ s3://ecco-processed-data/SASSIE/N1/GREENLAND/ZARR/${x}_AVG_DAILY.ZARR/"
  
  echo $cmd
  eval $cmd

  # then clean up nvme
  echo "wiping nvme"
  cmd="rm -rf /nvme_data${disk}/nc_tmp/"
  echo $cmd
  eval $cmd
  cmd="rm -rf /nvme_data${disk}/zarr_tmp/"
  echo $cmd
  eval $cmd
  
  wait
  
done

