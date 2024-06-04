#!/bin/bash

# Script that extracts just the surface layer (k=0) of a single 3D field and saves as a new dataset.
# This script allows you to specify how many files you'd like to process
# Takes two arguments, the first is the netCDF file index to start, the second is the netCDf file index to end
# You need to manually add the s3 bucket access keys below

# Specify which datasets you want to process
declare -a arr=("THETA")

## now loop through the above array
for x in "${arr[@]}"
do

  for i in `seq $1 $2`
  do
    sn=$((i*10))
    en=$((sn+10))
    fsn=$(printf "%0*d" 3 $sn)
    fen=$(printf "%0*d" 3 $en)
    disk=$((sn / 2 +1))
    
    echo $sn $en $fsn $fen $disk
    ## keep files saved to disk
    #     cmd="python extract-surface-layer-from-sassie-3d-granules.py --var_3d $x --sassie_s3_netcdf_dir s3://podaac-dev-sassie/ECCO_model/N1/V1/HH/NETCDF/ --ec2_nvme_scratch_dir /nvme_data${disk} --sassie_key --sassie_secret --dest_s3_name s3://podaac-dev-sassie/ECCO_model/N1/V1/HH/NETCDF_3D_SURF/  --files_to_process $sn $en  --push_to_s3 --save_nc_to_disk 1> ${x}_${fsn}_${fen}.log 2> ${x}_${fsn}_${fen}.err.log &"

    ## do not keep files saved to disk
        cmd="python extract-surface-layer-from-sassie-3d-granules.py --var_3d $x --sassie_s3_netcdf_dir s3://podaac-dev-sassie/ECCO_model/N1/V1/HH/NETCDF/ --ec2_nvme_scratch_dir /nvme_data${disk} --sassie_key --sassie_secret --dest_s3_name s3://podaac-dev-sassie/ECCO_model/N1/V1/HH/NETCDF_3D_SURF/  --files_to_process $sn $en  --push_to_s3 1> ${x}_${fsn}_${fen}.log 2> ${x}_${fsn}_${fen}.err.log &"
    
    echo $cmd
    eval $cmd

  done

  # If wait is called without any arguments, it waits for all currently active child processes to complete.
  wait
done


