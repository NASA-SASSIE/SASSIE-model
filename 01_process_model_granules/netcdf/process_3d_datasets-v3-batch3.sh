#!/bin/bash

# Processes raw, binary SASSIE-ECCO model output into two separate datasets in netCDF format: 1) HH and 2) Greenland cutout.

# Generates logs to keep track of progress and any errors produced during the process.

# Batch 3 to process the following datasets on one EC2
declare -a arr=("EXF_day_mean" "seaice_state_day_mean" "KPP_mix_day_mean" "tr_adv_x_3D_day_mean" "vol_adv_day_mean")

## now loop through the above array
for x in "${arr[@]}"
do

  for i in `seq 0 19`
  do
    sn=$((i*10))
    en=$((sn+10))
    fsn=$(printf "%0*d" 3 $sn)
    fen=$(printf "%0*d" 3 $en)
    disk=$((sn / 20 +1))
    echo $sn $en $fsn $fen $disk
    #s3://ecco-processed-data/SASSIE/N1/V1/HH/NETCDF/ 
    cmd="python generate-sassie-ecco-netcdfs-s3-v3.py --root_filenames $x --root_s3_name s3://ecco-model-granules/SASSIE/N1/ --root_dest_s3_name s3://ecco-processed-data/SASSIE/N1/ --files_to_process $sn $en -l /nvme_data${disk} --push_to_s3 --save_nc_to_disk 1> ${x}_${fsn}_${fen}.log 2> ${x}_${fsn}_${fen}.err.log &"
    echo $cmd
    eval $cmd

  done

  # If wait is called without any arguments, it waits for all currently active child processes to complete.
  wait
done


