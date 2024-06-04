#!/bin/bash

# Transfer files from ECCO s3 bucket to SASSIE PODAAC s3 bucket

# This script loops through the variables listed below and transfers them from one private s3 bucket (ecco) to another private bucket (sassie). 
# Both s3 buckets require a private access key which is why we are transferring the files to the local ec2 and then transferring them to the final s3 bucket destination.

# List variables to transfer

# all 39 2D fields
declare -a arr=("ETAN" "PHIBOT" "sIceLoad" "KPPhbl" "EXFaqh" "EXFatemp" "EXFempmr" "EXFevap" "EXFpreci" "EXFroff" "EXFqnet" "EXFhl" "EXFhs" "EXFlwdn" "EXFlwnet" "EXFswdn" "EXFswnet" "EXFuwind" "EXFtaux" "EXFvwind" "EXFtauy" "oceFWflx" "oceQnet" "oceQsw" "oceTAUX" "oceTAUY" "SFLUX" "TFLUX" "SIatmFW" "SIatmQnt" "SIarea" "SIheff" "SIhsnow" "SIuice" "SIvice" "ADVxHEFF" "ADVxSNOW" "ADVyHEFF" "ADVySNOW")
# Specify which nvme disk will be used for each variable
declare -a disk=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "11" "12" "13" "14" "15" "16" "1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "11" "12" "13" "14" "15" "16" "1" "2" "3" "4" "5" "6" "7")

# # remaining 3d fields
# declare -a arr=("ADVy_TH" "DFrE_SLT" "DFrE_TH" "DFrI_SLT" "DFrI_TH" "DFxE_SLT" "DFxE_TH" "DFyE_SLT" "DFyE_TH" "UVELMASS" "VVELMASS" "WVELMASS")
# declare -a disk=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "11" "12")

# now loop through the above arrays
for ((i=0; i<"${#arr[@]}"; i++)); do
  x="${arr[i]}"
  d="${disk[i]}"

  printf "\n%s\n" ">>> Transferring ${x} to SASSIE bucket using disk ${d}."

  # copy dataset from bucket to local ec2
  # copy dataset from ec2 to sassie bucket
  # then clean up nvme

  # CODE FOR TRANSFERRING NETCDF DIRECTORIES
  # cmd="aws s3 sync s3://ecco-processed-data/SASSIE/N1/GREENLAND/NETCDF/${x}_AVG_DAILY/ /nvme_data${d}/ds_tmp/${x}_AVG_DAILY/ 1> ${x}_tx_ec2.log 2> ${x}_tx_ec2.err.log && \
  #      aws s3 sync /nvme_data${d}/ds_tmp/${x}_AVG_DAILY/ s3://podaac-dev-sassie/ECCO_model/N1/V1R1/GREENLAND/NETCDF/${x}_AVG_DAILY/ --profile sassie 1> ${x}_tx_sassie_bucket.log 2> ${x}_tx_sassie_bucket.err.log && \
  #      rm -rf /nvme_data${d}/ds_tmp/${x}_AVG_DAILY/"

  # CODE FOR TRANSFERRING ZARR DIRECTORIES
  cmd="aws s3 sync s3://ecco-processed-data/SASSIE/N1/GREENLAND/ZARR/${x}_AVG_DAILY.ZARR/ /nvme_data${d}/ds_tmp/${x}_AVG_DAILY.ZARR/ 1> ${x}_tx_ec2.log 2> ${x}_tx_ec2.err.log && \
       aws s3 sync /nvme_data${d}/ds_tmp/${x}_AVG_DAILY.ZARR/ s3://podaac-dev-sassie/ECCO_model/N1/V1R1/GREENLAND/ZARR/${x}_AVG_DAILY.ZARR/ --profile sassie 1> ${x}_tx_sassie_bucket.log 2> ${x}_tx_sassie_bucket.err.log && \
       rm -rf /nvme_data${d}/ds_tmp/${x}_AVG_DAILY.ZARR/"
  
  echo $cmd
  eval $cmd &
  
done