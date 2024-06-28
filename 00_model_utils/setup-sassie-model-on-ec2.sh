#!/bin/bash

# this script requires two arguments: 1) the aws sassie access key and 2) the aws sassie secret access key
# just pass the two keys without quotes (just the code)

# this script executes the following:
# 1: creates a credential file containing the sassie credentials that will be used in plotting and analyzing the model output stored on the cloud
# 2: configures sassie access keys to aws cli so that you can use --profile sassie to access model files
# 3: copies the HH (pan Arctic domain) geometry file, a single netCDF that is useful to have on your local ec2

cd ~

## this code manually adds a credential file but it is not needed if you use the aws commands below
# # add credentials file for sassie s3 bucket access
# cred_file="/home/jpluser/.aws/credentials"
# # write credentials to new file
# cat > $cred_file <<EOF
# [sassie_test]
# aws_access_key_id = $1
# aws_secret_access_key = $2
# EOF

# configure aws credentials to use --profile sassie
aws configure set aws_access_key_id $1 --profile sassie
aws configure set aws_secret_access_key $2 --profile sassie
aws configure set output json --profile sassie
aws configure set region us-west-2 --profile sassie
aws s3 ls s3://podaac-dev-sassie --profile sassie

# check to see if it's there
if test -f ~/.aws/credentials; then
  echo "credentials file $cred_file created.";
else ! test -f ~/.aws/credentials
  echo "ERROR: credentials file $cred_file was not created."; 
fi

# make directory to store files on local ec2
mkdir -p ~/sassie_model
echo "directory ~/sassie_model created"

# copy geometry file to local ec2
cmd="aws s3 cp s3://podaac-dev-sassie/ECCO_model/N1/V1R1/HH/GRID/GRID_GEOMETRY_SASSIE_HH_V1R1_NATIVE_LLC1080.nc ./sassie_model/ --profile sassie"

echo $cmd
eval $cmd

