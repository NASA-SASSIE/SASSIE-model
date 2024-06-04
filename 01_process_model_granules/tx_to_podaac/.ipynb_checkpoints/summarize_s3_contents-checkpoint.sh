#!/bin/bash

# Summarize contents of a s3 bucket
# Print number of files and size of subdirectories

# Pass the s3 bucket path as the only argument to this script
# For example, you would pass: s3://podaac-dev-sassie/ECCO_model/N1/V1R1/HH/NETCDF/

S3_PATH=$1

# Initialize or clear the output file
OUTPUT_FILE="s3_dir_info.txt"
> $OUTPUT_FILE

# List all objects in the S3 path, extract the directories
subdirectories=$(aws s3 ls $S3_PATH --profile sassie | awk '{print $2}' | grep "/" | sort)

# Print the number of subdirectories
num_subdirectories=$(echo "$subdirectories" | wc -l)

echo "Directory: $S3_PATH" >> $OUTPUT_FILE
echo "Number of subdirectories: $num_subdirectories" >> $OUTPUT_FILE
echo "---------------------------" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE
echo "Number of objects and total size for each subdirectory:" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

# Iterate over each subdirectory from provided s3 path
for dir in $subdirectories
do
    # Prints number of objects in directory and total size
    num_and_size=$(aws s3 ls --summarize --human-readable --recursive $S3_PATH$dir --profile sassie | tail -2)
    
    # Write the directory info to the output file
    echo $S3_PATH$dir >> $OUTPUT_FILE
    echo $num_and_size >> $OUTPUT_FILE
    echo "" >> $OUTPUT_FILE
    wait
done

echo "Directory information has been saved to $OUTPUT_FILE"