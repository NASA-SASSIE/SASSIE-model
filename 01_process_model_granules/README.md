# Process model granules

**This directory includes the code and files used to process the raw, binary SASSIE-ECCO model granules to NetCDF and ZARR formats.**

Additional code is included that 1) extracts just the surface layer (k=0) of the 3D model fields, and 2) transfers processed files from one private s3 bucket to another.

You can view/access the model granules using:<br>
`aws s3 ls s3://podaac-dev-sassie/ECCO_model/N1/V1R1/ --profile sassie`<br>

And see the directory structure as:<br>

./HH/<br>
>GRID/<br>
>NETCDF/<br>
>NETCDF_3D_SURF/<br>
>ZARR/<br>

./GREENLAND/<br>
>GRID/<br>
>NETCDF/<br>
>ZARR/<br>

**Note**: The `GRID` directories include both NetCDF and ZARR geometry file formats.
                               
## /netcdf
Scripts used to process the binary model output to clean datasets in netCDF format of the HH Arctic domain and Greenland cutout.

To expedite processing time, datasets were processed in batches using 3 EC2 remote machines:

`generate-sassie-ecco-netcdfs-s3-v3.py`: Python script that processes raw model output to netCDF datasets.

`process_3d_datasets-v3-batch1.sh`: Batch 1 run on first EC2 to process model granules.<br>
`process_3d_datasets-v3-batch2.sh`: Batch 2 run on second EC2 to process model granules.<br>
`process_3d_datasets-v3-batch3.sh`: Batch 3 run on third EC2 to process model granules.

## /zarr
Scripts used to convert netCDF datasets to Zarr stores.

`create-zarr-from-netcdf-greenland.py`: Python script used to convert netCDF datasets to Zarr for the Greenland cutout.<br>
`create-zarr-from-netcdf-HH.py`: Python script used to convert netCDF datasets to Zarr for the HH Arctic field.

`generate-zarr-from-netcdf-batch-3d-greenland.sh`: Converts netCDF to Zarr in parallel (batches of 400) for larger 3D fields.<br>
`generate-zarr-from-netcdf-batch-2d-greenland.sh`: Converts netCDF to Zarr in parallel (batches of 100) for smaller 2D fields.

`generate-zarr-from-netcdf-batch-3d-HH.sh`: Converts netCDF to Zarr in parallel (batches of 400) for larger 3D fields.<br>
`generate-zarr-from-netcdf-batch-2d-HH.sh`: Converts netCDF to Zarr in parallel (batches of 100) for smaller 2D fields.

`create_geometry_file_zarr.ipynb`: Notebook converting HH and Greenland geometry files from netcdf to zarr.

**Note**: 3D fields were processed in larger batches (n=400) across nvme disks to balance processing time to open/write the larger netCDF files.

## /extract_surface_layer

`extract-surface-layer-from-sassie-3d-granules.py`: Python script to extract only the surface layer (k=0) of a 3D field and save as a new netCDF dataset.

`process-surface-fields-from-3d-single.sh`: Extracts surface layer from a single 3D field where you can specify number of files to process. <br>
`process-surface-fields-from-3d.sh`: Extracts surface layer from an array of 3D variables, saves as new netcdf datasets, and pushes to the cloud.

## /tx_to_podaac

`tx_to_podaac.sh`: Transfers data directories from one private s3 bucket to another private s3 bucket on an EC2.<br>
`summarize_s3_contents.sh`: Summarizes the total size (GB) and number of files for each subdirectory within a parent s3 bucket.

**Note**: These scripts are configured to use private access keys stored in a local credentials file.
