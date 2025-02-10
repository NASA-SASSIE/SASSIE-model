# SASSIE-ECCO model

This repository includes code used to process and analyze the pan-Arctic SASSIE-ECCO ocean model solution.

The SASSIE ocean model simulation was produced by downscaling the global Estimating the Circulation and Climate of the Ocean (ECCO) state estimate from 1/3 to 1/12-degree (LLC1080) grid cells. The ECCO v5 Alpha (LLC270) global solution provided initial and boundary conditions and atmospheric forcing. Model ocean and sea-ice state estimates are dynamically and kinematically consistent reconstructions of the three-dimensional time-evolving ocean, sea-ice, and surface atmospheric states. The SASSIE ECCO model dataset consists of daily averages of diagnostic variables for seven years (January 15, 2014 to February 7, 2021). A complete User Guide with additional details about the model configuration and file formats is available [here](https://github.com/NASA-SASSIE/SASSIE-model/blob/main/SASSIE%20ECCO%20Model%20User%20Guide.pdf)

Code used to process model granules from the binary output to NetCDF and Zarr formats is available in the [01_process_model_granules](https://github.com/NASA-SASSIE/SASSIE-model/tree/main/01_process_model_granules) directory.

Code for example analyses is also provided in [02_analyses](https://github.com/NASA-SASSIE/SASSIE-model/tree/main/02_analyses), including plotting example diagnostic fields and computing budgets.<br><br>

![image](https://github.com/NASA-SASSIE/SASSIE-model/blob/main/sassie-ecco_salinity_09-15-2020.png)
