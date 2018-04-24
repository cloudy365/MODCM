# MODCM

This project is for generating MODIS CM using Blue Waters.


MODIS data: ~/scratch/data/MODIS/MOD021KM


Marble data (0.05 deg):  

(1) Daily (.npz): ~/scratch/results/MODIS_ClimateMarble_005deg/daily 
    (6 sub-folders)
    This dataset is stored by 6 categories, each of them is further sorted by each year from 2000 to 2015.
    ClimateMarble.py/ClimateMarble_Solar.py and helper_func.py are used.

(2) Daily Mean (.npz): ~/scratch/results/MODIS_ClimateMarble_005deg/daily_mean 
    (no sub-folder)
    This dataset is separated for 6 categories, each of them is further stored in 366 files (days).
    postprocess_climate_mean.py -> climate_marble_daily_mean(icat, iday) is used to process the daily (1) data.

(3) Monthly (.npz): ~/scratch/results/MODIS_ClimateMarble_005deg/monthly   
    (no sub-folder)
    This dataset is separated for 5 categories (no SOLAR for now), each of them is further sorted by each month from 2000 to 2015.
    postprocess_split_by_month.py is used to process the daily (1) data.

(4) Monthly Mean (.npz): ~/scratch/results/MODIS_ClimateMarble_005deg/monthly_mean
    (no sub-folder)
    This dataset is separated for 5 categories (no SOLAR for now), each of them is further stored in 12 files (months).
    postprocess_climate_mean.py -> climate_marble_monthly_mean() is used to process the monthly (3) data.

(5) Monthly Mean (.nc): ~/scratch/results/MODIS_ClimateMarble_005deg/monthly_mean_nc
    (no sub-folder)
    This dataset is separated for 38 channels (38 files, no SOLAR for now).
    postprocess_climate_mean.py -> monthly_mean_rewrite_netCDF4(icat) is used to process the monthly mean (4) data.

(6) Moving Average (.npz): ~/scratch/results/MODIS_ClimateMarble_005deg/moving_average
    (3 sub-folders for 17-day, 31-day, and 41-day running window)
    The dataset is first separated for 3 running window, and then separated by band (17 bands for now).
    postprocess_moving_average.py is used to process the daily mean (2) data.



