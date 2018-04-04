

from zyz_core import np, os, Dataset







#if __name__ == '__main__':
def climate_marble_monthly_mean():
    working_dir = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/monthly/"
    output_dir = "/u/sciteam/smzyz/"


    categories = ['SWIR_P1', 'SWIR_P2', 'LW_P1', 'LW_P2']
    for icat in categories:
        for imon in range(1, 13):
            
            if icat in ['VIS', 'SWIR_P2']:
                rad_all = np.zeros((3600, 7200, 7))
                num_all = np.zeros((3600, 7200, 7))
            else:
                rad_all = np.zeros((3600, 7200, 8))
                num_all = np.zeros((3600, 7200, 8))

            for iyr in range(2000, 2016):
                ifile = "{}_{}_{}.npz".format(icat, iyr, imon)
                inpz = np.load(os.path.join(working_dir, ifile))

                rad_all += inpz['rad_all']
                num_all += inpz['num_all']
        
            mean_rad = np.array(rad_all / num_all)
            np.savez_compressed(os.path.join(output_dir, "{}_{}.npz".format(icat, imon)), mean_rad = mean_rad)


def monthly_mean_rewrite_netCDF4(icat):
    working_dir = "/u/sciteam/smzyz/"
    output_dir = working_dir
    
    if icat in ['VIS', 'SWIR_P2']:
        tot_bands = 7
    else:
        tot_bands = 8
        
    if icat == 'VIS':
        channels = [1, 2, 3, 4, 5, 6, 7]
    elif icat == 'SWIR_P1':
        channels = [8,9,10,11,12,'13lo','13hi','14lo']
    elif icat == 'SWIR_P2':
        channels = ['14hi',15,16,17,18,19,26]
    elif icat == 'LW_P1':
        channels = [20,21,22,23,24,25,27,28]
    elif icat == 'LW_P2':
        channels = [29,30,31,32,33,34,35,36]
        
    
    for iband in range(tot_bands):
        print ">> Process band: {}".format(channels[iband])
        data_out = []
        for imon in range(1, 13):
            ifile = "{}_{}.npz".format(icat, imon)
            inpz = np.load(os.path.join(working_dir, ifile))
            idata = inpz['mean_rad'][:, :, iband]
        
            data_out.append(idata)
        
        
        # Handle bad values and nan values
        data_out = np.array(data_out)
        data_out = np.nan_to_num(data_out)
        np.place(data_out, data_out<=0, -999.)
        
        
        # Start writing netCDF4 file
        nc_file = Dataset(os.path.join(output_dir, "monthly_climate_band_{}.nc".format(channels[iband])), 'w')
        print ">> Created variables with attributes"
        # createDimension
        dim_lat = nc_file.createDimension('lat', 3600)
        dim_lon = nc_file.createDimension('lon', 7200)
        dim_month = nc_file.createDimension('month', 12)
        # createVariable
        lat = nc_file.createVariable('lat', 'f4', ('lat',))
        lat.units = 'degree'
        lat.standard_name = 'latitude'
        lon = nc_file.createVariable('lon', 'f4', ('lon',))
        lon.units = 'degree'
        lon.standard_name = 'longitude'
        month = nc_file.createVariable('month', 'f4', ('month',))
        month.standard_name = 'month'
        data = nc_file.createVariable('mean_radiance', 'f4', ('month', 'lat', 'lon'), fill_value=-999.)
        data.standard_name = 'Climatic monthly mean radiances (VZA<40 degree)'
        data.units = 'W m-2 sr-1 um-1'


        print ">> Writing data..."
        lat[:] = np.arange(90, -90, -0.05) - 0.025
        lon[:] = np.arange(-180, 180, 0.05) + 0.025
        month[:] = np.arange(1, 13)
        data[:] = data_out
        nc_file.close()
        
        
def climate_marble_daily_mean(icat, iday):
    working_dir = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/daily/"
    output_dir = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/daily_mean/"

    # iterating each month and collect available monthly mean results of the specified category.
    # for iday in tqdm(range(1, 367), miniters=1):

    if icat in ['VIS', 'SWIR_P2']:
        rad_all = np.zeros((3600, 7200, 7))
        num_all = np.zeros((3600, 7200, 7))
    else:
        rad_all = np.zeros((3600, 7200, 8))
        num_all = np.zeros((3600, 7200, 8))

    for iyr in range(2000, 2016):
        ifile = "{}.{}.npz".format(iyr, str(iday).zfill(3))
        try:
            inpz = np.load(os.path.join(working_dir+"{}/{}".format(icat, iyr), ifile))

            rad_all += inpz['rad_sum']
            num_all += inpz['rad_num']
        except Exception as err:
            print "Error: {}".format(err)

    mean_rad = np.array(rad_all / num_all)

    np.savez_compressed(os.path.join(output_dir, "{}_{}.npz".format(icat, str(iday).zfill(3))), mean_rad = mean_rad)        
        
        
        
if __name__ == '__main__':
    
    # Task: get MODIS climate daily mean (i.e., 1--366 files, for all 5 categories individually)
    # Nodes: 92
    # Cores: 366
    CATS = ['VIS', 'SWIR_P1', 'SWIR_P2', 'LW_P1', 'LW_P2']
    
    import mpi4py.MPI as MPI
    
    comm = MPI.COMM_WORLD()
    comm_rank = comm.Get_rank()
    
    # days_all = range(1, 367)
    iday = comm_rank + 1
    for icat in CATS:
        climate_marble_daily_mean(icat, iday)
    
    print ">> I'm PE: {}, and I have finished my job: day_{}.".format(comm_rank, iday)
        