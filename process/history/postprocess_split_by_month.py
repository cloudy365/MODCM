from my_module import np, h5py, os, tqdm
from my_module.data.comm import save_data_hdf5



def run_one_year(icat, iyr, imon):
    doy_normal = [1, 1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366]
    doy_leap = [1, 1, 32, 61, 92, 122, 153, 183, 214, 245, 275, 306, 336, 367]
    
    data_folder = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/daily/{}/{}".format(icat, iyr)
    data_files = os.listdir(data_folder)
    output_dir = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/monthly/"
    output_file = os.path.join(output_dir, "{}_{}_{}.h5".format(icat, iyr, str(imon).zfill(2)))
    
    
    if iyr in [2000, 2004, 2008, 2012]:
        doy = doy_leap
    else:
        doy = doy_normal
    
    
    print ">> work on {}.{}".format(iyr, imon)
    if icat in ['vis']:
        radiance_all = np.zeros((3600, 7200, 7))
        insolation_all = np.zeros((3600, 7200, 7))
        num_all = np.zeros((3600, 7200, 7))
    else:
        print ">> err: only supports visible bands right now."
        return
        
        
    doy_0 = doy[imon]
    doy_1 = doy[imon+1]
    
    
    for ifile in data_files:
        iday = int(ifile.split('.')[0][-3:]) # Example: ifile --> A2000265.h5
        
        if iday in range(doy_0, doy_1):
            ifile = os.path.join(data_folder, ifile)
            
            try:
                with h5py.File(ifile, 'r') as h5f:
                    solar = h5f['insolation_sum'][:]
                    rad = h5f['radiance_sum'][:]
                    num = h5f['radiance_num'][:]
                
                
                insolation_all = insolation_all + solar
                radiance_all = radiance_all + rad
                num_all = num_all + num
            except Exception as err:
                print ">> err: {}".format(err)
                continue

                
    mean_rad = np.array(radiance_all / num_all)
    mean_sol = np.array(insolation_all / num_all)
    
    save_data_hdf5(output_file, 'monthly_radiance', mean_rad)
    save_data_hdf5(output_file, 'monthly_insolation', mean_sol)




if __name__ == '__main__':
#     """
#     e.g., aprun -n 16 python split_by_month.py VIS
#     """
#     CATEGORY = sys.argv[1]

#     NUM_CORES = 16
    
#     import mpi4py.MPI as MPI

#     comm = MPI.COMM_WORLD
#     comm_rank = comm.Get_rank()

#     yrs_all = range(2000, 2016)

#     iyr = comm.scatter(yrs_all if comm_rank == 0 else None, root=0)

#     print ">> Rank: {}, processing {}...".format(comm_rank, iyr)
#     run_one_year(iyr, CATEGORY)

#     if comm_rank == 0:
#         print ">> Done."


    for imon in range(1, 13):
        run_one_year('vis', 2001, imon)
