from my_module import np, os, tqdm
import sys


doy_normal = [1, 1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366]
doy_leap = [1, 1, 32, 61, 92, 122, 153, 183, 214, 245, 275, 306, 336, 367]


def run_one_year(iyr, icat):
    data_folder = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/daily/{}/{}".format(icat, iyr)
    data_files = os.listdir(data_folder)
    
    if iyr in [2000, 2004, 2008, 2012]:
        doy = doy_leap
    else:
        doy = doy_normal
    
    for imon in range(1, 13):
        print iyr, imon
        
        if icat in ['VIS', 'SWIR_P2', 'SOLAR']:
            rad_all = np.zeros((3600, 7200, 7))
            num_all = np.zeros((3600, 7200, 7))
        else:
            rad_all = np.zeros((3600, 7200, 8))
            num_all = np.zeros((3600, 7200, 8))
        
        doy_0 = doy[imon]
        doy_1 = doy[imon+1]
        
        for ifile in data_files:
            iday = int(ifile.split('.')[1])
            
            if iday in range(doy_0, doy_1):
                ifile_path = os.path.join(data_folder, ifile)
                fnpz = np.load(ifile_path)

                if icat == 'SOLAR':
                    tmp_rad = fnpz['solar_sum'][:]
                    tmp_num = fnpz['solar_num'][:]
                else:
                    tmp_rad = fnpz['rad_sum'][:]
                    tmp_num = fnpz['rad_num'][:]
                
                rad_all += tmp_rad
                num_all += tmp_num
        
        file_out = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/monthly/{}_{}_{}.npz".format(icat, iyr, imon)
        
        np.savez_compressed(file_out, rad_all=rad_all, num_all=num_all)




if __name__ == '__main__':
    """
    e.g., aprun -n 16 python split_by_month.py VIS
    """
    CATEGORY = sys.argv[1]

    NUM_CORES = 16
    
    import mpi4py.MPI as MPI

    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()

    yrs_all = range(2000, 2016)

    iyr = comm.scatter(yrs_all if comm_rank == 0 else None, root=0)

    print ">> Rank: {}, processing {}...".format(comm_rank, iyr)
    run_one_year(iyr, CATEGORY)

    if comm_rank == 0:
        print ">> Done."
