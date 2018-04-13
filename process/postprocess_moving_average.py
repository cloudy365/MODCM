

from my_module import np, os, sys, toimage


def get_fname_bandindex(iband):
    
    iband = str(iband)
    
    # predefined categories
    VIS = np.array([1, 2, 3, 4, 5, 6, 7], dtype=str)
    SWIR_P1 = np.array([8,9,10,11,12,'13lo','13hi','14lo'], dtype=str)
    SWIR_P2 = np.array(['14hi',15,16,17,18,19,26], dtype=str)
    LW_P1 = np.array([20,21,22,23,24,25,27,28], dtype=str)
    LW_P2 = np.array([29,30,31,32,33,34,35,36], dtype=str)
    
    # retrieve the index of band in a specified category as well as the data file
    if iband in VIS:
        ifile = "VIS_{}.npz"
        iband_idx = np.where(iband==VIS)[0][0]
    elif iband in SWIR_P1:
        ifile = "SWIR_P1_{}.npz"
        iband_idx = np.where(iband==SWIR_P1)[0][0]
    elif iband in SWIR_P2:
        ifile = "SWIR_P2_{}.npz"
        iband_idx = np.where(iband==SWIR_P2)[0][0]
    elif iband in LW_P1:
        ifile = "LW_P1_{}.npz"
        iband_idx = np.where(iband==LW_P1)[0][0]
    elif iband in LW_P2:
        ifile = "LW_P2_{}.npz"
        iband_idx = np.where(iband==LW_P2)[0][0]
    
    return ifile, iband_idx

      
# 
def get_process_days(iday, num_window):
    # successsive 3 years
    _days_3yrs = np.append(np.append(range(1, 366), range(1, 366)), range(1, 366)) 
    # given a day (iday) as the central day, the required processing days are
    process_days = _days_3yrs[365+iday-(num_window-1)/2-1:365+iday+(num_window-1)/2]
    
    return process_days


def example_HowManyDaysToBeatNoise():
    data_folder = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/daily_mean"
    iband = 1
    fname, band_idx = get_fname_bandindex(iband)

    tmp_array = np.zeros((3600, 7200))
    for iday in range(1, 367):
        data_file = fname.format(str(iday).zfill(3))
        data_path = os.path.join(data_folder, data_file)
        data = np.load(data_path)['mean_rad'][:, :, band_idx]
        tmp_array = np.nansum([tmp_array, data], axis=0)

        data_new = np.nan_to_num(tmp_array/iday)
        toimage(data_new).save("/u/sciteam/smzyz/b1_{}.png".format(iday))
        

def main_process_one_time(iband, iday, num_window):
    """
    Main function, process one time moving average.
    """
    data_folder = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/daily_mean"
    # 1. use get_fname_bandindex to retrieve fname and band_idx
    fname, band_idx = get_fname_bandindex(iband)
    
    # 2. use get_process_days to retrieve process_days
    process_days = get_process_days(iday, num_window)
    
    # 3. do process
    sum_moving = np.zeros((3600, 7200))
    for iday in process_days:
        #print iday
        data_file = fname.format(str(iday).zfill(3))
        data_path = os.path.join(data_folder, data_file)
        data = np.load(data_path)['mean_rad'][:, :, band_idx]
        sum_moving = np.nansum([sum_moving, data], axis=0)

    avg_moving = np.nan_to_num(sum_moving / iday)
    np.savez("/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/moving_average/b{}_{}".format(iband, iday), avg_moving=avg_moving)

    
if __name__ == '__main__':
    """
    Task: get MODIS climate moving average (i.e., 1--365 files, for each band individually)
    """
    
    NUM_CORES = int(sys.argv[1])
    NUM_WINDOW = int(sys.argv[2])
    BANDs = [2, 22, 27, 28, 29, 30, 31, 32]#[1, 4, 3, 6] #sys.argv[3:]

    import mpi4py.MPI as MPI
    
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    
    for iband in BANDs:
        for i in range(0, 366, NUM_CORES):
            iday = comm_rank + i
            try:
                main_process_one_time(iband, iday, NUM_WINDOW)
            except Exception as err:
                print ">> I'm PE: {}, I have a problem: {}".format(comm_rank, err)
                continue
    print ">> I'm PE: {}, I have finished my job..".format(comm_rank)
        
