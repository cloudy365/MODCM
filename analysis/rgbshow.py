

from my_module import np, os, toimage, sys
from my_module.plot import enhance_rgb



def load_data(iband, iday):
    fname = 'b{}_{}.npz'.format(iband, iday)
    data_dir = os.path.join(WORKING_DIR, fname)
    data_file = np.load(data_dir)
    data = data_file['avg_moving'][:]
    np.place(data, data<0, 0)    
    return data


WORKING_DIR = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/moving_average/31days"


def main_process_one_day(iday, three_bands):
    r = load_data(three_bands[0], iday)
    g = load_data(three_bands[1], iday)
    b = load_data(three_bands[2], iday)
    
    rgb = np.dstack((r, g, b))
    
    # enhance and save image
    #enhanced_image = enhance_rgb(rgb, scale_method=['discrete', 'RLT', 'RLT'], scale_factors=[1, 1.1, 1.1], cmins=[0]*3, cmaxs=[180, 280, 440])

    # 
    enhanced_image = enhance_rgb(rgb, scale_method=['RLT']*3, scale_factors=[1]*3, cmins=[0]*3, cmaxs=[282, 333, 402])
    toimage(enhanced_image).save('vis_rgb_999th/{}.png'.format(iday))



if __name__ == '__main__':
    import mpi4py.MPI as MPI

    NUM_CORES = int(sys.argv[1])
    bands = [1, 4, 3]

    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()

    for i in range(1, 366, NUM_CORES):
        iday = comm_rank + i
        try:
            print ">> PE: {}, on day: {}.".format(comm_rank, iday)
            main_process_one_day(iday, bands)
        except Exception as err:
            print ">> I'm PE: {}, and there is an error: {}".format(comm_rank, err)
            continue

    print ">> I'm PE: {}, and finished my job...".format(comm_rank)
