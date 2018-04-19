

from my_module import np, os, toimage, sys
from my_module.plot import enhance_rgb


def retrieve_ref(iband, iday):
    """
    Retrieve global mean reflectance of band 'iband'
    """
    
    # read data
    data_folder = '/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/moving_average/31days'
    rad = np.load("{0}/b{1}/b{1}_{2}.npz".format(data_folder, iband, iday))['avg_moving'][:]
    sol = np.load("{0}/s{1}/sS{1}_{2}.npz".format(data_folder, iband, iday))['avg_moving'][:]
    
    # tidy up data
    # eliminate bad values for both radiance and insolation
    np.place(sol, (sol<3)|(rad==0), 0)
    np.place(rad, (sol<3)|(rad==0), 0)
    
    ref = rad/sol
    np.place(ref, ref>2, np.nan)
    ref = np.nan_to_num(ref)
    np.place(ref, ref>1, 1)
    
    return ref
    

def retrieve_day_natural_ref_rgb(iday):    
    ref_1 = retrieve_ref(1, iday)
    ref_2 = retrieve_ref(2, iday)
    ref_6 = retrieve_ref(6, iday)
    
    mask = (ref_1==0)|(ref_2==0)|(ref_6==0)
    np.place(ref_1, mask, 0)
    np.place(ref_2, mask, 0)
    np.place(ref_6, mask, 0)
    
    rgb = np.dstack((ref_6, ref_2, ref_1))
    
    
    enhanced_image = enhance_rgb(rgb, scale_method=['RLT']*3, scale_factors=[1]*3, cmins=[0]*3, cmaxs=[1]*3)
    toimage(enhanced_image).save('day_natural/{}.png'.format(iday))



if __name__ == '__main__':
    import mpi4py.MPI as MPI
    
    NUM_CORES = int(sys.argv[1])
    
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    
    for i in range(1, 366, NUM_CORES):
        iday = comm_rank + i
        try:
            print ">> PE: {}, on day: {}.".format(comm_rank, iday)
            retrieve_day_natural_ref_rgb(iday)
        except Exception as err:
            print ">> I'm PE: {}, and there is an error: {}".format(comm_rank, err)
            continue
    
    print ">> PE: {}, finished job...".format(comm_rank)



