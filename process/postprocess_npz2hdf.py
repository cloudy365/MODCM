



from my_module import h5py, np, tqdm, os
from my_module.data.comm import save_data_hdf5


def npz_to_h5_VIS(iyr):
    # When icat is VIS, combine radiance with insolation.
    icat = 'VIS'


    WORKING_DIR_0 = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/daily/{}/{}".format(icat, iyr)
    WORKING_DIR_1 = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/daily/SOLAR/{}".format(iyr)
    for iday in range(1, 367):
        ifile_0 = os.path.join(WORKING_DIR_0, '{}.{}.npz'.format(iyr, str(iday).zfill(3)))
        ifile_1 = os.path.join(WORKING_DIR_1, '{}.{}.npz'.format(iyr, str(iday).zfill(3)))

        if os.path.isfile(ifile_0) & os.path.isfile(ifile_1):
            npz = np.load(ifile_0)
            rad_sum = npz['rad_sum'][:]
            rad_num = npz['rad_num'][:]
            npz = np.load(ifile_1)
            sol_sum = npz['solar_sum'][:]
            sol_num = npz['solar_num'][:]

            

            #if (rad_num == sol_num).all():
            #    print ">> Number consistent, good."
            #    flag_consistent = True
            #else:
            #    flag_consistent = False


            h5f = os.path.join(WORKING_DIR_0, '{}.{}.h5'.format(iyr, str(iday).zfill(3)))
            if os.path.isfile(h5f):
                os.remove(h5f)
                print ">> {}.{}.h5 exists, program continue...".format(iyr, str(iday).zfill(3))
                #continue
            #elif flag_consistent == False:
            #    print ">> Number inconsistent, not good."
            #    continue
            save_data_hdf5(h5f, '/rad_sum', rad_sum)
            save_data_hdf5(h5f, '/solar_sum', sol_sum)
            save_data_hdf5(h5f, '/rad_num', rad_num)
            save_data_hdf5(h5f, '/solar_num', sol_num)

        
if __name__ == '__main__':
    """
    e.g., aprun -n 16 python npz_to_h5.py VIS
    """
    import mpi4py.MPI as MPI

    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()

    
    npz_to_h5_VIS(comm_rank+2000)
    print ">> PE: {}, finished my job.".format(comm_rank)
