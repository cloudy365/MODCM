


from my_module import np, os, sys
import tarfile



def untar(tar_file, out_path):
    tar = tarfile.open(tar_file)
    tar.extractall(path=out_path)
    tar.close()


if __name__ == '__main__':
    NUM_CORES = int(sys.argv[1])
    import mpi4py.MPI as MPI


    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()


    for iyr in range(2008, 2009):
        for iday in range(1, 367, NUM_CORES):

            tar_file = "/u/sciteam/smzyz/scratch/data/MODIS/MOD03/{}/{}.tar".format(iyr, str(iday+comm_rank).zfill(3))
            out_path = "/u/sciteam/smzyz/scratch/data/MODIS/MOD03/{}/".format(iyr)
            try:
                print ">> PE {}: untarring MOD03 data on {}/{}".format(comm_rank, iyr, iday+comm_rank)
                untar(tar_file, out_path)
            except:
                print ">> Err occur: {}".format(tar_file)
                continue
