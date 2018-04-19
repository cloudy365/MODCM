


from ClimateMarble_Solar import main_process_one_day
import mpi4py.MPI as MPI


comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()


left = []
left.append('/scratch/sciteam/clipp/terra_L1B/MODIS/MOD021KM/2008/227')
left.append('/scratch/sciteam/clipp/terra_L1B/MODIS/MOD021KM/2009/225')


print ">> PE: {}, work on {}".format(comm_rank, left[comm_rank])
main_process_one_day(left[comm_rank])


