

# common
import mpi4py.MPI as MPI


comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()

# trivial 1
#from ClimateMarble_Solar import main_process_one_day
#left = []
#left.append('/scratch/sciteam/clipp/terra_L1B/MODIS/MOD021KM/2008/227')
#left.append('/scratch/sciteam/clipp/terra_L1B/MODIS/MOD021KM/2009/225')
#print ">> PE: {}, work on {}".format(comm_rank, left[comm_rank])
#main_process_one_day(left[comm_rank])


# trivial 2
from my_module import np, plt, Basemap
lats = np.arange(-90., 90., 0.05)
lons = np.arange(-180., 180., 0.05)
for i in range(1, 366, comm_size):
    iday = i+comm_rank
    try:
        img = np.imread("/scratch/sciteam/smzyz/results/shared_with_AVL/MODIS_31_days_moving_average_daynatural_rgb/{}.png".format(iday))
    except:
        print ">> PE: {}, cannot access image: {}.png".format(comm_rank, iday)
        continue
    plt.figure()
    m = Basemap(projection='ortho',lat_0=-90,lon_0=0, resolution='l')
    m.drawcoastlines(linewidth=0.25)
    res = np.zeros_like(img)
    for i in range(3):
        res[:, :, i] = m.transform_scalar(img[::-1, :, i],lons,lats,7200,3600)
    m.imshow(res)
    plt.savefig('{}.png'.format(iday), dpi=300)
    close()
