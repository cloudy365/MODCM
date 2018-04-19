# -*- coding: utf-8 -*-
# Process all channels at the same time. Record each band individually.
# Please use "python xx.py 160" to set the number of cores as 160.
# NUM_CHAN is set to 38 because 13.5 and 14.5 channels (althought they are not important).
# Use MOD02_retrieve_radiance_all function to replace MOD02_select_chs function, the former largerly improved
# the efficiency.


# Import
from helper_func import *



SPATIAL_RESOULITON = 0.05
NUM_POINTS = 1/SPATIAL_RESOULITON

NUM_LATS = int(180/SPATIAL_RESOULITON)
NUM_LONS = int(360/SPATIAL_RESOULITON)
NUM_CHAN = 7 # 15 # 16
CATEGORY = 1 # 2 # 3



def main_process_single_MOD02file(mod_file_dir):
    """
    write something.
    """
    # We may want to retrieve VZA first and apply a criterion on it (need to be tested).    
    BOUNDING_LOW_VZA = 48#28 #48 #70
    BOUNDING_HIG_VZA = 233#242 #233 #201


    # Read Lat/Lon.
    lats = MOD02_retrieve_field(mod_file_dir, 'Latitude')[:, BOUNDING_LOW_VZA:BOUNDING_HIG_VZA]
    lons = MOD02_retrieve_field(mod_file_dir, 'Longitude')[:, BOUNDING_LOW_VZA:BOUNDING_HIG_VZA]
    geo_shape = lats.shape

        
    # Read radiances and resample into coarser resolution (5 km).
    rads, max_values = MOD02_retrieve_radiance_all(mod_file_dir, CATEGORY)
    rads_1km = rads[:, BOUNDING_LOW_VZA*5:BOUNDING_HIG_VZA*5]
    rads_5km = np.array([[rads_1km[j*5:(j+1)*5, i*5:(i+1)*5, :].mean(axis=1).mean(axis=0) 
                            for i in range(geo_shape[1])] for j in range(geo_shape[0])])
    rads_shape = rads_5km.shape
    
    
    # Calculate Lat/Lon integers and decimals, which will be used as indexes later. 
    lats_int_2d = lats.astype('int32')
    lons_int_2d = lons.astype('int32')
    lats_decimal_2d = lats - lats_int_2d
    lons_decimal_2d = lons - lons_int_2d

    
    # Now, what if we use 0.1 degree resolution (1800*3600), the corresponding indexes are showing below
    global_radiance_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    global_radiance_num = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
 

    # The following part is just sorting the radiances into lat/lon bins. 
    for i in range(geo_shape[0]):
        for j in range(geo_shape[1]):
            
            if (lats_int_2d[i, j]==-999) | (lons_int_2d[i, j]==-999) | (lats[i, j]>lats[i-1, j]):
                continue
            
            
            # Calculate index for x and y.
            y_idx, x_idx = latlon_to_idx(lats_int_2d[i, j], lats_decimal_2d[i, j], 
                                         lons_int_2d[i, j], lons_decimal_2d[i, j],
                                         NUM_POINTS)
            
            
            if (y_idx == NUM_LATS) | (x_idx == NUM_LONS):
                print lats_int_2d[i, j], lats_decimal_2d[i, j], lons_int_2d[i, j], lons_decimal_2d[i, j]
                continue
                
            # 
            rads_5km_new = [0 if rad>rad_max else rad for rad, rad_max in zip(rads_5km[i, j, :], max_values)]
            number_new = [1 if rad>0 else 0 for rad in rads_5km_new]

            global_radiance_sum[y_idx, x_idx, :] += rads_5km_new
            global_radiance_num[y_idx, x_idx, :] += number_new
        
    return global_radiance_sum, global_radiance_num



def main_process_one_day(working_dir):
    
    global_radiance_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    global_radiance_num = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
        
    files = os.listdir(working_dir)
        
    for imod_file in files:
    #for i in tqdm(range(len(files)), miniters=10):
        #imod_file = files[i]

        if imod_file.startswith('MOD021KM'):
    
            mod_file_dir = os.path.join(working_dir, imod_file)
    
            tmp_rad_sum, tmp_rad_num = main_process_single_MOD02file(mod_file_dir)
    
            global_radiance_sum += tmp_rad_sum
            global_radiance_num += tmp_rad_num
    
    np.savez_compressed('/u/sciteam/smzyz/scratch/results/VIS/'+'.'.join(working_dir.split('/')[-2:]), 
                        rad_sum=global_radiance_sum, rad_num=global_radiance_num)




if __name__ == '__main__':
    NUM_CORES = int(sys.argv[1])

    import mpi4py.MPI as MPI

    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()

    days_all = np.load("process_days.npz")['folders'][:]

    
    for ibatch in range(0, len(days_all), NUM_CORES):
        
        if comm_rank == 0:
            days_batch = range(ibatch, ibatch+NUM_CORES)
        day = comm.scatter(days_batch if comm_rank == 0 else None, root=0)
        
        
        try:
            print ">> Batch: {}, processing {}...".format(ibatch, day)
            main_process_one_day(days_all[day])
        except Exception as e:
            print ">>>> Fail of {}.{}: {}".format(ibatch, day, e)


    if comm_rank == 0:
        print ">> done."

