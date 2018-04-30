# -*- coding: utf-8 -*-
# Process one channels' category at a time. 
# Record each band individually.
#
# The code has been implemented for MPI, use "aprun -n x python ClimateMarble.py x" to set the number of cores (x).
#
# All 38 MODIS channels (including 13.5 and 14.5) are mannually divided into 4 groups (categories):
# VIS: channels 1--7, including corresponding insolation; 
# SWIR_P1: channels 8--13, 13.5, 14;
# SWIR_P2: channels 14.5, 15--19, 26;
# LW_P1: channels 20--25, 27, 28;
# LW_P2: channels 29--36.
#
# The main function is <main_process_one_day(working_dir)>, which uses <main_process_single_MOD02file(mod_file_dir)> to
# iteratively process each MOD021KM file within the specified working directory.
#
#
# Last update: 2018.04.26
# All Rights Reserved.



# Import
from helper_func import *
from my_module.data.comm import save_data_hdf5



CATEGORY = 1   # Choose one from 1, 2, 3, 4, 5


# CONSTANT PARAMETERS
SPATIAL_RESOULITON = 0.05
NUM_POINTS = 1/SPATIAL_RESOULITON
NUM_LATS = int(180/SPATIAL_RESOULITON)
NUM_LONS = int(360/SPATIAL_RESOULITON)
NUM_CHAN = 7 if CATEGORY in [1, 3] else 8
VZA_BOUND = 40 # degree, could also set to 30 and 50.

# Flag of insolation processing
if CATEGORY == 1:
    flg_solar = True
else:
    flg_solar = False


def main_process_single_MOD02file(mod_file_dir):
    """
    Main function processes one MOD021KM file. 
    """
    # We may want to retrieve VZA first and apply a criterion on it (need to be tested).
    # VZA range,    lower bound,     upper bound
    # 0 -- 30          70                201
    # 0 -- 40          48                233
    # 0 -- 50          28                242
    if VZA_BOUND == 30:
        BOUNDING_LOW_VZA = 70
        BOUNDING_HIG_VZA = 201
    elif VZA_BOUND == 40:
        BOUNDING_LOW_VZA = 48
        BOUNDING_HIG_VZA = 233
    elif VZA_BOUND == 50:
        BOUNDING_LOW_VZA = 28
        BOUNDING_HIG_VZA = 242


    # Read Lats/Lons and separate them into integers and decimals
    # that will be used to estimate indexes later.
    lats = MOD02_retrieve_field(mod_file_dir, 'Latitude')[:, BOUNDING_LOW_VZA:BOUNDING_HIG_VZA]
    lons = MOD02_retrieve_field(mod_file_dir, 'Longitude')[:, BOUNDING_LOW_VZA:BOUNDING_HIG_VZA]
    geo_shape = lats.shape                # get the shape of lat/lon data
    lats_int_2d = lats.astype('int32')    # integer part
    lons_int_2d = lons.astype('int32')
    lats_decimal_2d = lats - lats_int_2d  # decimal part
    lons_decimal_2d = lons - lons_int_2d

        
    # Read radiances and resample into coarser resolution (5 km).
    rads, max_rads = MOD02_retrieve_rads(mod_file_dir, CATEGORY)
    rads_1km = rads[:, BOUNDING_LOW_VZA*5:BOUNDING_HIG_VZA*5, :]
    rads_5km = np.array([[rads_1km[j*5:(j+1)*5, i*5:(i+1)*5, :].mean(axis=1).mean(axis=0) 
                            for i in range(geo_shape[1])] for j in range(geo_shape[0])])
    rads_shape = rads_5km.shape            # get the shape of radiance data
    
    # For shortwave/NIR channels, we also need insolation.
    if flg_solar:
        solars_5km = MOD02_retrieve_solar(mod_file_dir, CATEGORY)[:, BOUNDING_LOW_VZA:BOUNDING_HIG_VZA]
    
    
    # Initialize the outputs
    global_radiance_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    global_radiance_num = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    if flg_solar:
        global_insolation_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))


    # The following part sorts the radiances into the corresponding lat/lon bin. 
    for i in range(geo_shape[0]):
        for j in range(geo_shape[1]):
            
            # Get corresponding values for each variable.
            ilat = lats[i, j]
            ilat_p = lats[i-1, j]
            ilat_int = lats_int_2d[i, j]
            ilon_int = lons_int_2d[i, j]
            ilat_decimal = lats_decimal_2d[i, j]
            ilon_decimal = lons_decimal_2d[i, j]
            irads = rads_5km[i, j, :]
            if flg_solar:
                isols = solars_5km[i, j, :]
                        

            # 1st Check: (should be)
            #   (1.1) descending mode;
            #   (1.2) valid lat/lon.
            if (ilat>ilat_p) | (ilat_int==-999) | (ilon_int==-999):
                continue
            
            
            # Calculate lat/lon indexes of the sample.
            y_idx, x_idx = latlon_to_idx(ilat_int, ilat_decimal, ilon_int, ilon_decimal, NUM_POINTS) 
            
            # 2nd Check:  
            #   (2.1) lat/lon indexes within the boundary. 
            if (y_idx == NUM_LATS) | (x_idx == NUM_LONS):
                #print lats_int_2d[i, j], lats_decimal_2d[i, j], lons_int_2d[i, j], lons_decimal_2d[i, j]
                continue
                

            # 3rd Check:
            #   (3.1) 0 < radiance < max_rad;
            #   (3.2) valid insolation (only when flg_solar == True)
            if flg_solar:
                iflg_rads = (irads>0) & (irads<max_rads) & (isols>0)
            else:
                iflg_rads = (irads>0) & (irads<max_rads)
            
            irads_valid = [irad if iflg else 0 for irad, iflg in zip(irads, iflg_rads)]
            inums_valid = [1 if iflg else 0 for iflg in iflg_rads]
            if flg_solar:
                isols_valid = [isol if iflg else 0 for isol, iflg in zip(isols, iflg_rads)]

            global_radiance_sum[y_idx, x_idx, :] += irads_valid
            global_radiance_num[y_idx, x_idx, :] += inums_valid
            if flg_solar:
                global_insolation_sum[y_idx, x_idx, :] += isols_valid

    if flg_solar:
        return global_radiance_sum, global_radiance_num, global_insolation_sum
    else:
        return global_radiance_sum, global_radiance_num



def main_process_one_day(working_dir):
    """
    For each MOD021KM file in the working_dir, use <main_process_single_MOD02file(mod_file_dir)> to
    do the process.
    """
    # Output directory
    h5f = '/u/sciteam/smzyz/scratch/results/'+'.'.join(working_dir.split('/')[-2:])+'.hdf'

    # Initialize output arrays
    global_radiance_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    global_radiance_num = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    if flg_solar:
        global_insolation_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    
    # Process each MOD021KM file within the working_dir.
    files = os.listdir(working_dir)        
    for imod_file in files:
        print imod_file
        if imod_file.startswith('MOD021KM'):
            mod_file_dir = os.path.join(working_dir, imod_file)
            
            if flg_solar:
                tmp_rad_sum, tmp_rad_num, tmp_sol_sum = main_process_single_MOD02file(mod_file_dir)
                global_radiance_sum += tmp_rad_sum
                global_radiance_num += tmp_rad_num
                global_insolation_sum += tmp_sol_sum
    
    # Writing results.
    print ">> Saving daily results: {}...".format(h5f)
    save_data_hdf5(h5f, '/rad_sum', global_radiance_sum)
    save_data_hdf5(h5f, '/rad_num', global_radiance_num)
    if flg_solar:
        save_data_hdf5(h5f, '/sol_sum', global_insolation_sum)



if __name__ == '__main__':
    NUM_CORES = int(sys.argv[1])

    import mpi4py.MPI as MPI

    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()

    days_all = np.load("process_days.npz")['folders'][:]

    
    for ibatch in range(0, len(days_all), NUM_CORES):
        
        
        try:
            iday = days_all[ibatch + comm_rank]
            print "> PE: {}, processing {}...".format(comm_rank, iday)
            main_process_one_day(iday)
        except Exception as e:
            print ">>> Fail: {}".format(e)


    print ">>>> PE: {}, has finished.".format(comm_rank)
