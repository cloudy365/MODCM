


from my_module import np, h5py, os, sys
from my_module.data.comm import save_data_hdf5
from helper_func import latlon_to_idx



"""
2018.04.30
This is a new version MODIS climate marble, which sorts each 1-km sample into lat/lon bins. 
Instead of processing original MOD021KM and MOD03 products, I firstly organized both products into daily format using 
../organize/MOD_organize.py. The resulting products are then used here.

This script processes each daily granule-based hdf5 data into latlon-based hdf5 data. Because the number of samples are 
25 times more than original stratedy, it needs longer time to process. However, now we can get very high spatial resolution
global mean radiance by simply changing the SPATIAL_RESOLUTION in the main function /main_process_one_day/.

I showed number of available days for each year in the following,
     year            number of days
     2000            300
     2001            348
     2002            356
     2003            358
     2004            366
     2005            365
     2006            365
     2007            365
     2008            364
     2009            365
     2010            365
     2011            365
     2012            366
     2013            365
     2014            365
     2015            365

I showed path of organized MOD02, MOD03 data in the following,
MOD021KM: /u/sciteam/smzyz/scratch/data/MODIS/MOD021KM_daily
MOD03:    /u/sciteam/smzyz/scratch/data/MODIS/MOD03_daily
"""


def times_gen():
    """
    Call once at the very begining.
    """
    times = []
    for ihr in range(24):
        for imin in range(0, 60, 5):
            tmp = "{}{}".format(str(ihr).zfill(2), str(imin).zfill(2))
            times.append(tmp)
    
    times = np.array(times)
    return times
    

def main_process_one_day(mod02_file, mod03_file, output_folder):
    """
    Set parameters and initialize data file interfaces
    """
    # CONSTANT PARAMETERS
    SPATIAL_RESOLUTION = 0.05
    NUM_POINTS = 1 / SPATIAL_RESOLUTION
    NUM_LATS = int(180 / SPATIAL_RESOLUTION)
    NUM_LONS = int(360 / SPATIAL_RESOLUTION)
    NUM_CHAN = 7
    VZA_MAX = 40
    
    
    # Initialize organized MOD02/03 data interfaces
    mod_date = mod03_file.split('.')[1]
    try:
        mod02 = h5py.File(mod02_file, 'r')
        mod03 = h5py.File(mod03_file, 'r')
    except IOError as err:
        print ">> {} has a IOError, program terminated.".format(mod_date)
        return
    
    
    # Initialize output arrays and output hdf5 file
    daily_insolation_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    daily_radiance_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    daily_radiance_num = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
    daily_h5f_out = os.path.join(output_folder, '{}.h5'.format(mod_date))
    

    """
    Main loop: the following part sorts the radiances into the corresponding lat/lon bins
    """
    for itime in times_gen()[:]:
        
        
        # 1ST GRANULE-LEVEL CHECK: 
        # 1.1    0 <= SZA < 90.0
        # 1.2    0 <= VZA < 40.0
        try:
            sza = mod03['{}/SolarZenith'.format(itime)][:, :]/100.
            vza = mod03['{}/SensorZenith'.format(itime)][:, :]/100.
        except KeyError as err:
            #print ">> {}.{} has a KeyError".format(mod_date, itime)
            continue
            
        valid_y, valid_x = np.where((sza>=0)&(sza<90.0)&(vza>=0)&(vza<VZA_MAX))
        valid_num = len(valid_x)
        if valid_num == 0:
            #print ">> log: {}.{} has no valid sample".format(mod_date, itime)
            continue
        #else:
            #print ">> log: {}.{} has {} valid samples fit into geometry criterion".format(mod_date, itime, valid_num)
        
        
        # 2ND PIXEL-LEVEL CHECK:
        # 2.1    descending node (lat < lat_previous)
        # 2.2    valid lat/lon   (lat != -999 and lon != -999 and idx_lat/lon are valid)
        # 2.3    valid radiance  (0 < rad <= rad_max)
        
        # Calculate spectral radiances and insolation
        mdata = mod02['{}/Scaled_Integers'.format(itime)][:]
        rad_scales = mod02['{}/Radiance_Scales'.format(itime)][:]
        rad_offset = mod02['{}/Radiance_Offsets'.format(itime)][:]
        ref_scales = mod02['{}/Reflectance_Scales'.format(itime)][:]
        ref_offset = mod02['{}/Reflectance_Offsets'.format(itime)][:]
        coeffs = rad_scales / ref_scales
        cosine_sza = np.cos(np.deg2rad(sza))
        
        sols = []
        rads = []
        rads_max = []
        for iband in range(NUM_CHAN):
            tmp_sol = cosine_sza * coeffs[iband]
            tmp_rad = (mdata[iband] - rad_offset[iband]) * rad_scales[iband]
            sols.append(tmp_sol)
            rads.append(tmp_rad)
            rads_max.append((32767-rad_offset[iband])*rad_scales[iband])
            
        sols = np.array(sols)
        rads = np.array(rads)
        
        # Read Lats/Lons and separate them into integers and decimals 
        # that will be used to estimate indexes later.
        lats = mod03['{}/Latitude'.format(itime)][:]
        lons = mod03['{}/Longitude'.format(itime)][:]
        
        lats_int = lats.astype('int32')
        lons_int = lons.astype('int32')
        lats_dec = lats - lats_int
        lons_dec = lons - lons_int
        
        
        # The following part sorts the radiances into the corresponding lat/lon bin.
        for isample in xrange(valid_num):
            
            # Get corresponding values for each variable.
            i = valid_y[isample]
            j = valid_x[isample]
            ilat = lats[i, j]
            ilat_pre = lats[i-1, j]
            ilat_int = lats_int[i, j]
            ilon_int = lons_int[i, j]
            
            # 2.1 + 2.2
            if (ilat>ilat_pre) | (ilat_int==-999) | (ilon_int==-999):
                # print ">>>> err: {}.{} has an invalid lat/lon".format(mod_date, itime)
                continue
            
            ilat_dec = lats_dec[i, j]
            ilon_dec = lons_dec[i, j]
            
            # Calculate lat/lon indexes of the sample.
            y_idx, x_idx = latlon_to_idx(ilat_int, ilat_dec, ilon_int, ilon_dec, NUM_POINTS)
            
            # 2.2 more
            if (y_idx == NUM_LATS) | (x_idx == NUM_LONS):
                # print ">>>> err: {}.{} has an invalid lat/lon".format(mod_date, itime)
                continue
                
            irads = rads[:, i, j]
            isols = sols[:, i, j]
            
            # 2.3
            iflg_rads = (irads>0) & (irads <= rads_max)
            irads_valid = [irad if iflg else 0 for irad, iflg in zip(irads, iflg_rads)]
            isols_valid = [isol if iflg else 0 for isol, iflg in zip(isols, iflg_rads)]
            inums_valid = [1 if iflg else 0 for iflg in iflg_rads]
            
            
            # Add the valid sample to daily result arrays
            daily_insolation_sum[y_idx, x_idx, :] += isols_valid
            daily_radiance_sum[y_idx, x_idx, :] += irads_valid
            daily_radiance_num[y_idx, x_idx, :] += inums_valid
        
    
    """
    Save results to a hdf5 file
    """
    # print ">> log: Saving MODIS daily as {}".format(daily_h5f_out)
    save_data_hdf5(daily_h5f_out, '/radiance_sum', daily_radiance_sum)
    save_data_hdf5(daily_h5f_out, '/radiance_num', daily_radiance_num)
    save_data_hdf5(daily_h5f_out, '/insolation_sum', daily_insolation_sum)
    
    


if __name__ == '__main__':
    NUM_CORES = int(sys.argv[1])
    
    import mpi4py.MPI as MPI
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    
    output_folder = '/u/sciteam/smzyz/scratch/results/tmp'
    
    for iyr in range(2000, 2016):
        for idd in range(1, 367, NUM_CORES):
            iday = idd + comm_rank
            mod02_file = 'MOD021KM.A{}{}.006.h5'.format(iyr, str(iday).zfill(3))
            mod03_file = 'MOD03.A{}{}.006.h5'.format(iyr, str(iday).zfill(3))
            
            mod02_path = '/u/sciteam/smzyz/scratch/data/MODIS/MOD02_VIS_daily/{}/{}'.format(iyr, mod02_file)
            mod03_path = '/u/sciteam/smzyz/scratch/data/MODIS/MOD03_daily/{}/{}'.format(iyr, mod03_file)
            
            print ">> log: PE {} -- process {}".format(comm_rank, mod02_file)
            #try:
            main_process_one_day(mod02_path, mod03_path, output_folder)
            #except Exception as err:
                #print ">> err: {}".format(err)





