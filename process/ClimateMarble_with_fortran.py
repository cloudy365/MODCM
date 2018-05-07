from my_module import os, h5py, np, tqdm
from my_module.data.comm import save_data_hdf5
from helper_func import latslons_to_idxs
from fort import test


# test data
mod03 = '/u/sciteam/smzyz/scratch/data/MODIS/MOD03_daily/2006/MOD03.A2006001.006.h5'
mod02 = '/u/sciteam/smzyz/scratch/data/MODIS/MOD02_VIS_daily/2006/MOD021KM.A2006001.006.h5'
output_folder = '/u/sciteam/smzyz'


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

    
# CONSTANT PARAMETERS
SPATIAL_RESOLUTION = 0.05
NUM_POINTS = 1 / SPATIAL_RESOLUTION
NUM_LATS = int(180 / SPATIAL_RESOLUTION)
NUM_LONS = int(360 / SPATIAL_RESOLUTION)
NUM_CHAN = 7
VZA_MAX = 40


# Initialize organized MOD02/03 data interfaces
mod_date = mod03.split('.')[1]
try:
    mod02 = h5py.File(mod02, 'r')
    mod03 = h5py.File(mod03, 'r')
except IOError as err:
    print ">> {} has a IOError, program terminated.".format(mod_date)


# Initialize output arrays and output hdf5 file
daily_insolation_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
daily_radiance_sum = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
daily_radiance_num = np.zeros((NUM_LATS, NUM_LONS, NUM_CHAN))
daily_h5f_out = os.path.join(output_folder, '{}.h5'.format(mod_date))


"""
Main loop: the following part sorts the radiances into the corresponding lat/lon bins
"""
times = times_gen()[:]
for i in tqdm(range(len(times))):
# for i in range(7):
    itime = times[i]

    # 1ST GRANULE-LEVEL CHECK: 
    # 1.1    0 <= SZA < 90.0
    # 1.2    0 <= VZA < 40.0
    try:
        sza = mod03['{}/SolarZenith'.format(itime)][:, :]/100.
        vza = mod03['{}/SensorZenith'.format(itime)][:, :]/100.
    except KeyError as err:
        continue

    valid_y, valid_x = np.where((sza>=0)&(sza<90.0)&(vza>=0)&(vza<VZA_MAX))
    valid_num = len(valid_x)
    if valid_num == 0:
        continue



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
    sols = np.rollaxis(sols, 0, 3)
    rads = np.rollaxis(rads, 0, 3)
    rads_max = np.array(rads_max)

    # Read Lats/Lons and separate them into integers and decimals 
    # that will be used to estimate indexes later.
    lats = mod03['{}/Latitude'.format(itime)][:]
    lons = mod03['{}/Longitude'.format(itime)][:]

    # Calculate lat/lon indexes of all sample.
    lats_idx, lons_idx = latslons_to_idxs(lats, lons, NUM_POINTS)
    
    # print itime, len(valid_x), valid_x.shape, len(lats_idx), lats_idx.shape
    # print rads.shape, sols.shape, len(rads_max)
    
    
    daily_insolation_sum, daily_radiance_sum, daily_radiance_num = test(len(valid_x), valid_x, valid_y, \
    len(lats_idx), lats, lons, lats_idx, lons_idx, \
    rads, sols, rads_max, \
    daily_insolation_sum, daily_radiance_sum, daily_radiance_num)
    
    
save_data_hdf5(daily_h5f_out, '/radiance_sum', daily_radiance_sum)
save_data_hdf5(daily_h5f_out, '/radiance_num', daily_radiance_num)
save_data_hdf5(daily_h5f_out, '/insolation_sum', daily_insolation_sum)
