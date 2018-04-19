# -*- coding: utf-8 -*-

"""
These helper functions are from zyz_data with some modifications.
"""


from my_module import np, SD, plt, os, time, tqdm, sys



def latlon_to_idx(lat_int, lat_decimal, lon_int, lon_decimal, num):
    """
    num: 1/resolution
    """
    # Latitude part
    if lat_int + lat_decimal < 0:
        idx_lat = (90 - lat_int) * num - int(lat_decimal * num)
    else:
        idx_lat = (90 - lat_int) * num - int(lat_decimal * num) - 1
        
    # Longitude part
    if lon_int + lon_decimal < 0:
        idx_lon = (180 + lon_int) * num + int(lon_decimal * num) - 1
    else:
        idx_lon = (180 + lon_int) * num + int(lon_decimal * num)
    
    
    if idx_lon == 360 * num:
        idx_lon = 0
    
    return int(idx_lat), int(idx_lon)


def MOD02_retrieve_field(mod02_file, ifld):
    # retrieve a specified field from MOD021KM product

    mfile = SD(mod02_file)
    mdata = mfile.select(ifld)
    return mdata[:]


def MOD02_retrieve_solar(mod02_file, icat=1):
    """
    Retrieve MODIS021KM normal insolation for VIS 1--7 bands.
    """

    szas = MOD02_retrieve_field(mod02_file, 'SolarZenith')/100.
    cosine_szas = np.cos(np.deg2rad(szas))
    
    mfile = SD(mod02_file)

    solar_array = []
    if icat == 1:
        # Bands 1, 2
        mdata = mfile.select('EV_250_Aggr1km_RefSB')
        rad_scales = mdata.attributes()['radiance_scales']
        ref_scales = mdata.attributes()['reflectance_scales']
    
        for i in range(2):
            solar_array.append( cosine_szas*rad_scales[i]/ref_scales[i] )
        
        # Bands 3 -- 7
        mdata = mfile.select('EV_500_Aggr1km_RefSB')
        rad_scales = mdata.attributes()['radiance_scales']
        ref_scales = mdata.attributes()['reflectance_scales']

        for i in range(5):
            solar_array.append( cosine_szas*rad_scales[i]/ref_scales[i] )
    
    solar_array = np.rollaxis(np.array(solar_array), 0, 3)
    return solar_array


def MOD02_retrieve_radiance_all(mod02_file, icat):
    # retrieve MODIS021KM radiances from all 36 channels
    mod_array = []
    max_value = []
    
    mfile = SD(mod02_file)
    
    if icat == 1:
        # Bands 1, 2
        mdata = mfile.select('EV_250_Aggr1km_RefSB')
        scales = mdata.attributes()['radiance_scales']
        offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()
    
        for i in range(2):
            mod_array.append( (tt[i] - offset[i]) * scales[i] )
            max_value.append( (32767 - offset[i]) * scales[i] )
        
    
        # Bands 3 -- 7
        mdata = mfile.select('EV_500_Aggr1km_RefSB')
        scales = mdata.attributes()['radiance_scales']
        offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()
    
        for i in range(5):
            mod_array.append( (tt[i] - offset[i]) * scales[i] )
            max_value.append( (32767 - offset[i]) * scales[i] )

    
    if icat == 2:
        # Bands 8 -- 15
        mdata = mfile.select('EV_1KM_RefSB')
        scales = mdata.attributes()['radiance_scales']
        offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()
    
        for i in range(8):
            mod_array.append( (tt[i] - offset[i]) * scales[i] )
            max_value.append( (32767 - offset[i]) * scales[i] )
        
    
    if icat == 3:
        # Bands 16 -- 19, 26
        mdata = mfile.select('EV_1KM_RefSB')
        scales = mdata.attributes()['radiance_scales']
        offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()

        for i in range(8, 15):
            mod_array.append( (tt[i] - offset[i]) * scales[i] )
            max_value.append( (32767 - offset[i]) * scales[i] )


    if icat == 4:
        # LW part 1
        mdata = mfile.select('EV_1KM_Emissive')
        rad_scales = mdata.attributes()['radiance_scales']
        rad_offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()

        for i in range(8):
            mod_array.append( (tt[i] - rad_offset[i]) * rad_scales[i] )
            max_value.append( (32767 - rad_offset[i]) * rad_scales[i] )
    

    if icat == 5:
        # LW part 2
        mdata = mfile.select('EV_1KM_Emissive')
        rad_scales = mdata.attributes()['radiance_scales']
        rad_offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()

        for i in range(8, 16):
            mod_array.append( (tt[i] - rad_offset[i]) * rad_scales[i] )
            max_value.append( (32767 - rad_offset[i]) * rad_scales[i] )


    mod_array = np.rollaxis(np.array(mod_array), 0, 3)
    return mod_array, max_value
  
    
    
if __name__ == "__main__":
    import sys

    yrs = sys.argv[1:]
    
    for iyr in yrs:
        data_folder = "/u/sciteam/smzyz/scratch/results/VIS/{}".format(iyr)
    
        rad_all = np.zeros((3600, 7200, 7))
        num_all = np.zeros((3600, 7200, 7))
    
        files = os.listdir(data_folder)
        length = len(files)
    
        for i in tqdm(range(length), miniters=10):
            ifile = files[i]
        
            if ifile.endswith('.npz') == False:
                continue
        
            fnpz = np.load(os.path.join(data_folder, ifile))
            tmp_rad = fnpz['rad_sum'][:]
            tmp_num = fnpz['rad_num'][:]
        
            rad_all += tmp_rad
            num_all += tmp_num

        np.savez(iyr, rad_all=rad_all, num_all=num_all)
