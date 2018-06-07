# -*- coding: utf-8 -*-
#
# These helper functions are originally from zyz_data with some modifications.
# Used by ClimateMarble.py to conduct the main process.
#
# Last update: 2018.04.26
# All Rights Reserved.


from my_module import np, SD, plt, os, time, tqdm, sys



def latlon_to_idx(lat_int, lat_decimal, lon_int, lon_decimal, num):
    """
    Key function that determines the lat/lon indexes for a given resolution map based on the integer and decimals of lats and lons. 
    
    Current version only works on the single sample. Maybe update in the future (>.<).
    
    num: 1/resolution.
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


def latslons_to_idxs(lats, lons, num):
    """
    An updated key function that determines the lat/lon indexes for a given resolution map based on the input lats and lons.

    This function is exactly the same as latlon_to_idx but working on the whole lat/lon array at the same time.

    num: 1/resolution
    """

    lats_int = lats.astype('int32')
    lons_int = lons.astype('int32')
    lats_dec = lats - lats_int
    lons_dec = lons - lons_int

    # Latitude
    lats_idx = (90-lats_int) * num - (lats_dec*num).astype('int32')
    lats_idx[lats>=0] -= 1

    # Longitude
    lons_idx = (180+lons_int) * num + (lons_dec*num).astype('int32')
    lons_idx[lons<0] -= 1
    np.place(lons_idx, lons_idx==360*num, 0)

    return lats_idx, lons_idx


def MOD02_retrieve_field(mod02_file, ifld):
    """
    Retrieve a specified field from MOD021KM data file.

    Currently used for retrieving 'Latitude', 'Longitude', and 'SolarZenith'. 
    """

    mfile = SD(mod02_file)
    mdata = mfile.select(ifld)
    return mdata[:]


def MOD02_retrieve_solar(mod02_file, icat=1):
    """
    Retrieve MODIS021KM normal insolation for VIS 1--7 bands.
    Store valid spectral insolation when SZA < 90.
    """

    szas = MOD02_retrieve_field(mod02_file, 'SolarZenith')/100.
    cosine_szas = np.cos(np.deg2rad(szas))
    np.place(cosine_szas, szas>=90, 0)


    mfile = SD(mod02_file)
    solar_array = []
    if icat == 1:
        # Bands 1, 2
        mdata = mfile.select('EV_250_Aggr1km_RefSB')
        rad_scales = mdata.attributes()['radiance_scales']
        ref_scales = mdata.attributes()['reflectance_scales']
    
        for i in range(2):
            solar_array.append( cosine_szas*rad_scales[i]/ref_scales[i] )
        
        # Bands 3--7
        mdata = mfile.select('EV_500_Aggr1km_RefSB')
        rad_scales = mdata.attributes()['radiance_scales']
        ref_scales = mdata.attributes()['reflectance_scales']

        for i in range(5):
            solar_array.append( cosine_szas*rad_scales[i]/ref_scales[i] )

    # Put the dimension of channel last
    solar_array = np.array(solar_array)
    solar_array = np.rollaxis(solar_array, 0, 3)
    return solar_array


def MOD02_retrieve_rads(mod02_file, icat):
    """
    Retrieve MODIS021KM radiances for a specified category.
    """

    mod_array = []
    max_value = []
    
    mfile = SD(mod02_file)
    
    if icat == 1:
        # Bands 1--2, 3--7
        mdata = mfile.select('EV_250_Aggr1km_RefSB')
        scales = mdata.attributes()['radiance_scales']
        offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()

        for i in range(2):
            mod_array.append( (tt[i] - offset[i]) * scales[i] )
            max_value.append( (32767 - offset[i]) * scales[i] )
        
        mdata = mfile.select('EV_500_Aggr1km_RefSB')
        scales = mdata.attributes()['radiance_scales']
        offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()
    
        for i in range(5):
            mod_array.append( (tt[i] - offset[i]) * scales[i] )
            max_value.append( (32767 - offset[i]) * scales[i] )


    if icat == 2:
        # Bands 8--13, 13.5, 14
        mdata = mfile.select('EV_1KM_RefSB')
        scales = mdata.attributes()['radiance_scales']
        offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()
    
        for i in range(8):
            mod_array.append( (tt[i] - offset[i]) * scales[i] )
            max_value.append( (32767 - offset[i]) * scales[i] )
        
    
    if icat == 3:
        # Bands 14.5, 15--19, 26
        mdata = mfile.select('EV_1KM_RefSB')
        scales = mdata.attributes()['radiance_scales']
        offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()

        for i in range(8, 15):
            mod_array.append( (tt[i] - offset[i]) * scales[i] )
            max_value.append( (32767 - offset[i]) * scales[i] )


    if icat == 4:
        # Bands 20--25, 27, 28
        mdata = mfile.select('EV_1KM_Emissive')
        rad_scales = mdata.attributes()['radiance_scales']
        rad_offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()

        for i in range(8):
            mod_array.append( (tt[i] - rad_offset[i]) * rad_scales[i] )
            max_value.append( (32767 - rad_offset[i]) * rad_scales[i] )
    

    if icat == 5:
        # Bands 29--36
        mdata = mfile.select('EV_1KM_Emissive')
        rad_scales = mdata.attributes()['radiance_scales']
        rad_offset = mdata.attributes()['radiance_offsets']
        tt = mdata.get()

        for i in range(8, 16):
            mod_array.append( (tt[i] - rad_offset[i]) * rad_scales[i] )
            max_value.append( (32767 - rad_offset[i]) * rad_scales[i] )


    # Put the dimension of channel last
    mod_array = np.rollaxis(np.array(mod_array), 0, 3)
    return mod_array, max_value
  
