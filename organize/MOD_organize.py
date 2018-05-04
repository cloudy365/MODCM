
"""
2018.04.30
Combine MOD021KM and MOD03 organize scripts.
"""



from my_module import np, SD, os, sys
from my_module.data.comm import save_data_hdf5



def MOD021KM_organize(mod02_files, h5f_path):
    
    for ifile in mod02_files:
        if ifile.endswith('.hdf') == False:
            continue

        # Define empty arrays to store scales/offsets.
        radiance_scales = []
        radiance_offsets = []
        #reflectance_scales = []
        #reflectance_offsets = []
        scaled_integers = []

        # Initialize hdf4 interface
        try:
            h4f = SD(os.path.join(SUBDIR, ifile))
        except:
            print ">> err: Cannot open {}, program moves on...".format(os.path.join(SUBDIR, ifile))


        #for ifld in ['EV_250_Aggr1km_RefSB', 'EV_500_Aggr1km_RefSB']:
        #for ifld in ['EV_1KM_RefSB']:
        for ifld in ['EV_1KM_Emissive']:
         
            # Get scaled bands integers and their corresponding conversion coefficients
            sds = h4f.select(ifld)
            rad_scales = sds.attributes()['radiance_scales']
            rad_offsets = sds.attributes()['radiance_offsets']
            #ref_scales = sds.attributes()['reflectance_scales']
            #ref_offsets = sds.attributes()['reflectance_offsets']
            data_int = sds.get()
            sds.endaccess()
            

            # Save arrays
            if ifld == 'EV_250_Aggr1km_RefSB':
                num_band = 2
            elif ifld == 'EV_500_Aggr1km_RefSB':
                num_band = 5
            elif ifld == 'EV_1KM_RefSB':
                num_band = 15
            elif ifld == 'EV_1KM_Emissive':
                num_band = 16

            for i in range(num_band):
                radiance_scales.append( rad_scales[i]  )
                radiance_offsets.append( rad_offsets[i] )
                #reflectance_scales.append( ref_scales[i] )
                #reflectance_offsets.append( ref_offsets[i] )
                scaled_integers.append( data_int[i] )

        
        # Save arrays to the daily hdf5 file
        data_path = ifile.split('.')[2]
        save_data_hdf5(h5f_path, os.path.join(data_path, 'Radiance_Scales'), radiance_scales)
        save_data_hdf5(h5f_path, os.path.join(data_path, 'Radiance_Offsets'), radiance_offsets)
        #save_data_hdf5(h5f_path, os.path.join(data_path, 'Reflectance_Scales'), reflectance_scales)
        #save_data_hdf5(h5f_path, os.path.join(data_path, 'Reflectance_Offsets'), reflectance_offsets)
        save_data_hdf5(h5f_path, os.path.join(data_path, 'Scaled_Integers'), scaled_integers)



def MOD03_organize(mod03_files, h5f_path):
    for ifile in mod03_files:
        if ifile.endswith('.hdf') == False:
            continue
       
        h4f = SD(os.path.join(SUBDIR, ifile))
        sds = h4f.select('Latitude')
        lat = sds.get()
        sds.endaccess()

        sds = h4f.select('Longitude')
        lon = sds.get()
        sds.endaccess()

        sds = h4f.select('SolarZenith')
        sza = sds.get()
        sds.endaccess()

        sds = h4f.select('SensorZenith')
        vza = sds.get()
        sds.endaccess()
        h4f.end()

        data_path = ifile.split('.')[2]
        #print data_path
        save_data_hdf5(h5f_path, os.path.join(data_path, 'Latitude'), lat)
        save_data_hdf5(h5f_path, os.path.join(data_path, 'Longitude'), lon)
        save_data_hdf5(h5f_path, os.path.join(data_path, 'SolarZenith'), sza)
        save_data_hdf5(h5f_path, os.path.join(data_path, 'SensorZenith'), vza)
        
        

if __name__ == "__main__":

    # Get the number of available cores
    NUM_CORES = int(sys.argv[1])


    # Initialize MPI
    import mpi4py.MPI as MPI
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()


    # Main iterations
    for iyr in range(2000, 2016):
        for iday in range(1, 367, NUM_CORES):

            WORKING_DATE = "{}/{}".format(iyr, str(iday+comm_rank).zfill(3))  # specified date
            # WORKING_DATE = "{}/186".format(iyr+comm_rank)
            # WORKING_DIR = "/u/sciteam/smzyz/scratch/data/MODIS/MOD03"       # path of MOD03 data
            WORKING_DIR = "/u/sciteam/smzyz/scratch/data/MODIS/MOD021KM"      # path of MOD021KM data
            SUBDIR = os.path.join(WORKING_DIR, WORKING_DATE)                  # SUBDIR = WORKING_DIR + WORKING_DATE
          

            # ignore unavailable date (not exists)
            try:
                files = os.listdir( SUBDIR )
            except OSError as err:
                print ">> err: {}".format(err)
                continue
            

            # call main function
            print ">> PE:{}, organizing MODIS data on {}...".format(comm_rank, WORKING_DATE)
            h5f_name = '.'.join(files[0].split('.')[:2])                     # output hdf5 file name

            # h5f_path = '/u/sciteam/smzyz/scratch/data/MODIS/MOD03_daily/{}/{}.006.h5'.format(iyr, h5f_name)
            h5f_path = '/u/sciteam/smzyz/scratch/data/MODIS/MOD02_LW_daily/{}/{}.006.h5'.format(iyr, h5f_name)
            # h5f_path = '/u/sciteam/smzyz/{}.006.h5'.format(h5f_name)

            # MOD03_organize(files, h5f_path)
            MOD021KM_organize(files, h5f_path)
            

