

import sys
sys.path.append("/u/sciteam/smzyz/applications/lib/python2.7/site-packages")
from my_module import np, plt, os, toimage
from my_module.plot import scale_image_2d, enhance_rgb


def load_data(iband, iday):
    fname = 'b{}_{}.npz'.format(iband, iday)
    data_dir = os.path.join(WORKING_DIR, fname)
    data_file = np.load(data_dir)
    data = data_file['avg_moving'][:]
    return data


WORKING_DIR = "/u/sciteam/smzyz/scratch/results/MODIS_ClimateMarble_005deg/moving_average"


for iday in range(1, 366):
    r = load_data(1, iday)
    g = load_data(4, iday)
    b = load_data(3, iday)
    
    rgb = np.dstack((r, g, b))
    
    # enhance and save image
    enhanced_image = enhance_rgb(rgb, scale_method='RLT', scale_factors=[1, 1, 1])
    toimage(enhanced_image).save('{}.png'.format(iday))