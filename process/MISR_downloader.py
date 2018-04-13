from my_module import np, mtk, tqdm, os
from ftplib import FTP


# definitions
def data_type(itype):
    if itype == 'alb':
        folder = 'MIL2TCAL.002'    # MIL2TCAL directory
    elif itype == 'rad':
        folder = 'MI1B2E.003'      # MI1B2E directory
    return folder
        
    
iyr = 2010
# path_list = mtk.latlon_to_path_list(72.58, -38.48)
path_list = range(1, 234)
folder = data_type('rad')


# change to the download folder
try:
    os.chdir('/u/sciteam/smzyz/scratch/data/MISR/MI1B2E.003/{}'.format(iyr))
    
except:
    os.mkdir('/u/sciteam/smzyz/scratch/data/MISR/MI1B2E.003/{}'.format(iyr))
    os.chdir('/u/sciteam/smzyz/scratch/data/MISR/MI1B2E.003/{}'.format(iyr))

    
# login MISR FTP
ftp = FTP('l5ftl01.larc.nasa.gov')
ftp.login(passwd='smzyiz@gmail.com')
ftp.cwd('MISR')
ftp.cwd(folder)                           


# download data
def download_date(year, month, day): 
    iyr = str(year)
    imon = str(month).zfill(2)
    iday = str(day).zfill(2)
    ifolder = '.'.join([iyr, imon, iday])
    
    try:
        ftp.cwd(ifolder)
        files = ftp.nlst()
        
        # generate download list
        download_list = []
        if folder == 'MI1B2E.003':
            hdf_files = np.array([i for i in files if ('DF' in i) & (i.endswith('hdf'))])
            for ifile in hdf_files:
                ipath = int(ifile.split('_')[5][1:])
                if ipath in path_list:
                    download_list.append(ifile)
        else:
            hdf_files = np.array([i for i in files if i.endswith('hdf')])
            for ifile in hdf_files:
                ipath = int(ifile.split('_')[4][1:])
                if ipath in path_list:
                    download_list.append(ifile)            
        
        # download list
        print "Processing ... {}".format(ifolder)
        for ifile in tqdm(download_list, miniters=1):
            exist_files = os.listdir('/u/sciteam/smzyz/scratch/data/MISR/MI1B2E.003/{}'.format(iyr))
            if ifile in exist_files:
                continue
            with open(ifile, 'wb') as f:
                ftp.retrbinary('RETR {}'.format(ifile), f.write)
                    
        # back to the parent folder  
        ftp.cwd('/pub/MISR/{}/'.format(folder))       
    
    except: # In case of bad days (e.g. 31th June)
        print "Processing ... {} but failed...".format(ifolder)
        ftp.cwd('/pub/MISR/{}/'.format(folder))
        
            
# loop through 1st May to 30th September
for imon in range(5, 6):
    for iday in range(1, 17):
        download_date(iyr, imon, iday)