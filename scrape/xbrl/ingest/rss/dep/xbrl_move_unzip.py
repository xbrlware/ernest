

import zipfile
import re
import os
from os import listdir
from os.path import isfile, join


def __unzip(year, month): 
    dr = ('/home/ubuntu/sec/' + year + '/' + month + '/')
    onlyfiles = [f for f in listdir(dr) if isfile(join(dr, f))]
    for f in onlyfiles: 
        try: 
            fh = open(dr + f, 'rb')
            z = zipfile.ZipFile(fh)
            drct = '/home/ubuntu/xbrl/' + year + '/' \
                + month + '/' + f + '/'
            if not os.path.exists(drct):
                os.makedirs(drct)
            for name in z.namelist():
                z.extract(name, drct)
            fh.close()
        except: 
            print(f)


__unzip('2016', '01')
