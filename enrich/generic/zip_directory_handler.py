#!/usr/bin/env python2.7

import re
import zipfile

from os import listdir, remove, makedirs
from os.path import isfile, join, exists


class ZIP_DIRECTORY_HANDLER:
    def __file_list(self, directory):
        if exists(directory):
            return [f for f in listdir(directory) if isfile(join(directory, f))]
        else:
            return []

    def __unzip(self, file_name):
        return zipfile.ZipFile(file_name)

    def __write_directory(self, file_list, read_directory, write_directory):
        for f in file_list:
            self.__write_file(read_directory + '/' + f, write_directory)

    def __write_file(self, file_location, write_directory):
        if not exists(write_directory):
            makedirs(write_directory)

        zip_content = self.__unzip(file_location)
        for filename in zip_content.namelist():
            if re.match('[a-zA-Z]+-\d+\.xml', filename):
                content = zip_content.read(filename)
                with open((write_directory + '/' + filename), 'w') as outf:
                    outf.write(content)
            # remove(read_directory + '/' + f)



    def unzip_directory(self, read_directory, write_directory):
        self.__write_directory(self.__file_list(read_directory),
                             read_directory,
                             write_directory)

    def unzip_file(self, file_url, write_directory):
        self.__write_file(file_url, write_directory)
