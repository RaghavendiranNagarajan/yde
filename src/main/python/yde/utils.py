# /usr/bin/env python

import sqlite3
import requests
import os
import zipfile
import subprocess
import json
import re
import time
import csv


class Util:

    @staticmethod
    def get_csvoutput_path():

        current_wd = os.getcwd()

        try:
            os.chdir("src/main/resources/csvoutput")
        except FileNotFoundError:
            subprocess.run("mkdir csvoutput", shell=True)

        os.chdir(current_wd)
        return current_wd + "/src/main/resources/csvoutput/"

    @staticmethod
    def creatdirectoryforcsvoutputs():

        timeinsec = time.time()

        current_wd = os.getcwd()
        os.chdir("src/main/resources/csvoutput")
        directory_path = os.getcwd()

        try:
            subprocess.run("mkdir " + str(timeinsec))
            directory_path = directory_path + "/" + str(timeinsec)
        except FileNotFoundError:
            print("ERROR: Couldn't create a folder to export CSV results to")
            print("Resorting to the root CSV folder for exporting results")

        os.chdir(current_wd)
        return str(timeinsec)

    @staticmethod
    def gettimeinsec():
        return time.time()

    @staticmethod
    def write_csvresults(filename, cursor):

        filepathwithname = Util.get_csvoutput_path() + str(Util.gettimeinsec()) + "_" + filename + ".csv"
        print(filepathwithname)

        with open(filepathwithname, "w") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter="\t")
            csv_writer.writerows(cursor)


if __name__ == '__main__':

    csv_op_path = Util.get_csvoutput_path()
    print(csv_op_path)