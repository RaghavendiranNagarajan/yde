# /usr/bin/env python

import sqlite3
import requests
import os
import zipfile
import subprocess
import json
import re

class RequestUtil:

    @staticmethod
    def downloadfilesandunzip():

        filename = ""
        is_unzipped = False
        print("Downloading the SQLite DB from the source - https://dataengineeringexercise.s3-us-west-1.amazonaws.com/Yelp_dataengineering_dataset.zip ..")

        try:
            current_wd = os.getcwd()
            os.chdir("src/main/resources/")

            url = "https://dataengineeringexercise.s3-us-west-1.amazonaws.com/Yelp_dataengineering_dataset.zip"
            response = requests.get(url)

            filename = "yelpde.zip"
            zfile = open(filename, 'wb')
            zfile.write(response.content)
            zfile.close()

        except Exception as e:
            print("Exception while downloading the file :: " + str(e))

        try:
            print(
                "Unzipping the file ..")
            with zipfile.ZipFile(os.getcwd() + "/" + filename) as zip_file:
                zip_file.extractall()

            is_unzipped = True

        except Exception as e:
            print("Exception occured when unzipping the file :: " + filename)

            print("Deleting the downloaded file now..")
            #subprocess.call("rm yelpde.zip")
            is_unzipped = False

        os.chdir(current_wd)
        return is_unzipped

class SQLInit:


    columnsBusinessCompositionInfo = None
    columnsBusinessInfo = None
    columnsUserInfo = ["User - Id", "user - Name", "User - Yelping Since", "User - Fans", "User - Years Elite"]
    columnsReviewInfo = ["Review - Id", "User - Id", "Business - Id", "User - Compliments Cool",
                         "User - Compliments Cute", "User - Compliments List", "User - Compliments More",
                         "User - Compliments Note", "User - Compliments Photos", "User - Compliments Plain",
                         "User - Compliments Profile", "User - Compliments Writer", "User - Votes Cool",
                         "User - Votes Useful", "Users - Funny"]

    @staticmethod
    def get_dbpath():

        current_wd = os.getcwd()
        os.chdir("src/main/resources/output/")

        dbpath = os.getcwd() + "/user.sqlite"
        os.chdir(current_wd)
        return dbpath

    @staticmethod
    def get_businesscompositionpath():
        current_wd = os.getcwd()
        os.chdir("src/main/resources/output/")

        json_path = os.getcwd() + "/business_composition.json"
        os.chdir(current_wd)
        return json_path


    @staticmethod
    def create_businessinfo_table():

        try:
            connection = sqlite3.connect(SQLInit.get_dbpath())
            cursor = connection.cursor()
            cursor.execute("create table BusinessInfo as select distinct * from business_attributes")

            print("BusinessInfo table is created!")

        except sqlite3.OperationalError:
            print("Table BusinessInfo already Created")
        except Exception as e:
            print(type(e))


    @staticmethod
    def create_businesscompositioninfo_table():

        query = """Create table BusinessCompositionInfo as Select bi."Business - Restaurant?", 
        bi."Business - Accepts Credit Cards", bi."Business - Accepts Insurance", bi."Business - Ages Allowed", bi."Business - Alcohol", 
        bi."Business - Attire", bi."Business - BYOB/Corkage", bi."Business - BYOB", 
        bi."Business - By Appointment Only", bi."Business - Caters", bi."Business - Coat Check", bi."Business - Corkage", bi."Business - Delivery", 
        bi."Business - Dietary Restrictions", bi."Business - Dogs Allowed", bi."Business - Drive-Thru", 
        bi."Business - Good For Dancing", bi."Business - Good For Groups", bi."Business - Good For Kids", 
        bi."Business - Good for Kids2", bi."Business - Happy Hour", bi."Business - Has TV", bi."Business - Noise Level", 
        bi."Business - Open 24 Hours", bi."Business - Order at Counter", bi."Business - Outdoor Seating", bi."Business - Parking", 
        bi."Business - Payment Types", bi."Business - Price Range", bi."Business - Smoking", 
        bi."Business - Take-out", bi."Business - Takes Reservations", bi."Business - Waiter Service", 
        bi."Business - Wheelchair Accessible", bi."Business - Wi-Fi", bi."Business - Id", bi."Business - Categories", bi."Business - Name", 
        bi."Business - Neighborhoods", bi."Business - Open?", bc."Longitude",bc."Latitude",bc."Business - State",
        bc."Business - City",bc."Business - Address",bc."day_of_the_week",bc."close",bc."open",
        bc."Business - Zipcode" from BusinessInfo bi LEFT JOIN (select * from BusinessComposition group by ("Business - Id")) bc on 
        bi."Business - Id" = bc."Business - Id" """

        try:
            connection = sqlite3.connect(SQLInit.get_dbpath())
            cursor = connection.cursor()
            cursor.execute(query)
            print("BusinessCompositionInfo table is created!")
        except sqlite3.OperationalError:
            print("Table BusinessCompositionInfo already Created")
        except Exception as e:
            print(type(e))

    @staticmethod
    def create_userinfo_table():

        query = "create table UserInfo as select distinct " + '"User - Id"' + "," + '"User - Name"' + "," + '"User - ' \
                                                                                                            'Yelping ' \
                                                                                                            'Since"' \
                + "," + '"User - Fans"' + "," + '"User - Years Elite"' + "from Users2"

        try:
            connection = sqlite3.connect(SQLInit.get_dbpath())
            cursor = connection.cursor()
            cursor.execute(query)
            cursor.fetchone()

            print("UserInfo table is created!")

        except sqlite3.OperationalError:
            print("Table UserInfo already Created")
        except Exception as inst:
            print(type(inst))


    @staticmethod
    def create_reviewinfo_table():

        query = """create table ReviewInfo as select "Review - Id", "User - Id", "Business - Id", "User - Compliments 
        Cool", "User - Compliments Cute", "User - Compliments List", "User - Compliments More", "User - Compliments 
        Note", "User - Compliments Photos", "User - Compliments Plain", "User - Compliments Profile", 
        "User - Compliments Writer", "User - Votes Cool", "User - Votes Useful", "Users - Funny" from Users2; """

        try:
            connection = sqlite3.connect(SQLInit.get_dbpath())
            cursor = connection.cursor()
            cursor.execute(query)

            print("ReviewInfo table is created!")

        except sqlite3.OperationalError:
            print("Table ReviewInfo already Created")
        except Exception as inst:
            print(type(inst))

    @staticmethod
    def create_businesscomposition_table():

        print("Creating a table for Business Composition ..")
        connection = sqlite3.connect(SQLInit.get_dbpath())
        cursor = connection.cursor()

        query = """CREATE TABLE IF NOT EXISTS BusinessComposition (
            Longitude text,
            Latitude text,
            'Business - State' text,
            'Business - City' text,
            'Business - Address' text,
            'Business - Id' text,
            'Business - Name' text,
            'business_id' text,
            'day_of_the_week' text,
            'close' text,
            'open' text,
            'Business - Zipcode' text)"""

        cursor.execute(query)
        connection.commit()


    @staticmethod
    def readjson_insertrecord():

        print("Pulling values from Business Composition json file into the SQLite table .. ")
        connection = sqlite3.connect(SQLInit.get_dbpath())
        cursor = connection.cursor()

        buffer_limit = 10000
        rows_batch = []

        query = """INSERT INTO BusinessComposition ('Longitude', 'Latitude', 'Business - State', 'Business - City', 
           'Business - Address','Business - Id', 'Business - Name', 'business_id', 'day_of_the_week', 'close', 
           'open', 'Business - Zipcode') VALUES (?,?,?,?,?,?,?,?,?,?,?,?); """

        columns = ['Longitude', 'Latitude', 'Business - State', 'Business - City', 'Business - Address',
                   'Business - Id', 'Business - Name', 'business_id', 'day_of_the_week', 'close', 'open',
                   'Business - Zipcode']

        with open(SQLInit.get_businesscompositionpath(), 'rb') as json_file:

            business_list = json.load(json_file)

            for business in business_list:

                address = business['Business - Address']

                zipcode = ""
                if address is not None:
                    zipcode = re.search(".* AZ ([0-9]{5}).*", address)
                    if zipcode is not None:
                        zipcode = zipcode.group(1)

                row = [val for val in business.values()]
                row.append(zipcode)

                rows_batch.append(row)

                if len(rows_batch) == buffer_limit:
                    cursor.executemany(query, rows_batch)
                    rows_batch = []

            if len(rows_batch) > 0:
                cursor.executemany(query, rows_batch)

        connection.commit()

    @staticmethod
    def list_yelptables():

        connection = sqlite3.connect(SQLInit.get_dbpath())
        cursor = connection.cursor()
        
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' and name LIKE '%Info%'")
            return cursor.fetchall()

        except sqlite3.OperationalError:
            print("Couldn't fetch tables in the database")
        except Exception as inst:
            print(type(inst))
            print(cursor.fetchall())


if __name__ == '__main__':

    downloadstatus = RequestUtil.downloadfilesandunzip()

    if downloadstatus:

        SQLInit.create_businesscomposition_table()
        SQLInit.readjson_insertrecord()

        SQLInit.create_businessinfo_table()
        SQLInit.create_reviewinfo_table()
        SQLInit.create_userinfo_table()
        SQLInit.create_businesscompositioninfo_table()

        print("All relevant tables for this application are created!")
        print("You're all set for running tests")
