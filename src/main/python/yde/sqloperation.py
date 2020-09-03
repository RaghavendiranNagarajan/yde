# /usr/bin/env python

import sqlite3
import enum

from yde.dbinit import SQLInit
from yde.utils import Util

# This is to ensure that users cannot query any tables beyond what's defined here in enum class

class Table(enum.Enum):
    User = 1
    Business_Attribute = 2
    Business_Info = 3
    Review_info = 4
    User_Info = 5
    BusinessComposition_Info = 6


class SQLTable:

    def __init__(self, tablename):

        self.connection = sqlite3.connect(SQLInit.get_dbpath())
        self.cursor = self.connection.cursor()

        if tablename == Table.User:
            self.tablename = "Users2"
        elif tablename == Table.Business_Info:
            self.tablename = "BusinessInfo"
        elif tablename == Table.Review_info:
            self.tablename = "ReviewInfo"
        elif tablename == Table.BusinessComposition_Info:
            self.tablename = "BusinessCompositionInfo"
        elif tablename == Table.User_Info:
            self.tablename = "UserInfo"
        else:
            raise Exception("Sorry, the table you're trying to query doesn't exist in this DB!")

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def num_rows(self):
        self.cursor.execute("select count(*) from " + self.tablename)
        return self.cursor.fetchone()

    def fetch_nrows(self, numrows):
        self.cursor.execute("select * from " + self.tablename + " limit (?)", (numrows,))
        return self.cursor.fetchall()

    def fetch_columnnames(self):
        self.cursor.execute("select * from " + self.tablename + " limit 1")
        columns = list(map(lambda x: x[0], self.cursor.description))
        return columns


class UserTable(SQLTable):

    def __init__(self):
        self.tablename = Table.User_Info
        super().__init__(self.tablename)


class ReviewTable(SQLTable):

    def __init__(self):
        self.tablename = Table.Review_info
        super().__init__(self.tablename)

    def meanreviews_business(self, exporttocsv):

        self.cursor.execute(
            """select count(distinct("Review - Id"))/count(distinct("Business - Id")) from """ + self.tablename)
        if exporttocsv:
            Util.write_csvresults("meanreviewsbybusiness", self.cursor)
        else:
            return self.cursor.fetchone()

    def meanreviews_business_supplement(self, exporttocsv):

        query = """select ("Business - Id") as BusinessId, count("Review - Id") as reviews from """ + self.tablename + """ group by "Business - Id" order by reviews desc """

        self.cursor.execute(query)

        if exporttocsv:
            Util.write_csvresults("meanreviewsbybusiness_supplement", self.cursor)
        else:
            return self.cursor.fetchall()

    def meanreviews_zipcode(self, exporttocsv):

        self.cursor.execute(
            """With top5zip as (select bc."Business - Zipcode" as zipcode, count(ri."Review - Id") as cnt from 
            BusinessCompositionInfo bc, ReviewInfo ri where ri."Business - Id" = bc."Business - Id" group by 
            ri."Business - Id" order by cnt desc limit 5) select sum(top5zip.cnt)/count(top5zip.zipcode) from 
            top5zip""")

        if exporttocsv:
            Util.write_csvresults("meanreviews_zipcode", self.cursor)
        else:
            return self.cursor.fetchone()

    def meanreviews_zipcode_supplement(self, exporttocsv):

        query = """select bc."Business - Zipcode" as zipcode, count(ri."Review - Id") as cnt from 
            BusinessCompositionInfo bc, ReviewInfo ri where ri."Business - Id" = bc."Business - Id" group by 
            ri."Business - Id" order by cnt desc"""
        self.cursor.execute(query)

        if exporttocsv:
            Util.write_csvresults("meanreviews_zipcode_supplement", self.cursor)
        else:
            return self.cursor.fetchall()

    def top_reviewers(self, exporttocsv):

        self.cursor.execute(
            """select ui."User - Id" as UserId,ui."User - Name" as UserName,count(ri."User - Id") NumReviews from 
            UserInfo ui, ReviewInfo ri where ui."User - Id" = ri."User - Id" group by ui."User - Id",ui."User - Name" 
            order by NumReviews desc limit 10""")

        if exporttocsv:
            Util.write_csvresults("top_reviewers", self.cursor)
        else:
            return self.cursor.fetchall()

    def top_reviewers_supplement(self, exporttocsv):
        query = """select ui."User - Id" as UserId,ui."User - Name" as UserName,count(ri."User - Id") NumReviews from 
            UserInfo ui, ReviewInfo ri where ui."User - Id" = ri."User - Id" group by ui."User - Id",ui."User - Name" 
            order by NumReviews desc"""
        self.cursor.execute(query)

        if exporttocsv:
            Util.write_csvresults("top_reviewers_supplement", self.cursor)
        else:
            return self.cursor.fetchall()


class BusinessCompositionTable(SQLTable):

    def __init__(self):
        self.tablename = Table.BusinessComposition_Info
        super().__init__(self.tablename)

    def validate_zipcodecolumn(self):
        query = """select count("Business - Zipcode") as cnt from """ + self.tablename + """ where "Business - Zipcode"
        LIKE '8%' """
        self.cursor.execute(query)
        return self.cursor.fetchone()


if __name__ == '__main__':

    csvexport = True

    rt = ReviewTable()
    rt.meanreviews_business(csvexport)
    rt.meanreviews_business_supplement(csvexport)
    rt.meanreviews_zipcode(csvexport)
    rt.meanreviews_zipcode_supplement(csvexport)
    rt.top_reviewers(csvexport)
    rt.top_reviewers_supplement(csvexport)

