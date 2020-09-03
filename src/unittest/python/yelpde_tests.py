import unittest2

import yde
from yde.dbinit import *
from yde.sqloperation import *

class YelpDETest(unittest2.TestCase):


    def test_sqlitetable_userinfo_row_validation(self):
        expected_userinforows = 71949
        ut = UserTable()
        numrows = ut.num_rows()
        self.assertEqual(expected_userinforows, numrows[0])

    def test_sqlitetable_businesscompoinfo_row_validation(self):
        expected_bizcomporows = 5773
        bct = BusinessCompositionTable()
        numrows = bct.num_rows()
        self.assertEqual(expected_bizcomporows, numrows[0])

    def test_sqlitetable_reviewinfo_row_validation(self):
        expected_reviewrows = 285764
        rt = ReviewTable()
        numrows = rt.num_rows()
        self.assertEqual(expected_reviewrows, numrows[0])

    def test_sqlitetable_userinfo_column_validation(self):
        expected_usercolumns = 5
        ut = UserTable()
        num_columns = ut.fetch_columnnames()
        self.assertEqual(expected_usercolumns, len(num_columns))

    def test_sqlitetable_businesscompoinfo_column_validation(self):
        expected_businesscompocolumns = 49
        bct = BusinessCompositionTable()
        num_columns = bct.fetch_columnnames()
        self.assertEqual(expected_businesscompocolumns, len(num_columns))

    def test_sqlitetable_reviewinfo_column_validation(self):
        expected_reviewcolumns = 15
        rt = ReviewTable()
        num_columns = rt.fetch_columnnames()
        self.assertEqual(expected_reviewcolumns, len(num_columns))

    def test_sqlitetable_bizzipcode_validation(self):
        expected_validzipcode = 5761
        bct = BusinessCompositionTable()
        num_rows = bct.validate_zipcodecolumn()
        self.assertEqual(expected_validzipcode, num_rows[0])
