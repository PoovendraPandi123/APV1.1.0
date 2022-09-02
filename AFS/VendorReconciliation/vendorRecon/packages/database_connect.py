import cx_Oracle
import os
import pandas as pd
import logging
import json

logger = logging.getLogger("vendor_reconciliation")

class OracleConnection():

    _cursor = ''
    _con = ''
    _query = ''
    _write_file_path = ''
    _data = pd.DataFrame()
    _object_type = ''
    _query_output = ''

    def __init__(self, query, object_type):
        self._query = query
        self._object_type = object_type
        self.connect_db()

    def connect_db(self):
        try:
            lib_dir = r"C:\instantclient_21_3"
            os.environ["PATH"] = lib_dir + ";" + os.environ["PATH"]
            self._con = cx_Oracle.connect('xxtmx_view/vwapp5tmx@10.100.1.232:1571/R12UAT')

        except cx_Oracle.DatabaseError as e:
            # print("There is a problem with Oracle", e)
            logger.error("Error in Oracle Database Connection!!!", exc_info=True)
            logger.error(str(e))

        else:
            # Now execute the sqlquery
            self._cursor = self._con.cursor()

            # Creating a table employee
            # cursor.execute("select * from XXTMX.XXTMX_VRECO_VENDOR_MST_V")
            # rows = cursor.fetchall()
            # print(rows)
            #query = "select * from XXTMX.XXTMX_AP_PARTY_LEDGER_T"
            self._cursor.execute(self._query)

            if self._object_type == "table":
                column_names = [col[0] for col in self._cursor.description]
                rows = self.dict_fetch_all(self._cursor)
                table_output = {"headers":column_names, "data":rows}
                self._query_output = json.dumps(table_output)

            elif self._object_type in ["data"]:
                column_names = [col[0] for col in self._cursor.description]
                rows = self.dict_fetch_all(self._cursor)
                table_output = {"headers": column_names, "data": rows}
                self._query_output = table_output
            #print(query_out)

        # by writing finally if any error occurs
        # then also we can close the all database operation
        finally:
            if self._cursor:
                self._cursor.close()
            if self._con:
                self._con.close()

    def dict_fetch_all(self, cursor):
        "Return all rows from cursor as a dictionary"
        try:
            column_header = [col[0] for col in cursor.description]
            return [dict(zip(column_header, row)) for row in self._cursor.fetchall()]
        except Exception:
            logger.error("Error in converting cursor data to dictionary", exc_info=True)

    def get_query_output(self):
        return self._query_output
