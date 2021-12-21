import mysql.connector
import logging

logger = logging.getLogger("sources_one")

class MySqlDatabase:
    _host = ''
    _username = ''
    _password = ''
    _port = ''
    _query = ''
    _query_result = ''
    _query_result_proper = ''

    def __init__(self, host, username, password, port, query):
        try:
            self._host = host
            self._username = username
            self._password = password
            self._port = port
            self._query = query

            mydb = mysql.connector.connect(
                host = self._host,
                user = self._username,
                password = self._password,
                port = self._port
            )
            my_cursor = mydb.cursor()
            my_cursor.execute(self._query)
            self._query_result = my_cursor.fetchall()
            self._query_result_proper = [{"result" : query[0]} for query in self._query_result]
            mydb.close()
        except Exception:
            logger.error("Error in MySQL Database Class", exc_info = True)

    def get_query_result(self):
        return self._query_result

    def get_query_result_proper(self):
        return self._query_result_proper




# mysql_db = MySqlDatabase(host="127.0.0.1", username="reco", password="admin@1234", port="3306", query="show databases;")
# print(mysql_db.get_query_result())