import psycopg2
import logging

logger = logging.getLogger("sources_one")

class PostgresDatabase:
    _host = ''
    _database = ''
    _username = ''
    _password = ''
    _port = ''
    _query = ''
    _query_result_db_list = ''
    _query_result_proper_db_list = ''
    _query_result = ''
    _query_result_proper = ''

    def __init__(self, host, username, password, port, query, database):
        self._host = host
        self._username = username
        self._password = password
        self._port = port
        self._query = query
        self._database = database
        if len(self._database) > 0:
            self.schema_list()
        else:
            self.database_list()

    def database_list(self):
        try:
            psql_db = psycopg2.connect(
                host=self._host,
                user=self._username,
                password=self._password,
                port=self._port
            )
            psql_cursor = psql_db.cursor()
            psql_cursor.execute(self._query)
            self._query_result_db_list = psql_cursor.fetchall()
            self._query_result_proper_db_list = [{"result": query[0]} for query in self._query_result_db_list]
            psql_cursor.close()
        except Exception:
            logger.error("Error in Getting Database List Postgres!!!", exc_info=True)

    def schema_list(self):
        try:
            psql_db = psycopg2.connect(
                host=self._host,
                user=self._username,
                password=self._password,
                port=self._port,
                database=self._database
            )
            psql_cursor = psql_db.cursor()
            psql_cursor.execute(self._query)
            self._query_result = psql_cursor.fetchall()
            self._query_result_proper = [{"result": query[0]} for query in self._query_result]
            psql_cursor.close()
        except Exception:
            logger.error("Error in Getting Schema List Postgres!!!", exc_info=True)

    def get_query_result_db_list(self):
        return self._query_result_db_list

    def get_query_result_proper_db_list(self):
        return self._query_result_proper_db_list

    def get_query_result(self):
        return self._query_result

    def get_query_result_proper(self):
        return self._query_result_proper

psql_db = PostgresDatabase(host="127.0.0.1", username="postgres", password="admin@1234", port="5432", query="SELECT nspname FROM pg_catalog.pg_namespace;", database = '')
print(psql_db.get_query_result_proper())