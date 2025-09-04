import psycopg2 as postgres
import sys

class DiagDB():
    '''
    Usage:
    optics_db = diagDB(database, host, user, password, port)
    :function _fetch_query: Execute a query and return the result of the query
    :function _execute_query: Execute a query
    :function _create_tbl: Create a table if it doesn't already exist
    :function _execute_query_list: Execute a query for a list of data using executemany()
    :function _drop_table: Removes the table from the database
    '''

    ###########################################################################
    def __init__(self, db_database, db_schema, db_table, db_host, db_user, db_password, db_port):
        """
        Connects to either mysql or postgres database and returns a cursor
        which can be used to execute queries on the database.
        :param db_database: Name of the database
        :param db_schema: Schema name on the database
        :param db_table: Name of the table in the schema
        :param db_host: AWS host path where database is saved
        :param db_user: Username having write access privilege to the DB
        :param db_password: Password for this user
        :param db_port: Default port number on which the access goes through
        """
        self._database = db_database
        self._schema = db_schema
        self._table = db_table
        self._host = db_host
        self._user = db_user
        self._password = db_password
        self._port = db_port
        self._cur = None
        try:
            # connect to the database
            self._connect()

        except Exception as error:
            print(error)


    ###########################################################################
    def _connect(self):
        '''Connect to the database and obtain a cursor'''
        try:
            self._conn = postgres.connect(database=self._database,
                        host=self._host,
                        user=self._user,
                        password=self._password,
                        port=self._port)
            self._conn.autocommit = True

        except Exception as error:
            print(error)


    ###########################################################################
    def _disconnect(self):
        '''Disconnect from the database'''
        try:
            if self._conn:
                self._cur.close()
                self._conn.close()

        except Exception as error:
            print(error)


    ###########################################################################
    def _fetch_query(self, query):
        '''Execute the query provided and return the result fetched'''
        try:
            self._connect()
            if self._conn:
                self._cur = self._conn.cursor()
                self._cur.execute(query)
                rows = self._cur.fetchall()
                return rows

        except Exception as error:
            print(error)

        finally:
            self._disconnect()


    ###########################################################################
    def _execute_query(self, query, data):
        '''Execute the query provided'''
        try:
            self._connect()
            if self._conn:
                self._cur = self._conn.cursor()
                self._cur.execute(query, data)

        except Exception as error:
            print(error)

        finally:
            self._disconnect()


    ###########################################################################
    def _get_cursor(self):
        try:
            self._connect()
            if self._conn:
                self._cur = self._conn.cursor()
        except Exception as error:
            print(error)
            sys.exit(1)
        return self._cur

    
    ###########################################################################
    def _add_column(self, column_name, data_type):
        '''Add column to the table with the name and datatype provided'''
        try:
            self._connect()
            if self._conn:
                self._cur = self._conn.cursor()
                cmd = f'ALTER TABLE {self._table} ADD {column_name} {data_type}'
                self._cur.execute(cmd)

        except Exception as error:
            print(error)

        finally:
            self._disconnect()


    ###########################################################################
    def _create_tbl(self, tbl_sql_dict, primary_key):
        '''
        Create Table index and assign unique ID for the table rows
        consisting of system_name & port
        :param tbl_sql_dict: dict required column names and data types
        :primary_key: primary key for the database
        '''
        try:
            self._connect()
            if self._conn:
                self._cur = self._conn.cursor()
                if self._cur:
                    # construct the table based on the data field and type in the input dict
                    tbl_sql = "("
                    for fieldName, fieldType in tbl_sql_dict.items():
                        tbl_sql = tbl_sql + fieldName + ' ' + fieldType + ', '
                    tbl_sql = tbl_sql + f'{primary_key});'
                    cmd = 'CREATE TABLE IF NOT EXISTS ' + f'{self._schema}.{self._table} {tbl_sql}'
                    self._cur.execute(cmd)
                else:
                    print('Database connection not established')

        except Exception as error:
            print(error)

        finally:
            self._disconnect()


    ###########################################################################
    def _execute_query_list(self, query, data_list):
        '''
        Add new row entries based on the query for all elements in
        data_list
        :param query: command to insert the data into the database
        :param data_list: list of elements to be inserted
        '''
        try:
            self._connect()
            if self._conn:
                cur = self._conn.cursor()
                if self._cur:
                    cur.executemany(query, data_list)
                else:
                    print('Database connection not established')

        except Exception as error:
            print(error)

        finally:
            self._disconnect()


    ###########################################################################
    def _drop_table(self):
        '''Execute the query provided'''
        try:
            self._connect()
            if self._conn:
                self._cur = self._conn.cursor()
                cmd = f'DROP TABLE {self._schema}.{self._table}'
                self._cur.execute(cmd)

        except Exception as error:
            print(error)

        finally:
            self._disconnect()
