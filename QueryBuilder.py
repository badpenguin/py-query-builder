import sqlite3
import pymysql
import pymysql.cursors
import psycopg2
from urllib.parse import urlparse


class Database:
    def __init__(self, db_uri):
        """
        Open a connection to the specified database.

        :param db_uri: str, the URI of the database to connect to
            in the format "dialect://username:password@host:port/database".
        """
        self._db_uri: str = db_uri
        self._connection = None
        self.cursor = None
        self.db_type: str = ''

    def connect(self):
        """
        Connect to the database using the provided URL.


        """
        if self._connection:
            return self._connection
        url_parts = urlparse(self._db_uri)
        self.db_type = url_parts.scheme
        if url_parts.scheme == "sqlite":
            self._connection = sqlite3.connect(url_parts.path)
            self.cursor = self._connection.cursor()
        elif url_parts.scheme == "mysql":
            self._connection = pymysql.connect(
                host=url_parts.hostname,
                user=url_parts.username,
                password=str(url_parts.password),
                database=url_parts.path[1:],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            self.cursor = self._connection.cursor(pymysql.cursors.DictCursor)
        elif url_parts.scheme == "postgresql":
            self._connection = psycopg2.connect(
                host=url_parts.hostname,
                user=url_parts.username,
                password=url_parts.password,
                dbname=url_parts.path[1:],
            )
            self.cursor = self._connection.cursor()

    def close(self):
        """
        Close the connection to the database.
        """
        if self._connection:
            self._connection.close()

    def begin(self):
        """
        Begin a new transaction.
        """
        if self._connection:
            self.cursor.execute("BEGIN")

    def commit(self):
        """
        Commit the current transaction.
        """
        if self._connection:
            self._connection.commit()

    def rollback(self):
        """
        Rollback the current transaction.
        """
        if self._connection:
            self._connection.rollback()

    def execute(self, sql, params=None):
        """
         Execute a raw SQL query with optional parameters.

         :param sql: str, the SQL query to be executed.
         :param params: tuple, list or dictionary of parameters to be used in the query (default: None).
         """
        self.connect()
        if params:
            self.cursor.execute(sql, params)
        else:
            self.cursor.execute(sql)
        return self.cursor.rowcount

    def last_insert_id(self):
        """
        Get the last inserted ID in the database.

        :return: int, the last inserted ID.
        """
        if self._connection:
            return self.cursor.lastrowid

    def row_count(self):
        return self.cursor.rowcount

    """
    def last_insert_id2(self):
        Get the ID of the last inserted row.
        :return: int, the ID of the last inserted row.

        if self.db_uri.startswith("mysql://"):
            self.cursor.execute("SELECT LAST_INSERT_ID()")
        elif self.db_uri.startswith("postgresql://"):
            self.cursor.execute("SELECT lastval()")
        elif self.db_uri.startswith("sqlite://"):
            self.cursor.execute("SELECT last_insert_rowid()")
        return self.cursor.fetchone()[0]
    """

    def table(self, table_name):
        """
        Create a new QueryBuilder instance for the given table name.

        :param table_name: str, the name of the table to query.
        :return: QueryBuilder, a new QueryBuilder instance.
        """
        self.connect()
        return QueryBuilder(self, table_name)


class QueryBuilder:

    def __init__(self, db, table_name):
        """
        Initialize a new QueryBuilder instance.

        :param db: Database, the database connection object.
        :param table_name: str, the name of the table to query.
        """
        self._db = db
        self._table: str = table_name
        self._select: str = "*"
        self._join = []
        self._where = []
        self._group_by: str = ''
        self._having: str = ''
        self._order_by: str = ''
        self._limit: int = 0
        self._offset: int = 0
        self._params = {}
        self._query_type: str = "SELECT"
        self._query_data = {}
        self._executed: bool = False
        self._update_on_duplicate: bool = False

    def select(self, columns='*'):
        """
        Set the columns to select from the table.

        :param columns: str, the columns to select.
        """
        self._select = columns or "*"
        return self

    def where(self, condition, params=None):
        """
        Add a WHERE condition to the query.

        :param condition: str, the WHERE condition.
        :param params: tuple, list or dictionary of parameters to be used in the condition (default: None).
        :return: QueryBuilder, the current QueryBuilder instance.
        """
        self._where.append(condition)
        if params:
            self._params.update(params)
        return self

    def order_by(self, column):
        """
        Add an ORDER BY clause to the query.

        :param column: str, the column to order by.
        :return: QueryBuilder, the current QueryBuilder instance.
        """
        self._order_by = column
        return self

    def group_by(self, column):
        """
        Add a GROUP BY clause to the query.

        :param column: str, the column to group by.
        :return: QueryBuilder, the current QueryBuilder instance.
        """
        self._group_by = column
        return self

    def having(self, condition, params=None):
        self._having = condition
        if params:
            self._params.update(params)
        return self

    def limit(self, limit):
        """
        Add a LIMIT clause to the query.

        :param limit: int, the number of rows to limit the query to.
        :return: QueryBuilder, the current QueryBuilder instance.
        """
        self._limit = limit
        return self

    def offset(self, offset):
        """
        Add an OFFSET clause to the query.

        :param offset: int, the number of rows to skip before starting to return rows.
        :return: QueryBuilder, the current QueryBuilder instance.
        """
        self._offset = offset
        return self

    def join(self, table_name, on_condition, params=None, join_type="INNER JOIN"):
        """
        Add a JOIN clause to the query.

        :param table_name: str, the name of the table to join.
        :param on_condition: str, the ON condition for the join.
        :param params: tuple, list or dictionary of parameters to be used in the condition (default: None).
        :param join_type: str, the type of join to perform (default: "INNER JOIN").
        :return: QueryBuilder, the current QueryBuilder instance.
        """
        self._join.append((join_type, table_name, on_condition))
        if params:
            self._params.update(params)
        return self

    def left_join(self, table, on, params=None):
        return self.join(table, on, params, join_type="LEFT JOIN")

    def right_join(self, table, on, params=None):
        return self.join(table, on, params, join_type="RIGHT JOIN")

    def full_join(self, table, on, params=None):
        return self.join(table, on, params, join_type="FULL JOIN")

    def fetch_all(self):
        """
        Execute the query and return all the rows as a list of dictionaries.

        :return: list of dictionaries, each representing a row from the result set.
        """
        self.run()
        return self._db.cursor.fetchall()

    def fetch_row(self):
        """
        Execute the query and return the first row as a dictionary.

        :return: dictionary, representing the first row from the result set, or None if no rows are returned.
        """
        self.limit(1)
        self.run()
        return self._db.cursor.fetchone()
        # result = self.fetch_all()
        # return result[0] if result else None

    def fetch_column(self):
        """
        Execute the query and return the first column of the first row
        """
        self.limit(1)
        self.run()
        row = self._db.cursor.fetchone()
        return row[0] if row else None

    def insert(self, data, on_duplicate=False):
        """
        Set the query type to INSERT and specify the data to insert.

        :param data: dict, a dictionary containing the column names as keys and the corresponding values.
        :return: QueryBuilder instance.
        """

        if on_duplicate and self._db.db_type != "mysql":
            raise NotImplementedError("ON DUPLICATE KEY UPDATE is only supported in MySQL.")
        self._update_on_duplicate = on_duplicate
        self._query_type = "INSERT"
        self._query_data = data
        return self

    def update(self, data):
        """
        Set the query type to UPDATE and specify the data to update.

        :param data: dict, a dictionary containing the column names as keys and the corresponding values.
        :return: QueryBuilder instance.
        """
        self._query_type = "UPDATE"
        self._query_data = data
        return self

    def delete(self):
        """
        Set the query type to DELETE.

        :return: QueryBuilder instance.
        """
        self._query_type = "DELETE"
        return self

    def build_query(self):
        """
        Build the final SQL query string based on the current state of the QueryBuilder instance.

        :return: str, the SQL query string.
        """
        query: str = self._query_type
        if self._query_type == 'SELECT':
            # query = query + f" {self._select} FROM {self._table}"
            query = query + ' ' + self._select + ' FROM ' + self._table
        elif self._query_type == 'INSERT':
            columns = ",".join(self._query_data.keys())
            placeholders = ', '.join([f":{key}" for key in self._query_data.keys()])
            self._params.update(self._query_data)
            query = query + f" INTO {self._table} ({columns}) VALUES ({placeholders})"

            if self._update_on_duplicate:
                set_clause = ", ".join([f"{key} = :{key}" for key in self._query_data.keys()])
                query += f" ON DUPLICATE KEY UPDATE {set_clause}"

        elif self._query_type == 'UPDATE':
            set_clause = ", ".join([f"{key} = :{key}" for key in self._query_data.keys()])
            self._params.update(self._query_data)
            query = query + f" {self._table} SET {set_clause}"
        elif self._query_type == 'DELETE':
            query = f" FROM {self._table}"
        else:
            raise ValueError("Unsopported query type")

        if self._join:
            for join_type, table_name, on_condition in self._join:
                query += f" {join_type} {table_name} ON {on_condition}"

        if self._where:
            query += " WHERE " + " AND ".join(self._where)

        if self._group_by:
            query += " GROUP BY " + self._group_by

        if self._having:
            query += " HAVING " + self._having

        if self._order_by:
            query += " ORDER BY " + self._order_by

        if self._limit:
            query += f" LIMIT {self._limit}"

        if self._offset:
            query += f" OFFSET {self._offset}"

        return query

    def run(self):
        if self._executed:
            return self
        self._executed = True
        query = self.build_query()
        if self._params:
            self._db.cursor.execute(query, self._params)
        else:
            self._db.cursor.execute(query)
        return self

    def debug_params(self):
        return self._params
    
