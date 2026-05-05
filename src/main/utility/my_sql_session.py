import mysql.connector

from resources.dev import config


class MySQLConnectionProvider:

    def __init__(self):
        self.connection = mysql.connector.connect(
            host = config.host,
            user = config.properties['user'],
            password = config.properties['password'],
            database = config.database_name
        )

    def get_mysql_connection(self):
        return self.connection


