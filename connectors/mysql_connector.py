import logging
import mysql.connector


class MySQLConnection:
    def __init__(self, username: str, password: str, host: str, database: str):
        self.user = username
        self.password = password
        self.host = host
        self.db = database

    def get_connection(self):
        """
        Connects to the database with the provided connection details
        :return: Connection object to query a database
        """
        config = {
            "user": f"{self.user}@{self.host}",
            "password": self.password,
            "host": self.host,
            "database": self.db,
            "charset": "utf8",
        }
        try:
            connection = mysql.connector.connect(**config)
        except Exception as e:
            logging.exception("Error connecting to MySQL: ")
            raise e
        else:
            return connection

    def get_user(self) -> str:
        return self.user

    def get_host(self) -> str:
        return self.host

    def get_db(self) -> str:
        return self.db

    def get_password(self) -> str:
        return self.password

    def __str__(self):
        return f"MySQLConnection at: {self.user}@{self.host}/{self.db}"
