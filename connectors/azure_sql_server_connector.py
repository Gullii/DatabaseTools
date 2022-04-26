import logging

import pyodbc


class AzureSQLConnection:
    def __init__(
        self,
        server_name: str,
        server_db: str,
        username: str,
        password: str,
        port: str = "1433",
    ):
        self.server_name = server_name
        self.server_db = server_db
        self.username = username
        self.user_password = password
        self.server_port = port
        self.driver = "{ODBC Driver 17 for SQL Server}"

    def get_connection(self):
        """
        Connects to the database with the provided connection details
        :return: Connection object to query a database
        """
        try:
            connection_string = (
                f"DRIVER={self.driver};SERVER={self.server_name},"
                f"{self.server_port};DATABASE={self.server_db};UID"
                f"={self.username};PWD={self.user_password}"
            )
            connection = pyodbc.connect(connection_string)

            logging.info("Connection established successfully to Azure SQL Server")
        except pyodbc.Error as e:
            logging.exception("Connection to Azure SQL Server failed")
            raise e
        else:
            return connection

    def get_user(self):
        return self.username

    def get_server_name(self):
        return self.server_name

    def get_database(self):
        return self.get_database()

    def get_password(self):
        return self.user_password

    def get_port(self):
        return self.server_port

    def __str__(self):
        return f"Azure SQL Server Connection at: {self.username}@{self.server_name}/{self.server_db}"
