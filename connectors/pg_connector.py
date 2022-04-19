import logging
import urllib.parse
import psycopg2


class PostgresConnection:
    def __init__(
        self,
        postg_user: str,
        postg_pass: str,
        postg_host: str,
        postg_db: str,
        postg_port: int = None,
        ssl_mode: str = "require",
    ):
        self.postg_user = postg_user
        self.postg_pass = postg_pass
        self.postg_host = postg_host
        self.postg_db = postg_db
        self.portg_port = postg_port
        self.ssl_mode = ssl_mode

    def get_connection_string(self) -> str:
        return (
            f"postgres://{urllib.parse.quote_plus(self.postg_user)}:{urllib.parse.quote_plus(self.postg_pass)}@"
            f"{self.postg_host}:{self.portg_port or 5432}/{self.postg_db}?sslmode={self.ssl_mode}"
        )

    def get_connection(self):
        """
        Connect to the database with given credentials
        :return: Connection object to query a database
        """
        try:
            connection = psycopg2.connect(
                user=self.postg_user,
                password=self.postg_pass,
                host=self.postg_host,
                port=self.portg_port,
                database=self.postg_db,
                sslmode=self.ssl_mode,
            )
        except (Exception, psycopg2.Error) as e:
            logging.exception(f"Error connecting to Postgres:")
            raise e
        else:
            return connection

    def get_user(self) -> str:
        return self.postg_user

    def get_host(self) -> str:
        return self.postg_host

    def get_password(self) -> str:
        return self.postg_pass

    def get_db(self) -> str:
        return self.postg_db

    def get_sslmode(self) -> str:
        return self.ssl_mode

    def __str__(self):
        return f"PostgresConnection at: {self.postg_user}@{self.postg_host}/{self.postg_db}"
