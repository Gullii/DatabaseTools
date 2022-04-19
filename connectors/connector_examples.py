import pandas as pd
from gbq_connector import GoogleBigQueryConnection
from mysql_connector import MySQLConnection
from pg_connector import PostgresConnection


# Connect to a postgres database
pg_connector = PostgresConnection("some_user", "some_password", "some_host", "some_database")
pg_connection = pg_connector.get_connection()
pg_data = pd.read_sql("Some SQL", pg_connection)  # Use with pandas to create dataframe from table

# Connect to a MySQL database (also works for MariaDB)
mysql_connector = MySQLConnection("some_user", "some_password", "some_host", "some_database")
mysql_data = pd.read_sql("Some SQL", mysql_connector.get_connection())  # Use with pandas to create dataframe from table

# Connect to Google Big Query
gbq_conn = GoogleBigQueryConnection("path/to/service_account_file", "example_project")
gbq_data = gbq_conn.get_client().query("Some Query").to_dataframe()  # Get dataframe from table
