import pandas as pd

from connectors.gbq_connector import GoogleBigQueryConnection
from connectors.pg_connector import PostgresConnection

from updaters.pg_updater import PostgresTableUpdater
from updaters.gbq_updater import GBQTableUpdater

# Create example Dataframe
data = {"Id": [1, 2], "Val": [3, 4]}
example_dataframe = pd.DataFrame(data)

# Example on how to update data in a Postgres table
pg_conn = PostgresConnection(
    "example_user", "example_password", "example_host", "example_db"
)
pg_updater = PostgresTableUpdater(
    example_dataframe, "example_table", pg_conn.get_connection(), schema="example_schema"
)
# Deletes and replaces the whole Table
pg_updater.delete_and_insert_to_table()
# Delete entry with Id 2 and append Dataframe to table
pg_updater.delete_and_insert_to_table("Id=2")


# Example on how to update data in a Big query table
gbq_conn = GoogleBigQueryConnection(
    "example_service_account_file.json", "example_project"
)
gbq_updater = GBQTableUpdater(example_dataframe, "example_table", gbq_conn, "example_schema")
# Delete and replace whole Table
gbq_updater.delete_and_insert_to_table()
