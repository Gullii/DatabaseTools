import pandas as pd
from connectors.gbq_connector import GoogleBigQueryConnection


class GBQTableUpdater:
    def __init__(
        self,
        new_df: pd.DataFrame,
        table_name: str,
        connection: GoogleBigQueryConnection,
        schema,
    ):
        self.table_name = table_name
        self.schema = schema
        self.dataframe = new_df
        self.connection = connection

    def _delete_from_table(self, condition: str):
        sql = f"""DELETE FROM {self.schema}.{self.table_name} WHERE weekdate='{condition}'"""
        self._execute_query(sql)

    def _insert_into_table(self, if_exists):
        self.dataframe.to_gbq(
            f"{self.schema}.{self.table_name}",
            credentials=self.connection.credentials,
            chunksize=10000,
            if_exists=if_exists,
            reauth=False,
        )

    def _execute_query(self, sql):
        self.connection.get_client().query(sql)

    def delete_and_insert_to_table(self, delete_condition=None):
        if delete_condition is not None:
            self._delete_from_table(delete_condition)
            self._insert_into_table(if_exists="append")
        else:
            self._insert_into_table(if_exists="replace")

    def get_client(self):
        return self.connection.get_client()
