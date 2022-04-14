import pandas as pd
import sqlalchemy


class PostgresTableUpdater:
    def __init__(
        self, new_df: pd.DataFrame, table_name: str, connection, schema: str = "public"
    ):
        """
        :param new_df: Dataframe that is to be saved in a table
        :param table_name: the name of the table to interact with
        :param connection: connection object
        :param schema: schema where the table to interact is nested in
        """
        self.table_name = table_name
        self.connection = connection
        self.schema = schema
        self.dataframe = new_df
        self.engine = sqlalchemy.create_engine(
            f"postgresql+psycopg2://{self.connection.info.user}"
            f":{self.connection.info.password}"
            f"@{self.connection.info.host}:5432/{self.connection.info.dbname}"
        )

    def _upsert_to_table(self):
        """
        Upsert functionality to update already existing rows and insert new ones in one statement.
        Function currently untested
        :return: None
        """
        sql = f"""INSERT INTO {self.schema}.{self.table_name}
            VALUES {self._get_values_string()}
            ON CONFLICT 
            DO UPDATE SET ({self._get_column_names()})=({self._get_excluded_column_names()})"""
        self._execute_query(sql)

    def _get_values_string(self):
        return ",".join(
            str(row) for row in list(self.dataframe.itertuples(index=False, name=None))
        )

    def _get_column_names(self):
        return ",".join(col for col in list(self.dataframe.columns))

    def _get_excluded_column_names(self):
        return ",".join(
            ",".join(f"Excluded.{col}") for col in list(self.dataframe.columns)
        )

    def _delete_from_table(self, condition: str):
        sql = f"""DELETE FROM {self.schema}.{self.table_name} WHERE {condition}"""
        self._execute_query(sql)

    def _insert_into_table(self, if_exists):
        self.dataframe.to_sql(
            self.table_name,
            con=self.engine,
            schema=self.schema,
            chunksize=10000,
            method="multi",
            if_exists=if_exists,
            index=False,
        )

    def _execute_query(self, sql):
        """
        Execute a query on the Database
        :param sql: The SQL that should be executed
        :return: None
        """
        cur = self.connection.cursor()
        cur.execute(sql)
        self.connection.commit()
        cur.close()

    def delete_and_insert_to_table(self, delete_condition: str = None):
        """
        Insert new data into the table either replacing the table or deleting some data beforehand
        :param delete_condition: Str representing part of SQL-Delete Statement after WHERE-Clause
        :return: None
        """
        if delete_condition is not None:
            self._delete_from_table(delete_condition)
            self._insert_into_table(if_exists="append")
        else:
            self._insert_into_table(if_exists="replace")

    def get_table_data(self, columns=None) -> pd.DataFrame:
        """
        @param columns: specify columns to select
        @return: Dataframe with table
        """
        if columns is not None:
            sql = f"Select {','.join(i for i in columns)} from {self.schema}.{self.table_name}"
        else:
            sql = f"Select * from {self.schema}.{self.table_name}"
        return pd.read_sql(sql, self.connection)

    def get_connection(self):
        return self.connection
