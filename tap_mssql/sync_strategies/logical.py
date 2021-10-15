#!/usr/bin/env python3
# pylint: disable=duplicate-code

import pendulum
import singer
from singer import metadata

from tap_mssql.connection import (
    connect_with_backoff,
    get_azure_sql_engine,
)
import tap_mssql.sync_strategies.common as common

LOGGER = singer.get_logger()

BOOKMARK_KEYS = {
    "current_log_version",
    "last_pk_fetched",
    "initial_full_table_complete",
}

# do_sync_incremental(mssql_conn, config, catalog_entry, state, columns)

# do_sync_full_table(mssql_conn, config, catalog_entry, state, columns)


class log_based_sync():
    """
    Methods to validate the log-based sync of a table from mssql
    """

    def __init__(self, mssql_conn, config, catalog_entry, state, columns):
        self.logger = singer.get_logger()
        self.config = config
        self.catalog_entry = catalog_entry
        self.state = state
        self.columns = columns
        self.database_name = config.get("database")
        self.schema_name = common.get_database_name(
            self.catalog_entry
        )
        self.table_name = catalog_entry.table
        self.mssql_conn = mssql_conn

    def assert_log_based_is_enabled(self):

        database_is_change_tracking_enabled = self._get_change_tracking_database()
        table_is_change_tracking_enabled = self._get_change_tracking_tables()
        min_valid_version = self._get_min_valid_version()

        if (
            database_is_change_tracking_enabled
            & table_is_change_tracking_enabled
            & min_valid_version is not None
        ):
            self.logger.info("Asserted stream is log-based enabled!")
            return True
        else:
            return False # use this to break silently maybe if a table is not set properly and move to the next item?

    def _get_change_tracking_database(self):  # do this the first time only as required? future change for now
        self.logger.info("Validate the database for change tracking")

        sql_query = (
            "SELECT DB_NAME(database_id) AS db_name FROM sys.change_tracking_databases"
        )

        database_is_change_tracking_enabled = False

        with self.mssql_conn.connect() as open_conn:
            results = open_conn.execute(sql_query)
            row = results.fetchone()

            if row["db_name"] == self.database_name:
                database_is_change_tracking_enabled = True
            else:
                raise Exception(
                    "Cannot sync stream using log-based replication. Change tracking is not enabled for database: {}"
                ).format(self.database_name)

        return database_is_change_tracking_enabled

    def _get_change_tracking_tables(self):  # do this the first time only as required?

        self.logger.info("Validating the schemas and tables for change tracking")

        schema_table = (self.schema_name, self.table_name)

        sql_query = """
            SELECT OBJECT_SCHEMA_NAME(object_id) AS schema_name,
            OBJECT_NAME(object_id) AS table_name
            FROM sys.change_tracking_tables
            """

        table_is_change_tracking_enabled = False
        with self.mssql_conn.connect() as open_conn:
            change_tracking_tables = open_conn.execute(sql_query)

            enabled_change_tracking_tables = change_tracking_tables.fetchall()
            if schema_table in enabled_change_tracking_tables:
                table_is_change_tracking_enabled = True
            else:
                raise Exception(
                    "Cannot sync stream using log-based replication. Change tracking is not enabled for table: {}"
                ).format(self.schema_table)

        return table_is_change_tracking_enabled  # this should be the table name?

    def _get_min_valid_version(self):  # should be per table I think?

        self.logger.info("Validating the min_valid_version")

        sql_query = "SELECT CHANGE_TRACKING_MIN_VALID_VERSION({}) as min_valid_version"
        object_id = self._get_object_version_by_table_name()

        with self.mssql_conn.connect() as open_conn:
            results = open_conn.execute(sql_query.format(object_id))
            row = results.fetchone()

            min_valid_version = row["min_valid_version"]

        return min_valid_version  # return a valid version

    def _get_object_version_by_table_name(self):  # should be per table I think?

        self.logger.info("Getting object_id by name")

        schema_table = self.schema_name + "." + self.table_name
        # config.database_name
        # sel
        sql_query = "SELECT OBJECT_ID('{}') AS object_id"
        #    (-> (partial format "%s.%s.%s")
        with self.mssql_conn.connect() as open_conn:
            results = open_conn.execute(sql_query.format(schema_table))
            row = results.fetchone()

            object_id = row["object_id"]

        if object_id is None:
            raise Exception(
                "The min valid version for the table was null"
            ).format(self.schema_table)

        return object_id

    def log_based_init_state(self):
        # this appears to look for an existing state and gets the current log version if necessary
        # also setting the initial_full_table_complete state to false

        initial_full_table_complete = singer.get_bookmark(
            self.state, self.catalog_entry.tap_stream_id, "initial_full_table_complete"
        )

        if initial_full_table_complete is None:
            self.logger.info("Setting new current log version from db.")

            current_log_version = self._get_current_log_version()

            state = singer.write_bookmark(
                self.state, self.catalog_entry.tap_stream_id, "initial_full_table_complete", False
            )
        # singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))



    def _get_current_log_version(self):
        self.logger.info("Getting current change tracking version.")

        sql_query = "SELECT current_version = CHANGE_TRACKING_CURRENT_VERSION()"

        current_log_version = self._get_single_result(sql_query, "current_version")

        return current_log_version

    def log_based_initial_full_table(self):
        "Determine if we should run a full load of the table or use state."

        initial_full_table_complete = singer.get_bookmark(
            self.state, self.catalog_entry.tap_stream_id, "initial_full_table_complete"
        )
        current_log_version = self._get_current_log_version()
        min_valid_version = self._get_min_valid_version()

        min_version_out_of_date = min_valid_version < current_log_version
        self.logger.info(initial_full_table_complete)
        self.logger.info(min_version_out_of_date)
        if initial_full_table_complete == True and min_version_out_of_date == True:
            return False
        else:
            return True


        return True

    def log_based_sync(self):
        "Confirm we have state and run a log based query. This will be larger."
        return True

    def _get_single_result(self, sql_query, column):
        """
        This method takes a query and column name parameter
        and fetches then returns the single result as required.
        """
        with self.mssql_conn.connect() as open_conn:
            results = open_conn.execute(sql_query)
            row = results.fetchone()

            single_result = row[column]

        return single_result


# def sync_table(mssql_conn, config, catalog_entry, state, columns):
#     mssql_conn = MSSQLConnection(config)
#     common.whitelist_bookmark_keys(
#         generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state
#     )
