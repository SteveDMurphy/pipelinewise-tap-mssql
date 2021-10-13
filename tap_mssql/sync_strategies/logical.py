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


class log_based_sync:
    """
    Methods to validate the log-based sync of a table from mssql
    """

    def __init(self, mssql_conn, config, catalog_entry, state, columns):
        self.logger = singer.get_logger()
        self.config = config
        self.catalog_entry = catalog_entry
        self.state = state
        self.columns = columns
        self.database_name = common.get_database_name(
            self.catalog_entry
        )  # this is actually the schema name, database should be supplied elsewhere as required (config appears to be used today)
        self.table_name = catalog_entry.table
        self.mssql_conn = get_azure_sql_engine(config)

    def assert_log_based_is_enabled(self):

        database_is_change_tracking_enabled = _get_change_tracking_database()

        table_is_change_tracking_enabled = _get_change_tracking_tables()

        min_valid_version = _get_min_valid_version()

        if (
            database_is_change_tracking_enabled
            & table_is_change_tracking_enabled
            & min_valid_version
        ):
            return True
        else:
            return False

    def _get_change_tracking_database(
        self,
    ):  # do this the first time only as required? future change for now
        self.logger.info("Validate the database for change tracking")

        database_id = (
            self.database_name
        )  # TODO: this should actually come from the config instead to validate. Alternatively, it may just work if is not null

        sql_query = (
            "SELECT DB_NAME(database_id) AS db_name FROM sys.change_tracking_databases"
        )

        database_is_change_tracking_enabled = False

        with self.mssql_conn.connect() as open_conn:
            results = open_conn.execute(sql_query)
            row = results.fetchone()

            if row["db_name"] == database_id:
                database_is_change_tracking_enabled = True

        return database_is_change_tracking_enabled

    def _get_change_tracking_tables(self):  # do this the first time only as required?

        self.logger.info("Validating the schemas and tables for change tracking")

        schema_table = (self.database_name, self.table_name)

        sql_query = """
            SELECT OBJECT_SCHEMA_NAME(object_id) AS schema_name,
            OBJECT_NAME(object_id) AS table_name
            FROM sys.change_tracking_tables
            """

        table_is_change_tracking_enabled = False
        with self.mssql_conn.connect() as open_conn:
            change_tracking_tables = open_conn.execute(sql_query)

            if schema_table in change_tracking_tables.fetchall():
                table_is_change_tracking_enabled = True

        return table_is_change_tracking_enabled  # this should be the table name

    def _get_min_valid_version(self):  # should be per table I think?

        self.logger.info("Validating the min_valid_version")

        sql_query = "SELECT CHANGE_TRACKING_MIN_VALID_VERSION({}) as min_valid_version"
        object_id = _get_object_version_by_table_name()

        with self.mssql_conn.connect() as open_conn:
            results = open_conn.execute(sql_query.format(object_id))
            row = results.fetchone()

            min_valid_version = row["min_valid_version"]

        return min_valid_version  # return a valid version

    def _get_object_version_by_table_name(self):  # should be per table I think?

        self.logger.info("Getting object_id by name")

        schema_table = self.database_name + "." + self.table_name
        # config.database_name
        # sel
        sql_query = "SELECT OBJECT_ID({}) AS object_id"
        #    (-> (partial format "%s.%s.%s")
        with self.mssql_conn.connect() as open_conn:
            results = open_conn.execute(sql_query.format(schema_table))
            row = results.fetchone()

            object_id = row["object_id"]

        return object_id

    def log_based_init_state(self):
        # this appears to look for an existing state and gets the current log version if necessary
        # also setting the initial_full_table_complete state to false
        initial_full_table_complete = (
            None  # need to pull this from a bookmark/state area
        )
        if initial_full_table_complete is None:
            current_log_version = _get_current_log_version()
            print(
                "set the current_log_version to the received version and set initial_full_table_complete to False"
            )
            print("write state for stream-name to stdout")

    def _get_current_log_version(self):

        sql_query = "SELECT current_version = CHANGE_TRACKING_CURRENT_VERSION()"

        current_log_version = _get_single_result(sql_query, "current_version")

        return current_log_version

    def log_based_initial_full_table(self):
        "Determine if we should run a full load of the table or use state."
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
