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

BOOKMARK_KEYS = {"replication_key", "replication_key_value", "version"}


def sync_table(mssql_conn, config, catalog_entry, state, columns):
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    replication_key_metadata = stream_metadata.get("replication-key")
    replication_key_state = singer.get_bookmark(
        state, catalog_entry.tap_stream_id, "replication_key"
    )

    replication_key_value = None

    if replication_key_metadata == replication_key_state:
        replication_key_value = singer.get_bookmark(
            state, catalog_entry.tap_stream_id, "replication_key_value"
        )
    else:
        state = singer.write_bookmark(
            state,
            catalog_entry.tap_stream_id,
            "replication_key",
            replication_key_metadata,
        )
        state = singer.clear_bookmark(
            state, catalog_entry.tap_stream_id, "replication_key_value"
        )

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(
        state, catalog_entry.tap_stream_id, "version", stream_version
    )

    table_stream = common.set_schema_mapping(config, catalog_entry.stream)

    activate_version_message = singer.ActivateVersionMessage(
        stream=table_stream, version=stream_version
    )

    singer.write_message(activate_version_message)
    LOGGER.info("Beginning SQL")
    with mssql_conn.connect() as open_conn:
        select_sql = common.generate_select_sql(catalog_entry, columns)
        params = {}

        if replication_key_value is not None:
            if (
                catalog_entry.schema.properties[replication_key_metadata].format
                == "date-time"
            ):
                replication_key_value = pendulum.parse(replication_key_value)

            select_sql += ' WHERE "{}" >= ? ORDER BY "{}" ASC'.format(
                replication_key_metadata, replication_key_metadata
            )

            params["replication_key_value"] = replication_key_value
        elif replication_key_metadata is not None:
            select_sql += ' ORDER BY "{}" ASC'.format(replication_key_metadata)

        common.sync_query(
            open_conn, catalog_entry, state, select_sql, columns, stream_version, table_stream, params
        )
