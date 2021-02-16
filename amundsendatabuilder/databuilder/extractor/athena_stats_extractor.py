# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import json
from typing import (
    Iterator, Union,
)

from pyhocon import ConfigFactory, ConfigTree

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.models.table_stats import TableColumnStats


LOGGER = logging.getLogger(__name__)


class AthenaStatsExtractor(Extractor):
    """
    Get Athena table stats using SQLAlchemyExtractor
    """

    # CONFIG KEYS
    CATALOG_KEY = 'catalog_source'
    TARGET_SCHEMA = 'target_schema'
    TARGET_TABLE = 'target_table'
    COLUMN_LIST = 'column_list'

    # Default values
    DEFAULT_CLUSTER_NAME = 'AwsDataCatalog'
    DEFAULT_TARGET_SCHEMA = 'sample_schema'
    DEFAULT_TARGET_TABLE = 'sample_table'
    DEFAULT_COLUMN_LIST = "['col1']"

    DEFAULT_CONFIG = ConfigFactory.from_dict({
        CATALOG_KEY: DEFAULT_CLUSTER_NAME,
        TARGET_SCHEMA: DEFAULT_TARGET_SCHEMA,
        TARGET_TABLE: DEFAULT_TARGET_TABLE,
        COLUMN_LIST: DEFAULT_COLUMN_LIST
    })

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(AthenaStatsExtractor.DEFAULT_CONFIG)
        self._cluster = conf.get_string(AthenaStatsExtractor.CATALOG_KEY)
        self._target_schema = conf.get_string(AthenaStatsExtractor.TARGET_SCHEMA)
        self._target_table = conf.get_string(AthenaStatsExtractor.TARGET_TABLE)
        self._column_list = json.loads(conf.get_string(AthenaStatsExtractor.COLUMN_LIST))

        self.sql_stmt = self._create_sql(
            self._cluster,
            self._target_schema,
            self._target_table,
            self._column_list
        )

        LOGGER.info('SQL for Athena stats: %%s', self.sql_stmt)

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope())\
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self._alchemy_extractor.init(sql_alch_conf)  # execute_query
        self._extract_iter: Union[None, Iterator] = None

    def extract(self) -> Union[TableColumnStats, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.athena_metadata'

    def _get_extract_iter(self) -> Iterator[TableColumnStats]:
        """
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        """
        row = self._alchemy_extractor.extract()
        while row:
            yield TableColumnStats(
                row['table_name'],
                row['col_name'],
                row['stat_name'],
                row['stat_val'],
                row['start_epoch'],
                row['end_epoch'],
                row['db'],
                row['cluster'],
                row['schema']
            )
            row = self._alchemy_extractor.extract()

    def _create_sql(self, catalog_source, target_schema, target_table, column_list):
        col_name_sql = ', '.join(["'" + col + "'" for col in column_list])
        str_convert_sql = ', '.join([
            'cast("' + col + '" as varchar) as "' + col + '"' for col in column_list])
        max_col_sql = ','.join([f'''
            coalesce(
              cast(max(try_cast("{col}" as bigint)) as varchar),
              cast(max(try_cast("{col}" as double)) as varchar),
              cast(try(max("{col}")) as varchar)
            )''' for col in column_list])
        min_col_sql = ','.join([f'''
            coalesce(
              cast(min(try_cast("{col}" as bigint)) as varchar),
              cast(min(try_cast("{col}" as double)) as varchar),
              cast(try(min("{col}")) as varchar)
            )''' for col in column_list])
        avg_col_sql = ','.join([f'''
            coalesce(
              cast(avg(try_cast("{col}" as bigint)) as varchar),
              cast(avg(try_cast("{col}" as double)) as varchar),
              null
            )''' for col in column_list])
        stdev_col_sql = ','.join([f'''
            coalesce(
              cast(stddev(try_cast("{col}" as bigint)) as varchar),
              cast(stddev(try_cast("{col}" as double)) as varchar),
              null
            )''' for col in column_list])
        med_col_sql = ','.join([f'''
            coalesce(
              cast(approx_percentile(try_cast("{col}" as bigint), 0.5) as varchar),
              cast(approx_percentile(try_cast("{col}" as double), 0.5) as varchar),
              null
            )''' for col in column_list])
        cnt_col_sql = ','.join([
            f'cast(count("{col}") as varchar)' for col in column_list])
        uniq_col_sql = ','.join([
            f'cast(count(distinct "{col}") as varchar)' for col in column_list])
        nul_col_sql = ','.join([
            f'cast(sum(case when "{col}" is null then 1 else 0 end) as varchar)' for col in column_list])

        SQL_STATEMENT = f"""
        WITH str_convert AS (
          SELECT {str_convert_sql}
          FROM "{target_schema}"."{target_table}"
        ), max_col AS (
          SELECT
            'max' as stat_name,
            array[{col_name_sql}] as col_name,
            array[{max_col_sql}] as stat_val
          FROM str_convert
        ), min_col AS (
          SELECT
            'min' as stat_name,
            array[{col_name_sql}] as col_name,
            array[{min_col_sql}] as stat_val
          FROM str_convert
        ), avg_col AS (
          SELECT
            'avg' as stat_name,
            array[{col_name_sql}] as col_name,
            array[{avg_col_sql}] as stat_val
          FROM str_convert
        ), stdev_col AS (
          SELECT
            'std dev' as stat_name,
            array[{col_name_sql}] as col_name,
            array[{stdev_col_sql}] as stat_val
          FROM str_convert
        ), med_col AS (
          SELECT
            'median' as stat_name,
            array[{col_name_sql}] as col_name,
            array[{med_col_sql}] as stat_val
          FROM str_convert
        ), cnt_col AS (
          SELECT
            'num rows' as stat_name,
            array[{col_name_sql}] as col_name,
            array[{cnt_col_sql}] as stat_val
          FROM str_convert
        ), uniq_col AS (
          SELECT
            'num uniq' as stat_name,
            array[{col_name_sql}] as col_name,
            array[{uniq_col_sql}] as stat_val
          FROM str_convert
        ), nul_col AS (
          SELECT
            'num nulls' as stat_name,
            array[{col_name_sql}] as col_name,
            array[{nul_col_sql}] as stat_val
          FROM str_convert
        ), union_table AS (
          SELECT t1.stat_name, t2.col_name, t2.stat_val FROM max_col t1
          CROSS JOIN UNNEST (col_name, stat_val) AS t2(col_name, stat_val)
          UNION SELECT t1.stat_name, t2.col_name, t2.stat_val FROM min_col t1
          CROSS JOIN UNNEST (col_name, stat_val) AS t2(col_name, stat_val)
          UNION SELECT t1.stat_name, t2.col_name, t2.stat_val FROM avg_col t1
          CROSS JOIN UNNEST (col_name, stat_val) AS t2(col_name, stat_val)
          UNION SELECT t1.stat_name, t2.col_name, t2.stat_val FROM stdev_col t1
          CROSS JOIN UNNEST (col_name, stat_val) AS t2(col_name, stat_val)
          UNION SELECT t1.stat_name, t2.col_name, t2.stat_val FROM med_col t1
          CROSS JOIN UNNEST (col_name, stat_val) AS t2(col_name, stat_val)
          UNION SELECT t1.stat_name, t2.col_name, t2.stat_val FROM cnt_col t1
          CROSS JOIN UNNEST (col_name, stat_val) AS t2(col_name, stat_val)
          UNION SELECT t1.stat_name, t2.col_name, t2.stat_val FROM uniq_col t1
          CROSS JOIN UNNEST (col_name, stat_val) AS t2(col_name, stat_val)
          UNION SELECT t1.stat_name, t2.col_name, t2.stat_val FROM nul_col t1
          CROSS JOIN UNNEST (col_name, stat_val) AS t2(col_name, stat_val)
        )
        SELECT
          '{catalog_source}' as cluster,
          'athena' as db,
          '{target_schema}' as schema,
          '{target_table}' as table_name,
          col_name,
          stat_name,
          stat_val,
          to_unixtime(now()) as start_epoch,
          to_unixtime(now()) as end_epoch
        FROM union_table
        ORDER BY cluster, db, schema, table_name, col_name
        ;
        """
        return SQL_STATEMENT
