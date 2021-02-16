# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import uuid
import argparse
import csv
import json

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.athena_metadata_extractor import AthenaMetadataExtractor
from databuilder.extractor.athena_stats_extractor import AthenaStatsExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.task.neo4j_staleness_removal_task import Neo4jStalenessRemovalTask
from databuilder.transformer.base_transformer import NoopTransformer

# NEO4J cluster endpoints
neo4j_endpoint = 'bolt://127.0.0.1:7687'
neo4j_user = 'neo4j'
neo4j_password = 'test'
es = Elasticsearch([
    {'host': '127.0.0.1'},
])


def create_table_extract_job(target_schema_sql, connection_string):
    where_clause_suffix = f"where table_schema in {target_schema_sql}"

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = f'{tmp_folder}/nodes/'
    relationship_files_folder = f'{tmp_folder}/relationships/'

    job_config = ConfigFactory.from_dict({
        f'extractor.athena_metadata.{AthenaMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause_suffix,
        f'extractor.athena_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connection_string,
        f'extractor.athena_metadata.{AthenaMetadataExtractor.CATALOG_KEY}': "'AwsDataCatalog'",
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': False,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.FORCE_CREATE_DIR}': True,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j_endpoint,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j_user,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j_password,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(
        conf=job_config,
        task=DefaultTask(
            extractor=AthenaMetadataExtractor(),
            loader=FsNeo4jCSVLoader(),
            transformer=NoopTransformer()
        ),
        publisher=Neo4jCsvPublisher()
    )
    job.launch()


def create_table_stats_job(connection_string):
    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = f'{tmp_folder}/nodes/'
    relationship_files_folder = f'{tmp_folder}/relationships/'
    target_cols = f'{tmp_folder}/nodes/Column_2.csv'

    with open(target_cols, 'r') as r:
        reader = csv.DictReader(r)
        column_data = dict()
        for line in reader:
            key = line['KEY'].split('/')
            catalog_source = key[-3].split('.')[0]
            target_table = key[-3].split('.')[1] + '.' + key[-2]
            target_column = key[-1]
            if target_table in column_data.keys():
                column_data[target_table] += [target_column]
            else:
                column_data[target_table] = [target_column]

    for k, column_list in column_data.items():
        target_schema, target_table = k.split('.')
        column_list = json.dumps(column_list)

        job_config = ConfigFactory.from_dict({
            f'extractor.athena_metadata.{AthenaStatsExtractor.CATALOG_KEY}': catalog_source,
            f'extractor.athena_metadata.{AthenaStatsExtractor.TARGET_SCHEMA}': target_schema,
            f'extractor.athena_metadata.{AthenaStatsExtractor.TARGET_TABLE}': target_table,
            f'extractor.athena_metadata.{AthenaStatsExtractor.COLUMN_LIST}': column_list,
            f'extractor.athena_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connection_string,
            f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
            f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
            f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.FORCE_CREATE_DIR}': True,
            f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
            f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
            f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j_endpoint,
            f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j_user,
            f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j_password,
            f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag',  # should use unique tag here like {ds}
        })

        job = DefaultJob(
            conf=job_config,
            task=DefaultTask(
                extractor=AthenaStatsExtractor(),
                loader=FsNeo4jCSVLoader(),
                transformer=NoopTransformer()
            ),
            publisher=Neo4jCsvPublisher()
        )
        job.launch()


def create_es_publisher_sample_job():
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    # elastic search client instance
    elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())
    # related to mapping type from /databuilder/publisher/elasticsearch_publisher.py#L38
    elasticsearch_new_index_key_type = 'table'
    # alias for Elasticsearch used in amundsensearchlibrary/search_service/config.py as an index
    elasticsearch_index_alias = 'table_search_index'

    job_config = ConfigFactory.from_dict({
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.GRAPH_URL_CONFIG_KEY}': neo4j_endpoint,
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.MODEL_CLASS_CONFIG_KEY}':
            'databuilder.models.table_elasticsearch_document.TableESDocument',
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_USER}': neo4j_user,
        f'extractor.search_data.extractor.neo4j.{Neo4jExtractor.NEO4J_AUTH_PW}': neo4j_password,
        f'loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY}': extracted_search_data_path,
        f'loader.filesystem.elasticsearch.{FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY}': 'w',
        f'publisher.elasticsearch.{ElasticsearchPublisher.FILE_PATH_CONFIG_KEY}': extracted_search_data_path,
        f'publisher.elasticsearch.{ElasticsearchPublisher.FILE_MODE_CONFIG_KEY}': 'r',
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY}':
            elasticsearch_client,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY}':
            elasticsearch_new_index_key,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY}':
            elasticsearch_new_index_key_type,
        f'publisher.elasticsearch.{ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY}':
            elasticsearch_index_alias
    })

    job = DefaultJob(
        conf=job_config,
        task=DefaultTask(
            loader=FSElasticsearchJSONLoader(),
            extractor=Neo4jSearchDataExtractor(),
            transformer=NoopTransformer()
        ),
        publisher=ElasticsearchPublisher()
    )
    job.launch()


def remove_stale_data_in_neo4j():
    job_config_dict = {
        'job.identifier': 'remove_stale_data_job',
        'task.remove_stale_data.neo4j_endpoint': neo4j_endpoint,
        'task.remove_stale_data.neo4j_user': neo4j_user,
        'task.remove_stale_data.neo4j_password': neo4j_password,
        'task.remove_stale_data.staleness_max_pct': 1000,
        'task.remove_stale_data.minimum_milliseconds_to_expire': 0,
        'task.remove_stale_data.milliseconds_to_expire': 0,
        'task.remove_stale_data.target_nodes': ['Table', 'Column'],
    }
    job_config = ConfigFactory.from_dict(job_config_dict)
    job = DefaultJob(conf=job_config, task=Neo4jStalenessRemovalTask())
    job.launch()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch Athena metadata')
    parser.add_argument('--region', required=True)
    parser.add_argument('--s3output', required=True)
    parser.add_argument('--target_schema', nargs='+', required=True)
    args = parser.parse_args()

    connection_string = "awsathena+rest://@athena.%s.amazonaws.com:443/?s3_staging_dir=%s" % (args.region, args.s3output)
    target_schema_sql = "('{schemas}')".format(schemas="', '".join(args.target_schema))

    #remove_stale_data_in_neo4j()
    create_table_extract_job(target_schema_sql, connection_string)
    create_table_stats_job(connection_string)
    create_es_publisher_sample_job()
