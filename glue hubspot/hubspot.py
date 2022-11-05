catalog_connection = "redshift-prod"
dynamo_table = 'hubspot-normalize-landing-stage'
table_d = 'configuration'
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, ArrayType
# from normalize import Normalize as normalize
# from normalize import *
import json
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import regexp_replace


class Transformer():
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta
    from pyspark.sql.functions import col
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, ArrayType
    from pyspark.sql.functions import regexp_replace

    def read_s3_partitioned(glueContext, database, push_down_predicate, origin_table):
        print(glueContext, database, push_down_predicate, origin_table)
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database,
            push_down_predicate=push_down_predicate,
            table_name=origin_table,
            groupSize='10485760',
            transformation_ctx=origin_table,

        )

        return dynamic_frame

    def read_s3(glueContext, database, origin_table):
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database,
            table_name=origin_table,
            groupSize='10485760',
            transformation_ctx=origin_table,

        )

        return dynamic_frame

    def get_push_down_predicate(push_down_predicate_days):
        data = datetime.today() - relativedelta(days=+push_down_predicate_days)

        year = data.strftime("%Y")
        month = data.strftime("%m")
        day = data.strftime("%d")
        push_down_predicate = "partition_0 >= {} AND partition_1 >= {} AND partition_2 >= {}".format(year, month, day)
        # push_down_predicate="partition_0 = {} AND partition_1 = {} AND partition_2 = {}".format(year,month,day)
        # print(push_down_predicate)
        return push_down_predicate

    def flatten_struct(df):
        # compute Complex Fields (Lists and Structs) in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == StructType])
        while len(complex_fields) != 0:
            col_name = list(complex_fields.keys())[0]
            print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

            # if StructType then convert all sub element to columns.
            # i.e. flatten structs
            if (type(complex_fields[col_name]) == StructType):
                expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                            [n.name for n in complex_fields[col_name]]]
                df = df.select("*", *expanded).drop(col_name)

            # if ArrayType then add the Array Elements as Rows using the explode function
            # i.e. explode Arrays

            # recompute remaining Complex Fields in Schema
            complex_fields = dict([(field.name, field.dataType)
                                   for field in df.schema.fields
                                   if type(field.dataType) == StructType])
        return df

    def flatten_all(df):
        # compute Complex Fields (Lists and Structs) in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
        while len(complex_fields) != 0:
            col_name = list(complex_fields.keys())[0]
            print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

            # if StructType then convert all sub element to columns.
            # i.e. flatten structs
            if (type(complex_fields[col_name]) == StructType):
                expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                            [n.name for n in complex_fields[col_name]]]
                df = df.select("*", *expanded).drop(col_name)

            # if ArrayType then add the Array Elements as Rows using the explode function
            # i.e. explode Arrays
            elif (type(complex_fields[col_name]) == ArrayType):
                df = df.withColumn(col_name, explode_outer(col_name))

            # recompute remaining Complex Fields in Schema
            complex_fields = dict([(field.name, field.dataType)
                                   for field in df.schema.fields
                                   if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
        return df

    def deduplicate(df, deduplicates):
        for deduplicate in deduplicates:
            print(deduplicate['orderby'])
            window = Window.partitionBy([col(x) for x in deduplicate['partitionby']]).orderBy(
                [col(x).desc() for x in deduplicate['orderby']])
            df = df.withColumn("row", row_number().over(window)).filter(col("row") == 1).drop("row")
        return df

    def capitalize(df, columns):

        for column in columns:
            df = df.withColumn(column, upper(column))
            df = df.withColumn(column, translate(column, 'ÁÉÍÓÚÇÕÂÃÔÊ', 'AEIOUCOAAOE'))

        return df

    def capitalizewords(df, capitalizewords):

        for capitalizeword in capitalizewords:
            df = df.withColumn(capitalizeword['field'],
                               regexp_replace(capitalizeword['field'], capitalizeword['pattern'],
                                              capitalizeword['replacement']))
            print(capitalizeword['field'], capitalizeword['pattern'], capitalizeword['replacement'])

        return df

    def connection_options(redshift_database, stage_table, pre_query, post_query):
        connection_options = None
        connection_options = {
            "database": redshift_database,
            "dbtable": stage_table,
            "preactions": pre_query,
            "postactions": post_query,
            "bulkSize": "10",
            "extracopyoptions": "TRUNCATECOLUMNS",
            # "extracopyoptions":"TIMEFORMAT 'auto'",

        }
        return connection_options

    def upsert_amazonRedshift(glueContext, new_df, catalog_connection, connection_options_result):
        upsert_amazonRedshift = glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=new_df,
            catalog_connection=catalog_connection,
            connection_options=connection_options_result,
            redshift_tmp_dir="s3://aws-glue-assets-990857395650-us-east-2/temporary/",
            transformation_ctx="AmazonRedshift_node1659538045146",
        )

    def query(destination_table):

        target_table = "hubspot.{destination_table}".format(destination_table=destination_table)
        stage_table = "hubspot.{destination_table}_stage_table".format(destination_table=destination_table)

        pre_query = """

                    drop table if exists {stage_table};
                    create table {stage_table} as select * from {target_table} LIMIT 0;""".format(
            stage_table=stage_table, target_table=target_table)

        post_query = """
                    begin;

                    delete from {target_table} using {stage_table} where {stage_table}.id = {target_table}.id ;
                    insert into {target_table} select * from {stage_table}; 
                    drop table {stage_table}; 
                    end;""".format(stage_table=stage_table, target_table=target_table)
        return stage_table, pre_query, post_query

    def query_archived(destination_table):

        target_table = "hubspot.{destination_table}".format(destination_table=destination_table)
        stage_table = "hubspot.{destination_table}_stage_table".format(destination_table=destination_table)

        pre_query = """

                    drop table if exists {stage_table};
                    create table {stage_table} as select * from {target_table} LIMIT 0;""".format(
            stage_table=stage_table, target_table=target_table)

        post_query = """
                    begin;

                    delete from {target_table} using {stage_table} where {stage_table}.id = {target_table}.id and {stage_table}.dt_updated >={target_table}.dt_updated ;
                    insert into {target_table} select * from {stage_table}; 
                    drop table {stage_table}; 
                    end;""".format(stage_table=stage_table, target_table=target_table)
        return stage_table, pre_query, post_query

    def get_normalize(op):

        operacoes = {'connection_options': connection_options}
        return operacoes[op]()


class Access():
    def get_conf(dynamo_table, table_d):
        dynamo = boto3.client('dynamodb')
        dynamo_table = dynamo_table
        table_d = table_d
        response = dynamo.get_item(
            TableName=dynamo_table,
            Key={'table_name': {'S': table_d}}
        )
        result = response['Item']['select_columns']['S']

        h = result
        conf = json.loads(h)
        return conf

    def get_sql(dynamo_table, table):
        dynamo = boto3.client('dynamodb')
        dynamo_table = dynamo_table
        table = table
        response = dynamo.get_item(
            TableName=dynamo_table,
            Key={'table_name': {'S': table}}
        )
        sql = response['Item']['select_columns']['S']
        return sql

    def get_schema(dynamo_table, table):
        dynamo = boto3.client('dynamodb')
        dynamo_table = dynamo_table
        table = table
        response = dynamo.get_item(
            TableName=dynamo_table,
            Key={'table_name': {'S': table}}
        )
        result = response['Item']['schema']['S']
        # print(result)
        h = result
        conf = json.loads(h)

        mapping = []
        for i in conf['schema']:
            mapping.append((i['Name'], i['Type'], i['NameTo'], i['TypeTo']))
        # print('mapping',mapping)
        return mapping


m_map = Access.get_conf(dynamo_table, table_d)


def flow(glueContext, origin_table, glue_database, destination_table, redshift_database, catalog_connection,
         deduplicates, capitalize, capitalizewords, table):
    # contacts
    dynamic_frame = Transformer.read_s3(glueContext, glue_database, origin_table)
    df = dynamic_frame.toDF()
    df1 = Transformer.flatten_struct(df)
    df1 = Transformer.deduplicate(df1, deduplicates)
    mapping_schema = Access.get_schema(dynamo_table, table)
    new_dy = DynamicFrame.fromDF(df1, glueContext, "new_dy")
    new_dynamic_frame = new_dy.apply_mapping(mapping_schema)
    new_dynamic_frame.toDF().createOrReplaceTempView('table')
    sql = Access.get_sql(dynamo_table, table)
    df2 = spark.sql(sql)
    df2 = Transformer.capitalize(df2, capitalize)
    df2 = Transformer.capitalizewords(df2, capitalizewords)
    final_dy = DynamicFrame.fromDF(df2, glueContext, "new_dy")
    stage_table, pre_query, post_query = Transformer.query(destination_table)
    connection_options_result = Transformer.connection_options(redshift_database, stage_table, pre_query, post_query)

    Transformer.upsert_amazonRedshift(glueContext, final_dy, catalog_connection, connection_options_result)


def flow2(glueContext, origin_table, glue_database, destination_table, redshift_database, catalog_connection,
          deduplicates, capitalize, capitalizewords, table):
    # companies
    push_down_predicate_days = 1
    push_down_predicate = Transformer.get_push_down_predicate(push_down_predicate_days)
    dynamic_frame = Transformer.read_s3_partitioned(glueContext, glue_database, push_down_predicate, origin_table)
    # dynamic_frame=Transformer.read_s3(glueContext,glue_database,origin_table)
    df = dynamic_frame.toDF()
    df1 = Transformer.flatten_struct(df)
    df1 = Transformer.deduplicate(df1, deduplicates)
    mapping_schema = Access.get_schema(dynamo_table, table)
    new_dy = DynamicFrame.fromDF(df1, glueContext, "new_dy")
    new_dynamic_frame = new_dy.apply_mapping(mapping_schema)
    new_dynamic_frame.toDF().createOrReplaceTempView('table')
    sql = Access.get_sql(dynamo_table, table)
    df2 = spark.sql(sql)
    df2 = Transformer.capitalize(df2, capitalize)
    df2 = Transformer.capitalizewords(df2, capitalizewords)
    final_dy = DynamicFrame.fromDF(df2, glueContext, "new_dy")
    stage_table, pre_query, post_query = Transformer.query(destination_table)
    connection_options_result = Transformer.connection_options(redshift_database, stage_table, pre_query, post_query)

    Transformer.upsert_amazonRedshift(glueContext, final_dy, catalog_connection, connection_options_result)


def flow3(glueContext, origin_table, glue_database, destination_table, redshift_database, catalog_connection,
          deduplicates, capitalize, capitalizewords, table):
    # deal_pipelines
    # push_down_predicate_days=1
    # push_down_predicate=Transformer.get_push_down_predicate(push_down_predicate_days)
    # dynamic_frame=Transformer.read_s3_partitioned(glueContext, glue_database, push_down_predicate, origin_table)
    dynamic_frame = Transformer.read_s3(glueContext, glue_database, origin_table)
    df = dynamic_frame.toDF()
    df1 = Transformer.flatten_all(df)
    df1 = Transformer.deduplicate(df1, deduplicates)
    mapping_schema = Access.get_schema(dynamo_table, table)
    new_dy = DynamicFrame.fromDF(df1, glueContext, "new_dy")
    new_dynamic_frame = new_dy.apply_mapping(mapping_schema)
    new_dynamic_frame.toDF().createOrReplaceTempView('table')
    sql = Access.get_sql(dynamo_table, table)
    df2 = spark.sql(sql)
    df2 = Transformer.capitalize(df2, capitalize)
    df2 = Transformer.capitalizewords(df2, capitalizewords)
    final_dy = DynamicFrame.fromDF(df2, glueContext, "new_dy")
    stage_table, pre_query, post_query = Transformer.query(destination_table)
    connection_options_result = Transformer.connection_options(redshift_database, stage_table, pre_query, post_query)

    Transformer.upsert_amazonRedshift(glueContext, final_dy, catalog_connection, connection_options_result)


def flow4(glueContext, origin_table, glue_database, destination_table, redshift_database, catalog_connection,
          deduplicates, capitalize, capitalizewords, table):
    # deals
    dynamic_frame = Transformer.read_s3(glueContext, glue_database, origin_table)
    df = dynamic_frame.toDF()
    df1 = Transformer.flatten_struct(df)
    df1 = Transformer.deduplicate(df1, deduplicates)
    mapping_schema = Access.get_schema(dynamo_table, table)
    new_dy = DynamicFrame.fromDF(df1, glueContext, "new_dy")
    new_dynamic_frame = new_dy.apply_mapping(mapping_schema)
    new_dynamic_frame.toDF().createOrReplaceTempView('table')
    sql = Access.get_sql(dynamo_table, table)
    df2 = spark.sql(sql)
    df2 = Transformer.capitalize(df2, capitalize)
    df2 = Transformer.capitalizewords(df2, capitalizewords)
    final_dy = DynamicFrame.fromDF(df2, glueContext, "new_dy")
    stage_table, pre_query, post_query = Transformer.query(destination_table)
    connection_options_result = Transformer.connection_options(redshift_database, stage_table, pre_query, post_query)

    Transformer.upsert_amazonRedshift(glueContext, final_dy, catalog_connection, connection_options_result)


def flow5(glueContext, origin_table, glue_database, destination_table, redshift_database, catalog_connection,
          deduplicates, capitalize, capitalizewords, table):
    # owners
    # push_down_predicate_days=1
    # push_down_predicate=Transformer.get_push_down_predicate(push_down_predicate_days)
    # dynamic_frame=Transformer.read_s3_partitioned(glueContext, glue_database, push_down_predicate, origin_table)
    dynamic_frame = Transformer.read_s3(glueContext, glue_database, origin_table)
    df = dynamic_frame.toDF()
    df1 = Transformer.flatten_all(df)
    df1 = Transformer.deduplicate(df1, deduplicates)
    mapping_schema = Access.get_schema(dynamo_table, table)
    new_dy = DynamicFrame.fromDF(df1, glueContext, "new_dy")
    new_dynamic_frame = new_dy.apply_mapping(mapping_schema)
    new_dynamic_frame.toDF().createOrReplaceTempView('table')
    sql = Access.get_sql(dynamo_table, table)
    df2 = spark.sql(sql)
    df2 = Transformer.capitalize(df2, capitalize)
    df2 = Transformer.capitalizewords(df2, capitalizewords)
    final_dy = DynamicFrame.fromDF(df2, glueContext, "new_dy")
    stage_table, pre_query, post_query = Transformer.query(destination_table)
    connection_options_result = Transformer.connection_options(redshift_database, stage_table, pre_query, post_query)

    Transformer.upsert_amazonRedshift(glueContext, final_dy, catalog_connection, connection_options_result)


def flow6(glueContext, origin_table, glue_database, destination_table, redshift_database, catalog_connection,
          deduplicates, capitalize, capitalizewords, table):
    # archived_companies
    # push_down_predicate_days=1
    # push_down_predicate=Transformer.get_push_down_predicate(push_down_predicate_days)
    # dynamic_frame=Transformer.read_s3_partitioned(glueContext, glue_database, push_down_predicate, origin_table)
    dynamic_frame = Transformer.read_s3(glueContext, glue_database, origin_table)
    df = dynamic_frame.toDF()
    df1 = Transformer.flatten_struct(df)
    df1 = Transformer.deduplicate(df1, deduplicates)
    mapping_schema = Access.get_schema(dynamo_table, table)
    new_dy = DynamicFrame.fromDF(df1, glueContext, "new_dy")
    new_dynamic_frame = new_dy.apply_mapping(mapping_schema)
    new_dynamic_frame.toDF().createOrReplaceTempView('table')
    sql = Access.get_sql(dynamo_table, table)
    df2 = spark.sql(sql)
    df2 = Transformer.capitalize(df2, capitalize)
    df2 = Transformer.capitalizewords(df2, capitalizewords)
    final_dy = DynamicFrame.fromDF(df2, glueContext, "new_dy")
    stage_table, pre_query, post_query = Transformer.query_archived(destination_table)
    connection_options_result = Transformer.connection_options(redshift_database, stage_table, pre_query, post_query)

    Transformer.upsert_amazonRedshift(glueContext, final_dy, catalog_connection, connection_options_result)


def flow7(glueContext, origin_table, glue_database, destination_table, redshift_database, catalog_connection,
          deduplicates, capitalize, capitalizewords, table):
    # archived_contacts
    dynamic_frame = Transformer.read_s3(glueContext, glue_database, origin_table)
    df = dynamic_frame.toDF()
    df1 = Transformer.flatten_struct(df)
    df1 = Transformer.deduplicate(df1, deduplicates)
    mapping_schema = Access.get_schema(dynamo_table, table)
    new_dy = DynamicFrame.fromDF(df1, glueContext, "new_dy")
    new_dynamic_frame = new_dy.apply_mapping(mapping_schema)
    new_dynamic_frame.toDF().createOrReplaceTempView('table')
    sql = Access.get_sql(dynamo_table, table)
    df2 = spark.sql(sql)
    df2 = Transformer.capitalize(df2, capitalize)
    df2 = Transformer.capitalizewords(df2, capitalizewords)
    final_dy = DynamicFrame.fromDF(df2, glueContext, "new_dy")
    stage_table, pre_query, post_query = Transformer.query_archived(destination_table)
    connection_options_result = Transformer.connection_options(redshift_database, stage_table, pre_query, post_query)

    Transformer.upsert_amazonRedshift(glueContext, final_dy, catalog_connection, connection_options_result)


def flow8(glueContext, origin_table, glue_database, destination_table, redshift_database, catalog_connection,
          deduplicates, capitalize, capitalizewords, table):
    # deals
    dynamic_frame = Transformer.read_s3(glueContext, glue_database, origin_table)
    df = dynamic_frame.toDF()
    df1 = Transformer.flatten_struct(df)
    df1 = Transformer.deduplicate(df1, deduplicates)
    mapping_schema = Access.get_schema(dynamo_table, table)
    new_dy = DynamicFrame.fromDF(df1, glueContext, "new_dy")
    new_dynamic_frame = new_dy.apply_mapping(mapping_schema)
    new_dynamic_frame.toDF().createOrReplaceTempView('table')
    sql = Access.get_sql(dynamo_table, table)
    df2 = spark.sql(sql)
    df2 = Transformer.capitalize(df2, capitalize)
    df2 = Transformer.capitalizewords(df2, capitalizewords)
    final_dy = DynamicFrame.fromDF(df2, glueContext, "new_dy")
    stage_table, pre_query, post_query = Transformer.query_archived(destination_table)
    connection_options_result = Transformer.connection_options(redshift_database, stage_table, pre_query, post_query)

    Transformer.upsert_amazonRedshift(glueContext, final_dy, catalog_connection, connection_options_result)


def flow2():
    print('flow2')


for i in m_map['conf']:
    origin_table = i['origin_table']
    destination_table = i['destination_table']
    glue_database = i['glue_database']
    redshift_database = i['redshift_database']
    catalog_connection = i['catalog_connection']
    deduplicates = i['deduplicate']
    capitalize = i['capitalize']
    capitalizewords = i['capitalizeword']
    table = i['destination_table']
    # type(capitalizeword)
    flow1 = i['flow']

    # flow(glueContext,origin_table,glue_database,destination_table,redshift_database,catalog_connection,deduplicate,capitalize,capitalizeword)

    # print(flow)
    if flow1 == 'flow6':
        # print('aqui')
        flow7(glueContext, origin_table, glue_database, destination_table, redshift_database, catalog_connection,
              deduplicates, capitalize, capitalizewords, table)
        # flow(glueContext,i['origin_table'],i['glue_database'],i['destination_table'],i['redshift_database'],i['catalog_connection'],i['deduplicate'],i['capitalize'],i['capitalizeword'])
    # else:
    # flow2()

origin_table
job.commit()