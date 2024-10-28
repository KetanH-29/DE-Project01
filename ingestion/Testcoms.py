from pyspark.sql import SparkSession
import yaml
import os.path
import util.aws_utils as ut
impport pyspark.sql.functions import *

if __name__ == '__main__:':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    #Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise application") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')


    current_dir =os.path.abspath(os.path.dirname(__file__))
    app_config_path =os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + "secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secrets = open(app_secrets_path)
    app_secrets = yaml,load(conf, Loader=yaml.FullLoader)


    #Setting up spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])


    src_list = app_conf["source_list"]

    for src in src_list:
        src_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src
        src_conf = app_conf[src]

        #Read from mysql
        if src == "SB" :
            txn_df = ut.read_from_my_sql(spark,src_conf["mysql_conf"]["dbtable"],src_conf["mysql_conf"]["partition_column"],app_secrets)

        txn_df = txn_df.withColumn('ins_dt', current_date())
        txn_df.show(5, False)

        # Write data to s3 in parquet

        txn_df \
            .write \
            .mode("overwrite") \
            .partitionBy("ins_dt") \
            .parquet(src_path)



        #Read from sftp
        elif src = "OL" :
            txn_df = ut.read_from_sftp(spark,src_conf[][],os.path.abspath(current_dir + "/../../" + app_secrets[]),app_secrets)

        txn_df = txn_df.withColumn('ins_dt', current_date())
        txn_df.show(5, False)

        #write data to s3 in parquet

        txn_df \
            .write \
            .mode("overwrite") \
            .partitionBy("ins_dt") \
            .parquet(src_path)


        #Read from mongodb
        elif src = "ADDR" :
            txn_df = ut.read_from_mongoDB(spark,src_conf[][],src_conf[][],app_secrets)


        txn_df = txn_df.withColumn('ins_dt', current_date())
        txn_df.show(5, False)

        #write data to s3 in parquet

        txn_df \
            .write \
            .mode("overwrite") \
            .partitionBy("ins_dt") \
            .parquet(src_path)

        elif src = "CP" :
            txn_df = ut.read_parquet_from_s3(spark,"s3a://" + app_conf[][] + src_conf[])

        txn_df = txn_df.withColumn('ins_dt', current_date())
        txn_df.show(5, False)

        #write data to s3 in parquet

        txn_df \
            .write \
            .mode("overwrite") \
            .partitionBy("ins_dt") \
            .parquet(src_path)



    # spark-submit --packages "mysql:mysql-connector-java:8.0.15" FinalProjects/ingestion/Testcoms.py


