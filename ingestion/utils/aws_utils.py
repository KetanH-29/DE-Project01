#To Create jdbc redshift and mysql url automatically
def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config[][]
    port = redshift_config[][]
    database = redshift_config[][]
    username = redshift_config[][]
    password = redshift_config[][]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host,port,database,username,password)

def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config[][]
    port = mysql_config[][]
    database = mysql_config[][]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host,port,database)

#To connect to the databases automatically
def read_from_my_sql(spark,table_name,partition_col,table_name,secret_conf):
    print("\nReading data from MySQL DB")
    jdbc_params = {"url":get_mysql_jdbc_url(secret_conf),
        "lowerBound":"1",
        "upperBound":"100",
        "dbtable":table_name[][],
        "numPartitions":"2":,
        "partitionColumn":part_col[][],
        "user":secret_conf[][],
        "password":secret_conf[][]
    }
    df = spark \
        .read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .load()

    return df

def read_from_sftp(spark,app_secret,secret_file,filepath):
    return spark.read \
        .format("com.springml.spark.sftp") \
        .option("host",app_secret[][])\
        .option("port",app_secret[][]) \
        .option("username",app_secret[][]) \
        .option("pem",secret_file) \
        .option("fileType","csv") \
        .option("delimeter","|") \
        .load()

def read_from_mongoDB(spark,database,collection):
    print("\nRead from mongoDB")
    df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", app_secret[][]) \
        .option("collection", app_secret[][]) \
        .load()

    return df

def read_from_s3(spark,path,delimeter = '|', header = 'true'):
    print("\nReading data from s3")
    df = spark \
        .read \
        .format("csv") \
        .option("delimeter",app_secret[][]) \
        .option("header", app_secret[][]) \
        .load()

    return df

def read_parquet_from_s3(spark,file,path):
    return spark.read \
        .option("header", "true") \
        .option("delimeter", "|") \
        .parquet(filepath)


def write_data_to_redshift(spark,)