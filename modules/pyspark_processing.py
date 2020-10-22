import datetime
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

class pyspark_processing:
  def __init__(
      self, 
      spark, 
      archive_date):
    
    self.paths = dict()

    self.paths['ArchiveFolder'] = "./data/"
    self.paths['UsersDBFolder'] = "./users/"

    self.paths['ParquetPath'] = (
      self.paths['ArchiveFolder'] + archive_date + '/' + 'data.parquet/')
    self.paths['ParquetPathFeatured'] = (
      self.paths['ArchiveFolder'] + archive_date + '/' + 'data_Featured.parquet/')

    self.N_usersDB = 0
    self.N_users_from_DB = 0
    self.N_archive = 0

    self.spark = spark
        
  def load_archive(self):
    schema = StructType() \
        .add("White",StringType(),True) \
        .add("Black",StringType(),True) \
        .add("Result",FloatType(),True) \
        .add("WhiteElo",IntegerType(),True) \
        .add("BlackElo",FloatType(),True) \
        .add("ECO",StringType(),True) \
        .add("TimeControl",FloatType(),True) \
        .add("Termination",StringType(),True) \
        .add("PlyCount",FloatType(),True) \
        .add("TimeStamp",StringType(),True) 
          
    sdf = self.spark.read.schema(schema).parquet(self.paths['ParquetPath'])
    sdf = sdf.withColumn(
      'TimeStamp', 
      to_timestamp(sdf.TimeStamp,"yyyy-MM-dd HH:mm:ss"))

    self.N_archive = sdf.count()
               
    return sdf
  
  def load_UserDB(self):
    try:
      pba
      sdf = self.spark.read.parquet(self.paths['UsersDBFolder'])
      self.N_usersDB = sdf.count()
    except:
      schema = StructType([
        StructField('user', StringType(), True),
        StructField('userID', IntegerType(), True),
        ])    
      sdf = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(),schema)

      self.N_usersDB = 0

    return sdf

  def collect_users(self, sdf):
    sdf_white = self.collect_user_by_color(sdf, 'White')
    sdf_black = self.collect_user_by_color(sdf, 'Black')

    sdf_users = sdf_white.join(sdf_black, on=['user'], how='left').dropDuplicates()
    self.N_users = sdf_users.count()
    return sdf_users

  def collect_user_by_color(self, sdf, color):
    sdf = sdf.select(col(color).alias('user')).dropDuplicates()
    return sdf

  def join_users(self, sdf_usersDB, sdf_usersArchiv):
    sdf_users = sdf_usersArchiv.join(sdf_usersDB, on='user', how='leftanti')
    sdf_users = (
      sdf_users
        .withColumn(
            "userID", 
            row_number().over(Window.partitionBy().orderBy("user"))
            )
        .withColumn("userID", col("userID") + self.N_users_from_DB)
    )
    
    sdf_users = sdf_usersDB.join(sdf_users, on=['user', 'userID'], how='fullouter')
    return sdf_users

  def write_usersDB(sdf):
    sdf.write.parquet(self.paths['UsersDBFolder'], mode='overwrite')

  def write_full_featured():
    sdf.write.parquet(self.paths['ParquetPathFeatured'], mode='overwrite')

  def replace_user_by_ID(self, archivDB, usersDB):
    sdf = archivDB.join(
      usersDB.withColumnRenamed("user", "White"), 
      on='White',
      how='inner'
    ).drop('White') \
    .withColumnRenamed('userID','White') \
    .join(
      usersDB.withColumnRenamed("user", "Black"), 
      on='Black',
      how='inner'
    ).drop('Black') \
    .withColumnRenamed('userID','Black')

    return sdf
