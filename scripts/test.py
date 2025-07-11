from datetime import datetime
import pytz
import numpy as np
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

path = "s3://premier-league-data-sample/premier-league-leaderboard/20060206.csv"
df = spark.read.format("csv").option("header", "true").load(path)