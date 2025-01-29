from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ingest_nyc_taxi_data.config.ConfigStore import *
from ingest_nyc_taxi_data.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Ingest_NYC_Taxi_Data")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Ingest_NYC_Taxi_Data")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Ingest_NYC_Taxi_Data", config = Config)(pipeline)

if __name__ == "__main__":
    main()
