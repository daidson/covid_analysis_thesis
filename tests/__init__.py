from pyspark.sql import SparkSession

SPARK = SparkSession.builder.master('local') \
                            .appName("Main Class") \
                            .config('spark.submit.deployMode', 'client') \
                            .config('spark.driver.memory','16g') \
                            .config('spark.executor.memory','16g') \
                            .config('spark.executor.cores', 4) \
                            .config('spark.sql.debug.maxToStringFields', 1000) \
                            .config("spark.sql.execution.arrow.pyspark.enabled", True) \
                            .config("spark.sql.repl.eagerEval.enabled", True) \
                            .config("spark.sql.shuffle.partitions", 1000) \
                            .config("spark.driver.host", "localhost") \
                            .enableHiveSupport() \
                            .getOrCreate()
