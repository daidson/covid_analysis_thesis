from pyspark.sql import SparkSession

SPARK = SparkSession.builder.master('yarn') \
                            .appName("PySUS main executor job") \
                            .config('spark.driver.memory','10g') \
                            .config('spark.submit.deployMode','client') \
                            .config('spark.executor.memory','16g') \
                            .config('spark.executor.cores',4) \
                            .config('spark.yarn.queue','short') \
                            .config('spark.sql.debug.maxToStringFields', 1000) \
                            .config("spark.sql.execution.arrow.pyspark.enabled", True) \
                            .config("spark.sql.repl.eagerEval.enabled", True) \
                            .config("spark.sql.shuffle.partitions", 1000) \
                            .enableHiveSupport() \
                            .getOrCreate()
