from pyspark.sql import SparkSession

SPARK = SparkSession.builder.master('local') \
                            .appName("Test Class") \
                            .config('spark.submit.deployMode', 'client') \
                            .config('spark.driver.memory','1g') \
                            .config('spark.executor.memory','1g') \
                            .config('spark.executor.cores', 1) \
                            .config('spark.sql.debug.maxToStringFields', 1000) \
                            .config("spark.sql.execution.arrow.pyspark.enabled", True) \
                            .config("spark.sql.repl.eagerEval.enabled", True) \
                            .enableHiveSupport() \
                            .getOrCreate()

