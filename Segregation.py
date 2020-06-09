from pyspark.sql import SparkSession
from pyspark.sql import Row, functions as F
from pyspark.sql.functions import count
from pyspark.sql.functions import col, row_number
from pyspark.sql import Window

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

readCsvDF = spark.sparkContext.parallelize([
    (1, 'qwe', 'N'),
    (2, 'wer', 'X'),
    (4, 'tyu', 'N'),
    (3, 'ert', 'X'),
    (1, 'qwe', 'N'),
    (4, 'tyu', 'N'),
    (4, 'tyu', 'N'),
    (1, 'qwe', 'N'),
    (1, 'qwe', 'N'),
    (4, 'tyu', 'N'),
    (5, 'asd', 'N'),
    (6, 'sdf', 'N'),
    (4, 'tyu', 'N'),
    (1, 'qwe', 'N'),
    (7, 'dfg', 'N'),
    (5, 'asd', 'N'),
    (6, 'sdf', 'N'),
    (4, 'tyu', 'N'),
    (4, 'tyu', 'N'),
    (4, 'tyu', 'N'),
    (4, 'tyu', 'N'),
    (4, 'tyu', 'N')]).toDF(['CustomerId', 'CustomerName', 'DeliveryCode'])
readCsvDF.show()

delCodeDF = readCsvDF.orderBy("DeliveryCode", "CustomerId").filter(readCsvDF["DeliveryCode"].isin(["N"]))
delCodeDF.show()

disCustCountDF = delCodeDF.withColumn("Count", F.count("*").over(Window.partitionBy("CustomerId")))
disCustDF = disCustCountDF.select(disCustCountDF["CustomerId"]).distinct().rdd.map(lambda r: r[0]).collect()
for x in disCustDF:
    custDF = disCustCountDF.filter(disCustCountDF["CustomerId"].isin([x]))
    countDF = custDF.count()
    if countDF > 8 or countDF < 3:
        bundleDF = custDF.drop("Count").show()
    else:
        k = 2
        windowSpec = Window.partitionBy("CustomerId").orderBy("CustomerId")
        bundleDF = custDF.withColumn("row", F.row_number().over(windowSpec))
        bundleCountDF = bundleDF.count()
        lowLimit = 0
        highLimit = lowLimit + k
        while lowLimit < bundleCountDF:
            bundleDF.where(bundleDF["row"] <= highLimit).where(bundleDF["row"] > lowLimit).drop("Count", "row").show()
            lowLimit = lowLimit + k
            highLimit = highLimit + k
