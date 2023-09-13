from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

data = [("Продукт1", ["Категория1", "Категория2"]),
        ("Продукт2", ["Категория2"]),
        ("Продукт3", ["Категория1", "Категория3"]),
        ("Продукт4", [])]

schema = ["Имя продукта", "Категории"]

df = spark.createDataFrame(data, schema)

df_expanded = df.select("Имя продукта", explode("Категории").alias("Имя категории"))

df_no_category = df.filter(col("Категории").isNull()).select("Имя продукта").withColumn("Имя категории", col("Имя продукта"))

result_df = df_expanded.union(df_no_category)

result_df.show()