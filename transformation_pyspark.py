from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, count, trim

spark = SparkSession.builder \
    .appName("Netflix Data Transformation") \
    .getOrCreate()

df = spark.read.option("header", "true") \
    .csv(r"C:\Users\sindhu\Downloads\netflix_titles.csv", inferSchema=True)


df_cleaned = df.dropna(subset=["type", "title", "date_added"])

df_cleaned = df_cleaned.withColumn("date_added", to_date(trim(col("date_added")), "MMMM d, yyyy"))
df_cleaned = df_cleaned.withColumn("year_added", year("date_added"))

movies_only = df_cleaned.filter(col("type") == "Movie")

movies_per_year = movies_only.groupBy("year_added").agg(count("*").alias("movie_count"))

movies_per_year = movies_per_year.orderBy("year_added")

df_cleaned.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("file:///C:/Users/sindhu/Downloads/netflix_cleaned_output")

movies_per_year.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("file:///C:/Users/sindhu/Downloads/movies_per_year_output")

print("Cleaned Netflix Data:")
df_cleaned.show(5)

print("Movies per Year:")
movies_per_year.show()

spark.stop()

