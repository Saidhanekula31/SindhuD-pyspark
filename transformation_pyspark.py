from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, count, trim

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("Netflix Data Transformation") \
    .getOrCreate()

# 2. Load CSV dataset
df = spark.read.option("header", "true") \
    .csv(r"C:\Users\sindhu\Downloads\netflix_titles.csv", inferSchema=True)

# 3. Drop rows with null values in important columns
df_cleaned = df.dropna(subset=["type", "title", "date_added"])

# 4. Convert 'date_added' to DateType and extract year
df_cleaned = df_cleaned.withColumn("date_added", to_date(trim(col("date_added")), "MMMM d, yyyy"))
df_cleaned = df_cleaned.withColumn("year_added", year("date_added"))

# 5. Filter only Movies
movies_only = df_cleaned.filter(col("type") == "Movie")

# 6. Count of movies added each year
movies_per_year = movies_only.groupBy("year_added").agg(count("*").alias("movie_count"))

# 7. Sort by year
movies_per_year = movies_per_year.orderBy("year_added")

# 8. Save cleaned data to output folders (avoid winutils issue by using full Windows path)
df_cleaned.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("file:///C:/Users/sindhu/Downloads/netflix_cleaned_output")

movies_per_year.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv("file:///C:/Users/sindhu/Downloads/movies_per_year_output")

# 9. Show sample outputs
print("Cleaned Netflix Data:")
df_cleaned.show(5)

print("Movies per Year:")
movies_per_year.show()

# 10. Stop Spark session
spark.stop()

