 Netflix Data Transformation with PySpark

This project demonstrates how to use PySpark to clean, transform, and analyze Netflix title data. The script loads a raw dataset, processes it using common ETL steps, and outputs cleaned and aggregated results.

Project Structure

netflix-data-transform/
 netflix_titles.csv # Original raw dataset
 transform_netflix_pyspark.py # PySpark transformation script
 netflix_cleaned_output/ # Output folder with cleaned Netflix data
 movies_per_year_output/ # Output folder with yearly movie counts
 sample_output/ # Sample output CSVs for quick preview
 netflix_cleaned_sample.csv
 movies_per_year_sample.csv
 README.md # Project documentation (this file)


 Transformations Performed

The following steps are performed using PySpark:

1. Load the CSV file using `spark.read` with headers.
2. Drop rows with null values in key columns: `type`, `title`, and `date_added`.
3. Parse `date_added` column from string to `DateType`.
4. Extract `year_added` from `date_added`.
5. Filter the dataset to include only entries of type `"Movie"`.
6. Group by `year_added` to count movies released each year.
7. Write both cleaned data and aggregated data as CSV files.

 Requirements

Make sure you have the following installed:

- **Java 17+**
- **Python 3.7+**
- **PySpark**

To install PySpark:

```bash
pip install pyspark
 How to Run
Open terminal or VS Code integrated terminal.

 Sample Output Preview
Cleaned Netflix Data (Partial)
show_id	type	title	date_added	year_added
s1	Movie	Jaws	2019-01-01	2019
s2	Movie	The Irishman	2019-11-27	2019

Movies Per Year (Aggregated)
year_added	movie_count
2016	300
2017	420
2018	512

Output Files

netflix_cleaned_output/

movies_per_year_output/
