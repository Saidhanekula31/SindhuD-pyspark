 Netflix Data Transformation with PySpark

This project demonstrates how to use PySpark to clean, transform, and analyze Netflix title data. The script loads a raw dataset, processes it using common ETL steps, and outputs cleaned and aggregated results.

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



Output Files

netflix_cleaned_output/

movies_per_year_output/
