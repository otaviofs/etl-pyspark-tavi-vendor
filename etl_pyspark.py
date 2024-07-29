from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import glob
import shutil
from pyspark.sql.functions import year, weekofyear

# Load environment variables from the .env file
load_dotenv()

# Define environment variables
HADOOP_HOME = os.getenv("HADOOP_HOME")
JAVA_HOME = os.getenv("JAVA_HOME")
INPUT_PATH_TRIP = os.getenv("INPUT_PATH_TRIP")
INPUT_PATH_VENDOR_LP = os.getenv("INPUT_PATH_VENDOR_LP")
INPUT_PATH_PAYMENT_LP = os.getenv("INPUT_PATH_PAYMENT_LP")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("JSON to CSV") \
    .config("spark.master", "local") \
    .getOrCreate()

# Function to read JSON files
def read_json(path):
    return spark.read.json(path)

# Function to read CSV files
def read_csv(path, header=True):
    return spark.read.option("header", str(header).lower()).csv(path)

# Function to write results to a CSV file
def write_csv(df, output_subdir, output_filename):
    output_path = os.path.join(OUTPUT_DIR, output_subdir)
    df.write.mode("overwrite").option("header", "True").csv(output_path)
    part_file = glob.glob(os.path.join(output_path, "part-*.csv"))[0]
    final_output_path = os.path.join(output_path, output_filename)
    shutil.move(part_file, final_output_path)

# Main function to process the data
def process_data():
    df_trip = read_json(INPUT_PATH_TRIP)
    df_vendor = read_csv(INPUT_PATH_VENDOR_LP)
    df_payment = read_csv(INPUT_PATH_PAYMENT_LP)

    df_with_year = df_trip.withColumn("year", year(df_trip["dropoff_datetime"])) \
                          .withColumn("week", weekofyear(df_trip["dropoff_datetime"]))
    
    df_with_year.createOrReplaceTempView("trip_taxi")
    df_vendor.createOrReplaceTempView("vendor")

    # Top Vendor Query
    query_top_vendor = """
        SELECT
            t.vendor_id,
            v.name AS vendor_name,
            year,
            COUNT(*) AS qty_trip,
            SUM(trip_distance) AS trip_distance,
            ROW_NUMBER() OVER(PARTITION BY year ORDER BY COUNT(*) DESC) AS top_qty_trip,
            ROW_NUMBER() OVER(PARTITION BY year ORDER BY SUM(trip_distance) DESC) AS top_distance
        FROM trip_taxi t
        LEFT JOIN vendor v ON t.vendor_id = v.vendor_id
        GROUP BY t.vendor_id, v.name, year
        ORDER BY v.name, year
    """
    result_top_vendor = spark.sql(query_top_vendor)
    write_csv(result_top_vendor, "top_vendor", "result_top_vendor.csv")

    # Top Week Query
    query_top_week = """
        SELECT year, week, qty_trip FROM (
            SELECT
                year,
                week,
                COUNT(*) AS qty_trip,
                ROW_NUMBER() OVER(PARTITION BY year ORDER BY COUNT(*) DESC) AS top_qty_trip
            FROM trip_taxi
            GROUP BY year, week
        ) WHERE top_qty_trip = 1
    """
    result_top_week = spark.sql(query_top_week)
    write_csv(result_top_week, "top_week", "result_top_week.csv")

    # Top Vendor Week Query
    df_top_week = result_top_week.alias("week")
    df_top_week.createOrReplaceTempView("week")
    query_top_vendor_week = """
        SELECT * FROM (
            SELECT
                t.vendor_id,
                v.name AS vendor_name,
                t.year,
                t.week,
                COUNT(*) AS qty_trip,
                ROW_NUMBER() OVER(PARTITION BY t.year ORDER BY COUNT(*) DESC) AS top_qty_trip
            FROM trip_taxi t
            LEFT JOIN vendor v ON t.vendor_id = v.vendor_id
            INNER JOIN week w ON t.week = w.week
            GROUP BY t.vendor_id, v.name, t.year, t.week
            ORDER BY t.vendor_id, v.name, t.year, t.week
        ) WHERE top_qty_trip = 1
    """
    result_top_vendor_week = spark.sql(query_top_vendor_week)
    write_csv(result_top_vendor_week, "top_vendor_week", "result_top_vendor_week.csv")

# Execute the processing
process_data()

# Stop the Spark session
spark.stop()
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import glob
import shutil
from pyspark.sql.functions import year, weekofyear

# Load environment variables from the .env file
load_dotenv()

# Define environment variables
HADOOP_HOME = os.getenv("HADOOP_HOME")
JAVA_HOME = os.getenv("JAVA_HOME")
INPUT_PATH_TRIP = os.getenv("INPUT_PATH_TRIP")
INPUT_PATH_VENDOR_LP = os.getenv("INPUT_PATH_VENDOR_LP")
INPUT_PATH_PAYMENT_LP = os.getenv("INPUT_PATH_PAYMENT_LP")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("JSON to CSV") \
    .config("spark.master", "local") \
    .getOrCreate()

# Function to read JSON files
def read_json(path):
    return spark.read.json(path)

# Function to read CSV files
def read_csv(path, header=True):
    return spark.read.option("header", str(header).lower()).csv(path)

# Function to write results to a CSV file
def write_csv(df, output_subdir, output_filename):
    output_path = os.path.join(OUTPUT_DIR, output_subdir)
    df.write.mode("overwrite").option("header", "True").csv(output_path)
    part_file = glob.glob(os.path.join(output_path, "part-*.csv"))[0]
    final_output_path = os.path.join(output_path, output_filename)
    shutil.move(part_file, final_output_path)

# Main function to process the data
def process_data():
    df_trip = read_json(INPUT_PATH_TRIP)
    df_vendor = read_csv(INPUT_PATH_VENDOR_LP)
    df_payment = read_csv(INPUT_PATH_PAYMENT_LP)

    df_with_year = df_trip.withColumn("year", year(df_trip["dropoff_datetime"])) \
                          .withColumn("week", weekofyear(df_trip["dropoff_datetime"]))
    
    df_with_year.createOrReplaceTempView("trip_taxi")
    df_vendor.createOrReplaceTempView("vendor")

    # Top Vendor Query
    query_top_vendor = """
        SELECT
            t.vendor_id,
            v.name AS vendor_name,
            year,
            COUNT(*) AS qty_trip,
            SUM(trip_distance) AS trip_distance,
            ROW_NUMBER() OVER(PARTITION BY year ORDER BY COUNT(*) DESC) AS top_qty_trip,
            ROW_NUMBER() OVER(PARTITION BY year ORDER BY SUM(trip_distance) DESC) AS top_distance
        FROM trip_taxi t
        LEFT JOIN vendor v ON t.vendor_id = v.vendor_id
        GROUP BY t.vendor_id, v.name, year
        ORDER BY v.name, year
    """
    result_top_vendor = spark.sql(query_top_vendor)
    write_csv(result_top_vendor, "top_vendor", "result_top_vendor.csv")

    # Top Week Query
    query_top_week = """
        SELECT year, week, qty_trip FROM (
            SELECT
                year,
                week,
                COUNT(*) AS qty_trip,
                ROW_NUMBER() OVER(PARTITION BY year ORDER BY COUNT(*) DESC) AS top_qty_trip
            FROM trip_taxi
            GROUP BY year, week
        ) WHERE top_qty_trip = 1
    """
    result_top_week = spark.sql(query_top_week)
    write_csv(result_top_week, "top_week", "result_top_week.csv")

    # Top Vendor Week Query
    df_top_week = result_top_week.alias("week")
    df_top_week.createOrReplaceTempView("week")
    query_top_vendor_week = """
        SELECT * FROM (
            SELECT
                t.vendor_id,
                v.name AS vendor_name,
                t.year,
                t.week,
                COUNT(*) AS qty_trip,
                ROW_NUMBER() OVER(PARTITION BY t.year ORDER BY COUNT(*) DESC) AS top_qty_trip
            FROM trip_taxi t
            LEFT JOIN vendor v ON t.vendor_id = v.vendor_id
            INNER JOIN week w ON t.week = w.week
            GROUP BY t.vendor_id, v.name, t.year, t.week
            ORDER BY t.vendor_id, v.name, t.year, t.week
        ) WHERE top_qty_trip = 1
    """
    result_top_vendor_week = spark.sql(query_top_vendor_week)
    write_csv(result_top_vendor_week, "top_vendor_week", "result_top_vendor_week.csv")

# Execute the processing
process_data()

# Stop the Spark session
spark.stop()