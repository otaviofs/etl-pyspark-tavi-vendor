# ETL in PySpark

This project involves constructing an ETL (Extract, Transform, Load) process using PySpark to analyze taxi trip data from New York City. The ETL process generates specific outputs based on the following requirements:

- **Identify the vendor with the most taxi trips each year.**
  - Tiebreaker: the vendor with the longest total trip distance.
  - Secondary tiebreaker: alphabetical order of vendor names.
- **Determine the week with the most taxi trips each year.**
- **Count the number of trips made by the top vendor (with the most trips in a year) during the busiest week of that year.**

## Expected Outcomes

- Demonstrate understanding of programming language fundamentals and data structures.
- Follow best programming practices, including environment setup, documentation, Object-Oriented Programming, Design Patterns, and S.O.L.I.D principles.
- Deliver clean and simple code that is easy to understand.

## Delivery

The deliverable for this exercise is a .zip package containing the code and the ETL execution output in CSV format. Ensure the submission includes step-by-step instructions for running the solution. The chosen framework/technology for developing the pipeline will be considered during evaluation (preferably PySpark), and we expect adherence to good software engineering practices (code structuring, environment setup, testing, etc.).

## Bonus

Include comments in the code explaining the thought process behind each code block.

## Project Structure

- **Data Inputs:**
  - `input/*.json`: Taxi trip data in JSON format.
  - `input/data-vendor_lookup.csv`: Vendor lookup data in CSV format.
  - `input/data-payment_lookup.csv`: Payment lookup data in CSV format.
- **Data Outputs:**
  - `output/top_vendor/result_top_vendor.csv`: Top vendors per year.
  - `output/top_week/result_top_week.csv`: Busiest weeks per year.
  - `output/top_vendor_week/result_top_vendor_week.csv`: Top vendor trips during the busiest week of each year.

## Setup Instructions

1. **Environment Variables:**
   - Create a `.env` file in the project root directory with the following content:
     ```env
     HADOOP_HOME=your_hadoop_home_path
     JAVA_HOME=your_java_home_path
     INPUT_PATH_TRIP=path/*.json
     INPUT_PATH_VENDOR_LP=path/input/data-vendor_lookup.csv
     INPUT_PATH_PAYMENT_LP=path/input/data-payment_lookup.csv
     OUTPUT_DIR=path/output
     ```
     Replace the placeholders with your actual paths.

2. **Install Dependencies:**
   - Ensure you have PySpark and python-dotenv installed. You can install them using pip:
     ```sh
     pip install pyspark python-dotenv
     ```

3. **Run the ETL Process:**
   - Execute the `etl.py` script to start the ETL process:
     ```sh
     python etl.py
     ```
