import datetime
import logging
import csv
from io import StringIO
from urllib.request import urlopen
import mysql.connector

from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class WriteToMySQL(beam.DoFn):
    def __init__(self, host, database, user, password, batch_size=100):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.insert_buffer = []
        self.update_buffer = []
        self.conn = None
        self.cursor = None

    def start_bundle(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as e:
            logging.error(f"Error connecting to MySQL: {e}")
            raise

    def process(self, element):
        try:
            self.cursor.execute("SELECT * FROM customer WHERE CustomerID = %s", (element['CustomerID'],))
            existing_record = self.cursor.fetchone()

            if existing_record:
                logging.info(f"CustomerID {element['CustomerID']} already exists. Updating record...")
                self.update_buffer.append((element['Gender'], element['Age'], element['Annual_Income'], element['Spending_Score'], element['CustomerID']))
            else:
                logging.info(f"Inserting new record for CustomerID {element['CustomerID']}...")
                self.insert_buffer.append((element['CustomerID'], element['Gender'], element['Age'], element['Annual_Income'], element['Spending_Score']))

            if len(self.insert_buffer) >= self.batch_size:
                self.write_batch_inserts()

            if len(self.update_buffer) >= self.batch_size:
                self.write_batch_updates()

        except mysql.connector.Error as e:
            logging.error(f"Error processing element {element}: {e}")

    def finish_bundle(self):
        try:
            if self.insert_buffer:
                self.write_batch_inserts()

            if self.update_buffer:
                self.write_batch_updates()
        except mysql.connector.Error as e:
            logging.error(f"Error finishing bundle: {e}")

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

    def write_batch_inserts(self):
        try:
            sql = "INSERT INTO customer (CustomerID, Gender, Age, Annual_Income, Spending_Score) VALUES (%s, %s, %s, %s, %s)"
            self.cursor.executemany(sql, self.insert_buffer)
            self.conn.commit()
            self.insert_buffer.clear()
        except mysql.connector.Error as e:
            logging.error(f"Error writing batch inserts: {e}")
            self.conn.rollback()  # Rollback on error

    def write_batch_updates(self):
        try:
            update_sql = """
            UPDATE customer
            SET Gender = %s, Age = %s, Annual_Income = %s, Spending_Score = %s
            WHERE CustomerID = %s
            """
            self.cursor.executemany(update_sql, self.update_buffer)
            self.conn.commit()
            self.update_buffer.clear()
        except mysql.connector.Error as e:
            logging.error(f"Error writing batch updates: {e}")
            self.conn.rollback()  # Rollback on error


class ParseCSV(beam.DoFn):
    def process(self, element):
        expected_columns = 5
        reader = csv.reader(StringIO(element))
        next(reader)  # Skip header row
        for row in reader:
            row_with_none = [None] * expected_columns
            for i in range(min(len(row), expected_columns)):
                if row[i]:  # Only update if value is not empty
                    row_with_none[i] = row[i]

            try:
                # Validate and process each column
                customer_id = self.validate_customer_id(row_with_none[0])
                if not customer_id:
                    logging.warning(f"Missing or invalid CustomerID in row {row}")
                    yield beam.pvalue.TaggedOutput('malformed', row)
                    continue

                gender = self.validate_gender(row_with_none[1])
                age = self.validate_age(row_with_none[2])
                annual_income = self.validate_annual_income(row_with_none[3])
                spending_score = self.validate_spending_score(row_with_none[4])

                yield {
                    'CustomerID': customer_id,
                    'Gender': gender,
                    'Age': age,
                    'Annual_Income': annual_income,
                    'Spending_Score': spending_score,
                }
            except ValueError as err:
                logging.warning(f"Value error in row {row}: {err}")
                yield beam.pvalue.TaggedOutput('malformed', element)
            except Exception as e:
                logging.warning(f"Unexpected error in row {row}: {e}")
                yield beam.pvalue.TaggedOutput('malformed', element)

    def validate_customer_id(self, customer_id):
        if customer_id:
            try:
                customer_id = int(customer_id)
                return customer_id
            except ValueError:
                logging.warning("Invalid CustomerID: Not an integer")
                return None
        else:
            logging.warning("Missing CustomerID")
            return None

    def validate_gender(self, gender):
        if gender and gender in ["Male", "Female"]:
            return gender
        else:
            logging.warning(f"Invalid gender: {gender}")
            return None

    def validate_age(self, age):
        if age:
            try:
                age = int(age)
                if age > 0:
                    return age
                else:
                    logging.warning(f"Invalid age: {age}")
                    return None
            except ValueError:
                logging.warning(f"Invalid age format: {age}")
                return None
        return None

    def validate_annual_income(self, annual_income):
        if annual_income:
            try:
                annual_income = float(annual_income)
                if annual_income > 0:
                    return annual_income
                else:
                    logging.warning(f"Invalid annual income: {annual_income}")
                    return None
            except ValueError:
                logging.warning(f"Invalid annual income format: {annual_income}")
                return None
        return None

    def validate_spending_score(self, spending_score):
        if spending_score:
            try:
                spending_score = int(spending_score)
                if 1 <= spending_score <= 100:
                    return spending_score
                else:
                    logging.warning(f"Invalid spending score: {spending_score}")
                    return None
            except ValueError:
                logging.warning(f"Invalid spending score format: {spending_score}")
                return None
        return None


def fetch_csv(url):
    try:
        response = urlopen(url)
        if response.status != 200:
            logging.error(f"Failed to fetch {url}: {response.status}")
            return None
        return response.read().decode('utf-8')
    except Exception as err:
        logging.error(f"Error fetching URL {url}: {err}")
        return None


def run_pipeline():
    input_file_url = 'https://media.githubusercontent.com/media/Wells-Fargo-Bootcamp/Customer_segmentation/main/Mall_Customers.csv'
    pipeline_options = PipelineOptions(runner='DirectRunner', streaming=False)
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        data = (
            pipeline
            | 'Read CSV from GitHub' >> beam.Create([input_file_url])
            | 'Fetch CSV Data' >> beam.Map(fetch_csv)
        )
        
        parsed_data = data | 'Parse CSV Data' >> beam.ParDo(ParseCSV()).with_outputs('malformed', main='valid')

        parsed_data.valid | 'Write to MySQL' >> beam.ParDo(WriteToMySQL(
            host='34.30.157.44',
            database='customer_segmentation',
            user='root',
            password='T%%ZI<#e7qh2=8HH',
            batch_size=100
        ))

        parsed_data.malformed | 'Log Malformed Rows' >> beam.Map(lambda row: logging.warning(f"Malformed row logged: {row}"))


YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    "owner": "Composer Example",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
}

with models.DAG(
    "composer_quickstart",
    catchup=False,
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    print_dag_run_conf = BashOperator(
        task_id="print_dag_run_conf", bash_command="echo {{ dag_run.id }}"
    )
    task1 = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_pipeline
    )

    task2 = BashOperator(
        task_id='echo_hello',
        bash_command='echo "Hello, Airflow!"',
    )

    print_dag_run_conf >> task1 >> task2
