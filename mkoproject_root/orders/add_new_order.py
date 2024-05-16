import pymysql
from pyspark.sql.functions import udf, col, lit, sum as spark_sum, greatest, least
from pyspark.sql.types import IntegerType
from datetime import datetime, timedelta
from dateutil.easter import easter
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType


spark = SparkSession.builder.appName("Production Planning").getOrCreate()


def calculate_working_days(start_date, end_date):
    current_year = datetime.now().year
    holidays = [
        datetime(current_year, 1, 1),
        datetime(current_year, 1, 6),
        datetime(current_year, 5, 1),
        datetime(current_year, 5, 3),
        datetime(current_year, 11, 1),
        datetime(current_year, 11, 11),
        datetime(current_year, 12, 25),
        datetime(current_year, 12, 26),
        easter(current_year) + timedelta(days=1),  # Poniedziałek Wielkanocny
        easter(current_year) + timedelta(days=60)  # Boże Ciało
    ]

    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            count += 1
        current += timedelta(days=1)

    return count

calculate_working_days_udf = udf(calculate_working_days, IntegerType())
spark.udf.register("calculate_working_days", calculate_working_days_udf)


def calculate_load(orders_df, production_line, year, week_number):
    week_start_date = datetime.strptime(f'{year}-W{week_number-1}-1', "%Y-W%W-%w").strftime('%Y-%m-%d')
    week_end_date = (datetime.strptime(week_start_date, '%Y-%m-%d') + timedelta(days=6)).strftime('%Y-%m-%d')

    # Filtrowanie zleceń, które zazębiają się z danym tygodniem
    orders_df = orders_df.filter(
        (col("ProductionLine") == production_line) &
        (col("StartDate") <= lit(week_end_date)) &
        (col("EndDate") >= lit(week_start_date))
    )

    # Definicja funkcji do obliczania tygodniowych godzin pracy
    def calculate_weekly_hours(start_date, end_date, man_hours_offer, week_start_date, week_end_date):
        total_days = calculate_working_days(start_date, end_date)
        daily_hours = man_hours_offer / total_days if total_days > 0 else 0
        working_days_in_week = calculate_working_days(
            max(start_date, week_start_date),
            min(end_date, week_end_date)
        )
        return daily_hours * working_days_in_week

    # Rejestracja funkcji jako UDF
    calculate_weekly_hours_udf = udf(calculate_weekly_hours, FloatType())

    # Dodanie kolumny z tygodniowymi godzinami pracy
    weekly_load = orders_df.withColumn(
        "weekly_hours",
        calculate_weekly_hours_udf(
            col("StartDate").cast("string"),
            col("EndDate").cast("string"),
            col("ManHoursOffer"),
            lit(week_start_date).cast("string"),
            lit(week_end_date).cast("string")
        )
    ).agg(
        spark_sum("weekly_hours").alias("total_hours")
    )

    return weekly_load.collect()[0]["total_hours"] if weekly_load.count() > 0 else 0

def import_db_hadoop(db):
    connection = pymysql.connect(host='iu51mf0q32fkhfpl.cbetxkdyhwsb.us-east-1.rds.amazonaws.com', user='gagjxd2qh66bywiq', password='t0u2mazwyo55e827', db='fgvomip29s41qom2')
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {db}")
    rows = cursor.fetchall()
    schema = StructType([
        StructField("OrderID", IntegerType(), True),
        StructField("OrderName", StringType(), True),
        StructField("Offer", StringType(), True),
        StructField("Client", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("ManHoursOffer", FloatType(), True),
        StructField("ProductionLine", IntegerType(), True),
        StructField("StartDate", DateType(), True),
        StructField("EndDate", DateType(), True),
        StructField("ManHoursAfterProduction", FloatType(), True),
        StructField("ProjectManager", IntegerType(), True),
        StructField("Status", StringType(), True),
        StructField("Comment", StringType(), True),
    ])

    # Uruchomienie sesji Spark
    spark = SparkSession.builder.appName("MariaDB to Hadoop").getOrCreate()
    df = spark.createDataFrame(rows, schema)
    df.write.mode('overwrite').format("parquet").save("hdfs://localhost:9000/user/hadoop/orders")
    cursor.close()
    connection.close()


def load_data_from_hdfs(table):
    spark = SparkSession.builder.appName("Load HDFS Data").getOrCreate()
    orders_df = spark.read.format("parquet").load(f"hdfs://localhost:9000/user/hadoop/{table}")
    return orders_df


def main():
    # sd = '2024-01-01'
    # ed = '2024-01-15'
    # work_days_a = work_days(sd,ed)
    import_db_hadoop('orders')
    orders_df = load_data_from_hdfs('orders')
    line_id = 1
    year = 2024
    week_number  = 10
    planned_hours = calculate_load(orders_df, line_id, year, week_number)
    print(planned_hours)


if __name__ == '__main__':
    main()

