from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
from datetime import datetime, timedelta
from dateutil.easter import easter
from pyspark.sql import SparkSession

def work_days(start_date, end_date, include_start=True):
    current_year = datetime.now().year
    holidays = [f"{current_year}-01-01", f"{current_year}-01-06", f"{current_year}-05-01", f"{current_year}-05-03", f"{current_year}-11-01", f"{current_year}-11-11", f"{current_year}-12-25", f"{current_year}-12-26" ]
    easter_date = easter(current_year)
    easter_monday_date = easter_date + timedelta(days=1)
    corpus_cristi_date = easter_date + timedelta(days=60)
    holidays.append(easter_monday_date.strftime('%Y-%m-%d'))
    holidays.append(corpus_cristi_date.strftime('%Y-%m-%d'))
    holidays = [datetime.strptime(day, "%Y-%m-%d").date() for day in holidays]

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    count = 0
    current = start_date if include_start else start_date + timedelta(days=1)
    while current <= end_date: 
        if current.weekday()<5 and current not in holidays:
            count += 1
        current += timedelta(days=1)
    return count


def main():
    sd = '2024-01-01'
    ed = '2024-01-15'
    work_days_a = work_days(sd,ed)
    print(work_days_a)


if __name__ == '__main__':
    main()

