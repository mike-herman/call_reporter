import os
import logging

from datetime import date
from math import ceil
from itertools import product, zip_longest
import codecs
import re

import boto3
import duckdb

logger = logging.getLogger(__name__)
logging.basicConfig(filename='../.logs/data_loader.log', encoding='utf-8', level=logging.INFO)

# TODO: Move this info to a yaml config. Make it generalizable to more tables.
known_fields = [
    'cu_number','cycle_date','join_number','rssd',
    'cu_type','cu_name','city','state','charterstate',
    'state_code','zip_code','county_code','cong_dist','smsa',
    'attention_of','street','region','se','district',
    'year_opened','tom_code','limited_inc','issue_date','peer_group',
    'quarter_flag','ismdi','insured_date'
]


def get_col_list(year: int, month: int, file_name: str) -> list[str]:
    """ get_col_list(year: int, month: int, file_name: str) -> list[str]
    Get the column list from the first line of the file in S3.
    """
    s3_object_key = f"ncua/{year:0>4}/{month:0>2}/{file_name}.txt"
    s3 = boto3.resource("s3")
    s3_object = s3.Object('call-reporter', s3_object_key)
    line_stream = codecs.getreader("utf-8")
    lines_read = 0
    col_list = []
    for line in line_stream(s3_object.get()['Body']):
        while lines_read < 1:
            read_cols = line.split(',')
            read_cols = [re.sub(r'\W','', col.lower()) for col in read_cols]
            col_list.extend(read_cols)
            lines_read += 1
    return col_list

def load_table_to_db(file_name: str, year: int, month: int, duck_db_file: str = 'call_reporter_db.duckdb'):
    """ load_table_to_db(file_name: str, year: int, month: int, duck_db_file: str = 'call_reporter_db.duckdb')
    Load a file from S3 into a DuckDB database.
    """
    # TODO: Move this to a config yaml file.
    acceptable_filenames = ['foicu']
    
    if file_name not in acceptable_filenames:
        raise Exception(f"file_name must be one of {acceptable_filenames}")

    s3_key_url = f"s3://call-reporter/ncua/{year:0>4}/{month:0>2}/{file_name}.txt"
    logger.info(f"Starting extraction from {s3_key_url}")
    # Some year data doesn't have all the columns. If we try to select columns that don't exist, we'll get an error.
    # The code chunk below compensates for this by creating a SELECT statement that uses the data column
    # when available and `null` otherwise. For `source_year`,`source_month`, and `source` which are meta-data,
    # the appropriate value is used.
    cols_in_file = get_col_list(year,month,file_name)
    col_query_mapper = dict(zip_longest(known_fields,cols_in_file,fillvalue='null'))
    col_query_mapper['source_year'] = year
    col_query_mapper['source_month'] = month
    col_query_mapper['source'] = "'"+s3_key_url+"'"
    # Note: oder matters here. This only works because in python 3.7+ regular dicts are ordered by default.
    # If we use an earlier version of python, the mapper would have to be a collections.OrderedDict.
    logger.info("Column query mapper created.")
    select_query_statement = ",\n".join([f"{v} as {k}" for k, v in col_query_mapper.items()])
    logger.debug(f"The select query statement is {select_query_statement}")

    qry = f"""
    insert or replace into ncua.{file_name}
        select
            {select_query_statement},
            concat_ws('-',    -- join strings with '-' between them
                lpad(cu_number::text,6,'0'),    -- 6-digit 0-padded cu_number string.
                source_year::text,    -- 4-digit year string.
                lpad(source_month::text,2,'0'))    -- 2-digit 0-padded month string.
                as cu_number_mon_year_key
        from read_csv('{s3_key_url}', header=true)
    """
    duck = duckdb.connect(duck_db_file)
    try:
        duck.execute(qry)
        logger.info("DuckDB insert successful.")
        return True
    except:
        logger.info("DuckDB insert failed.")
        logger.debug(f"Failed connection: {duck}. Failed query: {qry}")
        raise Exception("The duckdb insert query didn't work.")
    finally:
        duck.close()


def make_year_month_range(start_date: date, end_date: date) -> list[tuple[int,int]]:
    """ make_year_month_range(start_date: date, end_date: date) -> list[tuple[int,int]]
    Create a list of tuples of year and quarter-end month that are within the range of the start and end dates.
    The start_date and end_date are inclusive. So a start_date of 2020-01-01 includes (2020,03) in the list.
    An end_date of 2023-10-01 includes (2023,12) in the list.
    """
    start_year = start_date.year
    start_month = ceil(start_date.month/3)*3
    end_year = end_date.year
    end_month = ceil(end_date.month/3)*3
    ym_combos = product(range(start_year,end_year+1),[3,6,9,12])
    return [(y,m) for y, m in ym_combos if \
        date(y,m,1) >= date(start_year,start_month,1) and \
        date(y,m,1) <= date(end_year,end_month,1)]

for y, m in make_year_month_range(date(2020,1,1),date(2024,3,1)):
    logger.info(f"Starting upload for ({y},{m})")
    load_table_to_db('foicu',y,m)
