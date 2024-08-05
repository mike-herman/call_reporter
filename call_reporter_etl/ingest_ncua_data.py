# Script to ingest NCUA data files and load to S3 bucket.
from dataclasses import dataclass, field


import tempfile

import zipfile
import os
from itertools import product
from datetime import date
from math import ceil

import argparse
import logging

import requests
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
os.makedirs('.logs', exist_ok = True)
# logging.basicConfig(filename='.logs/injest_ncua_data.log', encoding='utf-8', level=logging.INFO)
logging.basicConfig(filename='.logs/injest_ncua_data.log', encoding='utf-8', level=logging.INFO)

_DEFAULT_FILE_NAMES = [
        'AcctDesc.txt','Acct-DescCUSO.txt','Acct-DescGrants.txt','Acct-DescTradeNames.txt','ATM.txt',
        'Credit.txt','FOICU.txt','FOICUDES.txt','FS220.txt','FS220A.txt',
        'FS220B.txt','FS220C.txt','FS220CUSO.txt','FS220D.txt','FS220E.txt',
        'FS220G.txt','FS220H.txt','FS220I.txt','FS220J.txt','FS220K.txt',
        'FS220L.txt','FS220M.txt','FS220N.txt','FS220P.txt','FS220Q.txt',
        'FS220R.txt','FS220S.txt','Grants.txt','TradeNames.txt', 'fs220.txt',
        'Acct_Des.txt'
    ]

# URL Links by date range:
# https://ncua.gov/files/publications/analysis/call-report-data-{yyyy}-{mm}.zip for January 2016 onward.
# https://ncua.gov/files/publications/analysis/Call-Report-Data-{yyyy}-{mm}.zip for June 2015 thtrough December 2015.
# https://ncua.gov/files/publications/data-apps/QCR{yyyymm}.zip March 2015 and earlier.



# CLI Parser
# parser = argparse.ArgumentParser(
#     prog="ingest_ncua_data",
#     description="Ingests NCUA call report data."
# )
# parser.add_argument('-s','--start_yyyy_mm', type=str, nargs=1, required=True, help='The year-month you want data for. Must be format "yyyy-mm". Months accepted: [03,06,09,12].')
# parser.add_argument('-e','--end_yyyy_mm', type=str, nargs=1, required=False, help='(Optional) The last year-month you want data for. Will also pull data for all munths in between. Must be format "yyyy-mm". Months accepted: [03,06,09,12].')
# args = parser.parse_args()



    


@dataclass
class NCUA_Ingester:
    '''Class to ingest NCUA data files into S3 bucket.'''

    DOWNLOAD_URL_TEMPLATE_PRE_JUN_2015: str = 'https://ncua.gov/files/publications/data-apps/QCR{}{}.zip'
    DOWNLOAD_URL_TEMPLATE_JUN_2015_DEC_2015: str = 'https://ncua.gov/files/publications/analysis/Call-Report-Data-{}-{}.zip'
    DOWNLOAD_URL_TEMPLATE: str = 'https://ncua.gov/files/publications/analysis/call-report-data-{}-{}.zip'
    S3_BUCKET_NAME: str = 'call-reporter'
    data_file_names: list[str] = field(default_factory=lambda: _DEFAULT_FILE_NAMES)

    def __post_init__(self):
        logger.info(f"Created NCUA_Ingester instance. S3_BUCKET_NAME={self.S3_BUCKET_NAME}.")
        logger.debug(f"Default file names are {self.data_file_names}")

    
    def _find_containing_dir(self, check_file_path: str):
        dir_contents = os.listdir(check_file_path)
        target_file_list = self.data_file_names
        dir_contains_target_files = any([f.lower() in [tf.lower() for tf in target_file_list] for f in dir_contents])

        if dir_contains_target_files:
            logger.debug(f"{check_file_path} contains a target file? {dir_contains_target_files}. Returning.")
            return check_file_path
        elif not dir_contains_target_files:
            logger.debug(f"Target files not found in {check_file_path}.")
            sub_dirs = [os.path.join(check_file_path,sub_dir) for sub_dir in dir_contents if os.path.isdir(os.path.join(check_file_path,sub_dir))]
            if len(sub_dirs) > 0:
                for sub_dir in sub_dirs:
                    return self._find_containing_dir(sub_dir)
            else:
                logger.debug(f"No further directories to check.")
                return False
        else:
            raise Exception("Unexpected case encountered in recursive if statement.")
        

    def ingest_quarter_data(self, data_year: int, data_month: int, max_download_attempts=3):
        # Get the right URL template.
        if data_year+data_month/100 < 2015+6/100:
            url_template = self.DOWNLOAD_URL_TEMPLATE_PRE_JUN_2015
        elif data_year+data_month/100 < 2016:
            url_template = self.DOWNLOAD_URL_TEMPLATE_JUN_2015_DEC_2015
        else:
            url_template = self.DOWNLOAD_URL_TEMPLATE
        data_year = f'{data_year:0>4}'
        data_month = f'{data_month:0>2}'
        download_url = url_template.format(data_year,data_month)
        
        # Connect and download.
        logger.info(f'Attempting download from {download_url}')
        response = requests.get(download_url)
        if response.status_code != 200:
            logger.info(f'Failed download from {download_url}. Status code: {response.status_code}.')
            return
        else:
            logger.info('Download successful.')

        with tempfile.TemporaryDirectory() as temp_dir:
            logger.debug(f'temp_dir created: {temp_dir}')
            zip_path = os.path.join(temp_dir, f'call-report-data-{data_year}-{data_month}.zip')
            
            with open(zip_path, 'wb') as file:
                file.write(response.content)
            logger.debug(f'Zip file saved to {zip_path}')
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            logger.debug('Zip files extracted.')
            logger.debug(f'Here are all the files in the directory: {os.listdir(temp_dir)}')
        
            s3 = boto3.client('s3')
            logger.info(f'Starting file upload to S3.')
            
            # All the contents are unzipped into temp_dir. If the contents are just the files, we're good.
            # Sometimes the contents are a directory containing the files.
            # Here's a helper method that recursively searches `temp_dir` for a folder containing the files we want.
            dir_with_files = self._find_containing_dir(temp_dir)
            if not dir_with_files:
                logger.info("Unzipped file did not contain valid files")
                return
            else:
                logger.debug(f"Directory with files is {dir_with_files}")
                pass
            
            for file in os.listdir(dir_with_files):
                logger.debug(f"Should we upload {file}?")
                if file.lower() in [s.lower() for s in self.data_file_names]:
                    logger.debug(f"Found {file} in {self.data_file_names}")
                    object_name = '/'.join(['ncua',data_year,data_month,file.lower()])
                    logger.debug(f'Uploading {os.path.join(dir_with_files,file.lower())} to S3 as {object_name}.')
                    try:
                        s3_response = s3.upload_file(os.path.join(dir_with_files,file), self.S3_BUCKET_NAME, object_name)
                        logger.info(f'Successfully uploaded {object_name} to S3 bucket {self.S3_BUCKET_NAME}.')
                    except ClientError as e:
                        logger.error(e)
                        return False
                else:
                    logger.debug("{} not found in {}".format(file.lower(), [s.lower() for s in self.data_file_names]))
                    pass
            
        
        logger.info(f'All files uploaded to S3 for year={data_year}, month={data_month}.')
        return True


# Ingest each quarter starting from March 1994 through March 2024

def ingest_ncua_in_range(start_date_iso: str, end_date_iso: str) -> None:

    start_date = date.fromisoformat(start_date_iso)
    start_year = start_date.year
    start_quarter_month = ceil(start_date.month/3)*3

    end_date = date.fromisoformat(end_date_iso)
    end_year = end_date.year
    end_quarter_month = ceil(end_date.month/3)*3
    # Get a set of all years in the range and of all quarter-end months, properly string fromatted.
    year_set = list(range(start_year,end_year+1))
    month_set = [3,6,9,12]
    # Generate a list of all year-quarter combinations that appear in the given range.
    year_month_list = [year_month for year_month in product(year_set,month_set) \
        if year_month[0] + year_month[1]/100 >= start_year+start_quarter_month/100 \
        and year_month[0]+year_month[1]/100 <= end_year+end_quarter_month/100]
    # Ingest data for each (year,month) combo.
    ncua_ingester = NCUA_Ingester()
    for ym in year_month_list:
        logger.info(f"Starting ingestion for {ym}")
        ncua_ingester.ingest_quarter_data(*ym)

# Earliest available date is March 1994
# As of writing, last available date is March 2024
ingest_ncua_in_range('2020-01-01','2024-03-31')