# Automating ETL Process with Python 
import pandas as pd      # this is the pandas library
import logging

# setup the logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#extract the data from pandas library
def extract_data(file_path):
    '''extract the data from csv file'''

    try:
        df = pd.read_csv(file_path)
        logger.info('Data extracted successful.')
        return df
    except Exception as e:
        logger.error(f'Error extracting data: {e}')
        raise

def transform_data(df):

    #transform the data
    try:
   
        df = df.dropna()
        logger.info("Data transformation is successful.")
        return df
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise


def load_data(df,output_path):
    #loading the data
    try:
        df.to_csv(output_path, index=False)
        logger.info('Data loading is successful')
    except Exception as e:
        logger.error(f'Error loading data: {e}')
        raise   

def etl():
    try:
       input_file_path = "sales data.csv"
       output_path_file = 'transformed_data.csv'

       data = extract_data(input_file_path) #calling the extract_data funtion

       data = transform_data(data)  #calling the transform_data function

       load_data(data,output_path_file)  #calling the load_data function  

       logger.info("ETL process completed successfully")

    except Exception as e:
        logger.info("Error ETL process failed: {e} ")


if __name__ =='main':
    etl()              
