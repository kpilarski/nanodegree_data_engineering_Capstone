#from path_and_credentials import sas_label_descriptions_path, immigration_path, temperature_path, demographics_path, airports_path
#import pandas as pd
import re


def gather_immigration_data(spark, filepath, pandas_dataframe=False, print_data=False):
    """
    Function responsible for gathering raw data.
    Function simply read immigration data from given path and returning spark data frame.
    1) Function could be also used for printing information about dataset -
    parameter print_data is used for it (default to False)
    2) Function could be also used for preparing pandas data frame -
    parameter pandas_dataframe is used for it (default to False)
    """   
    
    data_information = """Report contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries). Data sources include: Overseas DHS/CBP I-94 Program data; Canadian visitation data (Stats Canada) and Mexican visitation data (Banco de Mexico).
    """   
     
    if pandas_dataframe:
        return pd.read_sas(filepath, 'sas7bdat', encoding="ISO-8859-1")
    elif print_data:
        return print(data_information)
    return spark.read.format('com.github.saurfang.sas.spark').load(filepath)


def gather_demographics_data(spark, filepath, delimiter=';', pandas_dataframe=False, print_data=False):
    """
    Function responsible for gathering raw data.
    Function simply read demographic data from given path and returning spark data frame.
    1) Function could be also used for printing information about dataset -
    parameter print_data is used for it (default to False)
    2) Function could be also used for preparing pandas data frame -
    parameter pandas_dataframe is used for it (default to False)
    """   
    
    data_information = """This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This data comes from the US Census Bureau's 2015 American Community Survey.
    """
    
    if pandas_dataframe:
        return pd.read_csv(filepath, delimiter=';')
    elif print_data:
        return print(data_information)
    return spark.read.format("csv").option("header", "true").option("delimiter", ';').load(filepath)


def gather_temperature_data(spark, filepath, delimiter=',', pandas_dataframe=False, print_data=False):
    """
    Function responsible for gathering raw data.
    Function simply read temperature data from given path and returning spark data frame.
    1) Function could be also used for printing information about dataset -
    parameter print_data is used for it (default to False)
    2) Function could be also used for preparing pandas data frame -
    parameter pandas_dataframe is used for it (default to False)
    """   
    
    data_information = """This dataset came from Kaggle and contains information about temperature. More info in: https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data
    """
    
    if pandas_dataframe:
        return pd.read_csv(filepath, delimiter=delimiter)
    elif print_data:
        return print(data_information)
    return spark.read.format("csv").option("header", "true").option("delimiter", delimiter).load(filepath)


def gather_airport_data(spark, filepath, delimiter=',', pandas_dataframe=False, print_data=False):
    """
    Function responsible for gathering raw data.
    Function simply read airport data from given path and returning spark data frame.
    1) Function could be also used for printing information about dataset -
    parameter print_data is used for it (default to False)
    2) Function could be also used for preparing pandas data frame -
    parameter pandas_dataframe is used for it (default to False)
    """   
    
    data_information = """This is a simple table of airport codes and corresponding cities.
    """ 
    
    if pandas_dataframe:
        return pd.read_csv(filepath, delimiter=delimiter)
    elif print_data:
        return print(data_information)
    return spark.read.format("csv").option("header", "true").option("delimiter", ',').load(filepath)


def gather_label_descriptions(filepath):
    """
    Function responsible for gathering labels description.
    Function simply read SAS file with descriptions and print them.
    """   
    with open(filepath) as f:
        lines = f.readlines() 
        
    comments = [line for line in lines if '/*' in line and '*/\n' in line]
    regexp = re.compile(r'^/\*\s+(?P<code>.+?)\s+-\s+(?P<description>.+)\s+\*/$')
    matches = [regexp.match(comment) for comment in comments]
    description_list = ''
    
    for m in matches:
        description_list = f"{description_list}{m.group('code')} : {m.group('description')}\n"
        
    print(description_list)


