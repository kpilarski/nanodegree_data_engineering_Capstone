from data_paths import *
from data_gathering import *
from data_cleaning import *
from data_saving import *
from data_validation import *
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
import re
import pandas as pd


def get_raw_data(spark):
    """
    Technical function in ETL process for importing source data. 
    It runs all functions related to import raw data.
    The function returns four spark dataframes 
    """
    immigration_spark = gather_immigration_data(spark, immigration_path)
    temperature_spark = gather_temperature_data(spark, temperature_path, delimiter=',')
    demographics_spark = gather_demographics_data(spark, demographics_path, delimiter=';')
    airports_spark = gather_airport_data(spark, airports_path, delimiter=',')
    
    return immigration_spark, temperature_spark, demographics_spark, airports_spark



def prepare_and_clean_data(spark, immigration_spark, demographics_spark, airports_spark, filepath_raw):
    """
    Technical function in ETL process for preparing and cleaning data. 
    It runs all functions related to cleaning and preparing all facts and dimensions tables.
    The function returns all fact and dimension tables (and also validation lists) 
    """
    countries_dimension, valid_country_codes_list = creating_countries_dimension(sas_label_descriptions_path, spark)
    ports_dimension, valid_port_codes_list = creating_ports_dimension(sas_label_descriptions_path, spark)
    modes_dimension, valid_mode_codes_list = creating_modes_dimension(sas_label_descriptions_path, spark)
    states_dimension, valid_state_codes_list = creating_states_dimension(sas_label_descriptions_path, spark)
    visa_dimension, valid_visa_codes_list = creating_visa_dimension(sas_label_descriptions_path, spark)
    immigration_fact = clean_immigration_fact(immigration_spark, valid_country_codes_list, valid_port_codes_list, valid_mode_codes_list, valid_visa_codes_list)
    demographics_dimension = clean_demographics_dimension(demographics_spark, valid_state_codes_list)
    airports_dimension = clean_airports_dimension(airports_spark, valid_port_codes_list)
    temperature_dimension = clean_temperature_dimension(filepath_raw, ports_dimension, spark)
    
    return countries_dimension, valid_country_codes_list, ports_dimension, valid_port_codes_list, modes_dimension\
    ,valid_mode_codes_list, states_dimension, valid_state_codes_list, visa_dimension, valid_visa_codes_list\
    ,immigration_fact, demographics_dimension, airports_dimension, temperature_dimension  


def main():
    """
    Main function for running ETL process. 
    It runs all functions responsible for the entire ETL process .
    """
    
    spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()
    
    # Step 1: Scope the Project and Gather Data
    try:
        print(f"\n\n\n\n[1/4] GATHERING RAW DATA\nGathering raw data - please wait...")
        immigration_spark, temperature_spark, demographics_spark, airports_spark = get_raw_data(spark=spark)
        print(f"All raw data sucessfully retrieved - immigration, temperature, demographics and airports")
    except Exception as e:
        print(e)
        
    # Step 2: Explore and Assess the Data  
    try:
        print(f"\n[2/4] CLEANING AND PREPARING DATA\nCleaning and preparing all the tables (both facts and dimension) - please wait...")
        countries_dimension, valid_country_codes_list, ports_dimension, valid_port_codes_list, modes_dimension\
        ,valid_mode_codes_list, states_dimension, valid_state_codes_list, visa_dimension, valid_visa_codes_list\
        ,immigration_fact, demographics_dimension, airports_dimension\
        , temperature_dimension = prepare_and_clean_data(spark, immigration_spark, demographics_spark, airports_spark, temperature_path)
        print(f"All fact and dimension tables have been prepared")
    except Exception as e:
        print(e)
        
    # Step 3: Define the Data Model | Step 4: Run ETL to Model the Data
    try:
        print(f"\n[3/4] SAVING FACT AND DIMENSION TABLES\nSaving all the dimension and fact tables to the parquet files...")       
        fact_to_parquet(immigration_fact, demographics_dimension,airports_dimension,ports_dimension,
        temperature_dimension, countries_dimension, visa_dimension, modes_dimension, states_dimension, mode='overwrite')
        dimension_to_parquet(countries_dimension, "country_dimension", path_parquet="./DWH_IMMIGRATION_PARQUET/countries_dimension.parquet", mode='overwrite')
        dimension_to_parquet(ports_dimension, "port_dimension", path_parquet="./DWH_IMMIGRATION_PARQUET/ports_dimension.parquet", mode='overwrite')
        dimension_to_parquet(modes_dimension, "mode_dimension",  path_parquet="./DWH_IMMIGRATION_PARQUET/modes_dimension.parquet", mode='overwrite')
        dimension_to_parquet(states_dimension, "state_dimension",  path_parquet="./DWH_IMMIGRATION_PARQUET/states_dimension.parquet", mode='overwrite')
        dimension_to_parquet(visa_dimension, "visa_dimension",  path_parquet="./DWH_IMMIGRATION_PARQUET/visa_dimension.parquet", mode='overwrite')
        dimension_to_parquet(demographics_dimension, "demographic_dimension",  path_parquet="./DWH_IMMIGRATION_PARQUET/demographics_dimension.parquet", mode='overwrite')
        dimension_to_parquet(airports_dimension, "airport_dimension",  path_parquet="./DWH_IMMIGRATION_PARQUET/airports_dimension.parquet", mode='overwrite')
        dimension_to_parquet(temperature_dimension, "temperature_dimension",  path_parquet="./DWH_IMMIGRATION_PARQUET/temperature_dimension.parquet", mode='overwrite')
        print(f"Data Warehouse extracted to the parquet files")
    except Exception as e:
        print(e)
        
    # Step 4: Run ETL to Model the Data    
    try:
        print(f"\n[4/4] CHECKING DATA QUALITY\n")
        quality_check_rows(spark, "./DWH_IMMIGRATION_PARQUET/countries_dimension.parquet", 'country_dimension')
        quality_check_rows(spark, "./DWH_IMMIGRATION_PARQUET/demographics_dimension.parquet", 'demographic_dimension')
        quality_check_joins(spark, path_fact="./DWH_IMMIGRATION_PARQUET/immigration_fact.parquet", path_dim_to_check="./DWH_IMMIGRATION_PARQUET/demographics_dimension.parquet", fact_col='code_state', dim_col='state_code')
    except Exception as e:
        print(e)     
        
    finally:
        print(f"\nETL PROCESS HAS BEEN FINISHED\n")
        
        
if __name__ == "__main__":
    main()
    
    