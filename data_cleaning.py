from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.types import *


def creating_countries_dimension(filepath, spark):
    """
    Function responsible for preparing the country dimension.
    After cleaning data from appropriate file
    the function created country dimension.
    The function returns a spark data frame with country dimension
    and also list with valid country codes.
    """   
    
    print('Preparing country_dimension...')
    
    schema = StructType([
    StructField("country_code", StringType(), False),
    StructField("country_name", StringType(), False)])
    
    with open(filepath) as f:
        lines = f.readlines()
        
    valid_country_codes_raw = []
    valid_country_codes_list = []

    for index, line in enumerate(lines[9:245]):
        valid_country_codes_list.append(int(line.split('=')[0].strip()))
        valid_country_codes_raw.append({
            'country_code' : int(line.split('=')[0].strip()),
            'country_name' : line.split('=')[1].strip().replace('\n','').replace("'","")
        })
    valid_country_codes_raw[0]['country_name'] = "MEXICO"
    
    return spark.createDataFrame(pd.DataFrame(valid_country_codes_raw), schema=schema), valid_country_codes_list


def creating_ports_dimension(filepath, spark):
    """
    Function responsible for preparing the port dimension.
    After cleaning data from appropriate file
    the function created port dimension.
    The function returns a spark data frame with port dimension
    and also list with valid port codes.
    """  
    
    print('Preparing port_dimension...')
    
    schema = StructType([
    StructField("port_code", StringType(), False),
    StructField("port_city", StringType(), False)])
    
    with open(filepath) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    ports = content[302:962]
    splitted_ports = [port.split("=") for port in ports]
    port_codes = [x[0].replace("'","").strip() for x in splitted_ports]
    port_locations = [x[1].replace("'","").strip() for x in splitted_ports]
    port_cities = [x.split(",")[0] for x in port_locations]
    port_states = [x.split(",")[-1] for x in port_locations]
    valid_port_codes_raw = pd.DataFrame({"port_code" : port_codes, "port_city": port_cities, "port_state": port_states})
    valid_port_codes_raw = valid_port_codes_raw[~valid_port_codes_raw['port_city'].str.contains('No PORT Code')]
    valid_port_codes_raw = valid_port_codes_raw[~valid_port_codes_raw['port_city'].str.contains('Collapsed')]
    irregular_ports_df = valid_port_codes_raw[valid_port_codes_raw["port_city"] == valid_port_codes_raw["port_state"]]
    valid_port_codes_raw = valid_port_codes_raw[~valid_port_codes_raw['port_code'].isin(irregular_ports_df.port_code.tolist())]
    valid_port_codes_list = pd.DataFrame(valid_port_codes_raw)['port_code'].tolist()

    return spark.createDataFrame(pd.DataFrame(valid_port_codes_raw)[['port_code', 'port_city']], schema=schema), valid_port_codes_list


def creating_modes_dimension(filepath, spark):
    """
    Function responsible for preparing the mode dimension.
    After cleaning data from appropriate file
    the function created mode dimension.
    The function returns a spark data frame with mdoe dimension
    and also list with valid mode codes.
    """ 
    
    print('Preparing mode_dimension...')
        
    schema = StructType([
    StructField("mode_id", IntegerType(), False),
    StructField("mode_name", StringType(), False)])
    
    with open(filepath) as f:
        lines = f.readlines()

    valid_mode_codes_raw = []
    valid_mode_codes_list = []

    for index, line in enumerate(lines[972:976]):
        #print(index, line.split('='))
        valid_mode_codes_list.append(int(line.split('=')[0].strip()))
        valid_mode_codes_raw.append({
            'mode_id':int(line.split('=')[0].strip()),
            'mode_name':line.split('=')[1].replace(';','').replace('\n','').replace("'","").strip()
        })

    return spark.createDataFrame(pd.DataFrame(valid_mode_codes_raw)[['mode_id','mode_name']], schema=schema), valid_mode_codes_list


def creating_states_dimension(filepath, spark):
    """
    Function responsible for preparing the state dimension.
    After cleaning data from appropriate file
    the function created state dimension.
    The function returns a spark data frame with state dimension
    and also list with valid state codes.
    """ 
    
    print('Preparing state_dimension...')
    
    schema = StructType([
    StructField("state_code", StringType(), False),
    StructField("state_name", StringType(), False)])
    
    with open(filepath) as f:
        lines = f.readlines()

    valid_state_codes_raw = []
    valid_state_codes_list = []

    for index, line in enumerate(lines[981:1036]):
        #print(index, line.split('='))
        valid_state_codes_list.append(line.split('=')[0].strip().replace('\t','').replace("'",""))
        valid_state_codes_raw.append({
            'state_code': line.split('=')[0].strip().replace('\t','').replace("'","") ,
            'state_name': line.split('=')[1].replace('\n','').replace("'","").strip()
        }) 

    return spark.createDataFrame(pd.DataFrame(valid_state_codes_raw), schema=schema), valid_state_codes_list



def creating_visa_dimension(filepath, spark):
    """
    Function responsible for preparing the visa dimension.
    After cleaning data from appropriate file
    the function created visa dimension.
    The function returns a spark data frame with visa dimension
    and also list with valid visa codes.
    """ 
    
    print('Preparing visa_dimension...')
    
    schema = StructType([
    StructField("visa_id", IntegerType(), False),
    StructField("visa_type", StringType(), False)])
    
    with open(filepath) as f:
        lines = f.readlines()

    valid_visa_codes_raw = []
    valid_visa_codes_list = []

    for index, line in enumerate(lines[1046:1049]):
        #print(index, line.split('='))
        valid_visa_codes_list.append(int(line.split('=')[0].strip()))
        valid_visa_codes_raw.append({
            'visa_id': int(line.split('=')[0].strip()),
            'visa_type': line.split('=')[1].strip().replace('\n','').replace("'","") 
        })

    return spark.createDataFrame(pd.DataFrame(valid_visa_codes_raw), schema=schema), valid_visa_codes_list


def clean_immigration_fact(spark_dataframe, valid_countries, valid_ports, valid_modes, valid_visa):
    """
    Function responsible for preparing main fact table.
    After reading spark data frame this function clean
    data and prepare fact table.
    The function returns a spark data frame with immigration fact.
    """ 
    
    print('Preparing immigration_fact...')
    immigration_spark = spark_dataframe.withColumnRenamed("i94addr", "code_state") \
    .withColumnRenamed("i94port", "code_port") \
    .withColumn("code_visa", col("i94visa").cast("integer")).drop("i94visa") \
    .withColumn("code_mode", col("i94mode").cast("integer")).drop("i94mode") \
    .withColumn("code_country_origin", col("i94res").cast("integer")).drop("i94res") \
    .withColumn("code_country_city", col("i94cit").cast("integer")).drop("i94cit") \
    .withColumn("year", col("i94yr").cast("integer")).drop("i94yr") \
    .withColumn("month", col("i94mon").cast("integer")).drop("i94mon") \
    .withColumn("birth_year", col("biryear").cast("integer")).drop("biryear") \
    .withColumn("age", col("i94bir").cast("integer")).drop("i94bir") \
    .withColumn("counter_summary", col("count").cast("integer")).drop("count") \
    .withColumn("date_base_SAS", to_date(lit("01/01/1960"), "MM/dd/yyyy")) \
    .withColumn("arrival_date", expr("date_add(date_base_SAS, arrdate)")) \
    .withColumn("departure_date", expr("date_add(date_base_SAS, depdate)")).drop("date_base_SAS", "arrdate", "depdate")\
    .withColumn("arrival_date-split", split(col("arrival_date"), "-")) \
    .withColumn("arrival_year", col("arrival_date-split")[0].cast("integer")) \
    .withColumn("arrival_month", col("arrival_date-split")[1].cast("integer")) \
    .withColumn("arrival_day", col("arrival_date-split")[2].cast("integer")) \
    .drop("arrival_date-split").drop('dtadfile')
    
    
    immigration_spark = immigration_spark.filter(immigration_spark['code_country_city'].isin(valid_countries))\
    .filter(immigration_spark['code_country_origin'].isin(valid_countries))\
    .filter(immigration_spark['code_port'].isin(valid_ports))\
    .filter(immigration_spark['code_mode'].isin(valid_modes))\
    .filter(immigration_spark['code_visa'].isin(valid_visa))
    
    return immigration_spark


def clean_demographics_dimension(spark_dataframe, valid_countries):
    """
    Function responsible for preparing the demographic dimension
    After reading spark data frame this function clean data and
    prepare demographic dimension.
    The function returns a spark data frame with demographic dimension.
    """ 
    
    print('Preparing demographic_dimension...')
    demographics_spark = spark_dataframe.filter(spark_dataframe["State Code"].isin(valid_countries))
    demographics_dimension = demographics_spark.withColumnRenamed("City", "city") \
    .withColumnRenamed("State", "state") \
    .withColumnRenamed("Median Age", "median_age") \
    .withColumnRenamed("Male Population", "male_population") \
    .withColumnRenamed("Female Population", "female_population") \
    .withColumnRenamed("Total Population", "total_population") \
    .withColumnRenamed("Number of Veterans", "number_of_veterans") \
    .withColumnRenamed("Foreign-born", "foreign_born") \
    .withColumnRenamed("Average Household Size", "average_household_size") \
    .withColumnRenamed("State Code", "state_code") \
    .withColumnRenamed("Race", "race") \
    .withColumnRenamed("Count", "count") \
    .groupBy(col("city"), col("state"), col("median_age"), col("male_population")\
             ,col("female_population"), col("total_population"), col("number_of_veterans")\
             ,col("foreign_born"), col("average_household_size"), col("state_code"))\
    .pivot("race").agg(sum("count").cast("integer"))\
    .withColumnRenamed("American Indian and Alaska Native", "american_indian_and_alaska_native") \
    .withColumnRenamed("Asian", "asian") \
    .withColumnRenamed("Black or African-American", "black_or_african_american") \
    .withColumnRenamed("Hispanic or Latino", "hispanic_or_atino") \
    .withColumnRenamed("White", "white") \
    .fillna({"american_indian_and_alaska_native": 0,
             "asian": 0,
             "black_or_african_american": 0,
             "hispanic_or_atino": 0,
             "white": 0})
    
    return demographics_dimension


def clean_airports_dimension(spark_dataframe, valid_ports):
    """
    Function responsible for preparing the airports dimension
    After reading spark data frame this function clean data and
    prepare airport dimension.
    The function returns a spark data frame with airport dimension.
    """
    print('Preparing airport_dimension...')
    return spark_dataframe.filter(spark_dataframe["iata_code"].isin(valid_ports))


def clean_temperature_dimension(filepath_raw, port_dimension, spark):
    """
    Function responsible for preparing the temperature dimension
    After reading spark data frame this function clean data and
    prepare temperature dimension.
    The function returns a spark data frame with temperature dimension.
    """ 
    
    print('Preparing temperature_dimension...')
    
    schema = StructType([
    StructField("port_code", StringType(), False),
    StructField("month", IntegerType(), False),
    StructField("avg_tempertature", DoubleType(), False)
    ])
    
    temperature_pandas = pd.read_csv(filepath_raw)
    temperature_pandas = temperature_pandas[~temperature_pandas['AverageTemperature'].isnull() ]
    temperature_pandas['datetime'] = pd.to_datetime(temperature_pandas['dt'], format="%Y/%m/%d")
    temperature_pandas['month'] = temperature_pandas['datetime'].dt.month
    temperature_pandas = temperature_pandas.groupby(['City','month'])[['AverageTemperature']].mean().reset_index()
    temperature_pandas['City']=temperature_pandas['City'].apply(lambda x: x.upper())
    temperature_pandas = temperature_pandas.merge(port_dimension.toPandas(),how='inner', left_on = 'City', right_on='port_city')[['port_code','month','AverageTemperature']]
    temperature_pandas = temperature_pandas.rename({'AverageTemperature':'avg_tempertature'}, axis=1)
    temperature_pandas['avg_tempertature'] = temperature_pandas['avg_tempertature'].round(decimals=2)
    return spark.createDataFrame(temperature_pandas, schema)

