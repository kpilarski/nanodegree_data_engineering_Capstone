def fact_to_parquet(fact_table, demographics_dimension,airports_dimension,ports_dimension,
                    temperature_dimension, countries_dimension, visa_dimension,
                    modes_dimension, states_dimension, mode='overwrite'):
    """
    Function responsible for inserting fact table into parquet.
    Function read from arguments fact_table, all dimension tables and then put them together with joins
    and finally write to parquet.
    """  
    
    print('Saving Immigration fact table...')
    immigration_fact = fact_table.join(demographics_dimension, fact_table["code_state"] == demographics_dimension["state_code"], "left_semi") \
    .join(airports_dimension, fact_table["code_port"] == airports_dimension["iata_code"], "left_semi") \
    .join(ports_dimension, fact_table["code_port"] == ports_dimension["port_code"], "left_semi") \
    .join(temperature_dimension, (fact_table["code_port"] == temperature_dimension["port_code"]) & (fact_table["arrival_month"] == temperature_dimension["month"]), "left_semi") \
    .join(countries_dimension, fact_table["code_country_origin"] == countries_dimension["country_code"], "left_semi") \
    .join(visa_dimension, fact_table["code_visa"] == visa_dimension["visa_id"], "left_semi") \
    .join(modes_dimension, fact_table["code_mode"] == modes_dimension["mode_id"], "left_semi") \
    .join(states_dimension, fact_table["code_state"] == states_dimension["state_code"], "left_semi")
    
    return immigration_fact.write.mode('overwrite').partitionBy("arrival_year", "arrival_month", "arrival_day").parquet("./DWH_IMMIGRATION_PARQUET/immigration_fact.parquet")


def dimension_to_parquet(dimension_table, dimension_name, path_parquet, mode='overwrite'):
    """
    Function responsible for inserting dimension table into parquet.
    """  
    print(f'Saving {dimension_name} table...')
    return dimension_table.write.mode('overwrite').parquet(path_parquet)
    
