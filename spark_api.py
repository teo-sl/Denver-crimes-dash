import pyspark
from pyspark.sql.functions import *





def get_pandas_filtered(df,neighborhood_p,crime_type_p,num_victims_p):
    if(num_victims_p is None):
        num_victims=-1
    else:
        num_victims=num_victims_p
    if(neighborhood_p is None):
        neighborhood='All'
    else:
        neighborhood=neighborhood_p
    if(crime_type_p is None):
        crime_type='All'
    else:
        crime_type=crime_type_p
    
    df_filtered = df

    if(neighborhood!='All'):
        df_filtered = df_filtered.filter(df.NEIGHBORHOOD_ID == neighborhood)
    if(crime_type!='All'):
        df_filtered = df_filtered.filter(df.OFFENSE_CATEGORY_ID == crime_type)

    if(num_victims!=-1):
        df_filtered = df_filtered.filter(df.VICTIM_COUNT >= num_victims)
    
    if(df.count()>30_000):
        return df_filtered.sample(False, 0.01, seed=42).toPandas()
    else:
        return df_filtered.sample(False,0.1,seed=42).toPandas()


def get_pandas_by_month(df):
    df_count_month = (
        df
            .withColumn("month", month("FIRST_OCCURRENCE_DATE"))
            .withColumn("year", year("FIRST_OCCURRENCE_DATE"))
            .select("month","year")
            .groupBy("month", "year")
            .agg(count("*").alias("count"))
    )
    df_count_month = df_count_month.withColumn("date", concat(col("year"), lit("-"), col("month"))).withColumn("date", date_format("date", "yyyy-M"))
    df_count_month = df_count_month.drop("month", "year")
    return df_count_month.toPandas()

def get_pandas_by_day(df):
    df_count_day = (df.withColumn("day", dayofmonth("FIRST_OCCURRENCE_DATE"))
    .withColumn("month", month("FIRST_OCCURRENCE_DATE"))
    .withColumn("year", year("FIRST_OCCURRENCE_DATE"))
    .select("day","month","year")
    .groupBy("day", "month", "year")
    .agg(count("*").alias("count"))
    )
    df_count_day = df_count_day.withColumn("date", concat(col("year"), lit("-"), col("month"), lit("-"), col("day"))).withColumn("date", to_date("date", "yyyy-M-d"))
    df_count_day = df_count_day.drop("day", "month", "year")
    return df_count_day.toPandas()

def get_pandas_by_year(df):
    df_count_year = (
        df
            .withColumn("year", year("FIRST_OCCURRENCE_DATE"))
            .select("year")
            .groupBy("year")
            .agg(count("*").alias("count"))
    )
    df_count_year = df_count_year.withColumn("date", concat(col("year"))).withColumn("date", date_format("date", "yyyy"))
    df_count_year = df_count_year.drop("year")
    return df_count_year.toPandas()


def get_pandas_num_crimes_on_neighborhood(df):
    # aggregate by NEIGHBORHOOD_ID and count all rows
    df_count_neighborhood = (
        df
            .select("NEIGHBORHOOD_ID")
            .groupBy("NEIGHBORHOOD_ID")
            .agg(count("*").alias("count"))
    )
    return df_count_neighborhood.toPandas()

def get_pandas_num_crimes_on_crime_type(df):
    # aggregate by OFFENSE_CATEGORY_ID and count all rows
    df_count_crime_type = (
        df
            .select("OFFENSE_CATEGORY_ID")
            .groupBy("OFFENSE_CATEGORY_ID")
            .agg(count("*").alias("count"))
    )
    return df_count_crime_type.toPandas()

def get_pandas_timeline(df):
    df_animated_neighborhood = (
        df
            .withColumn("month", month("FIRST_OCCURRENCE_DATE"))
            .withColumn("year", year("FIRST_OCCURRENCE_DATE"))
            .groupBy("month", "year", "NEIGHBORHOOD_ID")
            .agg(count("*").alias("count_crimes"))
            .orderBy("NEIGHBORHOOD_ID")
    )
    df_animated_neighborhood = df_animated_neighborhood.withColumn("date_month",concat(col("year"), lit("-"), col("month"))).withColumn("date_month", to_date("date_month", "yyyy-M"))
    df_animated_neighborhood = df_animated_neighborhood.drop("month", "year")
    return df_animated_neighborhood.toPandas()


