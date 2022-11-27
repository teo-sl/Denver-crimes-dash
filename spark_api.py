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

    df_filtered = df_filtered.drop("OFFENSE_TYPE_ID","FIRST_OCCURRENCE_DATE","REPORTED_DATE","GEO_X","GEO_Y","IS_CRIME","IS_TRAFFIC")


    if(neighborhood!='All'):
        df_filtered = df_filtered.filter(df.NEIGHBORHOOD_ID == neighborhood)
    if(crime_type!='All'):
        df_filtered = df_filtered.filter(df.OFFENSE_CATEGORY_ID == crime_type)

    if(num_victims!=-1):
        df_filtered = df_filtered.filter(df.VICTIM_COUNT >= num_victims)
    
    if(df.count()>30_000):
        return df_filtered.sample(False, 0.01).toPandas()
    elif (df.count()< 20_000):
        return df_filtered.toPandas()
    else:
        return df_filtered.sample(False,0.5).toPandas()


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

def get_pandas_by_month_victims(df):
    df_count_month = (
        df
            .withColumn("month", month("FIRST_OCCURRENCE_DATE"))
            .withColumn("year", year("FIRST_OCCURRENCE_DATE"))
            #.select("month","year")
            .groupBy("month", "year")
            .agg(sum("VICTIM_COUNT").alias("count"))
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

def get_pandas_by_day_victims(df):
    df_count_day = (df.withColumn("day", dayofmonth("FIRST_OCCURRENCE_DATE"))
    .withColumn("month", month("FIRST_OCCURRENCE_DATE"))
    .withColumn("year", year("FIRST_OCCURRENCE_DATE"))
    #.select("day","month","year")
    .groupBy("day", "month", "year")
    .agg(sum("VICTIM_COUNT").alias("count"))
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

def get_pandas_by_year_victims(df):
    df_count_year = (
        df
            .withColumn("year", year("FIRST_OCCURRENCE_DATE"))
            #.select("year")
            .groupBy("year")
            .agg(sum("VICTIM_COUNT").alias("count"))
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
            .orderBy(desc("count"))
    )
    return df_count_neighborhood.toPandas()

def get_pandas_num_victims_on_neighborhood(df):
    # aggregate by NEIGHBORHOOD_ID and count all rows
    df_count_neighborhood = (
        df
            #.select("NEIGHBORHOOD_ID")
            .groupBy("NEIGHBORHOOD_ID")
            .agg(sum("VICTIM_COUNT").alias("count"))
            .orderBy(desc("count"))
    )
    return df_count_neighborhood.toPandas()

def get_pandas_num_crimes_on_crime_type(df):
    # aggregate by OFFENSE_CATEGORY_ID and count all rows
    df_count_crime_type = (
        df
            .select("OFFENSE_CATEGORY_ID")
            .groupBy("OFFENSE_CATEGORY_ID")
            .agg(count("*").alias("count"))
            .orderBy(desc("count"))
    )
    return df_count_crime_type.toPandas()

def get_pandas_num_victims_on_crime_type(df):
    # aggregate by OFFENSE_CATEGORY_ID and count all rows
    df_count_crime_type = (
        df
            #.select("OFFENSE_CATEGORY_ID")
            .groupBy("OFFENSE_CATEGORY_ID")
            .agg(sum("VICTIM_COUNT").alias("count"))
            .orderBy(desc("count"))
    )
    return df_count_crime_type.toPandas()


def get_pandas_timeline(df):
    df_animated_neighborhood = (
        df
            .withColumn("month", month("FIRST_OCCURRENCE_DATE"))
            .withColumn("year", year("FIRST_OCCURRENCE_DATE"))
            .groupBy("month", "year", "NEIGHBORHOOD_ID")
            .agg(count("*").alias("count_crimes"))
    )
    df_animated_neighborhood = df_animated_neighborhood.withColumn("date_month",concat(col("year"), lit("-"), col("month"))).withColumn("date_month", to_date("date_month", "yyyy-M"))
    df_animated_neighborhood = df_animated_neighborhood.orderBy("date_month", "NEIGHBORHOOD_ID")
    df_animated_neighborhood = df_animated_neighborhood.drop("month", "year")
    return df_animated_neighborhood.toPandas()

def get_pandas_timeline_victims(df):
    df_animated_neighborhood = (
        df
            .withColumn("month", month("FIRST_OCCURRENCE_DATE"))
            .withColumn("year", year("FIRST_OCCURRENCE_DATE"))
            .groupBy("month", "year", "NEIGHBORHOOD_ID")
            .agg(sum("VICTIM_COUNT").alias("count_crimes"))
            .orderBy("NEIGHBORHOOD_ID")
    )
    df_animated_neighborhood = df_animated_neighborhood.withColumn("date_month",concat(col("year"), lit("-"), col("month"))).withColumn("date_month", to_date("date_month", "yyyy-M"))
    df_animated_neighborhood = df_animated_neighborhood.drop("month", "year")
    return df_animated_neighborhood.toPandas()

def get_pandas_victims_num_ranked(df):
    # get the number of crimes for each value of VICTIM_COUNT
    df_count_victims = (
        df
            .groupBy("VICTIM_COUNT")
            .agg(count("*").alias("num_crimes"))
            .select(col("VICTIM_COUNT").alias("num_victims"), "num_crimes")
            .orderBy("num_crimes", ascending=False)
    )
    return df_count_victims.toPandas()

def get_pandas_scatter_plot_matrix(df):
    df_grouped = (
        df
            .groupBy("OFFENSE_CATEGORY_ID", "NEIGHBORHOOD_ID")
            .agg(count("*").alias("count_crimes"))
    )

    # pivot the dataframe by OFFENSE_CATEGORY_ID and NEIGHBORHOOD_ID, fill nul with 0
    df_pivot = df_grouped.groupBy("OFFENSE_CATEGORY_ID").pivot("NEIGHBORHOOD_ID").sum("count_crimes").na.fill(0)

    
    return df_pivot.toPandas().set_index("OFFENSE_CATEGORY_ID")