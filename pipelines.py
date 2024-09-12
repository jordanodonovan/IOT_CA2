import pandas as pd
import boto3
import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col

print("#########################################################################")
print("                            BEGINNING                                    ")
print("#########################################################################")

# Setting up the PyFlink environment
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().set("parallelism.default", "1")

stream_env = StreamExecutionEnvironment.get_execution_environment()
table_stream_env = StreamTableEnvironment.create(stream_env)

# Connecting to Amazon S3
s3 = boto3.resource(
    's3',
    aws_access_key_id='######################',
    aws_secret_access_key='######################'
)

s3_bucket_name = 'iottaba'

# Opening the CSV as a DataFrame and fixing the data type of the "event_time" column
df = pd.read_csv('uncleaned_data.csv')
df = df.iloc[0:200]
df['event_time'] = pd.to_datetime(df["event_time"], utc=True)
df = df.sort_values('event_time', ascending=True)

# My first Flink pipeline - cleaning the data
# Creating a PyFlink table from the DataFrame and dropping the "storedtime" column because it's empty
table = t_env.from_pandas(
    df,
    schema=DataTypes.ROW([DataTypes.FIELD("station_id", DataTypes.INT()),
                            DataTypes.FIELD("datapoint_id", DataTypes.INT()),
                            DataTypes.FIELD("alarm_id", DataTypes.INT()),
                            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP()),
                            DataTypes.FIELD("value", DataTypes.FLOAT()),
                            DataTypes.FIELD("valueThreshold", DataTypes.FLOAT()),
                            DataTypes.FIELD("isActive", DataTypes.BOOLEAN()),
                            DataTypes.FIELD("storedtime", DataTypes.STRING())]))
table = table.drop_columns(col('storedtime'))

# Using PyFlink and SQL to create a "alarm_type" column with the values matching to those found in the README file for for the dataset
new_column = t_env.sql_query("""
                SELECT *,
                    CASE
                        WHEN alarm_id = 316 THEN 'smoke'
                        WHEN alarm_id = 319 THEN 'motion'
                        WHEN alarm_id = 320 THEN 'failed temperature sensor'
                        WHEN alarm_id = 303 THEN 'failed equipment'
                        WHEN alarm_id = 306 THEN 'high moisture'
                        WHEN alarm_id = 307 THEN 'low moisture'
                        WHEN alarm_id = 308 THEN 'high AC voltage'
                        WHEN alarm_id = 309 THEN 'low AC voltage'
                        WHEN alarm_id = 310 THEN 'high AC load current'
                        WHEN alarm_id = 311 THEN 'low AC load current'
                        WHEN alarm_id = 312 THEN 'high DC voltage'
                        WHEN alarm_id = 313 THEN 'low DC voltage'
                        WHEN alarm_id = 314 THEN 'high DC load current'
                        WHEN alarm_id = 315 THEN 'low DC load current'
                        WHEN alarm_id = 322 THEN 'high room temperature'
                        WHEN alarm_id = 323 THEN 'connection loss'
                        WHEN alarm_id = 302 THEN 'equipment connection loss'
                        WHEN alarm_id = 304 THEN 'high outdoor temperature'
                        WHEN alarm_id = 321 THEN 'lack of gass in air conditioner'
                        WHEN alarm_id = 317 THEN 'door open'
                        WHEN alarm_id = 318 THEN 'flooding'
                        WHEN alarm_id = 324 THEN 'increasing temperature'
                        WHEN alarm_id = 325 THEN 'batterys high temperature'
                        WHEN alarm_id = 301 THEN 'AC power loss'
                        WHEN alarm_id = 305 THEN 'low temperature'
                        ELSE 'no error'
                    END AS alarm_type
                FROM %s 
            """ % table) \
         .execute()

rows = new_column.collect()
df = pd.DataFrame(rows, columns=new_column.get_table_schema().get_field_names())

# Creating a PyFlink table from the DataFrame containing the new "alarm_type" column
table = t_env.from_pandas(
    df,
    schema=DataTypes.ROW([DataTypes.FIELD("station_id", DataTypes.INT()),
                            DataTypes.FIELD("datapoint_id", DataTypes.INT()),
                            DataTypes.FIELD("alarm_id", DataTypes.INT()),
                            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP()),
                            DataTypes.FIELD("value", DataTypes.FLOAT()),
                            DataTypes.FIELD("valueThreshold", DataTypes.FLOAT()),
                            DataTypes.FIELD("isActive", DataTypes.BOOLEAN()),
                            DataTypes.FIELD("alarm_type", DataTypes.STRING())]))

# Using PyFlink and SQL to create a "sensor_type" column with the values matching to those found in the README file for for the dataset
new_column = t_env.sql_query("""
                SELECT *,
                    CASE
                        WHEN datapoint_id = 111 THEN 'Room Temperature'
                        WHEN datapoint_id = 112 THEN 'Temperature of Airconditioner 1'
                        WHEN datapoint_id = 126 THEN 'Frequency of Power Generator'
                        WHEN datapoint_id = 123 THEN 'Frequency of Power Grid'
                        WHEN datapoint_id = 124 THEN 'Voltage of Power Generator'
                        WHEN datapoint_id = 114 THEN 'Outdoor temperature'
                        WHEN datapoint_id = 121 THEN 'Voltage of Power Grid'
                        WHEN datapoint_id = 113 THEN 'Temperature of Airconditioner 2'
                        WHEN datapoint_id = 162 THEN 'Runtime of Airconditioner 1'
                        WHEN datapoint_id = 163 THEN 'Runtime of Airconditioner 2'
                        WHEN datapoint_id = 164 THEN 'Runtime of AC'
                        WHEN datapoint_id = 165 THEN 'Runtime of Power Generator'
                        WHEN datapoint_id = 153 THEN 'Motion sensor'
                        WHEN datapoint_id = 151 THEN 'Smoke sensor'
                        WHEN datapoint_id = 152 THEN 'Door sensor'
                        WHEN datapoint_id = 154 THEN 'Water leak sensor'
                        WHEN datapoint_id = 125 THEN 'Load of power generator'
                        WHEN datapoint_id = 122 THEN 'Load of Power Grid'
                        WHEN datapoint_id = 161 THEN 'Capacity'
                        WHEN datapoint_id = 155 THEN 'Heat increase'
                        WHEN datapoint_id = 141 THEN 'Total Battery Voltage'
                        WHEN datapoint_id = 115 THEN 'Humidity'
                        WHEN datapoint_id = 116 THEN 'Battery temperature'
                        WHEN datapoint_id = 143 THEN 'Voltage of Battery 1'
                        WHEN datapoint_id = 144 THEN 'Voltage of Battery 2'
                        WHEN datapoint_id = 142 THEN 'Load of Battery 1'
                        WHEN datapoint_id = 145 THEN 'Load of Battery 1'
                        ELSE 'no error'
                    END AS sensor_type
                FROM %s 
            """ % table) \
         .execute()

rows = new_column.collect()
df = pd.DataFrame(rows, columns=new_column.get_table_schema().get_field_names())

# Creating a PyFlink table from the DataFrame containing the new "sensor_type" column
table = t_env.from_pandas(
    df,
    schema=DataTypes.ROW([DataTypes.FIELD("station_id", DataTypes.INT()),
                            DataTypes.FIELD("datapoint_id", DataTypes.INT()),
                            DataTypes.FIELD("alarm_id", DataTypes.INT()),
                            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP()),
                            DataTypes.FIELD("value", DataTypes.FLOAT()),
                            DataTypes.FIELD("valueThreshold", DataTypes.FLOAT()),
                            DataTypes.FIELD("isActive", DataTypes.BOOLEAN()),
                            DataTypes.FIELD("alarm_type", DataTypes.STRING()),
                            DataTypes.FIELD("sensor_type", DataTypes.STRING())]))

# This is where I will simulate the streaming of my data. The batch_size variable will be the size of each batch to control the stream of data (proving it's scalable).
batch_size = 20
batches = [df[i:i+batch_size] for i in range(0, len(df), batch_size)]
num_of_batches = len(batches)

# Creating an empty DataFrame to store the initial batch and append the following batches to it
combined_batch = pd.DataFrame()

# Variables to control the size of each batch
batch_no = 0
first_row = 2
final_row = batch_size

# My pipeline to query the data and upload it to S3
def streaming():

    # Getting the first batch of data
    first_batch = t_env.sql_query(f"""SELECT *
                                            FROM {table}
                                            LIMIT 2
                                            """).execute()

    rows = first_batch.collect()
    first_df = pd.DataFrame(rows, columns=first_batch.get_table_schema().get_field_names())
    print(first_df)

    for batch in batches:

        global combined_batch, batch_no, num_of_batches, first_row, final_row

        # Iterating through each batch of data
        next_batch = t_env.sql_query(f"""SELECT *
                                            FROM {table}
                                            LIMIT {final_row} OFFSET {first_row}
                                            """).execute()
                                        
        first_row += batch_size

        rows = next_batch.collect()
        next_df = pd.DataFrame(rows, columns=first_batch.get_table_schema().get_field_names())

        # Converting first batch to DataFrame and appending follwing iterations to it
        if batch_no == 0:
            combined_df = pd.concat([first_df, next_df])
        else:
            combined_df = pd.concat([combined_df, next_df], ignore_index=True)

        print(combined_df)

        # Preparing the DataFrame to be uploaded
        csv_to_upload = combined_df.to_csv(index=False)

        print("#########################################################################")
        print(f"                    UPLOADING BATCH {batch_no}/{num_of_batches}         ")
        print("#########################################################################")

        # Uploading CSV file to AWS S3
        s3.Object('iottaba', 'csvfile/cleaned.csv').put(Body=csv_to_upload)

        print("#########################################################################")
        print(f"                     UPLOADED BATCH {batch_no}/{num_of_batches}         ")
        print("#########################################################################")

        batch_no += 1
        time.sleep(5)

streaming()
print("#########################################################################")
print("                            COMPLETE                                    ")
print("#########################################################################")
