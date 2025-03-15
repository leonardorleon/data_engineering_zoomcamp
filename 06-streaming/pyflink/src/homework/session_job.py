from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment


def create_taxi_events_sink_postgres(t_env):
    table_name = 'processed_aggregated_taxi_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID            INTEGER,
            DOLocationID            INTEGER,
            window_start            TIMESTAMP,
            window_end              TIMESTAMP,
            total_trips_in_session  BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = "taxi_events"  # The name of this table is not very important, it only exists on kafka context
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count VARCHAR,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_watermark AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR event_watermark AS event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    # env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_taxi_events_sink_postgres(t_env)
        
        # write records to postgres too!
        t_env.execute_sql(
            f"""
                INSERT INTO {postgres_sink}
                SELECT
                    PULocationID,
                    DOLocationID,
                    SESSION_START(event_watermark,INTERVAL '5' MINUTES)  AS window_start,
                    SESSION_END(event_watermark,INTERVAL '5' MINUTES)    AS window_end, 
                    count(*)    AS total_trips_in_session
                FROM {source_table}
                GROUP BY 
                    PULocationID, 
                    DOLocationID, 
                    SESSION(event_watermark,INTERVAL '5' MINUTES)
                ;
            """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
