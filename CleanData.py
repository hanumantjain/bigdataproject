from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, round
from pyspark.sql.window import Window

# Define DATA_SOURCE
DATA_SOURCE = 'hdfs://localhost:9000/input/split_data_a*.csv'
OUTPUT_PATH = 'hdfs://localhost:9000/cleaned_data'


def main():
    # Initialize SparkSession
    spark = SparkSession.builder.appName('Data_Cleaning').getOrCreate()
    
    try:
        # Read data from CSV source using wildcard
        all_data = spark.read.csv(DATA_SOURCE, header=True, escape='"')
        
        # Deleting 'Developer_Website' column
        all_data = all_data.drop('Developer_Website', 'Developer_Url')
        
        # Dropping rows where 'Released' is null
        all_data = all_data.filter(all_data['Released'].isNotNull())

        # Define the window specification over 'Primary_Genre'
        windowSpec = Window.partitionBy('Primary_Genre')

        # Calculate the mean for each group and fill missing values in 'Size_Bytes'
        all_data = all_data.withColumn('Size_MB', 
                                       when(col('Size_Bytes').isNull(),
                                            mean(col('Size_Bytes')).over(windowSpec))
                                       .otherwise(col('Size_Bytes') / (1024 * 1024)))
        all_data = all_data.withColumn('Size_MB', round(col('Size_MB'), 2))
        #deleting Size_Bytes column
        all_data = all_data.drop('Size_Bytes')
        
        # Calculate the mean for each group and fill missing values in 'Price'
        all_data = all_data.withColumn('Price', 
                                       when(col('Price').isNull(),
                                            mean(col('Price')).over(windowSpec))
                                       .otherwise(col('Price')))
        all_data = all_data.withColumn('Price', round(col('Price'), 2))
        
        # Display the final processed data
        
        all_data.show()
        print('Data ', all_data.count())

        all_data.write.csv(OUTPUT_PATH, mode='overwrite', header=True)
        print("Processed data has been successfully written to:", OUTPUT_PATH)
        
    except Exception as e:
        print("An error occurred:", str(e))

if __name__ == "__main__":
    main()