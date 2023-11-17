import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType



def run_transform(bucket_name):
	# Create a spark session
	spark = SparkSession.builder.appName('emr etl job').getOrCreate()
	# Setup the logger to write to spark logs
	# noinspection PyProtectedMember
	logger = spark._jvm.org.apache.log4j.Logger.getLogger('TRANSFORM')
	logger.info('Spark session created')
	logger.info('Trying to read data now.')



	# Schema for the reference data file
	data_schema = StructType()
	data_schema.add('track_id', 'integer')
	data_schema.add('track_name', 'string')
	data_schema.add('artist_name', 'string')
	# Read the reference data into a data frame
	data_path = 's3://{}/data/track_list.json'.format(bucket_name)
	df_track = spark.read.schema(data_schema).json(data_path)
	

	# Bucket the data by activity type and write the
	# results to S3 in overwrite mode
	writer = df_track.write
	writer.format('parquet')
	writer.mode('overwrite')
	write_path = 's3://{}/data/emr-processed-data/'.format(bucket_name)
	writer.option('path', write_path)
	writer.save()
	# Stop Spark
	spark.stop()


def main():
	# Accept bucket name from the arguments passed.
	# TODO: Error handling when there are no arguments passed.
	bucket_name = 'sihan-analytics-workshop-bucket'
	# Run the transform method
	run_transform(bucket_name)


if __name__ == '__main__':
	main()
