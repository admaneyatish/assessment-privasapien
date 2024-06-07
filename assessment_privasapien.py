import boto3
import os
from pyspark.sql import SparkSession

# Initialize the S3 client
s3 = boto3.client('s3')

# Function to list all files in a bucket
def list_files_in_bucket(bucket_name):
    files = []
    response = s3.list_objects_v2(Bucket=bucket_name)
    for content in response.get('Contents', []):
        files.append(content['Key'])
    return files

bucket_name = 'your-bucket-name'
files = list_files_in_bucket(bucket_name)
print(f"Total files found: {len(files)}")

# Classify files by extension
file_types = {'csv': [], 'json': [], 'parquet': []}
for file in files:
    ext = os.path.splitext(file)[1].lower()
    if ext == '.csv':
        file_types['csv'].append(file)
    elif ext == '.json':
        file_types['json'].append(file)
    elif ext == '.parquet':
        file_types['parquet'].append(file)
print(f"Classified files: {file_types}")

# Initialize Spark session
spark = SparkSession.builder.appName("S3DataSampling").getOrCreate()

# Function to sample data
def sample_data(file, file_type, sample_size=100):
    path = f"s3a://{bucket_name}/{file}"
    if file_type == 'csv':
        df = spark.read.csv(path, header=True)
    elif file_type == 'json':
        df = spark.read.json(path)
    elif file_type == 'parquet':
        df = spark.read.parquet(path)
    return df.sample(False, sample_size / df.count())

# Sample data
samples = {'csv': [], 'json': [], 'parquet': []}
sample_size = 100

for file in file_types['csv']:
    samples['csv'].append(sample_data(file, 'csv', sample_size))
for file in file_types['json']:
    samples['json'].append(sample_data(file, 'json', sample_size))
for file in file_types['parquet']:
    samples['parquet'].append(sample_data(file, 'parquet', sample_size))

# Function to save samples back to S3
def save_sample(df, path, file_type):
    if file_type == 'csv':
        df.write.csv(path, mode='overwrite', header=True)
    elif file_type == 'json':
        df.write.json(path, mode='overwrite')
    elif file_type == 'parquet':
        df.write.parquet(path, mode='overwrite')

for i, df in enumerate(samples['csv']):
    save_sample(df, f"s3a://{bucket_name}/samples/csv/sample_{i}", 'csv')
for i, df in enumerate(samples['json']):
    save_sample(df, f"s3a://{bucket_name}/samples/json/sample_{i}", 'json')
for i, df in enumerate(samples['parquet']):
    save_sample(df, f"s3a://{bucket_name}/samples/parquet/sample_{i}", 'parquet')

print("Data sampling and saving completed.")
