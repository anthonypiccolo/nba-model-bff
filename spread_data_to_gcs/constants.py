# Insert name of Google Cloud Storage bucket
BUCKET_NAME = 'nbamodel-223111.appspot.com'
# Insert Google Cloud project ID
PROJECT = 'nbamodel-223111'
# Insert required file format, either 'NEWLINE_DELIMITED_JSON', 'CSV' or 'AVRO'
FILE_FORMAT = 'NEWLINE_DELIMITED_JSON'
# Insert True or False depending on if you want it to be zipped
ZIPPED = False
# For JSON use GZIP, for AVRO use SNAPPY or DEFLATE. If none use None
COMPRESSION_FORMAT = 'None'
