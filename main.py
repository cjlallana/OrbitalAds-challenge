'''
:author: Carlos Lallana

:since: 2020/02/04
'''
# Python standard library
import re, logging

# Third-party libraries
from google.cloud import storage

# My own modules
import sheets_api_wrapper as SHEETS


# Global variables (actually used as constants)
DRIVE_API_KEYFILE = 'credentials/carlos-lallana_sheets-api_orbitalads.json'
SPREADSHEET_ID = '155V85GBod4w2Q8DRGfaAkW5dsjkGj0i3Im95CTllStY'

BUCKET_NAME = 'apache-beam-samples'

def main():

	# List of words which will contain the content of all books
	list_of_words = []

	# List of blobs which will be downloaded from GCS for analysis
	gcs_blobs = ['shakespeare/kinglear.txt', 
				'shakespeare/othello.txt',
				'shakespeare/romeoandjuliet.txt']

	# Create a client with anonymous credentials, which will have limited 
	# access to “public” buckets
	gcs_client = storage.Client.create_anonymous_client()

	# Loop through the list of blobs to get their text and put it together
	# in a list
	for blob in gcs_blobs:
		
		blob_str = get_blob_from_gcs_as_string(	gcs_client,
												BUCKET_NAME,
												blob)

		if blob_str:
			# Lowercase the string and split it by its non-word characters; this
			# way, we get rid of all punctuation marks and whitelines
			list_of_words = re.split(r'\W+', blob_str.decode("utf-8").lower())

	if not list_of_words:
		logging.error('No blobs could be retrieved')
		return -1

	# Count the number of occurrences of each word
	dict_of_words = count_words(list_of_words)

	## Write the results to a Google Spreadsheet
	# In order to do so, we need to convert the dict to a list of tuples, as
	# each 'tuple' (list of 2 elements) will be a row in the spreadsheet
	rows = [(k, v) for k, v in dict_of_words.items()]

	## Sheets API authorization flow ##
	keyfile = SHEETS.open_local_keyfile(DRIVE_API_KEYFILE)
	credentials = SHEETS.get_credentials_object(keyfile)
	service = SHEETS.authorize_credentials(credentials)
	## End of authorization flow ##

	if not credentials:
		return -2

	# Now we will append the row in batches of 1000
	batchsize = 1000

	for i in range(0, len(rows), batchsize):
		values = rows[i:i+batchsize]

		SHEETS.append_to_spreadsheet(service, 
									SPREADSHEET_ID, 
									values, 
									n_retries=3)


def get_blob_from_gcs_as_string(gcs_client, bucket, blob_name):
	'''
	Method that connects with Cloud Storage and downloads a given blob from
	a given bucket as a string.

	:param gcs_client: Client for interacting with the Google Cloud Storage API
	:param bucket: the source bucket name
	:param blob_name: the name of the blob to download
	:return: the data stored in this blob as bytes
	'''
	try:
		bucket = gcs_client.get_bucket(bucket)

		blob = bucket.blob(blob_name)

		return blob.download_as_string()

	except Exception as e:
		logging.error('Error downloading %s from the bucket: %s' % (blob_name, e))


def count_words(list_of_words):
	'''
	Method that counts the occurrences of each word within a given a list of 
	words. 
	For each new word found, it forces a KeyError, which is caught and the
	word added to the dict. This is more efficient than comparing on each
	loop if a word already exists ((if word in dict_of_words....))

	:param list_of_words: list of words to analyze
	:return: dictionary containing each word and its occurrences
	'''

	dict_of_words = {}

	for word in list_of_words:

		try:
			dict_of_words[word] += 1

		except KeyError:
			dict_of_words[word] = 1

	return dict_of_words


if __name__ == "__main__":
	main()