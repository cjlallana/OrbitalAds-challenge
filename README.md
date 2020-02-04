# OrbitalAds-challenge

## Shakespeare Research

For an exhaustive research on Shakespeare's literary works, we need to know how many times the
author uses every word in three of his masterpieces:

* King Lear:
https://storage.cloud.google.com/apache-beam-samples/shakespeare/kinglear.txt

* Othello:
https://storage.cloud.google.com/apache-beam-samples/shakespeare/othello.txt

* Romeo and Juliet:
https://storage.cloud.google.com/apache-beam-samples/shakespeare/romeoandjuliet.txt

The expected output should be a single file.

### Installation

You need to have Python 3.x installed.

1. Use the venv command to create a virtual copy of the entire Python installation:
```
$ python -m venv .env
```
2. Activate the virtual environment:
```
$ .env\Scripts\activate
```
3. Now install the dependencies included in *requirements.txt*
```
$ pip install -r requirements.txt
```

### Usage

In order to write the final data to a Google Spreadsheet, you need to create a Service Account, download and copy its related JSON file to the *credentials* folder (in the root of the application), and add its *client_email* as an editor of the Spreadsheet where you want to write. In this exercise, I won't share my credentials for security reasons.

To try the application, run:
```
$ python main.py
```

After the execution, the results will appear in the Google Spreadsheet