# Batch Processing for Data Mining

Using pyspark to process large datasets. We are going to merge a number of datasets and then perform some basic processing on them. 
Some of the processing will be doing will involve data cleansing and generating of ngrams on some data columns. The data used 
have been download locally and read into the jupyter notebook.

## Getting Started

Download the file unto your local machine. Run the script file in a jupyter notebook to be able to visualize it. You may download the same data from kaggle,
data scientist jobs market dataset and the US stocks.

### Prerequisites

First and foremost, use the requirements.txt file to install the dependencies needed to run this file. After you can run this file.


### Installing

Open terminal and move into your virtual environment where you will be running this script. Install the dependecies here using the
requirements.txt file. Run your jupiter notebook here.


```
Summary
```
By the running the scripts you would be running functions which uses the ngrams function in the .py file to create 2 spark data
frames which have 3 columns in the order of frequency:

i. {Ngram, City, Frequency}
ii. {Ngram, Industry, Frequency}

```
until finished
```

You should have your output in 3 columns as stated above.

## Running the tests

You can run automated tests for this project by using Apache Airflow to schedule the time interval for running the batch processing on the data 
you have. But with this you have to get your data on S3 for it to run on the cloud at specified times.


## Author

* **Jedidiah Madjanor**


