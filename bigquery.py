#note must have set enviroment variable GOOGLE_APPLICATION_CREDENTIALS to json file with credentials
from google.cloud import bigquery
from botocore.exceptions import ClientError
import boto3
import threading

def get_month(year, month):
    #set up bigquery client
    client = bigquery.Client()
    dataset_ref = client.dataset('bitcoin_blockchain', project='bigquery-public-data')
    dataset = client.get_dataset(dataset_ref)
    config = bigquery.job.QueryJobConfig()
    config.allow_large_results = True
    config.destination = "cs435-260122.blockchain.temp{}_{}".format(year, month)
    
    #create the end of the date range and modify into strings
    year2 = year
    month2 = month + 1

    if month == 12 :
        year2 = year + 1
        month2 = 0
    if(month<10):
        month = "0"+str(month)
    else:
        month = str(month)
    if(month2<10):
        month2 = "0"+str(month2)
    else:
         month2 = str(month2)

    #create and submit query
    QUERY = "SELECT * FROM `bigquery-public-data.crypto_bitcoin.transactions` WHERE block_timestamp BETWEEN '{}-01-{}' AND '{}-11-{}'".format(year, month,year2,month2)
    query_job = client.query(QUERY, job_config = config)  # API request
    rows = query_job.result()  # Waits for query to finish

    #create local temp file
    #potential improvement would be to store in memory and write in batches of a few gb
    f = open("./tempChain{}_{}.txt".format(year, month),"w+", buffering = 8192)
    f2 =open("./transactions{}_{}.txt".format(year, month),"w+", buffering = 8192)
    for row in rows:
        blockNum = row.block_number
        tHash = row.hash
        timestamp = row.block_timestamp
        ic = row.input_count
        oc = row.output_count
        iv = row.input_value
        ov = row.output_value
        fee = row.fee
        if row.is_coinbase: #skipping miner payouts
            continue
        f2.write(", ".join((str(tHash), str(blockNum), str(ic), str(oc), str(iv), str(ov), str(fee))) + "\n")
        for input in row.inputs:
            ad = input["addresses"]
            val = input["value"]
            f.write(", ".join(("input", str(timestamp), str(ad), str(val))) + "\n")
        for output in row.outputs:
            ad = input["addresses"]
            val = input["value"]
            f.write(", ".join(("output", str(timestamp), str(ad), str(val)))+ "\n")

    f.close()
    f2.close()

    #upload the temp files to amazon s3
    s3 = boto3.resource('s3') 
    s3.meta.client.upload_file('tempChain.txt', 'cs435blockchain', 'wallets{}_{}.txt'.format(year, month))
    s3.meta.client.upload_file('transactions.txt', 'cs435blockchain', 'transactions{}_{}.txt'.format(year, month))

if __name__ == "__main__":
    threads = list()
    #create a thread for each month in 2017 and 2018
    for yr in range(2017,2019):
        for month in range(1,12):
            thread = threading.Thread(target=get_month, args=(yr,month))
            thread.start()
            threads.append(thread)
    #wait for each thread to finish
    for index, thread in enumerate(threads):   
        print("waiting on thread: " + str(index))
        thread.join()

#final file format is
#input/output, timestamp, address, value
#value is in satoshi's?