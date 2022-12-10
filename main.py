import os
import subprocess
import pandas as pd
import numpy as np
import csv
import mysql.connector as msql
from mysql.connector import Error

## data fetch
""" https://archives.nseindia.com/content/historical/EQUITIES/2022/<MMM>/cm<DD><MMM>2022bhav.csv.zip :: url format
    fetched url from page source, when a request is made for download
    send a wget request for each <DD, MMM>, if available, zip will get downloaded
    no data for weekends
"""

dates = ['09', '08', '07', '06', '05', '04', '03', '02', '01']      # 9 days of december
for date in dates:
    p1 = 'https://archives.nseindia.com/content/historical/EQUITIES/2022/DEC/cm'
    p2 = 'DEC2022bhav.csv.zip'
    url = p1 + date + p2
    # os.system("wget "+ url)   # commenting after executing once to avoid repetititve downloads

for date in range(10,31):       # 20 days of november, total = 29 days
    p1 = 'https://archives.nseindia.com/content/historical/EQUITIES/2022/NOV/cm'
    p2 = 'NOV2022bhav.csv.zip'
    url = p1 + str(date) + p2
    # os.system("wget "+ url)     # commenting after executing once to avoid repetititve downloads

os.system("wget https://archives.nseindia.com/content/historical/EQUITIES/2022/NOV/cm09NOV2022bhav.csv.zip")  # 30 days


## data preprocessing
files = subprocess.check_output('find . -type f -iname "*.zip"', shell=True, text=True).split('\n')         # fetching file names in the current directory
for file in files: os.system("unzip "+ file)        # unzip command over all the zip files
os.system("mv *.zip zips")

os.system("wget https://archives.nseindia.com/content/equities/EQUITY_L.csv")       # other data file

bhav_files = subprocess.check_output('find . -type f -iname "cm*.csv"', shell=True, text=True).split('\n')[:-1]
bhavdf = pd.concat([pd.read_csv(f) for f in bhav_files], ignore_index=True)
equitydf = pd.read_csv("EQUITY_L.csv")
bhavdf.drop(bhavdf.columns[[-1]], axis=1, inplace=True)
bhavdf = bhavdf.replace('NaN', None)
bhavdf = bhavdf.replace('nan', None)
bhavdf = bhavdf.replace({np.nan: None})

## csv to SQL
try:           
    conn = msql.connect(host='localhost', user='root', password='pax123')       # establishing connection
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES LIKE 'task'")
        result = cursor.fetchone()
        if not result:
            cursor.execute("CREATE DATABASE task;")           
            print(">>> DB task created")                      
        else:
            cursor.execute("USE task")
            print(">>> Using database task")
            cursor.execute("SHOW TABLES")
            result = cursor.fetchall()
            print(f"task database has {result} tables\n\n")
except Error as e:
    print(e)

print(bhavdf)
try:            
    cursor.execute("SHOW TABLES LIKE 'bhavcopies'")         # bhavcopies data
    result = cursor.fetchone()
    if not result:
        cursor.execute('''CREATE TABLE bhavcopies(
                            SYMBOL varchar(50),
                            SERIES char(5),
                            OPEN float,
                            HIGH float,
                            LOW float,
                            CLOSE float,
                            LAST float,
                            PREVCLOSE float,
                            TOTTRDQTY int,
                            TOTTRDVAL decimal(65,30),
                            TIMESTAMP varchar(20),
                            TOTALTRADES int,
                            ISIN varchar(20)
                            );''')
        print(">>> Table bhavcopies created")

        for i,row in bhavdf.iterrows():
            query = "INSERT into bhavcopies values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            cursor.execute(query, tuple(row))
            print(f">>> Record{i} inserted")
            conn.commit()
    else:
        cursor.execute("SELECT COUNT(*) from bhavcopies")
        print(f"{cursor.fetchone()} records exist in the table bhavcopies\n\n")
except Error as e:
    print(e)

print(equitydf)
try:
    cursor.execute("SHOW TABLES LIKE 'equityl'")         # equity_l data
    result = cursor.fetchone()
    if not result:
        cursor.execute('''CREATE TABLE equityl(
                            SYMBOL varchar(50),
                            NAME_CO varchar(200),
                            SERIES char(5),
                            LISTING_DATE varchar(20),
                            PAIDUP_VALUE int,
                            MARKET_LOT int,
                            ISIN_NO varchar(20),
                            FACE_VALUE int
                            )''')
        print(">>> Table equityl created")

        for i,row in equitydf.iterrows():
            query = "INSERT into equityl values(%s,%s,%s,%s,%s,%s,%s,%s);"
            cursor.execute(query, tuple(row))
            print(f">>> Record{i} inserted")
            conn.commit()
    else:
        cursor.execute("SELECT COUNT(*) from equityl")
        print(f"{cursor.fetchone()} records exist in the table equityl\n\n")
except Error as e:
    print(e)


## performing queries
query1 = """
            WITH cte AS(
            SELECT SYMBOL, (CLOSE-OPEN)/OPEN AS gain, TIMESTAMP FROM bhavcopies
            WHERE TIMESTAMP LIKE "09-DEC%" 
            ORDER BY gain DESC limit 0,25)
            SELECT SYMBOL, gain, TIMESTAMP FROM cte;
        """

cursor.execute(query1)
result = cursor.fetchall()
print(">>> Query1 executed")
with open('query1_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    header = ['SYMBOL', 'gain', 'TIMESTAMP']
    writer.writerow(header)

    for row in result: writer.writerow(row)
    print(">>> Output written to query1_output.csv\n\n")


days = bhavdf.TIMESTAMP.unique()
with open('query2_output.csv', 'a', newline='') as file:
    writer = csv.writer(file)
    header = ['SYMBOL', 'gain', 'TIMESTAMP']
    writer.writerow(header)

    for day in days:
        query2 = f"""
                    WITH cte AS(
                    SELECT SYMBOL, (CLOSE-OPEN)/OPEN AS gain, TIMESTAMP FROM bhavcopies
                    WHERE TIMESTAMP LIKE "{day}" 
                    ORDER BY gain DESC limit 0,25)
                    SELECT SYMBOL, gain, TIMESTAMP FROM cte;
                """
        cursor.execute(query2)
        result = cursor.fetchall()
        print(f">>> Query2 executed for {day}")

        for row in result: writer.writerow(row)
        sep = [["\n\t\t-------------------------------------------------------------------------------\t\t\n"]]
        writer.writerow(sep[0])
        print(f">>> Output for {day} written to query2_output.csv\n\n")


query3 = """
            WITH cte1 AS(
            SELECT open, SYMBOL FROM bhavcopies WHERE TIMESTAMP LIKE "09-NOV%"),
            cte2 AS(
            SELECT close, SYMBOL FROM bhavcopies WHERE TIMESTAMP LIKE "09-DEC%")
            SELECT cte1.SYMBOL, (cte2.close-cte1.open)/cte1.open AS gain 
            FROM cte1, cte2 ORDER BY gain DESC limit 0,25;
        """
cursor.execute(query3)
result = cursor.fetchall()
print(">>> Query3 executed")
with open('query3_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    header = ['SYMBOL', 'gain']
    writer.writerow(header)

    for row in result: writer.writerow(row)
    print(">>> Output written to query3_output.csv\n\n")
