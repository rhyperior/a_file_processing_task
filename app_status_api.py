from datetime import datetime
from http.client import HTTPResponse
from flask import Flask, jsonify
from mysql.connector import connect
import config as cfg

app = Flask(__name__)

conn = connect(user=cfg.DB_USER, host=cfg.DB_HOST, database=cfg.DATABASE_NAME, password=cfg.DB_PASSWORD)
cur = conn.cursor()

@app.route('/')
def status():
    try:
        result = {
        "No. of records in Database": 'NA',
        "last 5 newly inserted filenames": ['abc.txt'],
        "No. of records inserted in last minute/ rate": 'NA'
        }
        
        # with conn.cursor() as cur:
        # print('reloaded again '+ str(datetime.now()))
        cur.execute('select count(*) from file_processing_status ;')
        no_of_records = cur.fetchall()[0][0]
        print(no_of_records)
        cur.execute('select filename from file_processing_status order by inserted_at desc  limit 5;')
        latest_files = []
        for item in cur.fetchall():
            latest_files.append(item[0])
        cur.execute('select count(*) from file_processing_status where inserted_at >= now() - INTERVAL 1 MINUTE ;')
        rate = cur.fetchall()[0][0]
        conn.commit()
        
        result['last 5 newly inserted filenames'] = latest_files
        result['No. of records in Database'] = no_of_records
        result['No. of records inserted in last minute/ rate'] = rate
        # print('------'+str(no_of_records))

    except Exception as e:
        print("error in getting status from database fn", str(e))
    return jsonify(result)

if __name__=="__main__":
    app.run(debug=True)