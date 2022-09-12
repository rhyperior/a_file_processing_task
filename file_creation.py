from os import listdir, system
from os.path import isfile, join
import asyncio
import time
import random
import string
from datetime import datetime
from mysql.connector import connect


import config as cfg

PROCESSING_FOLDER = cfg.PROCESSING_FOLDER
PROCESSED_FOLDER = cfg.PROCESSED_FOLDER
QUEUE_FOLDER = cfg.QUEUE_FOLDER
files_to_process_list = []

async def create_new_textfile(): 
    try:
        i = 5
        while(i > 0):
            text = "".join( [random.choice(string.ascii_letters) for i in range(50)])
            filename = str(datetime.now().strftime('%Y-%d-%m__%H-%M-%S-%f')) + '__' +text[:10]+'.txt'
            print(filename)
            with open(PROCESSING_FOLDER+filename, 'w') as f:
                f.write(text)
            print('Just created a new text file.')
            time.sleep(0.9) 
            i -= 1
        await asyncio.sleep(1)
    except Exception as e:
        print("error in create_new_textfile fn", str(e))

async def move_files(origin_folder, destination_folder):
    try:
        system(f"MOVE {origin_folder}*txt {destination_folder}")
    except Exception as e:
        print("Exception in move_files_to_queue fn", str(e))

def update_file_details_to_db(query=None):
    try:
        conn = connect(user=cfg.DB_USER, host=cfg.DB_HOST, database=cfg.DATABASE_NAME, password=cfg.DB_PASSWORD)
        print(conn) 

        with conn.cursor() as cur:
            cur.execute(query)

        conn.commit()
        # print('sleeping for 10 seconds..')
        # time.sleep(3)
        conn.close()
    except Exception as e:
        print("error in update_file_details_to_db fn", str(e))

def query_builder(filename_list, action='insert'):
    try:
        if action == 'insert':
            query = f'INSERT INTO {cfg.TABLE_NAME} (filename, status, inserted_at) VALUES '
            values = ''
            for filename in filename_list:
                values = values + f"('{filename}', 0, now()) ,"
            values = values[:-1] + ';'
            query = query + values
        else:
            query = f'UPDATE {cfg.TABLE_NAME} SET status=1, processed_at=now() WHERE filename in ('
            values = ''
            for filename in filename_list:
                values = values + f"'{filename}',"
            values = values[:-1] + ') ;'
            query = query + values
        return query

    except Exception as e:
        print("error in query_builder fn", str(e))

async def process_queued_files():
    try:
        text_files = [f for f in listdir(cfg.QUEUE_FOLDER) if isfile(join(cfg.QUEUE_FOLDER, f)) and '.txt' in f]
        files_to_process_list = text_files
        insert_query = query_builder(text_files)
        # print(insert_query)
        update_file_details_to_db(query=insert_query)
        task_move_files = asyncio.create_task(move_files(cfg.QUEUE_FOLDER, cfg.PROCESSED_FOLDER))
        await task_move_files
        print('updated details in db and moved files to processed')
    except Exception as e:
        print("error in process_queued_files fn", str(e))

async def finish_processing_files():
    try:
        update_query = query_builder(filename_list=files_to_process_list, action='update')
        print(update_query)
        update_file_details_to_db(query=update_query)
        print('finished processing')
    except Exception as e:
        print('error in finish_processing_files fn', str(e))

async def main():
    task_1 = asyncio.create_task(create_new_textfile())
    await task_1
    task_2 = asyncio.create_task(move_files(cfg.PROCESSING_FOLDER, cfg.QUEUE_FOLDER))
    task_3 = asyncio.create_task(process_queued_files())
    await task_2
    await task_3
    task_4 = asyncio.create_task(finish_processing_files())
    await task_4

if __name__=="__main__":
    while(True):
        asyncio.run(main())

    