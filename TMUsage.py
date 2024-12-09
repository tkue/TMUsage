#!/usr/bin/python3

import sqlite3
import os
import time
from datetime import datetime as dt
import re


SCHEMA_SCRIPT = 'create_schema.sql'
DB_NAME = 'tm.db'
STAGING_TABLE = 'Stag_UsageVoice'

VOICE = 'CDR_voice_7276672206.csv'
MESSAGE = 'CDR_message_7276672206.csv'
DATA = 'CDR_data_7276672206.csv'


#
# class Table(object):
#     def __init__(self. table_name: str, columns: dict, rows: []=None)
#         self.table_name = table_name
#         self.columns  = columns
#         self.rows = rows
#
# class VoiceStaging(Table):
#     def __init__(self, conn: sqlite3.Connection):
#         self.conn = conn
#         table_name = 'Stag_VoiceUsage'
#         columns = {
#             'Number': 'text',
#             'Date': 'text',
#             'Destination': 'text',
#             'Number': 'text',
#             'Minutes': 'integer',
#             'Call Type': 'text',
#             None: 'text' # used for money amount if overage
#         }
#         super().__init__(self, table_name, columns)
#
#     def get_rows(self):
#         cur = self.conn.cursor()
#         try:
#             cur.execute(""" SELECT
#                                 field1,
#                                 field2,
#                                 field3,
#                                 field3,
#                                 field5,
#                                 field6,
#                                 field7,
#                                 field8
#                             FROM Stag_VoiceUsage""")
#             rows = cur.fetchall()
#             if not rows:
#                 return
#
#
#             for row in rows:
#                 try:
#                     self.rows.append({
#                         'Number': row[0],
#                         'Date': row[1],
#                         ''
#                     })
#
# class Database(object):
#     def __init__(self)
#         self.db_name = DB_NAME
#         self.conn = sqlite3.connect(self.db_name)
#         self.create_schema()
#
#     def create_schema(self):
#         if not os.path.exists(SCHEMA_SCRIPT):
#             raise IOError
#
#         cur = self.conn.cursor()
#         try:
#             cur.executescript(SCHEMA_SCRIPT)
#             self.conn.commit()
#         except sqlite3.Error as e:
#             self.conn.rollback()
#             print(e)
#         finally:
#             cur.close()
#
#     def clean_voice_staging(self)

# def main():
#     db = Database()


class TmFiles(object):

    def __init__(self, path: str):
        self.path_voice = None
        self.path_message = None
        self.path_data = None

        for file in os.listdir(os.path.curdir):
            if 'CDR_voice' in file:
                self.path_voice = os.path.abspath(file)
            if 'CDR_message' in file:
                self.path_message = os.path.abspath(file)
            if 'CDR_data' in file:
                self.path_data = os.path.abspath(file)

        self.data_voice = self.get_dict_voice(self.path_voice)
        self.data_message = self.get_dict_message(self.path_message)
        self.data_data = self.get_dict_data(self.path_data)


    def readlines(self, file_path: str):
        lines = []
        with open(file_path, 'r') as f:
            for line in f:
                if re.match('\d{2}/\d{2}/\d{4}', line):
                    lines.append(line)

        return lines


    def get_dict_voice(self, file_path: str):
        info_voice = []
        lines = readlines(file_path)

        for line in lines:
            split = line.split(',')
            try:
                info_voice.append({
                    'Date': split[0],
                    'Time': split[1],
                    'Destination': split[2],
                    'Number': split[3],
                    'Minutes': split[4],
                    'CallType': split[5]
                })
            except Exception as e:
                print(line)
                print(split)
                print(e)

        return info_voice


    def get_dict_message(self, file_path):
        info = []
        lines = readlines(file_path)

        for line in lines:
            split = line.split(',')
            try:
                info.append({
                    'Date': split[0],
                    'Time': split[1],
                    'Destination': split[2],
                    'Number': split[3],
                    'Direction': split[4],
                    'Type': split[5]
                })
            except Exception as e:
                print(line)
                print(split)
                print(e)

        return info

    def get_dict_data(self, file_path):
        info = []
        lines = readlines(file_path)

        for line in lines:
            split = line.split(',')
            try:
                info.append({
                    'Date': split[0],
                    'Service': split[1],
                    'Volume': split[2],
                    'Measurement': split[3]
                })
            except Exception as e:
                print(line)
                print(split)
                print(e)

        return info

def create_db(db_name: str):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS `Voice` (
                                `VoiceID`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                `Date`	TEXT,
                                `Time`	TEXT,
                                `Destination`	TEXT,
                                `Number`	TEXT,
                                `Minutes`	TEXT,
                                `CallType`	TEXT
                            );""")

        cur.execute("""CREATE TABLE IF NOT EXISTS `Message` (
                                `MessageID`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                `Date`	TEXT,
                                `Time`	TEXT,
                                `Destination`	TEXT,
                                `Number`	TEXT,
                                `Direction`	TEXT,
                                `Type`	TEXT
                            );""")

        cur.execute("""CREATE TABLE IF NOT EXISTS `Data` (
                                `DataID`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                                `Date`	TEXT,
                                `Service`	TEXT,
                                `Volume`	TEXT,
                                `Measurement`	TEXT
                            );""")
        cur.execute('DELETE FROM Data')
        cur.execute('DELETE FROM Message')
        cur.execute('DELETE FROM Voice')
        conn.commit()

    except sqlite3.Error as e:
        print('Failed to create schema')
        print(e)
    finally:
        cur.close()


def insert_voice_rows():
    conn = sqlite3.connect('tmusage.db')
    cur = conn.cursor()

    values = get_dict_voice(VOICE)
    for val in values:
        # inserts.append(list(val.values()))
        try:
            query = """INSERT INTO `Voice` (
                                    Date,
                                    Time,
                                    Destination,
                                    Number,
                                    Minutes,
                                    CallType
                                )
                                VALUES ( "{0}", "{1}", "{2}", "{3}", "{4}", "{5}" )""".format(val['Date'],
                                                                                              val['Time'],
                                                                                              val['Destination'],
                                                                                              val['Number'],
                                                                                              val['Minutes'],
                                                                                              val['CallType'])
            cur.execute("""INSERT INTO `Voice` (
                                    Date,
                                    Time,
                                    Destination,
                                    Number,
                                    Minutes,
                                    CallType
                                )
                                VALUES ( ?, ?, ?, ?, ?, ? )""", (val['Date'],
                                                                 val['Time'],
                                                                 val['Destination'],
                                                                 val['Number'],
                                                                 val['Minutes'],
                                                                 val['CallType']))
            conn.commit()
        except sqlite3.Error as e:
            print(e)

def insert_message_rows(db_name: str, filepath: str):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    values = get_dict_message(filepath)

    for val in values:
        try:
            cur.execute("""INSERT INTO Message (
                            Date,
                            Time,
                            Destination,
                            Number,
                            Direction,
                            Type
                            )
                            VALUES ( ?, ?, ?, ?, ?, ? )""", (val['Date']))

if __name__ == '__main__':

    create_db('tmusage.db')

    conn = sqlite3.connect('tmusage.db')
    cur = conn.cursor()


    values = get_dict_voice(VOICE)
    inserts = []
    for val in values:
        # inserts.append(list(val.values()))
        try:
            query = """INSERT INTO `Voice` (
                                Date,
                                Time,
                                Destination,
                                Number,
                                Minutes,
                                CallType
                            )
                            VALUES ( "{0}", "{1}", "{2}", "{3}", "{4}", "{5}" )""".format(val['Date'],
                                val['Time'],
                                val['Destination'],
                                val['Number'],
                                val['Minutes'],
                                val['CallType'])
            cur.execute("""INSERT INTO `Voice` (
                                Date,
                                Time,
                                Destination,
                                Number,
                                Minutes,
                                CallType
                            )
                            VALUES ( ?, ?, ?, ?, ?, ? )""", (val['Date'],
                                                                val['Time'],
                                                                val['Destination'],
                                                                val['Number'],
                                                                val['Minutes'],
                                                                val['CallType']))
            conn.commit()
        except sqlite3.Error as e:
            print(e)
