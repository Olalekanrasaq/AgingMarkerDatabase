# import required libraries
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter # to parse command line arguments
import sqlite3
import re # for writing regex 
import pandas as pd
import matplotlib.pyplot as plt
import sys
import DataParse_2778496 as dp # a module containing functions for parsing data files to be stored in database
import warnings # to suppress future warning from the program
warnings.filterwarnings('ignore') # setting ignore as a parameter

# Parse command line arguments
parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("--createdb", action='store_true', help="create database")
parser.add_argument("--loaddb", action='store_true', help="load database")
parser.add_argument("--querydb", type=int, help="load database")
parser.add_argument("file", help="database file")
args = parser.parse_args()

class MarkerDatabase:
    '''A class used to create and load data into database and as well
    query such data from the dataset'''

    db_path = args.file # set the sqlite database file to a class variable 'db_path'

    # parse the datafiles provided and load them into pandas dataframe
    subject_df = dp.parse_subject('Subject.csv')
    metabolome_df = dp.parse_metabolome('HMP_metabolome_abundance.tsv')
    transcriptome_df = dp.parse_transcriptome('HMP_transcriptome_abundance.tsv')
    proteome_df = dp.parse_proteome('HMP_proteome_abundance.tsv')
    annotation_df = dp.parse_annotation('HMP_metabolome_annotation.csv')

    # sql statements for creating database table for subject
    create_table_subject = """CREATE TABLE IF NOT EXISTS 'subjects' (
        'SubjectID' TEXT,
        'Age' REAL,
        'Sex'	TEXT,
        'Race' TEXT,
        'BMI'	REAL,
        'SSPG' REAL,
        'IR-IS' Classification TEXT,
        PRIMARY KEY('SubjectID')
    );"""

    # sql statements for creating database table for metabolome
    create_table_metabolome = """CREATE TABLE IF NOT EXISTS metabolome (
        SampleID TEXT,
        VisitID INTEGER,
        FOREIGN KEY (SampleID) REFERENCES subjects (SampleID)
    );"""
    
    # sql statements for creating database table for proteome
    create_table_proteome = """CREATE TABLE IF NOT EXISTS proteome (
        SampleID TEXT,
        VisitID INTEGER,
        FOREIGN KEY (SampleID) REFERENCES subjects (SampleID)
    );"""

    # sql statements for creating database table for transcriptome
    create_table_transcriptome = """CREATE TABLE IF NOT EXISTS transcriptome (
        SampleID TEXT,
        VisitID INTEGER,
        A1BG REAL,
        FOREIGN KEY (SampleID) REFERENCES subjects (SampleID)
    );"""
    
    # sql statements for creating database table for annotation
    create_table_annotation = """CREATE TABLE IF NOT EXISTS 'peak_annotation' (
        'PeakID' TEXT,
        'Metabolite' TEXT,
        'KEGG' TEXT,
        'HMDB' TEXT,
        'Chemical_Class' TEXT,
        'Pathway' TEXT
    );"""

    def __init__(self, db_path):
        self.db_path = db_path

    def create_database(self, db_path):
        '''this method when called will create the database schema'''
        self.db_path = db_path
        conn = sqlite3.connect(self.db_path) # connect to the sqlite3 database file
        c = conn.cursor() # connection object
        if args.createdb:
            '''if true i.e --createdb option is provided in the command line,
            tables are created into the sqlite database file'''
            c.execute(MarkerDatabase.create_table_subject)
            c.execute(MarkerDatabase.create_table_metabolome)
            c.execute(MarkerDatabase.create_table_proteome)
            c.execute(MarkerDatabase.create_table_transcriptome)
            c.execute(MarkerDatabase.create_table_annotation)
            conn.commit()
            conn.close()
        return self.db_path

    def load_database(self, db_path):
        '''this method when called will load the parsed (correct) data into the database'''
        self.db_path = db_path
        conn = sqlite3.connect(db_path)
        if args.loaddb: 
            '''if true i.e --loaddb option is provided in the command line, the parsed
            dataframe data are loaded into the sqlite database tables respectively'''
            MarkerDatabase.subject_df.to_sql('subjects',conn,if_exists='replace',index=False)
            MarkerDatabase.metabolome_df.to_sql('metabolome',conn,if_exists='replace',index=False)
            MarkerDatabase.transcriptome_df.to_sql('transcriptome',conn,if_exists='replace',index=False)
            MarkerDatabase.proteome_df.to_sql('proteome',conn,if_exists='replace',index=False)
            MarkerDatabase.annotation_df.to_sql('peak_annotation',conn,if_exists='replace',index=False)
            conn.commit()
            conn.close()
        return self.db_path

    def query_database(self, db_path, sql, params=[]):
        '''this method when called will query data from the database'''
        # to query data stored in the databases
        self.db_path = db_path
        self.sql = sql # sql query statement
        self.params = params # parameters if any... params is set to empty list as default
        conn = sqlite3.connect(db_path) # connect to the database file
        c = conn.cursor() 
        c.execute(sql, params) # execute query statement
        rows = c.fetchall() # get the result of the query statement
        c.close() 
        conn.close() 
        return rows

path = MarkerDatabase.db_path # set path to the sqlite database file 
db_py = MarkerDatabase(path) # create an obect of the class MarkerDatabase
cdb = db_py.create_database(path) # create the database and tables
ldb = db_py.load_database(path) # load data into the database
if args.querydb == 1:
    sql = "SELECT SubjectID, Age FROM subjects WHERE Age > 70"
    results = db_py.query_database(path, sql) # get the results of the query
    print('SubjectID\tAge')
    sys.stdout # print on the command line
    for row in results:
        subjectID, age = row
        print('%s\t%s' % (subjectID, age))
        sys.stdout

elif args.querydb == 2:
    sql = '''SELECT SubjectID, Sex, BMI FROM subjects 
            WHERE Sex = 'F' AND BMI BETWEEN 18.5 AND 24.9 ORDER BY BMI DESC'''
    results = db_py.query_database(path, sql) # get the results of the query
    print('SubjectID\tSex\tBMI')
    sys.stdout
    for row in results:
        subjectID, sex, bmi = row
        print('%s\t%s\t%s' % (subjectID, sex, bmi))
        sys.stdout

elif args.querydb == 3:
    sql = "SELECT VisitID FROM proteome WHERE sampleID = 'ZNQOVZV'"
    results = db_py.query_database(path, sql) # get the results of the query
    print('VisitID')
    sys.stdout
    for row in results:
        visitID = row
        print('%s' % (visitID))
        sys.stdout

elif args.querydb == 4:
    sql = """SELECT DISTINCT SubjectID FROM subjects, metabolome
            WHERE metabolome.sampleID = subjects.SubjectID AND subjects.IR_IS_classification = 'IR';"""
    results = db_py.query_database(path, sql) # get the results of the query
    print('SubjectID')
    sys.stdout
    for row in results:
        subjectID = row
        print('%s' % (subjectID))
        sys.stdout

elif args.querydb == 5:
    sql = """SELECT PeakID, KEGG FROM peak_annotation WHERE PeakID 
            IN ('nHILIC_121.0505_3.5', 'nHILIC_130.0872_6.3', 'nHILIC_133.0506_2.3', 'nHILIC_133.0506_4.4')"""
    results = db_py.query_database(path, sql) # get the results of the query
    print('PeakID\tKEGG_ID')
    sys.stdout
    for row in results:
        PeakID, KEGG = row
        print('%s\t%s' % (PeakID, KEGG))
        sys.stdout

elif args.querydb == 6:
    sql = """SELECT min(Age), max(Age), 
            avg(Age) FROM subjects"""
    results = db_py.query_database(path, sql) # get the results of the query
    print('Minimum_Age\tMaximum_Age\tAverage_Age')
    sys.stdout
    for row in results:
        min, max, avg = row
        print('%s\t%s\t%s' % (min, max, avg))
        sys.stdout

elif args.querydb == 7:
    sql = """SELECT Pathway,COUNT(*) FROM peak_annotation GROUP BY Pathway
            HAVING count(*) >= 10 ORDER BY COUNT(*) DESC"""
    results = db_py.query_database(path, sql) # get the results of the query
    print('Pathway\tCount')
    sys.stdout
    for row in results:
        pathway, count = row
        print('%s\t%s' % (pathway, count))
        sys.stdout

elif args.querydb == 8:
    sql = """SELECT max(A1BG) FROM transcriptome WHERE sampleID = 'ZOZOW1T'"""
    results = db_py.query_database(path, sql) # get the results of the query
    print('Max Abundance A1BG')
    sys.stdout
    for row in results:
        abundance = row
        print('%s' % (abundance))
        sys.stdout

elif args.querydb == 9:
    sql = """SELECT Age, BMI FROM subjects
            WHERE (Age IS NOT NULL AND BMI IS NOT NULL)"""
    results = db_py.query_database(path, sql) # get the results of the query
    print('Age\tBMI')
    sys.stdout
    for row in results:
        age, bmi = row
        print('%s\t%s' % (age, bmi))
        sys.stdout
    age = [res[0] for res in results] # generate x-axis for the plot
    bmi = [res[1] for res in results] # generate y-axis for the plot
    plt.scatter(age, bmi, c='blue') # create a scatterplot of age against BMI
    plt.xlabel('Age')
    plt.ylabel('BMI')
    plt.title('Age_BMI_Scatterplot')
    plt.savefig("age_bmi_scatterplot.png") # save the image as PNG