import csv
import json
import sqlite3        ##sqlite3 is part of the standard python library
from xml.etree import ElementTree ##xml is part of the standard python library
from pony.orm import *


####### To unify the data, I have to first change the headers 
####### in the vehicle data to match those in the other files

# rename the headers of the vehicle data in the csvfile
with open('Data//user_data.csv', 'r', newline='') as file:
    with open('outputs//vehicle_data.csv', 'w', newline='') as newfile:
        reader = csv.reader(file)
        writer = csv.writer(newfile)
        #read the first row
        next(reader, None)
        #replace the values of the first row
        writer.writerow(['firstName', 'lastName','age','sex','vehicle_make',
                        'vehicle_model','vehicle_year','vehicle_type'])
        #write the remaining rows
        for row in reader:
            writer.writerow(row)



###### Next, I will convert the xml file to csv to make it easier to manipulate

#read the xml file and get the root
xml = ElementTree.parse('Data//user_data.xml')
root = xml.getroot()

#open a new csv file to write data into
statusfile = open('outputs//status_data.csv', 'w', encoding='utf-8', newline='')
statusfile_writer = csv.writer(statusfile)

#insert the column header names
statusfile_writer.writerow(['firstName', 'lastName', 'age', 'sex', 'retired', 
                        'dependants', 'marital_status', 'salary', 'pension', 
                        'company', 'commute_distance', 'address_postcode'])

#loop through the xml root and get all attributes
for child in root:
    firstName = child.attrib['firstName']
    lastName = child.attrib['lastName']
    age = child.attrib['age']
    sex = child.attrib['sex']
    retired = child.attrib['retired']
    dependants = child.attrib['dependants']
    marital_status = child.attrib['marital_status']
    salary = child.attrib['salary']
    pension = child.attrib['pension']
    company = child.attrib['company']
    commute_distance = child.attrib['commute_distance']
    address_postcode = child.attrib['address_postcode']
    #determine the content of each row
    csv_line = [firstName, lastName, age, sex, 
                    retired, dependants, marital_status,
                    salary, pension, company, commute_distance, 
                    address_postcode]

    #write each row to csv file
    statusfile_writer.writerow(csv_line)
#close the file
statusfile.close()


###### Next I would join the data together using a temporary sqlite table
###### and export the data into a csv file to form the main table

#open all files   
vehicle_file = csv.reader(open('outputs//vehicle_data.csv'))
status_file = csv.reader(open('outputs//status_data.csv'))
banking_file = json.load(open('Data//user_data.json'))

##preparing the json to be imported into the database
#get all the column header names
columns = []  ##empty list that would eventually contain the column header names
column = [] ##empty list that temporarily stores the column header names 
for data in banking_file:
    column = list(data.keys())
    for col in column:
        if col not in columns:
            columns.append(col)

#get all the values of the data
values = []  ##empty list that would eventually contain all the values 
value = [] ##empty list that temporarily hold the values
for data in banking_file:
    for i in columns:
        value.append(str(dict(data).get(i)))
    values.append(list(value))
    value.clear()

#create a connection to a temporary database in memory
conn = sqlite3.connect(':memory:')

#create a cursor
c=conn.cursor()

#define the tables
vehicle_table = '''CREATE TABLE vehicle(
                first_name TEXT,
                last_name TEXT,
                age INTEGER,
                sex TEXT,
                vehicle_make TEXT,
                vehicle_model TEXT,
                vehicle_year INTEGER,
                vehicle_type TEXT
                );'''

status_table = """CREATE TABLE status(
                first_name TEXT,
                last_name TEXT,
                age INTEGER,
                sex TEXT,
                retired TEXT,
                dependants INTEGER,
                marital_status TEXT,
                salary INTEGER,
                pension INTEGER,
                company TEXT,
                commute_distance REAL,
                address_postcode TEXT
                );"""

banking_table = """CREATE TABLE banking(
                first_name TEXT,
                last_name TEXT,
                age INTEGER,
                iban TEXT,
                credit_card_number TEXT,
                credit_card_security_code INTEGER,
                credit_card_start_date TEXT,
                credit_card_end_date TEXT,
                address_main TEXT,
                address_city TEXT,
                address_postcode TEXT,
                debt );"""

#create the tables
c.execute(vehicle_table)
c.execute(status_table)
c.execute(banking_table)

#define the table population
vehicle_insert_records = '''INSERT INTO vehicle(first_name, last_name, 
                            age, sex, vehicle_make, vehicle_model, 
                            vehicle_year, vehicle_type) 
                            VALUES (?,?,?,?,?,?,?,?)'''

status_insert_records = '''INSERT INTO status(first_name, last_name, age, 
                            sex, retired, dependants, marital_status, salary, 
                            pension, company, commute_distance, address_postcode)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''

banking_insert_records = '''INSERT INTO banking(first_name, last_name, age, 
                            iban, credit_card_number, credit_card_security_code,
                            credit_card_start_date, credit_card_end_date, 
                            address_main, address_city, address_postcode, debt)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''

#populate the tables with values from the files opened earlier
c.executemany(vehicle_insert_records, vehicle_file)
c.executemany(status_insert_records, status_file)
c.executemany(banking_insert_records, values)

#select needed columns from joins
select_all = '''SELECT b.first_name, b.last_name, b.age, s.sex, s.marital_status, s.retired, s.dependants, 
                        s.salary, s.pension, s.company, s.commute_distance, b.address_main, b.address_city, 
                        b.address_postcode, b.iban, b.credit_card_number, b.credit_card_security_code, 
                        b.credit_card_start_date, b.credit_card_end_date, b.debt, v.vehicle_make, v.vehicle_model, 
                        v.vehicle_year, v.vehicle_type  
                FROM banking AS b
                LEFT JOIN vehicle AS v
                ON b.first_name = v.first_name 
                AND b.last_name = v.last_name
                AND b.age = v.age
                LEFT JOIN status AS s
                ON b.first_name = s.first_name
                AND b.last_name = s.last_name
                AND b.age = s.age'''

c.execute(select_all)
#create an csvfile of the full data
with open('outputs//full_data.csv', 'w', newline='') as outfile:
    csv_writer = csv.writer(outfile, delimiter=',')
    csv_writer.writerow([i[0] for i in c.description])
    csv_writer.writerows(c)
#close the connection
c.close()




###### Next, I would create a main database using ponyorm

#create a database
db = Database()
##In order to use PonyORM with MySQL I need to install MySQLdb or pymysql
#db.bind(provider='sqlite', filename='outputs//laureltech.db', create_db=True) ##have a system database to check if file output is okay
#db.bind(provider='mysql', host='europa.ashley.work', user='student_bi24ae', passwd='iE93F2@8EhM@1zhD&u9M@K', db='student_bi24ae') ##bind to mysql server

#define the table
class Main(db.Entity):
    _table_ = 'customers'
    first_name = Required(str)
    last_name = Required(str)
    age = Required(int)
    sex = Required(str)
    marital_status = Optional(str)
    retired = Optional(str)
    dependants = Optional(str)
    salary = Optional(int)
    pension = Optional(int)
    company = Optional(str)
    commute_distance = Optional(float)
    address_main = Optional(str)
    address_city = Optional(str)
    address_postcode = Optional(str)
    iban = Required(str, unique=True)
    credit_card_number = Required(str, unique=True)
    credit_card_security_code = Required(str)
    credit_card_start_date = Required(str)
    credit_card_end_date = Required(str)
    debt = Optional(str)
    vehicle_make = Optional(str)
    vehicle_model =  Optional(str)
    vehicle_year = Optional(int)
    vehicle_type = Optional(str)

#generate the table
sql_debug(True)
db.generate_mapping(create_tables=True)

#create a session to populate the database
@db_session
def populate_database_main():
    #creates a database and populates it from given csv file
    with open('outputs//full_data.csv', 'r') as populate_file:
        populate_table_reader = csv.reader(populate_file)
        next(populate_table_reader) ##skips the first row
        for row in populate_table_reader:
            Main(first_name = row[0], last_name = row[1], age = row[2],
                sex = row[3], marital_status = row[4], retired = row[5],
                dependants = row[6], salary = row[7], pension = row[8],
                company = row[9], commute_distance = row[10], address_main = row[11],
                address_city = row[12], address_postcode = row[13], iban = row[14],
                credit_card_number = row[15], credit_card_security_code = row[16],
                credit_card_start_date = row[17], credit_card_end_date = row[18],
                debt = row[19], vehicle_make = row[20], vehicle_model =  row[21],
                vehicle_year = row[22], vehicle_type = row[23])

#run the populate function
with db_session:
        populate_database_main()