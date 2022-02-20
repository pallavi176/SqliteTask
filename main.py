from task_solution import TaskSolution
from sqliteoperation import SqliteOperation

"""
dataset -   https://archive.ics.uci.edu/ml/datasets/Bag+of+Words

    q1 = try to find out a count of each and every word in a respective file return a list of tuple with word and its 
    respective count 
       sample example -  [('sudh', 6 ) , ('kumar',3)]
"""

ts=TaskSolution()

#Reading 1 file
one_data = ts.read_one_file('vocab.enron.txt')
print(len(one_data))

#Reading all file
all_data = ts.read_all_files()
print(len(all_data))

# Solving q1 with 1 file data
print(ts.task_q1(one_data))

# Solving q1 with all files data
print(ts.task_q1(all_data))

"""
q2 = try to perform a reduce operation to get a count of all the word starting with same alphabet
        sample examle = [(a,56) , (b,34),...........]
"""

# Solving q2 with 1 file data
print(ts.task_q2(one_data))

# Solving q2 with all files data
print(ts.task_q2(all_data))

"""
q3 = Try to filter out all the words from dataset . 

    .001.abstract = abstract
    =.002 = delete
"""

# Solving q3 with 1 file data
print(ts.task_q3(one_data))

# Solving q3 with all files data
print(ts.task_q3(all_data))

"""
q4 = create a tuple set of all the records avaialble in all the five file and then store it in sqllite DB . 
    (aah,>=,354,fdsf,wer)
"""

# create db
conn=SqliteOperation('task_1.db')

table='bag_of_words'

# create table
table_structure = {"file1_enron": "text", "file2_kos": "text", "file3_nips": "text",
                   "file4_nytimes": "text", "file5_pubmed": "text"}
print(conn.create_table(table, table_structure))

# checking if any record is present in bag_of_words table
print(conn.select_data_table(table))

#Solving q4
print(ts.task_q4(conn,table))

res=conn.select_data_table(table)
for i in res:
    print(i)

conn.close_connection()

# Solving q4 in one go
print(ts.task_q4_all('task_final.db',table))

conn1=SqliteOperation('task_all.db')
res=conn1.select_data_table(table)
for i in res:
    print(i)