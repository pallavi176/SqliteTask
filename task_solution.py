import re
import os
from custom_logger import CustomLogger
from sqliteoperation import SqliteOperation

class TaskSolution:
    log = CustomLogger.log("task.log")

    def read_one_file(self,file_name):
        """
        This method will read 1 txt file and return its content in the form of list
        """
        try:
            if file_name.endswith('.txt'):
                self.log.info(f"Reading Data from file: {file_name}")
                f = open(file_name, encoding="utf8")
                data = f.read().splitlines()
                return data
            else:
                elf.log.error("Raising exception since file is not present or not a txt file")
                raise Exception(f"You have not entered a valid txt file: {file_name}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def read_all_files(self,path=None):
        """
        This method will read all txt file from the current/given path
        and return their content in the form of list
        """
        try:
            if path:
                file_names = os.listdir(path)
            else:
                file_names = os.listdir()
            self.log.info("Reading Data from all the files")
            data_list = []
            for file_name in file_names:
                if file_name.endswith('.txt'):
                    data = self.read_one_file(file_name)
                    data_list.extend(data)
            return data_list
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def task_q1(self,data_list):
        """
        This method will try to find out a count of each and every word in a respective file
        return a list of tuple with word and its respective count
        sample example -  [('sudh', 6 ) , ('kumar',3)]
        """
        try:
            if isinstance(data_list, list):
                self.log.info("Solving question 1")
                res = {}
                for i in data_list:
                    if i in res:
                        res[i] += 1
                    else:
                        res[i] = 1
                res_list = [(k, v) for k, v in res.items()]
                self.log.info("Returning list of tuples of count of each word")
                return res_list
            else:
                elf.log.error("Raising exception since list of data is not passed")
                raise Exception(f"You have not entered a list of data: {data_list}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def task_q2(self,data_list):
        """
        This method will try to perform a reduce operation to get a count of all the word
        starting with same alphabet
        sample examle = [(a,56) , (b,34),...........]
        """
        try:
            if isinstance(data_list, list):
                self.log.info("Solving question 2")
                res = {}
                for i in data_list:
                    if i[0] in res:
                        res[i[0]] += 1
                    else:
                        res[i[0]] = 1
                res_list = [(k, v) for k, v in res.items()]
                self.log.info("Returning list of tuples of count of each first character")
                return res_list
            else:
                elf.log.error("Raising exception since list of data is not passed")
                raise Exception(f"You have not entered a list of data: {data_list}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def task_q3(self,data_list):
        """
        This method will try to filter out all the words from dataset .
        .001.abstract = abstract
        =.002 = delete
        """
        try:
            if isinstance(data_list, list):
                self.log.info("Solving question 3")
                pattern = re.compile(r'[A-Za-z]+')
                res_list = []
                for d in data_list:
                    a = ''.join(pattern.findall(d))
                    if a:
                        res_list.append(a)
                self.log.info("Returning only list of words out of all the data")
                return res_list
            else:
                elf.log.error("Raising exception since list of data is not passed")
                raise Exception(f"You have not entered a list of data: {data_list}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def zip_all_files_data(self,path=None):
        """
        This method will read all txt file from the current/given path
        and return their list of zipped files
        """
        try:
            if path:
                file_names = os.listdir(path)
            else:
                file_names = os.listdir()
            self.log.info("Reading Data from all the files")
            c = 1
            for file_name in file_names:
                if file_name.endswith('.txt'):
                    globals()['data_{0}'.format(c)] = self.read_one_file(file_name)
                    c += 1
            data_list=list(zip(data_1,data_2,data_3,data_4,data_5))
            return data_list
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def task_q4(self,conn,table,path=None):
        """
        This method will create a tuple set of all the records avaialble in all the five file
        and then store it in sqllite DB .
        (aah,>=,354,fdsf,wer)
        """
        try:
            self.log.info("Solving question 4")
            data_list=self.zip_all_files_data(path)
            msg = conn.bulk_insert_table(table, data_list)
            self.log.info(f"{msg}")
            c = conn.select_count_table(table)
            self.log.info(f"Returning number of rows inserted to table: {table} is: {c}")
            return c
        except Exception as e:
            print(e)
            self.log.exception(str(e))


    def task_q4_all(self,db,table,v=1,path=None):
        """
        This method will create a tuple set of all the records avaialble in all the five file
        and then store it in sqllite DB .
        (aah,>=,354,fdsf,wer)
        """
        try:
            if db.endswith('.db'):
                self.log.info("Solving question 4")
                data_list=self.zip_all_files_data(path)
                conn_1 = SqliteOperation(db)
                msg=f"Connected with sqlite db: {db}"
                if v==1:
                    print(msg)
                self.log.info(f"{msg}")
                table_structure = {"file1_enron": "text", "file2_kos": "text", "file3_nips": "text",
                                   "file4_nytimes": "text", "file5_pubmed": "text"}
                msg = conn_1.create_table(table, table_structure)
                self.log.info(f"{msg}")
                if v==1:
                    print(msg)
                res=conn_1.select_data_table(table)
                if v==1:
                    print("No data present in",res)
                msg = conn_1.bulk_insert_table(table, data_list)
                if v==1:
                    print(msg)
                self.log.info(f"{msg}")
                c = conn_1.select_count_table(table)
                self.log.info(f"Returning number of rows inserted to table: {table} is: {c}")
                conn_1.close_connection()
                return c
            else:
                elf.log.error("Raising exception since valid sqlite db name is not passed")
                raise Exception(f"You have not entered a valid sqlite db name: {db}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))