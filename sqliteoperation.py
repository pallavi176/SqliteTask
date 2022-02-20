import sqlite3
from custom_logger import CustomLogger

class SqliteOperation:
    log = CustomLogger.log('sqlite.log')

    def __init__(self,db):
        """
        Intializing with Variables
        """
        try:
            self.db=sqlite3.connect(db)
            self.cursor=self.db.cursor()
            self.log.info(f"Initialized all the variables: {db}")
        except Exception as e:
            self.log.exception(str(e))
            print(e)

    def create_table(self, tabl, tabl_struct):
        """
        This method will create table
        """
        try:
            if type(tabl_struct) == dict:
                tabl_query='create table {} ('.format(tabl)
                for k in tabl_struct:
                    tabl_query += k + ' '+ tabl_struct[k]+', '
                tabl_query = tabl_query[:-2]
                tabl_query += ')'
                #print(tabl_query)
                self.cursor.execute(tabl_query)
                self.log.info(f"Executed Query: {tabl_query}")
                self.log.info(f"Table: {tabl} created successfully with format: {tabl_struct}")
                return f"Table: {tabl} created successfully"
            else:
                self.log.error(f"Table format not correct: {tabl_struct}")
                return "Table format not correct"
        except Exception as e:
            self.log.exception(str(e))
            print(e)

    def select_data_table(self, tabl):
        """
        Select Data from table
        """
        try:
            query = "select * from {}".format(tabl)
            self.cursor.execute(query)
            self.log.info(f"Executed Query: {query}")
            res = self.cursor.fetchall()
            self.log.warning(f"Returning data from Table: {tabl}.")
            return res
        except Exception as e:
            self.log.exception(str(e))
            print(e)

    def bulk_insert_table(self, tabl, data_list):
        """
        Bulk insertion to table
        """
        try:
            for i in data_list:
                # s = '"' + i[0].replace(',', '","') + '"'
                # print(s)
                query = f'insert into {tabl} values {i}'
                self.cursor.execute(query)
            self.db.commit()
            self.log.info(f"Executed sample Query: {query}")
            self.log.info(f"Bulk data inserted to Table: {tabl}")
            return f"Bulk data inserted to Table: {tabl}"
        except Exception as e:
            self.log.exception(str(e))
            print(e)


    def drop_table(self, tabl):
        """
        Drop table
        """
        try:
            query = "drop table {}".format(tabl)
            self.log.info(f"Executed Query: {query}")
            self.cursor.execute(query)
            self.log.info(f"Table: {tabl} dropped")
            return f"Table: {tabl} dropped"
        except Exception as e:
            self.log.exception(str(e))
            print(e)

    def select_count_table(self, tabl):
        """
        Select Number of records in a table
        """
        try:
            query = "select count(*) from {}".format(tabl)
            self.cursor.execute(query)
            self.log.info(f"Executed Query: {query}")
            res = self.cursor.fetchall()
            self.log.info(f"Number of records present in table: {tabl} is: {res}")
            return res
        except Exception as e:
            self.log.exception(str(e))
            print(e)

    def close_connection(self):
        """
        Select Number of records in a table
        """
        try:
            self.db.close()
            self.log.info("Db connection closed")
        except Exception as e:
            self.log.exception(str(e))
            print(e)


