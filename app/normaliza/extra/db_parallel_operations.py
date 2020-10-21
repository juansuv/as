from multiprocessing import Pool
import pandas as pd
import time
import cx_Oracle
import pyodbc
import uuid
from sqlalchemy import create_engine, event

import urllib

class SqlParallel:

    expected_data = 0
    chunk_size =500000
    chunk_size_save = 500000

    def __init__(self, expected_data=None):
        expected_data = 0
        chunk_size = 500000
        chunk_size_save = 500000


    def launch_query_oracle(self, chunk):
        db_info = chunk[2]
        dsn_tns = cx_Oracle.makedsn(db_info['server'], db_info['port'], service_name=db_info['sid'])
        ora_conn = cx_Oracle.connect(user=db_info['usuario'], password=db_info['password'], dsn=dsn_tns)

        df_ora = pd.read_sql(chunk[0], con=ora_conn, params=None)
        ora_conn.close()
        # self.dataframes[chunk[1]] = df_ora
        # df_ora.to_csv(f"{chunk[3]}/data_{chunk[1]}.csv")
        return df_ora

    def launch_query_sql(self, chunk):
        db_info = chunk[2]

        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + db_info['server'] + ';DATABASE=' + db_info['sid'] + '; UID=' +
            db_info['usuario'] + '; PWD=' + db_info['password'] )

        # conn2 = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\suarez\Desktop\testdb.accdb;')
        df_ora = pd.read_sql(chunk[0], con=conn, params=None)
        # query.to_sql('tabla', schema='dbo', con=engine)

        # df_ora = pd.read_sql(chunk[0], con=ora_conn, params=None)
        conn.close()
        # self.dataframes[chunk[1]] = df_ora
        # df_ora.to_csv(f"{chunk[3]}/data_{chunk[1]}.csv")
        return df_ora

    def launch_dataframe_oracle(self, chunk):
        db_info = chunk[2]

        engine = create_engine("oracle+cx_oracle://" + db_info['usuario'] + ":" + db_info[
            'password'] + "@(DESCRIPTION = (LOAD_BALANCE=on) (FAILOVER=ON) (ADDRESS = (PROTOCOL = TCP)(HOST = " +
                               db_info['server'] + ")(PORT = " + str(
            db_info['port']) + ")) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = " + db_info['sid'] + ")))")

        chunk[0].to_sql(db_info['unique_id'], con=engine, if_exists='append')

        return chunk

    def launch_dataframe_sql(self, chunk):
        db_info = chunk[2]


        quoted = urllib.parse.quote_plus(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + db_info['server'] + ';DATABASE=' + db_info['sid'] + '; UID=' +
            db_info['usuario'] + '; PWD=' + db_info['password'])
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))



        chunk[0].to_sql(db_info['unique_id'], con=engine, if_exists='append')

        return chunk

    def get_data_oracle(self, connection, query="", expected_data=0):
        # me conecto
        # creo el pool

        num_proces = connection['num_process']
        num_chunck = connection['num_chunck']

        num_process_final = int(expected_data) // int(num_chunck)
        if expected_data % num_chunck > 0:
            num_process_final = num_process_final + 1

        num_process_permit = int(num_process_final) // int(num_proces)

        if num_process_final % num_proces > 0:
            num_process_permit = num_process_permit + 1

        tamaño_chuck_final2 = expected_data // num_process_permit
        tamaño_chuck_final = num_proces * num_chunck

        print("num_proces", num_proces)
        print("num_chunck", num_chunck)
        print("num_process_final", num_process_final)
        print("num_process_permit", num_process_permit)
        print("tamaño_chuck_final", tamaño_chuck_final)



        chunks = []

        query = f"SELECT * FROM ({query})"

        df_data = pd.DataFrame()
        for itera in range(0, num_process_permit):
            print("interacion", itera)
            processes_to_launch = num_proces
            self.chunk_size = num_chunck
            chunks = []

            for i in range(1, processes_to_launch + 1):
                if expected_data < self.chunk_size:
                    current_query = query + f'WHERE indice BETWEEN {(itera*tamaño_chuck_final)+self.chunk_size * (i - 1)} and {(itera*tamaño_chuck_final)+self.chunk_size * (i - 1) + expected_data}'
                    chunks.append((current_query, i, connection))
                    break
                current_query = query + f'WHERE indice BETWEEN {(itera*tamaño_chuck_final)+self.chunk_size * (i - 1)} and {(itera*tamaño_chuck_final)+self.chunk_size * i}'
                chunks.append((current_query, i, connection))
                expected_data -= self.chunk_size

            pool = Pool(processes=processes_to_launch)  # process per core
            result = pool.map(self.launch_query_oracle, chunks)
            pool.close()
            pool.join()

            for df_result in result:
                print(df_result)
                print(type(df_result))
                print(type(df_data))
                df_data = pd.concat([df_data, df_result]).drop_duplicates(keep=False)
                print(df_data)
            del result
        return df_data

    def get_data_sql(self, connection, query="", expected_data=0):
        # me conecto
        # creo el pool

        num_proces=connection['num_process']
        num_chunck=connection['num_chunck']

        num_process_final= int(expected_data)//int(num_chunck)
        if expected_data % num_chunck > 0:
            num_process_final = num_process_final + 1

        num_process_permit= int(num_process_final)//int(num_proces)

        if num_process_final % num_proces > 0:
            num_process_permit = num_process_permit + 1

        tamaño_chuck_final2= expected_data // num_process_permit
        tamaño_chuck_final=num_proces*num_chunck

        print("num_proces",num_proces)
        print("num_chunck",num_chunck)
        print("num_process_final",num_process_final)
        print("num_process_permit",num_process_permit)
        print("tamaño_chuck_final",tamaño_chuck_final)
        query = f"WITH data AS ({query})"
        df_data=pd.DataFrame()
        for itera in range(0,num_process_permit):
            print("interacion",itera)
            processes_to_launch=num_proces
            self.chunk_size=num_chunck
            chunks = []



            for i in range(1, processes_to_launch + 1):
                print("creando el query para el procesor num",i)
                if expected_data < self.chunk_size:
                    current_query = query + f'SELECT * FROM data WHERE indice BETWEEN {(itera*tamaño_chuck_final)+self.chunk_size * (i - 1)} and {(itera*tamaño_chuck_final)+self.chunk_size * (i - 1) + expected_data}'
                    chunks.append((current_query, i, connection))
                    print("quedaquerfinal", current_query)
                    break
                current_query = query + f"SELECT * FROM data WHERE indice BETWEEN {(itera*tamaño_chuck_final)+self.chunk_size * (i - 1)} and {(itera*tamaño_chuck_final)+self.chunk_size * i}"
                print("quedaquer",current_query)
                chunks.append((current_query, i, connection))
                expected_data -= self.chunk_size
                print("expected data",expected_data)


            pool = Pool(processes=processes_to_launch)  # process per core
            result = pool.map(self.launch_query_sql, chunks)
            pool.close()
            pool.join()



            #df_data = pd.concat([df_data, df], ignore_index=True, axis=0).drop_duplicates(keep=False)
            for df_result in result:

                df_data = pd.concat([df_data, df_result]).drop_duplicates(keep=False)

            del result

        print("union df",df_data)
        return df_data

    def save_data_oracle(self, connection, query=pd.DataFrame, expected_data=0):
        # me conecto
        # creo el pool
        processes_to_launch = int(expected_data) // int(self.chunk_size_save)
        if expected_data % self.chunk_size_save > 0:
            processes_to_launch = processes_to_launch + 1
        chunks = []
        db_info = connection
        engine = create_engine("oracle+cx_oracle://" + db_info['usuario'] + ":" + db_info[
            'password'] + "@(DESCRIPTION = (LOAD_BALANCE=on) (FAILOVER=ON) (ADDRESS = (PROTOCOL = TCP)(HOST = " +
                               db_info['server'] + ")(PORT = " + str(
            db_info['port']) + ")) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = " + db_info['sid'] + ")))")



        for i in range(1, processes_to_launch + 1):
            if expected_data < self.chunk_size_save:
                current_query = query[self.chunk_size_save * (i - 1):self.chunk_size_save * (i - 1) + expected_data].copy()
                chunks.append((current_query, i, connection))
                break
            current_query = query[self.chunk_size_save * (i - 1):self.chunk_size_save * i].copy()

            chunks.append((current_query, i, connection))
            expected_data -= self.chunk_size_save

        table_new = chunks[0][0]

        print("tablenew", table_new)
        print("chunks", type(chunks[0]))
        chunks.pop(0)
        table_new.to_sql(db_info['unique_id'], con=engine, if_exists='append')
        del query,table_new

        pool = Pool(processes=processes_to_launch)  # process per core
        result = pool.map(self.launch_dataframe_oracle, chunks)
        pool.close()
        pool.join()
        return result

    def save_data_sql(self, connection, query=pd.DataFrame, expected_data=0):
        # me conecto sql
        # creo el pool
        first=0
        num_proces = connection['num_process']
        num_chunck = connection['num_chunck']

        num_process_final = int(expected_data) // int(num_chunck)
        if expected_data % num_chunck > 0:
            num_process_final = num_process_final + 1

        num_process_permit = int(num_process_final) // int(num_proces)

        if num_process_final % num_proces > 0:
            num_process_permit = num_process_permit + 1

        tamaño_chuck_final2 = expected_data // num_process_permit
        tamaño_chuck_final = num_proces * num_chunck

        print("num_proces", num_proces)
        print("num_chunck", num_chunck)
        print("num_process_final", num_process_final)
        print("num_process_permit", num_process_permit)
        print("tamaño_chuck_final", tamaño_chuck_final)

        df_data = pd.DataFrame()

        if 'indice' in query.columns:
            query.drop(columns=['indice'],inplace=True)
        print("columns query",query.columns)

        db_info = connection

        quoted = urllib.parse.quote_plus(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + db_info['server'] + ';DATABASE=' + db_info[
                'sid'] + '; UID=' +
            db_info['usuario'] + '; PWD=' + db_info['password'])
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

        for itera in range(0, num_process_permit):
            print("interacion", itera)
            processes_to_launch = num_proces
            self.chunk_size_save = num_chunck
            chunks = []

            for i in range(1, processes_to_launch + 1):
                print("Creando query para el proceo num",i,"Exepected data",expected_data)
                if expected_data < self.chunk_size_save:
                    current_query = query[(itera*tamaño_chuck_final)+self.chunk_size_save * (i - 1):(itera*tamaño_chuck_final)+self.chunk_size_save * (i - 1) + expected_data].copy()
                    chunks.append((current_query, i, connection))
                    print("query_final", current_query)
                    break
                current_query = query[(itera*tamaño_chuck_final)+self.chunk_size_save * (i - 1):(itera*tamaño_chuck_final)+self.chunk_size_save * i].copy()

                chunks.append((current_query, i, connection))
                print("query_final", current_query)
                expected_data -= self.chunk_size_save
                print("expected data", expected_data)

            if first== 0:
                table_new =chunks[0][0]
                chunks.pop(0)
                table_new.to_sql(db_info['unique_id'], con=engine, if_exists='append')
                first =1
                del table_new

            pool = Pool(processes=processes_to_launch)  # process per core
            result = pool.map(self.launch_dataframe_sql, chunks)
            pool.close()
            pool.join()

        del query

        return result





# if __name__ == '__main__':

