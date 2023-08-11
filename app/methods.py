import psycopg2
import os

PG_LOGIN = {
    'HOST': os.environ.get("DB_HOST", "localhost"),
    'DATABASE': os.environ.get("DB_NAME", "megasena"),
    'USER': os.environ.get("DB_USER", "defendi"),
    'PWD': os.environ.get("DB_PASS", "5849"),
    'SCHEMA': os.environ.get("DB_SCHEMA", "public")
}

class SqlMethods:
    
    def __init__(self):
        self.host = PG_LOGIN['HOST']
        self.db = PG_LOGIN['DATABASE']
        self.user = PG_LOGIN['USER']
        self.pwd = PG_LOGIN['PWD']
        self.sch = PG_LOGIN['SCHEMA']

        try:
            self.con = psycopg2.connect(host=self.host, database=self.db, user=self.user, password=self.pwd)
        except:
            raise TypeError('A conexão não pôde ser estabelecida!')

        self.cur = self.con.cursor()

    def bd_close_connection(self):
        self.con.close()
        self.cur.close()

    def sql_get_tables_name(self):
        sql_command = f"SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '{self.sch}'"

        try:
            self.cur.execute(sql_command)
            list_tuples = self.cur.fetchall()
            rows_list = [list(item) for item in list_tuples]

            table_list = []

            for item in rows_list:
                table_list.extend(item)

            return table_list
        except:
            self.con.rollback()
            return -1

    def sql_select_from_where(self, cols, table, filt):
        if isinstance(cols, list) == True:
            cols_list_str = ",".join(cols)
            sql_command = f"SELECT {cols_list_str} FROM {self.sch}.{table} WHERE {filt};"
        else:
            sql_command = f"SELECT {cols} FROM {self.sch}.{table} WHERE {filt};"

        try:
            self.cur.execute(sql_command)
            list_tuples = self.cur.fetchall()
            rows_list = [list(item) for item in list_tuples]

            return rows_list
        except:
            self.con.rollback()
            return -1

    def sql_max_val_col(self, col, table):
        sql_command = f"SELECT MAX({col}) FROM {self.sch}.{table}"

        try:
            self.cur.execute(sql_command)
            list_tuples = self.cur.fetchall()
            col_max = [list(item) for item in list_tuples]

            return col_max[0][0]
        except:
            self.con.rollback()
            return -1

    def sql_get_table_cols(self, table):
        sql_command = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{self.sch}' AND table_name = '{table}';"

        try:
            self.cur.execute(sql_command)
            list_tuples = self.cur.fetchall()
            rows_list = [list(item)[0] for item in list_tuples]

            return rows_list
        except:
            self.con.rollback()
            return -1

    def sql_insert_into(self, table, list_dict_data):
        table_str = table.replace("'", "")
        dict_cols = list(list_dict_data[0].keys())

        cols_sql_str = ",".join([str(item) for item in dict_cols])

        vals_str = ""

        for row_idx, row_dict in enumerate(list_dict_data):
            row_vals = []
            for col in dict_cols:
                row_vals.append(row_dict[col])

            cols_vals_str = ",".join(
                ["'" + str(item) + "'" for item in row_vals])
            if row_idx != len(list_dict_data) - 1:
                vals_str = vals_str + f"({cols_vals_str}),"
            else:
                vals_str = vals_str + f"({cols_vals_str});"

        sql_command = f"INSERT INTO {self.sch}.{table_str} ({cols_sql_str}) VALUES " + vals_str

        try:
            self.cur.execute(sql_command)
            self.con.commit()
            # print('[OK] POSTGRESQL - COMMIT')
            return 1
        except:
            self.con.rollback()
            # print('[ERRO] POSTGRESQL - ROLLBACK')
            return -1

    def sql_update(self, table, set_dict, filt):
        set_str = ''
        dict_len = len(set_dict)
        dict_count = 0
        for col in set_dict.keys():
            col_val = set_dict[col]

            if type(col_val) == int or type(col_val) == float:
                set_str += f"{str(col)} = {set_dict[col]}"
            else:
                set_str += f"{str(col)} = '{set_dict[col]}'"

            dict_count += 1

            if dict_count < dict_len:
                set_str = set_str + ','
            else:
                pass

        if filt == '':
            sql_command = f"UPDATE {self.sch}.{table} SET {set_str};"
        else:
            sql_command = f"UPDATE {self.sch}.{table} SET {set_str} WHERE {filt};"

        # print(sql_command)
        try:
            self.cur.execute(sql_command)
            self.con.commit()
            return 1
        except:
            self.con.rollback()
            return -1

    def sql_delete(self, table, filt):
        if filt == '':
            return -1
        else:
            sql_command = f"DELETE FROM {self.sch}.{table} WHERE {filt}"

        try:
            self.cur.execute(sql_command)
            self.con.commit()
            return 1
        except:
            self.con.rollback()
            return -1

    def sql_set_command(self, str_command):

        sql_command = str_command
        # print(sql_command)
        try:
            self.cur.execute(sql_command)
            list_tuples = self.cur.fetchall()
            rows_list = [list(item) for item in list_tuples]
            return rows_list

        except Exception as e:
            # print(e)
            self.con.rollback()
            return -1
        
