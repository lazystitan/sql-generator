from random import random, randint
from hashlib import md5

import pymysql.cursors


class CommonDao:
    connection = None
    tables_description = {}

    def __init__(self, host: str, user: str, password: str, dbname: str, charset: str = 'utf8mb4',
                 cursorclass: pymysql.cursors.Cursor = pymysql.cursors.DictCursor):
        config = {
            'host': host,
            'user': user,
            'password': password,
            'db': dbname,
            'charset': charset,
            'cursorclass': cursorclass
        }
        print(config)
        self.connection = pymysql.connect(**config)
        with self.connection.cursor() as c:
            sql = 'show tables;'
            c.execute(sql)
            tables_name = c.fetchall()
            for table_name_pre in tables_name:
                table_name = list(table_name_pre.values())[0]
                sql = 'describe ' + table_name + ';'
                c.execute(sql)
                self.tables_description[table_name] = c.fetchall()

    def tables_name(self):
        return list(self.tables_description.keys())

    def describe(self, table_name):
        try:
            return self.tables_description[table_name]
        except KeyError:
            return None

    @staticmethod
    def generate_params(fields_names):
        return str(fields_names)[1:-1].replace('\'', '')

    def generate_insert(self, table_name, auto_increment=False):
        result = 'insert into ' + table_name + ' ( %s ) values ( %s );'
        description = self.describe(table_name)
        fields = []
        for field in description:
            if field['Extra'].find('auto_increment') != -1 and not auto_increment:
                continue
            else:
                fields.append(field['Field'])
        counter = len(fields)
        values = self.generate_params(['%s'] * counter)
        fields = self.generate_params(fields)
        return result % (fields, values)

    def generate_update(self, table_name):
        pass

    def generate_delete(self, table_name):
        pass

    def generate_replace(self, table_name):
        pass

    def __del__(self):
        self.connection.close()


def main():
    email_tail = ['test.com', '163.com', 'outlook.com', 'gmail.com',
                  '126.com', 'qq.com', 'sina.com', 'shu.edu.com', '186.com',
                  'sohu.com', 'tetx.com', 'simple.com']
    dao = CommonDao('localhost', 'root', 'root', 'sql_test')
    print(dao.tables_name())
    print(dao.describe('users'))
    insert_sql = dao.generate_insert('users')
    with open('English_Names_Corpus（2W）.txt', 'r', encoding='utf8') as f:
        f.readline()
        f.readline()
        f.readline()
        names = f.readlines()

    names = [name.strip() for name in names]
    users = []
    m = md5()
    for name in names:
        m.update(str(randint(100000, 999999 + 1)).encode('utf-8'))
        pswd = m.hexdigest()
        temp = (name + '@' + email_tail[randint(0, len(email_tail) - 1)], pswd)
        users.append(temp)
    config = {'host': 'localhost',
              'user': 'root',
              'password': 'root',
              'db': 'sql_test',
              'charset': 'utf8mb4',
              'cursorclass': pymysql.cursors.DictCursor}

    print(insert_sql)
    print(len(users))
    conn = pymysql.connect(**config)
    with conn.cursor() as c:
        c.executemany(insert_sql, users)
    conn.commit()
    conn.close()


def test():
    config = {'host': 'localhost',
              'user': 'root',
              'password': 'root',
              'db': 'sql_test',
              'charset': 'utf8mb4',
              'cursorclass': pymysql.cursors.DictCursor}

    conn = pymysql.connect(**config)
    c = conn.cursor()
    c.execute('select count(*) as count from users')
    res = c.fetchall()
    print(res)
    conn.close()


if __name__ == '__main__':
    # main()
    test()
