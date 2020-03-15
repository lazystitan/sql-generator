from random import random, randint
from hashlib import md5

import pymysql.cursors


class CommonDao:
    __connection = None
    __tables_description = {}

    def __init__(self, host: str, user: str, password: str, dbname: str, charset: str = 'utf8mb4',
                 cursorclass: pymysql.cursors.Cursor = pymysql.cursors.DictCursor):
        """

        :param host: str
        :param user: str
        :param password: str
        :param dbname: str
        :param charset: str
        :param cursorclass: Cursor

        初始化连接，获取数据库信息
        """
        config = {
            'host': host,
            'user': user,
            'password': password,
            'db': dbname,
            'charset': charset,
            'cursorclass': cursorclass
        }
        print(config)
        self.__connection = pymysql.connect(**config)
        with self.__connection.cursor() as c:
            sql = 'show tables;'
            c.execute(sql)
            tables_name = c.fetchall()
            for table_name_pre in tables_name:
                table_name = list(table_name_pre.values())[0]
                sql = 'describe ' + table_name + ';'
                c.execute(sql)
                self.__tables_description[table_name] = c.fetchall()

    def get_tables_name(self):
        return list(self.__tables_description.keys())

    def get_describe(self, table_name : str):
        try:
            return self.__tables_description[table_name]
        except KeyError:
            return None

    @staticmethod
    def generate_params(fields_names):
        return str(fields_names)[1:-1].replace('\'', '')

    def generate_insert(self, table_name, auto_increment=False, ignore=False, replace=False):
        assert (ignore and not replace) or (not ignore and replace) or (not ignore and not replace)
        if ignore:
            header = 'insert ignore into '
        elif replace:
            header = 'replace into '
        else:
            header = 'insert into '
        result = header + table_name + ' ( %s ) values ( %s );'
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

    def __del__(self):
        self.__connection.close()


def init():
    return CommonDao('localhost', 'root', '', 'lambkingo')


class CommonDateTime:
    def __init__(self):
        pass

def main():
    dao = init()
    rs = dao.get_describe('goods')
    for p in rs:
        print(p)


if __name__ == '__main__':
    main()
