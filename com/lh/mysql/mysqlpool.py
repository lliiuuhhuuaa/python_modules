import os
import threading
from configparser import ConfigParser

import pymysql


# 使用方法
# 配置文件application.conf
# from 包名 import mysqlpool
# db = mysqlpool.DBPool()
# db.execute_by_single("select * from user limit %s", 10)
# 参数对象
class PoolInfo:
    # 主机
    host = "127.0.0.1"
    # 用户名
    user = "root"
    # 密码
    password = "root"
    # 数据库
    database = "test"
    # 端口
    port = 3306
    # 编码
    charset = "utf8"
    # 池连接数量
    count = 10
# 连接池实现
class DBPool:
    _instance_lock = threading.Lock()
    connects = []
    param = {}

    # 构造函数
    def __init__(self):
        pass

    # 实例
    def __new__(cls, *args, **kwargs):
        if not hasattr(DBPool, "_instance"):
            with DBPool._instance_lock:
                if not hasattr(DBPool, "_instance"):
                    DBPool._instance = object.__new__(cls)
                    DBPool.init_param(DBPool._instance)
        return DBPool._instance

    # 初始化参数
    def init_param(self):
        root_dir = os.path.dirname(os.path.abspath('../'))
        cp = ConfigParser()
        cp.read(root_dir + '/application.conf')
        host = cp.get("mysql", "host")
        if host:
            self.param["host"] = host
        user = cp.get("mysql", "user")
        if user:
            self.param["user"] = user
        password = cp.get("mysql", "password")
        if password:
            self.param["password"] = password
        db = cp.get("mysql", "database")
        if db:
            self.param["database"] = db
        port = cp.getint("mysql", "port")
        if port:
            self.param["port"] = port
        charset = cp.get("mysql", "charset")
        if charset:
            self.param["charset"] = charset
        count = cp.getint("mysql", "count")
        if count:
            self.param["count"] = count
        self.create_connection()

    #     创建连接池
    def create_connection(self):
        is_close = False
        count = self.param["count"]
        try:
            while count:
                count -= 1
                # 连接数据库
                cn = pymysql.connect(host=self.param["host"], user=self.param["user"],
                                     password=self.param["password"], database=self.param["database"],
                                     port=self.param["port"], charset=self.param["charset"])
                if cn.open:
                    # 连接正常放入连接池
                    self.connects.append(cn)
            if len(self.connects) < 1:
                # 连接池数量异常
                raise RuntimeError("数据库连接池异常")
        except pymysql.err.OperationalError as e:
            is_close = True
            raise RuntimeError("数据库连接失败", e)
        finally:
            if is_close:
                for cn in self.connects:
                    if cn.open:
                        cn.close()

    # 获取
    def get_conn(self):
        if len(self.connects) < 1:
            self.create_connection(2)
        return self.connects.pop(0)

    # 关闭
    def close(self, connection):
        if connection.open:
            self.connects.append(connection)
        elif len(self.connects) < self.param["count"]:
            self.create_connection(self.param["count"] - len(self.connects))

    # 查询单记录
    def execute_by_single(self, sql, *args):
        connection = self.get_conn()
        cursor = connection.cursor()
        result = {}
        try:
            count = cursor.execute(sql, args)
            if not count:
                return result
            field = cursor.description
            dt = cursor.fetchone()
            index = 0
            for val in dt:
                result[field[index][0]] = val
                index += 1
            return result
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.close(connection)

    # 查询多记录
    def execute_by_many(self, sql, *args):
        connection = self.get_conn()
        cursor = connection.cursor()
        result = []
        try:
            count = cursor.execute(sql, args)
            if not count:
                return result
            field = cursor.description
            for dt in cursor.fetchall():
                obj = {}
                result.append(obj)
                index = 0
                for val in dt:
                    obj[field[index][0]] = val
                    index += 1
            return result
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.close(connection)

    # 更新操作
    # 删除操作
    # 插入操作
    # 执行sql
    def execute(self, sql, *args):
        connection = self.get_conn()
        cursor = connection.cursor()
        try:
            return cursor.execute(sql, args)
        finally:
            if cursor:
                cursor.close()
            if connection:
                self.close(connection)
