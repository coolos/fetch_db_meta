
from __future__ import (absolute_import, division, print_function, unicode_literals)

try:
    from urllib2 import urlopen
except ImportError:
    from urllib.request import urlopen
import json
import re
import MySQLdb as mdb
from socket import *

dbuser="xx"
dbpass="yy"
dbhost =  "zz"
dbport = [3306, 3307, 3308, 3309, 3310]

maxtrix_config = {
    'host': 'zz',
    'port': 3307,
    'user': 'xx',
    'passwd': 'yy',
    'db': 'product_db',
    'charset': 'utf8'
}

all_dbhost = []
filename = 'ucc_prod.json'
json_url = "http://ssss"
response = urlopen(json_url)
req = response.read()
with open('ucc_prod.json','wb') as f:
    f.write(req)
file_urllib = json.loads(req)



def port_scan(host, port):
    conn = socket(AF_INET, SOCK_STREAM)
    try:
        conn.connect((host, port))
        print((host, port, 'is available'))
        return 0
    except Exception as e:
        #print(host, port, "is not available")
        return 1
    finally:
        conn.close()

def go_next(dicts):
    for key,value in dicts.items():
        #print("here is Key:{}, ||||||values is: {} == key type is {}, values type is {}".format(key, value, type(key),type(value)))
        #print("key is: {}".format(key))
        if (re.search("[Mm][Yy][Ss][Qq][Ll]",key)):
            print("--------------------------------------gotted key: {},    |||     value: {}".format(key, value))
            if(isinstance(value, list)):
                print("===================================gotted value: {};\n".format(value))
                for i in value:
                    print("within a ilist is;{}, type is is:".format(i,type(i)))
                    for j in i.split(","):
                        if(j.startswith("10.")):
                            all_dbhost.append(j)

        #print("value's type {},value is {}\n".format(type(value),value))
        if (isinstance(value,dict)):
            #print("hit")
            go_next(value)


def main():
    ### for key,value in file_urllib['data'].items():
    ###     go_next(value)

    """master host"""
    all_dbhost = []
    #all_dbhost_de = [item for item in all_dbhost if all_dbhost.count(item) == 1]
    all_dbhost_de = sorted(set(all_dbhost), key=all_dbhost.index)
    print("result is: {}".format(all_dbhost_de))
    try:
        for adb in all_dbhost_de:
            print("the db host is: {}".format(adb).upper())
            for aport in dbport:
                if(port_scan(adb, aport) == 0):
                    parent_ip = "";
                    parent_port = 0
                    has_child = 0
                    is_slave = 0
                    read_only =  0
                    is_rds = 0
                    platform_id = -9
                    dbs_sql = []
                    user_sql = []

                    """check slave status for if it is a slave"""
                    db_conn = mdb.connect(host=adb, port=aport,user=dbuser, passwd=dbpass, db="mysql", charset='utf8')
                    cursor = db_conn.cursor()
                    cursor.execute('show slave status')
                    rows = cursor.fetchall()
                    for row in rows:
                        print("row is: {}, row's type is : {}".format(row,type(row)))
                        parent_ip = row[1]
                        parent_port = row[3]
                        is_slave = 1
                        print("parents ip is {}, parents port is {}".format(parent_ip,parent_port))

                    """check binlog dump proc if it is a master"""
                    cursor.execute('select count(*) from information_schema.processlist where command = "Binlog Dump";')
                    rows = cursor.fetchall()
                    print("binlog dump process count is: {}, type is {}".format(rows, type(rows)))
                    for row in rows:
                        if(isinstance(row[0],int) and row[0] >= 1):
                            has_child = 1
                        else:
                            has_child = 0

                    """check read_only if it is a read only db"""
                    cursor.execute('show variables like "read_only";')
                    rows = cursor.fetchall()
                    print("read only status is: {}, type is {}".format(rows, type(rows)))
                    for row in rows:
                        if(row[1] == 'ON'):
                            read_only = 1
                        else:
                            read_only = 0

                    """show what DBs in the instance"""
                    cursor.execute('show databases;')
                    showdbs = cursor.fetchall()
                    for ashowdb in showdbs:
                        print(ashowdb[0])
                        if(ashowdb[0] != 'information_schema' and ashowdb[0] != 'mysql' and ashowdb[0] != 'performance_schema' \
                                and ashowdb[0] != 'test' and ashowdb[0] != 'sys'):
                            dbs_sql.append( "replace into db_db(ip,port,dbname) values('{}',{},'{}');".format(adb, aport, ashowdb[0]))
                    print(dbs_sql)
                    #for adb_sql in dbs_sql:
                        #print("doing format ----"+ adb_sql)


                    """generate db_instance's insert sql"""
                    maxtrx_sql = "replace into product_db.db_instance (ip, port, parents_ip, parents_port, is_master, is_slave, is_readonly,is_rds,platform_id ) values ('{}', {}, '{}', {}, {}, {}, {},{},{})".format(
                                    adb, aport, parent_ip, parent_port, has_child, is_slave, read_only,is_rds,platform_id)
                    print(maxtrx_sql)


                    """generate db user """
                    cursor.execute("select distinct user,db from mysql.db where user not in ('root','dbsa','repl','mha','dbadmin','databak','dbazabbix','dbamonitor') and db not like '%test%'")
                    dbusers = cursor.fetchall()
                    for adbuser in dbusers:
                        print(adbuser[0],adbuser[1])
                        user_sql.append(
                            "replace into db_db_user(master_host,master_port,dbname,account_name) values('{}',{},'{}','{}');".format(adb, aport,adbuser[1],adbuser[0]))

                    print(user_sql)

                    print("\n\n\n")
                    maxtrix_conn=mdb.connect(**maxtrix_config)
                    with mdb.connect(**maxtrix_config) as conn:
                        with conn as cur:
                    #         cur.execute(maxtrx_sql)
                    #         for adb_sql in dbs_sql:
                    #             cur.execute(adb_sql)
                            for auser_sql in user_sql:
                                cur.execute(auser_sql)
    except:
        import traceback
        traceback.print_exc()
        pass

    finally:
        cursor.close()
        db_conn.close()

def find_parents(ip,port):
    maxtrix_conn = mdb.connect(**maxtrix_config)
    with mdb.connect(**maxtrix_config) as conn:
        with conn as cur:
            cur.execute("select parents_ip,parents_port from db_instance where ip = '{}' and port={};".format(ip,port))
            rows = cur.fetchall()
            print("rows is {}, type is {}".format(rows, type(rows)))
            return rows


def find_children(ip,port):
    db_tree = {}
    maxtrix_conn = mdb.connect(**maxtrix_config)
    with mdb.connect(**maxtrix_config) as conn:
        with conn as cur:
            cur.execute("select ip,port from db_instance where parents_ip = '{}' and parents_port={};".format(ip,port))
            rows = cur.fetchall()
            if(rows):
                #pkey.append('% %'%(ip,port))
                pkey = ip,port
                db_tree[pkey] = rows
                print( " db_tree:" ,db_tree)
                print("--------------parent is {}:{}:rows is {}, type is {}----------------".format(ip, port,rows, type(rows)))
                pkey = []
            else:
                print("tree ends here!")

            for arow in rows:
                print("{}:{}----> {}:{} \n".format(ip, port, arow[0], arow[1]))
                find_children(arow[0],arow[1])
                # print("parent is {}:{}----> {}:{}".format(ip, port, rows[0], rows[1]))
                # print("arow is {}, type is {}".format(arow,type(arow)))
                # print("parent ip is {}, parent port is {}".format(arow[0],arow[1]))



if __name__ == '__main__':
    #main()
    #find_parents("qq.qq",3306)
    find_children("qq.qq",3306)
