#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pyodbc
import pymysql
#import pandas as pd
from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder
import datetime
from pathlib import Path
from config import *

DIFF_IDS = 0
"""
with SSHTunnelForwarder(
        ('', 22),
        ssh_username = '',
        ssh_password = '',
        remote_bind_address = ('127.0.0.1', 3306)) as tunnel:
        conn = pymysql.connect(host='127.0.0.1', user  ='',
                passwd = '', db='',
                port = tunnel.local_bind_port)
        query = 'SELECT * FROM USERINFO WHERE USERID<11'
        data = pd.read_sql_query(query, conn)
        print(data.head())

        conn.close()
""";


# In[19]:


def consulta(query):
    server = SSHTunnelForwarder(
            (URL, 22),
            ssh_username = SSH_USER,
            ssh_password = SSH_PWD,
            remote_bind_address = ('127.0.0.1', 3306)
    )
            #data = pd.read_sql_query('SELECT * FROM USERINFO', conn)

    server.start()
    
    cnx = pymysql.connect(host='127.0.0.1', user  = MYSQL_USER,
            use_unicode=True, charset='utf8',
            passwd = MYSQL_PASS, db = DB,
            port = server.local_bind_port)
    cursor = cnx.cursor()
    cursor.execute(query)
    #print(cursor.fetchall())
    """
    for row in cursor:
        return row
        print(row)
    """
    """
    now = datetime.datetime(2009,5,5)
    str_now = now.date().isoformat()
    cursor.execute('INSERT INTO table (name, id, datecolumn) VALUES (%s,%s,%s)', ('name',4,str_now))        
    """
    cnx.commit()
    cnx.close()
    #df = pd.read_sql_query(sql , conn)
    server.stop()
    return cursor
    #return df


# # IMPORTANDO USUARIOS NUEVOS

# In[126]:


max_userid = consulta('SELECT MAX(id) FROM USERINFO')
max_userid = max_userid.fetchall()[0][0]
if max_userid == None:
    max_userid = 0
    

conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ='+MDB_FILE+';'
    )
conn = pyodbc.connect(conn_str)
query = "SELECT USERID,Badgenumber,Name FROM USERINFO WHERE USERID > ? "

cursor = conn.cursor()
cursor.execute(query,str(max_userid))

# build list of column names to use as dictionary keys from sql results
#columns = [column[0] for column in cursor.description]

insert = """INSERT INTO USERINFO (id,dni,nombre_apellido) VALUES """
i = 0
for row in cursor.fetchall():
    if i == PORCION:
        break
    insert += "('"+str(row[0])+"','"+str(row[1])+"','"+str(row[2])+"'),"
    i = i + 1 
insert = insert[:-1]
#print(insert)
    #results.append(dict(zip(columns, row)))
    #output = {"MetaData": {}, "SRData": results}
#print(json.dumps(output, sort_keys=True, indent=4))
#query = "SELECT USERID,Badgenumber,Name FROM USERINFO WHERE USERID<10"
#dataf = pd.read_sql_query(query, cnxn)
conn.close()
if i > 0:
    print("Agregando usuarios nuevos")
    result = consulta(insert)
print("Anterior MAX USER ID: "+str(max_userid))
print("Nuevo MAX USER ID: "+str(max_userid+i))
#dataf


# # IMPORTANDO MARCAS NUEVAS

# In[123]:


#import json

max_chkidfecha = consulta("SELECT fecha FROM CHECKINOUT WHERE  id_inc=(SELECT MAX(ch1.id_inc) FROM CHECKINOUT ch1 WHERE YEAR(fecha)<'2100' )")

if max_chkidfecha.rowcount == 0:
    max_chkidfecha = '2000-01-01 00:00:00'
else:
    max_chkidfecha = max_chkidfecha.fetchall()[0][0]
#if max_chkidfecha == None:
    


conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    r'DBQ='+MDB_FILE+';'
    )
conn = pyodbc.connect(conn_str)
query = "SELECT TOP "+str(PORCION)+" id, USERID,CHECKTIME,SENSORID FROM CHECKINOUT WHERE CHECKTIME>? ORDER BY CHECKTIME ASC"
print("Seleccionando fechas superiores a: "+str(max_chkidfecha))
cursor = conn.cursor()
cursor.execute(query,str(max_chkidfecha))

# build list of column names to use as dictionary keys from sql results
#columns = [column[0] for column in cursor.description]

insert = """INSERT IGNORE INTO CHECKINOUT (id,fecha,sensor_id) VALUES """
i = 0
for row in cursor.fetchall():
    if i == PORCION:
        break
    insert += "('"+str(row[1])+"','"+str(row[2])+"','"+str(row[3])+"'),"
    max_fecha = row[2]
    i = i + 1 
insert = insert[:-1]
#print(insert)
    #results.append(dict(zip(columns, row)))
    #output = {"MetaData": {}, "SRData": results}
#print(json.dumps(output, sort_keys=True, indent=4))
#query = "SELECT USERID,Badgenumber,Name FROM USERINFO WHERE USERID<10"
#dataf = pd.read_sql_query(query, cnxn)
conn.close()
if i > 0:
    result = consulta(insert)
print("Anterior Max. Fecha: " + str(max_chkidfecha))
print("Nueva Max. Fecha importada: " + str(max_fecha))
#print("Anterior MAX ID: "+str(max_chkid))
#print("Nuevo MAX ID: "+str(max_chkid+i))
#dataf


# In[105]:


#print(len(insert))
#print(i)
consulta('UPDATE config SET last_update=NOW()')
#last_update = consulta('SELECT last_update FROM config')
#last_update.strftime("%d/%m/%y %H:%m")


# In[128]:


"""
#DATOS PARA SIMULAR
chks = consulta("SELECT id_inc,id,fecha FROM  CHECKINOUT WHERE id_inc<100000")
insert = "INSERT IGNORE INTO CHECKINOUT (id_inc,id,fecha) VALUES "

for row in chks.fetchall():
    insert += "('"+str(row[0])+"','"+str(row[1])+"','"+str(row[2])+"'),"
insert = insert[:-1]
""";

