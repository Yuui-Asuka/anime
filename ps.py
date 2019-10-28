import pymysql
import os

id_list = [f.split('_p0')[0] for f in os.listdir('./images')]
id_set = set(id_list)
sql_id_set = set()
db = pymysql.connect(host='localhost', user='root', password='123456', port=3306, db='Pixiv', use_unicode=True)
cursor = db.cursor()
all = cursor.execute('SELECT id from picture')
result = cursor.fetchall()
for res in result:
    sql_id_set.add(res)
print(len(id_set))
print(len(sql_id_set))
subtract = id_set - sql_id_set
insert = 'insert into picture(rank,post_date,title,id,path,crawl_date) values(%s,%s,%s,%s,%s,%s)'
for id in subtract:
    try:
        cursor.execute(insert, (1000, 'NULL', 'NULL', id, 'NULL', '2000-01-01'))
        db.commit()
    except:
        db.rollback()
db.close()

