import time
import pyodbc
import redis
from flask import Flask
from flask import render_template
from flask import request

app = Flask(__name__)
server = 'assignmentservershruthaja.database.windows.net'
database = 'assignemnt3'
username = 'shruthaja'
password = 'mattu4-12'
driver = '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

red = redis.StrictRedis(host='testredisshruthaja.redis.cache.windows.net', port=6379, db=0,
                        password='Y6DWGhZjh9rj00qePva4AgP9Fm9pN0R6kAzCaLeCErU=', ssl=False)
red.flushall()


@app.route('/', methods=['GET', 'POST'])
def hello_world():
    query_time = []
    time_query = []
    r = ''
    redis_time = []
    time_query = []
    query=""
    temp=[]
    tot_db = 0
    tot_red = 0
    if request.method=="POST":
        srange=request.form['srange']
        erange=request.form['erange']
        for i in range(30):
            time_query.append(i + 1)
        query_time = []
        query = "SELECT * FROM [dbo].[city1] where Population between ? and ?"
        cursor.execute(query,srange,erange)
        temp = cursor.fetchall()
        temp_result = ""
        for j in temp:
            temp_result = temp_result + str(j)
        red.set(1, temp_result)
        s = time.time()
        for i in time_query:
            start = time.time()
            cursor.execute(query, srange, erange)
            end = time.time()
            diff = end - start
            query_time.append(diff)
            tot_db=tot_db+diff
            s = time.time()
            red.get(1)
            e = time.time()
            redis_time.append(e - s)
            tot_red=tot_red+(e-s)
    return render_template("index.html", result=query_time, r=time_query, redis_time=redis_time,query=temp,tot_db=tot_db,tot_red=tot_red)


@app.route('/page2.html', methods=['GET', 'POST'])
def page2():
    query_time = []
    time_query = []
    r = ''
    redis_time = []
    time_query = []
    query = ""
    tot_db=0
    tot_red=0
    temp=[]
    if request.method == "POST":
        srange = request.form['srange']
        erange = request.form['erange']
        no=int(request.form['number'])
        for i in range(30):
            time_query.append(i + 1)
        query_time = []
        query = "SELECT TOP (?) * FROM [dbo].[city1] TABLESAMPLE(500 rows) where Population between ? and ?"
        cursor.execute(query,no,srange, erange)
        temp = cursor.fetchall()
        temp_result = ""
        for j in temp:
            temp_result = temp_result + str(j)
        red.set(1, temp_result)
        tot_db=0
        tot_red=0
        for i in time_query:
            start = time.time()
            cursor.execute(query, no, srange, erange)
            end = time.time()
            temp=cursor.fetchall()
            diff = end - start
            query_time.append(diff)
            tot_db=tot_db+diff
            s = time.time()
            red.get(1)
            e = time.time()
            redis_time.append(e - s)
            tot_red=tot_red+(e-s)
        print(temp)
    return render_template("page2.html", result=query_time, r=time_query, redis_time=redis_time, query=temp,tot_db=tot_db,tot_red=tot_red)


@app.route('/page3.html', methods=['GET', 'POST'])
def page3():
    query_time = []
    time_query = []
    result = []
    redis_time = []
    tot=0
    cities=''
    cities2=''
    if request.method == "POST":
        min=request.form['srange']
        max=request.form['erange']
        cname=request.form['cname']
        inc=request.form['pop']
        s=time.time()
        query="select * from dbo.city1 where population>? and population<? and state=?"
        cursor.execute(query,min,max,cname)
        cities=cursor.fetchall()
        query = "update city1 set population=population+? where City in (select City from dbo.city1 where population>? and population<? and state=?)"
        cursor.execute(query, inc, min, max, cname)
        cursor.commit()
        query = "select * from dbo.city1 where state=?"
        cursor.execute(query, cname)
        cities2 = cursor.fetchall()
        e=time.time()
        tot=e-s
    return render_template("page3.html", tot=tot,cities=cities,cities2=cities2)



if __name__ == '__main__':
    app.run(debug=True)
