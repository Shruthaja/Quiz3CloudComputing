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
    for i in range(30):
        time_query.append(i + 1)
    query_time = []
    query = "SELECT TOP 1000 * FROM [dbo].[earthquake]"
    cursor.execute(query)
    temp = cursor.fetchall()
    temp_result = ""
    for j in temp:
        temp_result = temp_result + str(j)
    red.set(1, temp_result)
    s = time.time()
    for i in time_query:
        start = time.time()
        cursor.execute(query)
        end = time.time()
        diff = end - start
        query_time.append(diff)
        s = time.time()
        red.get(1)
        e = time.time()
        redis_time.append(e - s)
    print(query_time)
    return render_template("index.html", result=query_time, r=time_query, redis_time=redis_time)


@app.route('/page2.html', methods=['GET', 'POST'])
def page2():
    query_time = []
    time_query = []
    result = []
    redis_time = []
    if request.method == "POST":
        minlat = request.form['lat']
        minlon = request.form['lon']
        maxlat = request.form['mlat']
        maxlon = request.form['mlon']
        query = "select top(1000) * from dbo.earthquake where latitude between ? and ? and longitude between ? and ?"
        cursor.execute(query, minlat, maxlat, minlon, maxlon)
        temp = cursor.fetchall()
        temp_result = ""
        for j in temp:
            temp_result = temp_result + str(j)
        red.set(2, temp_result)
        time_query = []
        redis_time = []
        time_query = []
        for i in range(30):
            time_query.append(i + 1)
        query_time = []
        for i in time_query:
            start = time.time()
            cursor.execute(query, minlat, maxlat, minlon, maxlon)
            end = time.time()
            diff = end - start
            query_time.append(diff)
            s = time.time()
            temp = red.get(2)
            e = time.time()
            redis_time.append(e - s)
    return render_template("page2.html", result=query_time, r=time_query, redis_time=redis_time)


@app.route('/page22.html', methods=['GET', 'POST'])
def page22():
    query_time = []
    time_query = []
    result = []
    redis_time = []
    if request.method == "POST":
        smag = request.form['smag']
        emag = request.form['emag']
        query = "select top(1000) * from dbo.earthquake where mag between ? and ? ;"
        cursor.execute(query, smag, emag)
        temp = cursor.fetchall()
        temp_result = ""
        for j in temp:
            temp_result = temp_result + str(j)
        red.set(3, temp_result)
        for i in range(30):
            time_query.append(i + 1)
        query_time = []
        for i in time_query:
            start = time.time()
            cursor.execute(query, smag, emag)
            end = time.time()
            diff = end - start
            query_time.append(diff)
            s = time.time()
            red.get(3)
            e = time.time()
            redis_time.append(e - s)
    return render_template("page2.html", result=query_time, r=time_query, redis_time=redis_time)


@app.route('/page23.html', methods=['GET', 'POST'])
def page23():
    query_time = []
    time_query = []
    result = []
    redis_time = []
    if request.method == "POST":
        lat = request.form['lat1']
        long = request.form['lon1']
        ran = request.form['range']
        query = "select top(1000) * from dbo.earthquake  WHERE ( 6371 * ACOS(COS(RADIANS(latitude)) * COS(RADIANS(?)) * COS(RADIANS(longitude) - RADIANS(?)) + SIN(RADIANS(latitude)) * SIN(RADIANS(?)) ))< ?;"
        cursor.execute(query, lat, long, lat, ran)
        temp = cursor.fetchall()
        temp_result = ""
        for j in temp:
            temp_result = temp_result + str(j)
        red.set(4, temp_result)
        time_query = []
        for i in range(30):
            time_query.append(i + 1)
        query_time = []
        for i in time_query:
            start = time.time()
            cursor.execute(query, lat, long, lat, ran)
            end = time.time()
            diff = end - start
            query_time.append(diff)
            s = time.time()
            red.get(4)
            e = time.time()
            redis_time.append(e - s)
    return render_template("page2.html", result=query_time, r=time_query, redis_time=redis_time)


if __name__ == '__main__':
    app.run(debug=True)
