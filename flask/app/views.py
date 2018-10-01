from app import app
import MySQLdb
from flask_mysqldb import MySQL
from flask import jsonify, send_from_directory, make_response, current_app
from flask import render_template, request
import copy
import time
import datetime
from collections import Counter
import redis
import pandas as pd
from bokeh.plotting import figure
from bokeh.plotting import show
from bokeh.models import ColumnDataSource
from bokeh.models import AjaxDataSource
from bokeh.embed import components
from six import string_types
from datetime import timedelta
from functools import update_wrapper, wraps

from datetime     import datetime as dt
from datetime     import timedelta as td

import redis
import psycopg2
import matplotlib.pyplot as plt


#Configuration of MySQL db
app.config['MYSQL_HOST']='ec2-54-82-188-230.compute-1.amazonaws.com'
app.config['MYSQL_USER']='nayoon'
app.config['MYSQL_PASSWORD']='haonayoon'
app.config['MYSQL_DB']='insight_data'

mysql=MySQL(app)

@app.route('/')
def choose_data():
    return render_template('intro.html')

@app.route('/static/<path:path>')
def send_js(path):
    return send_from_directory('static', path)


@app.route('/batch_result/hour/',methods=['GET','POST'])
def batch_hour():
    cur = mysql.connection.cursor()
    resultvalue=cur.execute("select * from Data_2015_Ver1")
    if resultvalue:
        details=cur.fetchall()
        cur.close()
        df = pd.read_sql('SELECT * FROM Data_2015_Ver1', con=mysql.connection)
        source = ColumnDataSource(df)
        plot = figure()
        plot.vbar(x = 'hour', source = source, width = 0.5, top = 'count')
        script, div = components(plot)
        return render_template('batch_hour.html', details=details, script = script, div = div)
    else:
        cur.close()

@app.route('/batch_result/dayofweek/',methods=['GET'])
def batch_dayofweek():

    cur = mysql.connection.cursor()
    resultvalue = cur.execute("select dayofweek,sum(count) from Data_2015_Ver1_dayofweek group by dayofweek")
    if resultvalue:
        details = cur.fetchall()
        cur.close()
        df = pd.read_sql("SELECT dayofweek,sum(count) FROM Data_2015_Ver1_dayofweek GROUP BY dayofweek", con=mysql.connection)
        source = ColumnDataSource(df)
        plot = figure()
        plot.vbar(x = 'dayofweek', source = source, width = 0.5, top = 'sum(count)')
        script, div = components(plot)
        return render_template('batch_dow.html', details = details, script = script, div = div)
    else:
        cur.close()


@app.route('/batch_result/dayofweek_by_hour/', methods = ['GET', 'POST'])
def batch_dayofweek_by_hour():
    hours = list(range(24))
    hour = request.args.get('hour')
    if hour == None:
        hour = '0'
    cur = mysql.connection.cursor()
    resultvalue = cur.execute("select * from Data_2015_Ver1_dayofweek where hour = " + hour)
    if resultvalue:
        details = cur.fetchall()
        cur.close()
        df = pd.read_sql("SELECT * FROM Data_2015_Ver1_dayofweek WHERE hour = " + hour,
                         con=mysql.connection)
        source = ColumnDataSource(df)
        plot = figure()
        plot.vbar(x='dayofweek', source=source, width=0.5, top='count')
        script, div = components(plot)
        return render_template('batch_dow_by_hour.html', details=details, current_hour = int(hour), hours = hours, script=script, div=div)
    else:
        cur.close()




@app.route('/batch_result/user/',methods=['GET'] )
def batch_user():
    cur = mysql.connection.cursor()
    resultvalue = cur.execute("select * from Data_2015_Ver1_user order by count desc limit 30")

    if resultvalue:
        details = cur.fetchall()
        cur.close()
        return render_template('batch_hour_usr.html', details=details)
    else:
        cur.close()





@app.route('/batch_result/user_by_hour/', methods = ['GET', 'POST'])
def batch_user_by_hour():
    hours = list(range(24))
    hour = request.args.get('hour')
    if hour == None:
        hour = '0'
    cur = mysql.connection.cursor()
    resultvalue = cur.execute("select * from Data_2015_Ver1_user where hour = " + hour+" order by count desc limit 20")


    if resultvalue:
        details = cur.fetchall()
        cur.close()
        df = pd.read_sql("SELECT * FROM Data_2015_Ver1_user WHERE hour = " + hour+" order by count desc limit 20",con=mysql.connection)
        df['index']=df.index
        source = ColumnDataSource(df)
        plot = figure(y_axis_type='log')
        plot.vbar(x='index', source=source, width=0.5, top='count', bottom=10)
        script, div = components(plot)
        return render_template('batch_user_by_hour.html', details=details, current_hour = int(hour), hours = hours, script=script, div=div)
    else:
        cur.close()

#########################################################
# Flask server related
#
# The following code has no relation to bokeh and it's only
# purpose is to serve data to the AjaxDataSource instantiated
# previously. Flask just happens to be one of the python
# web frameworks that makes it's easy and concise to do so
#########################################################


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    """
    Decorator to set crossdomain configuration on a Flask view
    For more details about it refer to:
    http://flask.pocoo.org/snippets/56/
    """
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))

    if headers is not None and not isinstance(headers, string_types):
        headers = ', '.join(x.upper() for x in headers)

    if not isinstance(origin, string_types):
        origin = ', '.join(origin)

    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        @wraps(f)
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            requested_headers = request.headers.get(
                'Access-Control-Request-Headers'
            )
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            elif requested_headers:
                h['Access-Control-Allow-Headers'] = requested_headers
            return resp
        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

x = 0

@app.route('/stream_result/data/', methods=['GET','POST','OPTIONS'])
@crossdomain(origin="*", methods=['GET', 'POST'], headers=None)
def data():
    global x
    x += 1
    y = 2**x
    return jsonify(x=x, y=y)
    """
    r = redis.StrictRedis(host='ec2-54-82-188-230.compute-1.amazonaws.com', port=6379, db=0)
    year = int(r.get('year'))
    month = int(r.get('month'))
    hour = int(r.get('hour'))
    date = int(r.get('date'))
    minn = int(r.get('min'))
    secc = int(r.get('sec'))
    count = int(r.get('counting'))
    seconds = int(time.mktime(datetime.datetime(year, month, date, hour, minn, secc).timetuple()))
    print('year: ' + str(year) + ' month: ' + str(month) + ' date: ' + str(date) + ' hour: ' + str(hour) + ' min: ' + str(minn) + ' sec: ' + str(secc) + ' count: ' + str(count) + ' seconds: ' + str(seconds))
    return jsonify(x = seconds, y = count)
    """


@app.route('/stream_result/',methods=['GET','POST'])
def streaming():
    source = AjaxDataSource(method='GET', data_url = request.url_root + 'stream_result/data/', polling_interval = 1000, mode = 'append')
    source.data = dict(x=[],y=[])
    time_start = int(time.mktime(datetime.datetime(2015, 8, 21, 23, 36, 0).timetuple()))
    time_end = int(time.mktime(datetime.datetime(2015, 8, 21, 23, 40, 59).timetuple()))
    plot = figure(plot_height=300,sizing_mode='scale_width')
    plot.line('x', 'y', source = source, line_width = 4)
    script, div = components(plot)
    return render_template('seconds.html', script = script, div = div)

"""
    timeCount_list = r.zrange('time', 0, -1, withscores=True)

    last = len(timeCount_list) + 1

    timeCount_list = sorted(timeCount_list, key=lambda x: x[0])
    timeCount_list = list(map(lambda x: x[1], timeCount_list))

    data_redis = ['Real-time'] + timeCount_list
    #data_25 = ['25th percentile'] + quan25[0:last]
    #data_75 = ['75th percentile'] + quan75[0:last]
    #data_95 = ['95th percentile'] + quan95[0:last]

    return render_template("seconds.html",title='seconds',data_redis=data_redis)
    """










"""
##############################


@app.route('/transactions/')
@app.route('/connections/')
def enter():
    return render_template("enter.html")


@app.route('/transactions/<int:user>')
def get_tran_data(user):
    db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
    cursor = db.cursor()
    query_string = "SELECT * FROM Transactions WHERE ID1={user} OR ID2={user} ORDER BY Time DESC limit 20".format(
        user=user)
    response = cursor.execute(query_string)
    response = cursor.fetchall()
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"Time": x[5], "From": x[2], "To": x[4], "Message": x[6],
                     "Amount": x[7], "Verified": "Verified" if x[8] else "Not verified"} for x in response_list]
    query_string = "SELECT * FROM Users WHERE ID={user}".format(user=user)
    response = cursor.execute(query_string)
    response = cursor.fetchall()
    cursor.close()
    if len(response) > 0:
        User = [{"Name": response[0][1]}]
    else:
        User = [{"Name": "user"}]
    return render_template("transactions.html", output=jsonresponse, User=User)


@app.route('/connections/<int:user>')
def get_friends_data(user):
    db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
    cursor = db.cursor()
    query_string = "SELECT F.ID2,U.FullName FROM Friends F JOIN Users U ON F.ID2=U.ID WHERE F.ID1={user} limit 7".format(
        user=user)
    response = cursor.execute(query_string)
    response = cursor.fetchall()
    response_list = []
    response_list_FN = []
    for val in response:
        response_list.append(val[0])
        response_list_FN.append(val[1])

    friends_lists = []
    for friend in response_list:
        query_string_ff = "SELECT F.ID2,U.FullName FROM Friends F JOIN Users U ON F.ID2=U.ID WHERE F.ID1={user} limit 7".format(
            user=friend)
        response = cursor.execute(query_string_ff)
        response = cursor.fetchall()
        response_listf = []
        for val in response:
            response_listf.append(val[1])
        friends_lists.append(response_listf)

    jsonresponse = [{"Friend": str(response_list_FN[i]).strip('[]'), "FriendID": response_list[i],
                     "Friends": str(friends_lists[i]).strip('[]').replace("\'", "")} for i in
                    range(0, len(response_list))]

    query_string = "SELECT * FROM Users WHERE ID={user}".format(user=user)
    response = cursor.execute(query_string)
    response = cursor.fetchall()
    cursor.close()
    if len(response) > 0:
        User = [{"Name": response[0][1]}]
    else:
        User = [{"Name": "user"}]
    return render_template("friends.html", output=jsonresponse, User=User)


@app.route('/statistics')
def statistica():
    rediska = redis.StrictRedis(host='ec2-34-207-202-197.compute-1.amazonaws.com', port=6379, db=0)

    all_t = []
    av_t = []

    st = time.time()

    for i in range(20, -1, -1):
        ts = datetime.datetime.fromtimestamp(st - i).strftime('%Y-%m-%d %H:%M:%S')

        listik = rediska.lrange(ts, 0, -1)
        all = len(listik)
        c = Counter(listik)
        trues = c['True']

        all_t.append([st - i, all])
        av_t.append([st - i, trues])

    return jsonify(all_t=all_t, av_t=av_t)


@app.route('/atvperformance')
def index():
    return render_template('system_perform.html')


@app.route('/potential_fraud')
def fraud_detection():
    rediska_from = redis.StrictRedis(host='ec2-34-207-202-197.compute-1.amazonaws.com', port=6379, db=1)
    rediska_to = redis.StrictRedis(host='ec2-34-207-202-197.compute-1.amazonaws.com', port=6379, db=2)
    from_users = []
    to_users = []

    st = time.time()
    for i in range(5, -1, -1):
        ts = datetime.datetime.fromtimestamp(st - i * 60).strftime('%Y-%m-%d %H:%M')

        from_list = rediska_from.lrange(ts, 0, -1)
        to_list = rediska_to.lrange(ts, 0, -1)

        from_users.extend(from_list)
        to_users.extend(to_list)

    from_c = Counter(from_users).most_common(20)
    to_c = Counter(to_users).most_common(20)

    db = MySQLdb.connect(host="ec2-54-158-19-194.compute-1.amazonaws.com", user="venmo", passwd="pass", db="VenmoDB")
    cursor = db.cursor()
    jsonresponse_from = [{"UserName": int(user[0]), "Number": int(user[1])} for user in from_c]
    jsonresponse_to = [{"UserName": int(user[0]), "Number": int(user[1])} for user in to_c]


return render_template("fraud.html", from_out=jsonresponse_from, to_out=jsonresponse_to)
"""