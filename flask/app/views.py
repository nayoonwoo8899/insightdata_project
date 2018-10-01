from app import app
from flask_mysqldb import MySQL
from flask import render_template, request, jsonify
import time
import datetime
import redis
import pandas as pd
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.models import AjaxDataSource
from bokeh.embed import components


#Configuration of MySQL db
app.config['MYSQL_HOST']='ec2-54-82-188-230.compute-1.amazonaws.com'
app.config['MYSQL_USER']='nayoon'
app.config['MYSQL_PASSWORD']='haonayoon'
app.config['MYSQL_DB']='insight_data'

mysql=MySQL(app)

@app.route('/')
def choose_data():
    return render_template('intro.html')

@app.route('/batch_result/hour/',methods=['GET'])
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

@app.route('/stream_result/data/', methods=['POST'])
def data():
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
    return jsonify(x = [seconds], y = [count])


@app.route('/stream_result/',methods=['GET'])
def streaming():
    source = AjaxDataSource(data_url = request.url_root + 'stream_result/data/', polling_interval = 1000, mode = 'append')
    plot = figure(plot_height=300,sizing_mode='scale_width')
    plot.line('x', 'y', source = source, line_width = 4)
    script, div = components(plot)
    return render_template('seconds.html', script = script, div = div)
