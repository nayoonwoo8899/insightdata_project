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
        plot = figure(plot_width=500, plot_height=400, x_axis_label='hour', y_axis_label='count', y_range=(0, 3.2*10**6),x_range=(-1, 24))

        plot.vbar(x = 'hour', source = source, width = 1, top = 'count',fill_color="orangered",fill_alpha=0.5,line_color="orangered")
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
        labels = []
        for dow in df['dayofweek']:
            if dow == 0:
                labels.append('Monday')
            elif dow == 1:
                labels.append('Tuesday')
            elif dow == 2:
                labels.append('Wednesday')
            elif dow == 3:
                labels.append('Thursday')
            elif dow == 4:
                labels.append('Friday')
            elif dow == 5:
                labels.append('Saturday')
            else:
                labels.append('Sunday')
        df['label'] = labels
        #df.iloc[:, -1] = df.iloc[:, -1].div(52)
        #df.iloc[-4, -1] = df.iloc[-4, -1].multiply(52).divide(53)
        source = ColumnDataSource(df)
<<<<<<< HEAD
        plot = figure(plot_width=500, plot_height=400, x_axis_label='day of week', y_axis_label='count', y_range=(0.55*10**7, 0.75*10**7),x_range=(-0.5, 6.5))
        plot.vbar(x = 'dayofweek', source = source, width =1, top = 'sum(count)',fill_alpha=0.5)
=======
        plot = figure(plot_width=500, plot_height=400, x_axis_label='dayofweek', y_axis_label='count', y_range=(0.55*10**7, 0.75*10**7),x_range=labels)
        plot.vbar(x = 'label', source = source, width =1, top = 'sum(count)', fill_alpha=0.5)
>>>>>>> 88b43afbe911e585ed1efaa94e436f9a0bc19232
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
        df = pd.read_sql("SELECT * FROM Data_2015_Ver1_dayofweek WHERE hour = " + hour,con=mysql.connection)
        labels = []
        for dow in df['dayofweek']:
            if dow == 0:
                labels.append('Monday')
            elif dow == 1:
                labels.append('Tuesday')
            elif dow == 2:
                labels.append('Wednesday')
            elif dow == 3:
                labels.append('Thursday')
            elif dow == 4:
                labels.append('Friday')
            elif dow == 5:
                labels.append('Saturday')
            else:
                labels.append('Sunday')
        df['label'] = labels
        source = ColumnDataSource(df)
        plot = figure(plot_width=500, plot_height=400,x_axis_label='hour', y_axis_label='count',y_range=(0, 5*10**5),x_range = labels)
        plot.vbar(x='label', source=source, width=1, top='count',fill_alpha=0.5)
        script, div = components(plot)
        return render_template('batch_dow_by_hour.html', details=details, current_hour = int(hour), hours = hours, script=script, div=div)
    else:
        cur.close()


@app.route('/batch_result/dayofweek_by_dow/', methods = ['GET', 'POST'])
def batch_dayofweek_by_dow():
    dows = list(range(7))
    dow = request.args.get('dow')
    if dow == None:
        dow = '0'
    cur = mysql.connection.cursor()
    resultvalue = cur.execute("select * from Data_2015_Ver1_dayofweek where dayofweek= " + dow)
    if resultvalue:
        details = cur.fetchall()
        cur.close()
        df = pd.read_sql("select * from Data_2015_Ver1_dayofweek where dayofweek = " + dow,con=mysql.connection)
        source = ColumnDataSource(df)
        plot = figure(plot_width=500, plot_height=400,x_axis_label='hour', y_axis_label='count')
        plot.vbar(x='hour', source=source, width=1, top='count',fill_alpha=0.5)
        script, div = components(plot)

        return render_template('batch_dow_by_dow.html', details=details, current_dow = int(dow), dows = dows, script=script, div=div)
    else:
        cur.close()

@app.route('/batch_result/dayofweek_by_dow2/', methods = ['GET','POST'])
def batch_dayofweek_by_dow2():
    dows = list(range(7))
    dows2 = list(range(7))
    dow = request.args.get('dow')
    dow2=request.args.get('dow2')
    if dow == None:
        dow = '0'
    if dow2==None:
        dow2='0'

    df = pd.read_sql("select * from Data_2015_Ver1_dayofweek where dayofweek = " + dow, con=mysql.connection)
    df2 = pd.read_sql("select * from Data_2015_Ver1_dayofweek where dayofweek = " + dow2, con=mysql.connection)

    df.iloc[:,-1] = df.iloc[:,-1].div(52)
    df2.iloc[:,-1] = df2.iloc[:,-1].div(52)

    if dow=='3':
        df.iloc[:, -1] = df.iloc[:, -1].multiply(52).div(53)
    if dow2=='3':
        df2.iloc[:, -1] = df2.iloc[:, -1].multiply(52).div(53)

    source = ColumnDataSource(df)
    source2 = ColumnDataSource(df2)

    plot = figure(plot_width=500, plot_height=400,x_axis_label='hour', y_axis_label='Average Count',y_range=(0, 10000),x_range=(-0.5, 23.5))

    plot.vbar(x='hour', source=source, width=1, top='count',fill_color="deeppink",fill_alpha=0.5,line_color="deeppink")
    plot.vbar(x='hour', source=source2, width=1, top='count',fill_color="gold",fill_alpha=0.5,line_color="gold")

    script, div = components(plot)
    return render_template('batch_dow_by_dow2.html', current_dow = int(dow), current_dow2 = int(dow2), dows=dows,dows2=dows2,script=script, div=div)



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
        #df['index']=df.index
        source = ColumnDataSource(df)
        plot = figure(y_axis_type='log',plot_width=500, plot_height=400, y_axis_label='Count',y_range=(1, 3000),x_range=(-0.5, 19.5))
        plot.vbar(x='index', source=source, width=1, top='count', bottom=1,fill_alpha=0.5,fill_color="deeppink")
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
    seconds = int(time.mktime(datetime.datetime(year, month, date, hour, minn, secc).timetuple()) - time.mktime(datetime.datetime(2011, 1, 1, 0, 0, 0).timetuple()))
    print('year: ' + str(year) + ' month: ' + str(month) + ' date: ' + str(date) + ' hour: ' + str(hour) + ' min: ' + str(minn) + ' sec: ' + str(secc) + ' count: ' + str(count) + ' seconds: ' + str(seconds))
    return jsonify(x = [seconds], y = [count])


@app.route('/stream_result/',methods=['GET'])
def streaming():
    source = AjaxDataSource(data_url = request.url_root + 'stream_result/data/', polling_interval = 1000, mode = 'append')
    #plot = figure(plot_width=300, plot_height=300)
    plot = figure(plot_height=300,sizing_mode='scale_width')
    plot.line('x', 'y', source = source, line_width = 4)
    script, div = components(plot)
    return render_template('seconds.html', script = script, div = div)
