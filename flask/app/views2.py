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
from bokeh.core.properties import value
from bokeh.io import show, output_file
#from bokeh.charts import Bar


# Configuration of MySQL db
app.config['MYSQL_HOST'] = 'ec2-54-82-188-230.compute-1.amazonaws.com'
app.config['MYSQL_USER'] = 'nayoon'
app.config['MYSQL_PASSWORD'] = 'haonayoon'
app.config['MYSQL_DB'] = 'insight_data_2'

mysql = MySQL(app)


@app.route('/ventra2/batch_result/year/', methods=['GET'])
def batch_hour():

    cur = mysql.connection.cursor()
    resultvalue = cur.execute("select year, sum(count) from Data_year_dayofweek_hour where year < 2017 group by year ")
    if resultvalue:
        details = cur.fetchall()
        cur.close()
        df = pd.read_sql("select year, sum(count) from Data_year_dayofweek_hour where year < 2017 group by year ", con=mysql.connection)
        source = ColumnDataSource(df)
        plot = figure(plot_width=500, plot_height=400, x_axis_label='year', y_axis_label='count')

        plot.vbar(x='year', source=source, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                  line_color="orangered")
        script, div = components(plot)
        return render_template('second_year.html', details=details, script=script, div=div)
    else:
        cur.close()



@app.route('/ventra2/batch_result/year_hour/', methods=['GET'])
def batch_year_hour():

    cur = mysql.connection.cursor()

    resultvalue = cur.execute(
        "select year,hour,sum(count) from Data_year_dayofweek_hour group by hour,year")

    if resultvalue:
        details = cur.fetchall()
        cur.close()

        df = pd.read_sql("select year,hour,sum(count) from Data_year_dayofweek_hour where year=2011 group by hour,year",
                         con=mysql.connection)
        source = ColumnDataSource(df)
        plot = figure(title='2011', plot_width=300, plot_height=300, x_axis_label='hour', y_axis_label='count')
        plot.vbar(x='hour', source=source, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                  line_color="orangered")

        df2 = pd.read_sql("select year,hour,sum(count) from Data_year_dayofweek_hour where year=2012 group by hour,year",
                         con=mysql.connection)
        source2 = ColumnDataSource(df2)
        plot2 = figure(title='2012', plot_width=300, plot_height=300, x_axis_label='hour', y_axis_label='count')
        plot2.vbar(x='hour', source=source2, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                  line_color="orangered")

        df3 = pd.read_sql(
            "select year,hour,sum(count) from Data_year_dayofweek_hour where year=2013 group by hour,year",
            con=mysql.connection)
        source3 = ColumnDataSource(df3)
        plot3 = figure(title='2013', plot_width=300, plot_height=300, x_axis_label='hour', y_axis_label='count')
        plot3.vbar(x='hour', source=source3, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                   line_color="orangered")

        df4 = pd.read_sql(
            "select year,hour,sum(count) from Data_year_dayofweek_hour where year=2014 group by hour,year",
            con=mysql.connection)
        source4 = ColumnDataSource(df4)
        plot4 = figure(title='2014', plot_width=300, plot_height=300, x_axis_label='hour', y_axis_label='count')
        plot4.vbar(x='hour', source=source4, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                   line_color="orangered")

        df5 = pd.read_sql(
            "select year,hour,sum(count) from Data_year_dayofweek_hour where year=2015 group by hour,year",
            con=mysql.connection)
        source5 = ColumnDataSource(df5)
        plot5 = figure(title='2015', plot_width=300, plot_height=300, x_axis_label='hour', y_axis_label='count')
        plot5.vbar(x='hour', source=source5, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                   line_color="orangered")

        df6 = pd.read_sql(
            "select year,hour,sum(count) from Data_year_dayofweek_hour where year=2016 group by hour,year",
            con=mysql.connection)
        source6 = ColumnDataSource(df6)
        plot6 = figure(title='2016', plot_width=300, plot_height=300, x_axis_label='hour', y_axis_label='count')
        plot6.vbar(x='hour', source=source6, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                   line_color="orangered")

        df7 = pd.read_sql(
            "select year,hour,sum(count) from Data_year_dayofweek_hour where year=2017 group by hour,year",
            con=mysql.connection)
        source7 = ColumnDataSource(df7)
        plot7 = figure(title='2017', plot_width=300, plot_height=300, x_axis_label='hour', y_axis_label='count')
        plot7.vbar(x='hour', source=source6, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                   line_color="orangered")


        script, div = components(plot)
        script2, div2 = components(plot2)
        script3, div3 = components(plot3)
        script4, div4 = components(plot4)
        script5, div5 = components(plot5)
        script6, div6 = components(plot6)
        script7, div7 = components(plot7)
        return render_template('second_year_hour.html', details=details, div=div,script=script,script2 = script2, div2 = div2,div3=div3,script3=script3,div4=div4,script4=script4,div5=div5,script5=script5,div6=div6,script6=script6,div7=div7,script7=script7)
    else:
        cur.close()



@app.route('/ventra2/batch_result/year_dayofweek/', methods=['GET'])
def batch_year_dayofweek():

    cur = mysql.connection.cursor()
    resultvalue = cur.execute(
        "select dayofweek,sum(count) from Data_year_dayofweek_hour where year=2012 group by dayofweek")

    if resultvalue:
        details = cur.fetchall()
        cur.close()

        df = pd.read_sql("select dayofweek,sum(count) from Data_year_dayofweek_hour where year=2011 group by dayofweek",
                         con=mysql.connection)
        source = ColumnDataSource(df)
        plot = figure(title='2011', plot_width=300, plot_height=300, x_axis_label='day of week', y_axis_label='count')
        plot.vbar(x='dayofweek', source=source, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                  line_color="orangered")

        df2 = pd.read_sql("select dayofweek,sum(count) from Data_year_dayofweek_hour where year=2012 group by dayofweek",
                         con=mysql.connection)
        source2 = ColumnDataSource(df2)
        plot2 = figure(title='2012', plot_width=300, plot_height=300, x_axis_label='day of week', y_axis_label='count')
        plot2.vbar(x='dayofweek', source=source2, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                  line_color="orangered")

        df3 = pd.read_sql("select dayofweek,sum(count) from Data_year_dayofweek_hour where year=2013 group by dayofweek",
                         con=mysql.connection)
        source3 = ColumnDataSource(df3)
        plot3 = figure(title='2013', plot_width=300, plot_height=300, x_axis_label='day of week', y_axis_label='count')
        plot3.vbar(x='dayofweek', source=source3, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                  line_color="orangered")

        df4 = pd.read_sql("select dayofweek,sum(count) from Data_year_dayofweek_hour where year=2014 group by dayofweek",
            con=mysql.connection)
        source4 = ColumnDataSource(df4)
        plot4 = figure(title='2014', plot_width=300, plot_height=300, x_axis_label='day of week', y_axis_label='count')
        plot4.vbar(x='dayofweek', source=source4, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                   line_color="orangered")
        df5 = pd.read_sql("select dayofweek,sum(count) from Data_year_dayofweek_hour where year=2015 group by dayofweek",
            con=mysql.connection)
        source5 = ColumnDataSource(df5)
        plot5 = figure(title='2015', plot_width=300, plot_height=300, x_axis_label='day of week', y_axis_label='count')
        plot5.vbar(x='dayofweek', source=source5, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                   line_color="orangered")
        df6 = pd.read_sql("select dayofweek,sum(count) from Data_year_dayofweek_hour where year=2016 group by dayofweek",
            con=mysql.connection)
        source6 = ColumnDataSource(df6)
        plot6 = figure(title='2016', plot_width=300, plot_height=300, x_axis_label='day of week', y_axis_label='count')
        plot6.vbar(x='dayofweek', source=source6, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                   line_color="orangered")
        df7 = pd.read_sql(
            "select dayofweek,sum(count) from Data_year_dayofweek_hour where year=2017 group by dayofweek",
            con=mysql.connection)
        source7= ColumnDataSource(df7)
        plot7 = figure(title='2017', plot_width=300, plot_height=300, x_axis_label='day of week', y_axis_label='count')
        plot7.vbar(x='dayofweek', source=source7, width=1, top='sum(count)', fill_color="orangered", fill_alpha=0.5,
                   line_color="orangered")

        script, div = components(plot)
        script2, div2 = components(plot2)
        script3, div3 = components(plot3)
        script4, div4 = components(plot4)
        script5, div5 = components(plot5)
        script6, div6 = components(plot6)
        script7, div7 = components(plot7)

        return render_template('second_year_dayofweek.html',details=details,div=div,script=script,div2=div2,script2=script2,div3=div3,script3=script3,div4=div4,script4=script4,div5=div5,script5=script5,div6=div6,script6=script6,div7=div7,script7=script7)
    else:
        cur.close()














































"""



@app.route('/ventra2/batch_result/year_dayofweek/',methods=['GET'])
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
        plot = figure(plot_width=500, plot_height=400, x_axis_label='dayofweek', y_axis_label='count', y_range=(0.55*10**7, 0.75*10**7),x_range=labels)
        plot.vbar(x = 'label', source = source, width =1, top = 'sum(count)', fill_alpha=0.5)
        script, div = components(plot)
        return render_template('batch_dow.html', details = details, script = script, div = div)
    else:
        cur.close()






@app.route('/ventra/batch_result/dayofweek_by_hour/', methods = ['GET', 'POST'])
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


@app.route('/ventra/batch_result/dayofweek_by_dow/', methods = ['GET', 'POST'])
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

@app.route('/ventra/batch_result/dayofweek_by_dow2/', methods = ['GET','POST'])
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



@app.route('/ventra/batch_result/user/',methods=['GET'] )
def batch_user():
    cur = mysql.connection.cursor()
    resultvalue = cur.execute("select * from Data_2015_Ver1_user order by count desc limit 30")

    if resultvalue:
        details = cur.fetchall()
        cur.close()
        return render_template('batch_hour_usr.html', details=details)
    else:
        cur.close()

@app.route('/ventra/batch_result/user_by_hour/', methods = ['GET', 'POST'])
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



@app.route('/ventra/stream_result/data/', methods=['POST'])
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


@app.route('/ventra/stream_result/',methods=['GET'])
def streaming():
    source = AjaxDataSource(data_url = request.url_root + 'ventra/stream_result/data/', polling_interval = 1000, mode = 'append')
    #plot = figure(plot_width=300, plot_height=300)
    plot = figure(plot_height=300,sizing_mode='scale_width')
    plot.line('x', 'y', source = source, line_width = 4)
    script, div = components(plot)
    return render_template('seconds.html', script = script, div = div)


"""
