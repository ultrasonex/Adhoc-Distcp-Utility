# -*- coding: utf-8 -*-


import os
import MySQLdb
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash
import subprocess
import time


# create our little application :)
app = Flask(__name__)

app.config.update(dict(

    DEBUG=True,
    SECRET_KEY='development key',
    USERNAME='distcp',
    PASSWORD='distcp'
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)


def connect_db():

    db = MySQLdb.connect(host="localhost",
                         user="nchakr200",
                         passwd="password1",
                         db="distcp")
    return db


def init_db():
    """Initializes the database."""
    db = get_db()
    with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().execute(f.read())

    db.commit()

def createAndRunDistcp(source, destination):
    distcp = """nohup hadoop distcp -Dmapreduce.map.maxattempts=999 -Dmapreduce.map.memory.mb=8192 -Dyarn.scheduler.minimum-allocation-mb=8192 -Dmapreduce.map.java.opts.max.heap=-Xmx8192  -Dmapreduce.job.queuename=root.Two -update -delete -p -i -m 100 -bandwidth 4"""
    meld=""" hdfs://sourcenamenodenamespaceid"""
    ice=""" hdfs://destnamenodenamespaceid"""
    output = """/home/distcppython/logs/distcp_""" + str(int(time.time()))
    backend = """ 2>&1 & """

    command = (distcp+meld+source+ice+destination + "> " +  output + backend)
    print "Hadoop Distcp %s  " %(command)
    os.system(command)
    #p1 = subprocess.Popen(command, stdout=None, shell=False)
    #time.sleep(20)
    #p2 = subprocess.Popen("grep 'The url to track' " + output, stdin=None, stdout=subprocess.PIPE, shell=False)
    #p1.stdout.close()
    #output = p2.communicate()[0]
    #print "Output %s" % output[38:]
    return "Log file located at "+output



@app.cli.command('initdb')
def initdb_command():
    """Creates the database tables."""
    init_db()
    print('Initialized the database.')


def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'mysql_db'):
        g.mysql_db = connect_db()
    return g.mysql_db


@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'mysql_db'):
        g.mysql_db.close()


@app.route('/')
def show_entries():
    print "in show entries"
    db = get_db()
    cur = db.cursor()
    cur.execute('select source, destination from distcp.distcp_detail2 order by id desc limit 10')
    entries = cur.fetchall()
    for entry in entries:
        print (entry[0],entry[1])

    for entry in entries:
        print (entry[0],entry[1])
    return render_template('show_entries.html', entries=entries)


@app.route('/add', methods=['POST'])
def add_entry():
    if not session.get('logged_in'):
        abort(401)

    db = get_db()
    cur = db.cursor();
    print "Inside Add method"
    source = request.form['source']
    destination = request.form['destination']
    url = createAndRunDistcp(source,destination)


    print request.form['source']
    cur.execute("""insert into distcp.distcp_detail2 (source, destination) values (%s, %s)""",(request.form['source'], request.form['destination']))

    db.commit()
    flash(url)
    return redirect(url_for('show_entries'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != app.config['USERNAME']:
            error = 'Invalid username'
        elif request.form['password'] != app.config['PASSWORD']:
            error = 'Invalid password'
        else:
            session['logged_in'] = True
            flash('You were logged in')
            return redirect(url_for('show_entries'))
    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    flash('You were logged out')
    return redirect(url_for('show_entries'))



if __name__ == "__main__":
    app.run(host='hostname', port=8001)

