#!/usr/bin/env python
# -*- coding: utf-8 -*-
# {{ ansible_managed }}

import cgi, os, sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append("/app/wsgi-scripts")
from webob import Request
from webob import Response
import time
from random import randint
from checker import csv_checker
import cgitb; cgitb.enable()
# db connection
try:
    import psycopg2
except ImportError as strerror:
    print "A problem occured during Import ({0})".format(strerror)
    exit()

def application(environ, start_response):
    # set utf-8
    res = Response()
    res.charset = 'utf8'

    debug = ''
    request = Request(environ)
    message = u""

    try:
        upload = request.params["upload"]
    except: 
        upload = "False"

    if upload == "True" and type(request.params["myfile"]) is not unicode:    
        #try:
        file_name = random_name()
        file_data = request.params["myfile"].value
        fp = open('/tmp/' + file_name + '.itf','wb')
        fp.write(file_data)
        fp.close()
        message = u'erfolgreiches uploaden der datei ' + file_name            
        # db
        try:
            conn = psycopg2.connect('') # pass connection parameters as PostgreSQL libpq env vars
            conn.set_isolation_level(0)
            cur = conn.cursor()
        except:
            pass
            #'The database connection arguments do not seem to be correct!'

        csvchecker = csv_checker.CsvChecker(cur)
        arr_message = csvchecker.check('csv', '/tmp/' + file_name + '.itf')
        del csvchecker 
        os.remove('/tmp/' + file_name + '.itf')
        message = u'<ol>'
        for myerror in arr_message:
            message += u'<li>' + myerror[0].decode('utf-8') + u'</li>'
            pass
        if len(arr_message) == 0:
            message += u'<li>Die Validierungsroutine konnte keine Fehler feststellen</li>'
        del arr_message
        message += u'</ol>'
        #except:
        #message = 'fehler beim uploaden der datei'
        
    cont = """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html>
    <head>
        <title>IS-Boden CSV-Validator</title>
        <meta http-equiv="Content-Type" content="text/html" charset=utf-8">
        <style type="text/css">        
body {
    margin-left:50px;
    margin-top:20px;
    font-family:arial;
}

.nice {
    background-color:#DDDDDD;
    padding:20px;
    width:800px;
    border:solid;
    border-color:#888888;
    display:block;
    margin-left:auto;
    margin-right:auto;
}

.textblau {
color:#009000;
width:80px;
margin-top:20px;
}
</style>
</head>
    <body>
        <div class="nice">
        <h1>IS-Boden</h1><h2>CSV Validator</h2>
        <p>Bitte w&auml;hlen Sie die zu validierende Datei aus:</p>
        <form action="" method="post" enctype="multipart/form-data">
        <input type="file" name="myfile" class="upload" /> <br />
             <input class="textblau" type="submit" name="submit" value="Submit" />
             <input type="hidden" name="upload" value="True" />
        </form>%s
        </div>
    </body>
</html>
""" % message.encode('utf-8')

    start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8')])

    return [cont]



def random_name():
    """guarantee unique names in the download directory"""
    str_random = str(randint(1,1000))
    str_unixtime = str(int(time.time()))
    return str(str_random + str_unixtime)
