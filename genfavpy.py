#!/usr/bin/python

'''
Created on 13/07/2012

@author: awmaximus
@version: 1.0 
'''

import os
import sys
import glob
import sqlite3
import time
import re
import urllib
from unicodedata import normalize
from xml.dom import minidom

#---------------------------------------------
#base = '/etc/enigma2/genfavpy.sqlite'
base = ':memory:'
lamedb = '/etc/enigma2/lamedb'
rules = '/etc/enigma2/genfavpy.conf'
satellites = '/etc/tuxbox/satellites.xml'
outdir = '/etc/enigma2'
favsat = '-700'
debug = 0
#---------------------------------------------

favfilenames = []
usersats = []
statusmsg = ""
statuspercent = 0
_punct_re = re.compile(r'[\t !"#$%&\'()*/<=>?@\[\\\]^_`{|},:]+')


def removeoldfiles():
    userbouquets = glob.glob(outdir + '/userbouquet.*')
    for userbouquet in userbouquets:
        os.remove(userbouquet)

    bouquetindexes = glob.glob(outdir + '/bouquets.*')
    for bouquetindex in bouquetindexes:
        os.remove(bouquetindex)

def log(msg):
    statusmsg = msg
    sys.stdout.write(msg)
    sys.stdout.flush()


def striplist(l):
    return([x.strip() for x in l])

def unicodeconv(text):
    return normalize('NFKD', text).encode('ascii','ignore')

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def slugify(text, delim=u'_'):
    """Generates an slightly worse ASCII-only slug."""
    result = []
    for word in _punct_re.split(text.lower()):
        word = unicodeconv(word)
        if word:
            result.append(word)
    return unicode(delim.join(result))


con = sqlite3.connect(base)
con.row_factory = dict_factory
cur = con.cursor()

def createtables():  
    cur.execute("""CREATE TABLE satellites
                (satcode TEXT, satname TEXT)""")
    cur.execute("""CREATE TABLE transponders
                (tpcode TEXT, tp TEXT, satcode TEXT)""")
    cur.execute("""CREATE TABLE channels
                (channelid INT, channelcode TEXT, tpcode TEXT,
                code2 TEXT, code3 TEXT, channeltype TEXT,
                code4 TEXT, channelname TEXT)""")   
    cur.execute("""CREATE TABLE rules
                (favname TEXT, channelid INT, satcode TEXT,
                channelnick TEXT)""")

def droptables():
    cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    tables = cur.fetchall()
    for table in tables:
        query = "DROP TABLE %s" % table['name']
        cur.execute(query)
    con.commit()

def cleardb():
    cur.execute('DELETE FROM satellites')
    cur.execute('DELETE FROM transponders')
    cur.execute('DELETE FROM channels')
    cur.execute('DELETE FROM rules')
    con.commit()

def preparedb():
    if base == ':memory:':
        createtables()
    else:
        droptables()
        createtables()

def closedb():
    cur.close()
    con.close()

def satellites2sqllite():
    xmlparse = minidom.parse(satellites)
    for satnode in xmlparse.getElementsByTagName('sat'):
        sat = {
               'satcode':   satnode.getAttribute('position'),
               'satname':   satnode.getAttribute('name')
               }
        cur.execute("""INSERT INTO satellites VALUES ('%(satcode)s',
                                                      '%(satname)s')"""
                    % sat)
    con.commit()

def satcodelist():
    satcodes = []
    query = """SELECT satcode
                    FROM satellites
                ORDER BY satname
            """
    cur.execute(query)
    sats = cur.fetchall()
    for sat in sats:
        satcode = unicodeconv(sat['satcode'])
        satcodes.append(satcode)
    return satcodes

def getsatname(satcode):
    query = """SELECT satname
                    FROM satellites
                WHERE satcode = '%s'
                ORDER BY satname
            """ % satcode
    cur.execute(query)
    satname = cur.fetchone()
    return satname['satname']

def lamedb2sqllite():
    filelamedb = open(lamedb)
    lines = filelamedb.readlines()
    control = 0
    channelid = 0
    cont = 1
    
    for line in lines:
        statuspercent = int((float(cont)/float(len(lines))*100))
        log("\r%d%%" % statuspercent)
        cont += 1
        
        line = line.replace('\n','')
        
        if control == 4:
            continue
              
        if line == 'transponders':
            control = 1
            continue
        elif control == 1 and line == 'end':
            control = 2
            continue
        elif line == 'services':
            control = 3
            continue
        elif control == 3 and line == 'end':
            control = 4
            continue
        elif line.startswith('eDVB') or line == '/' or line.startswith('p:'):
            continue
        
        if control == 1:
            if not line.startswith('\ts '):
                transline = line
            else:
                transline = transline + ':' + line.replace('\ts ','')
                transsplit = transline.split(':')
                transponder = {'tpcode':  transsplit[0],
                               'tp':      transsplit[3],
                               'satcode': transsplit[7]
                               }
                cur.execute("""INSERT INTO transponders VALUES ('%(tpcode)s',
                                                                '%(tp)s',
                                                                '%(satcode)s')"""
                            % transponder)
        elif control == 3:
            if re.match('^.{4}:.{8}:.{4}', line):
                serviceline = line
            else:
                serviceline = serviceline + ':' + line
                servicesplit = serviceline.split(':')
                if servicesplit[6] == '':
                    continue
                
                servicesplit[6] = servicesplit[6].replace("'","")
                channelid += 1
                service = {
                           'channelid':     channelid,
                           'channelcode':   servicesplit[0],
                           'tpcode':        servicesplit[1],
                           'code2':         servicesplit[2],
                           'code3':         servicesplit[3],
                           'channeltype':   servicesplit[4],
                           'code4':         servicesplit[5],
                           'channelname':   servicesplit[6]
                           }
                cur.execute("""INSERT INTO channels VALUES ('%(channelid)s',
                                                          '%(channelcode)s',
                                                          '%(tpcode)s',
                                                          '%(code2)s',
                                                          '%(code3)s',
                                                          '%(channeltype)s',
                                                          '%(code4)s',
                                                          '%(channelname)s')""" % service)

    con.commit()
    log('\n')



def parserules():
    cont = 1
    contlines = 0
    filerules = open(rules)
    for rule in filerules: contlines += 1
    filerules = open(rules)
    for rule in filerules:
        statuspercent = int((float(cont)/float(contlines)*100))
        log("\r%d%%" % statuspercent)
        cont += 1
        
        rule = rule.replace('\n','')
        favname, channellist = rule.partition("=")[::2]
        favname = favname.strip()
        channellist = channellist.split(',')
        channellist = striplist(channellist)
        for channel in channellist:
            channelname = ''
            satcode = ''
            channelnick = ''
            if len(channel.split(':')) == 1:
                satcode = favsat
                channelname = channel.split(':')[0]
            else:
                satcode = channel.split(':')[0]
                channelname = channel.split(':')[1]
            
            channelname = channelname.replace('*','%')
            query = """SELECT DISTINCT ch.channelid, ch.channelname, tp.satcode
                           FROM channels ch
                                INNER JOIN transponders tp
                                    ON ch.tpcode = tp.tpcode
                       WHERE ch.channelname like '%s'
                         AND tp.satcode = '%s'
                       ORDER BY channelname""" % (channelname,satcode)
            cur.execute(query)
            channelscode = cur.fetchall()
            for channelcode in channelscode:
                query = "INSERT INTO rules VALUES ('%s','%s','%s','%s')" % (favname,channelcode['channelid'],satcode,channelnick)
                cur.execute(query)
    con.commit()
    log('\n')

def mkservice(channel, channelnick = False, incsat = False):

    channel['channeltype'] = re.sub("^0+","",channel['channeltype']).upper()
    channel['channelcode'] = re.sub("^0+","",channel['channelcode']).upper()
    channel['code2'] = re.sub("^0+","",channel['code2']).upper()
    channel['tpcode'] = re.sub("^0+","",channel['tpcode']).upper()
        
    if channel['channeltype'] == '25':
        channel['channeltype'] = '19'
    
    service = ""
    if incsat:
        if channelnick:
            service = "#SERVICE 1:0:%(channeltype)s:%(channelcode)s:%(code2)s:1:%(tpcode)s:0:0:0::%(channelnick)s (%(satname)s)\n#DESCRIPTION %(channelnick)s (%(satname)s)" % channel
        else:
            service = "#SERVICE 1:0:%(channeltype)s:%(channelcode)s:%(code2)s:1:%(tpcode)s:0:0:0::%(channelname)s (%(satname)s)\n#DESCRIPTION %(channelname)s (%(satname)s)" % channel
    else:
        if channelnick:
            service = "#SERVICE 1:0:%(channeltype)s:%(channelcode)s:%(code2)s:1:%(tpcode)s:0:0:0::%(channelnick)s\n#DESCRIPTION %(channelnick)s" % channel
        else:
            service = "#SERVICE 1:0:%(channeltype)s:%(channelcode)s:%(code2)s:1:%(tpcode)s:0:0:0:" % channel

    return service

def mkfavfilename(filename):
    name = slugify(unicode(filename, 'UTF-8'))
    favfilenames.append(name)
    return name

def genfav():
    cur.execute("SELECT DISTINCT favname FROM rules")
    rulesall = cur.fetchall()
    cont = 1
    for rule in rulesall:
        if rule['favname'] == 'exclude': continue
        statuspercent = int((float(cont)/float(len(rulesall))*100))
        log("\r%d%%" % statuspercent)
        cont += 1
        if debug > 0: print rule['favname']
        favfilename = mkfavfilename('userbouquet.' + unicodeconv(rule['favname']) + '.genfavpy.tv')
        favfile = open(outdir + '/' + favfilename, 'w')
        favfile.write("#NAME " + rule['favname'].encode('utf-8') + "\n")

        query = """SELECT DISTINCT ch.*, sat.satname
                        FROM channels ch
                            INNER JOIN transponders tp
                                ON ch.tpcode = tp.tpcode
                            INNER JOIN satellites sat
                                ON tp.satcode = sat.satcode
                            WHERE ch.channelid NOT IN (SELECT channelid FROM rules WHERE UPPER(favname) = UPPER('exclude'))
                              AND ch.channelid IN (SELECT channelid FROM rules WHERE UPPER(favname) = UPPER('%s'))
                    ORDER BY ch.channelname
                """ % rule['favname']
        if debug > 1: print query

        cur.execute(query)
        channels = cur.fetchall()
        for channel in channels:
            if debug > 0: print '\t' + channel['channelname']
            #print rule['favname'] + " - " + channel['channelname'] + " (" + channel['satname'] + ")"
            if len(satcodelist()) > 1:
                favfile.write(mkservice(channel=channel, incsat=True).encode('utf-8')+"\n")
            else:
                favfile.write(mkservice(channel=channel, incsat=False).encode('utf-8')+"\n")
        favfile.close()
    log('\n')

def genfavall():
    favtvallfile = open(outdir + '/userbouquet.favourites.tv', 'w')
    favtvallfile.write("#NAME Favourites (TV)\n")
    query = """SELECT DISTINCT ch.*, sat.satname
                    FROM channels ch
                        INNER JOIN transponders tp
                            ON ch.tpcode = tp.tpcode
                        INNER JOIN satellites sat
                            ON tp.satcode = sat.satcode
                        WHERE ch.channeltype != '2'
                          AND ch.channelid NOT IN (SELECT channelid FROM rules WHERE UPPER(favname) = UPPER('exclude'))
                ORDER BY ch.channelname
            """
    cur.execute(query)
    tvchannels = cur.fetchall()
    for tvchannel in tvchannels:
        favtvallfile.write(mkservice(channel=tvchannel, incsat=True).encode('utf-8')+"\n")
    favtvallfile.close()

    favradioallfile = open(outdir + '/userbouquet.favourites.radio', 'w')
    favradioallfile.write("#NAME Favourites (RADIO)\n")
    query = """SELECT DISTINCT ch.*, sat.satname
                    FROM channels ch
                        INNER JOIN transponders tp
                            ON ch.tpcode = tp.tpcode
                        INNER JOIN satellites sat
                            ON tp.satcode = sat.satcode
                        WHERE ch.channeltype = '2'
                          AND ch.channelid NOT IN (SELECT channelid FROM rules WHERE UPPER(favname) = UPPER('exclude'))
                ORDER BY ch.channelname
            """
    cur.execute(query)
    radiochannels = cur.fetchall()
    for radiochannel in radiochannels:
        favradioallfile.write(mkservice(channel=radiochannel, incsat=True).encode('utf-8')+"\n")
    favradioallfile.close()
    

def genfavindex():
    favindexfile = open(outdir + '/bouquets.tv', 'w')
    favindexfile.write('#NAME User - bouquets (TV)\n')
    for favfilename in favfilenames:
        favindexfile.write('#SERVICE 1:7:1:0:0:0:0:0:0:0:FROM BOUQUET "%s" ORDER BY bouquet\n' % favfilename)
    favindexfile.write('#SERVICE 1:7:1:0:0:0:0:0:0:0:FROM BOUQUET "userbouquet.favourites.tv" ORDER BY bouquet\n')
    favindexfile.close()

    radioindexfile = open(outdir + '/bouquets.radio', 'w')
    radioindexfile.write('#NAME User - bouquets (RADIO)\n')
    radioindexfile.write('#SERVICE 1:7:1:0:0:0:0:0:0:0:FROM BOUQUET "userbouquet.favourites.radio" ORDER BY bouquet\n')
    radioindexfile.close()

def reload():
    f = urllib.urlopen("http://127.0.0.1/web/servicelistreload?mode=1")
    s = f.read()
    f.close()
    f = urllib.urlopen("http://127.0.0.1/web/servicelistreload?mode=2")
    s = f.read()
    f.close()

def main():
    log('Removing old files...\n')
    removeoldfiles()
    log('Preparing...\n')
    preparedb()
    log('Reading satellites...\n')
    satellites2sqllite()
    log('Reading channels and transponders...\n')
    lamedb2sqllite()
    log('Reading rules...\n')
    parserules()
    log('Generating favorites...\n')
    genfav()
    log('Generating global favorites...\n')
    genfavall()
    log('Generating favorites indexes...\n')   
    genfavindex()
    closedb()
    log('Reloading...\n')
    reload()
    
    


time1 = time.time()
main()
time2 = time.time()
if debug > 0: print 'Tempo de execucao: ' + str(time2-time1)
print 'Tempo de execucao: ' + str(time2-time1) 






