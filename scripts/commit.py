import sqlite3
import csv, json

ROOT = "/path/to/raw/data/"

conn = sqlite3.connect(ROOT + "cbp.sqlite")
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS states (year, fipstate, lfo, naics, empflag, emp, emp_nf, qp1, ap, ap_nf, est, censtate, UNIQUE(year, fipstate, lfo, naics))")

cur.execute("CREATE TABLE IF NOT EXISTS counties (year, fips, naics, empflag, emp, qp1, ap, est, censtate, UNIQUE(year, fips, naics))")

def place_st(yr):
    fields = ["year", "fipstate", "lfo", "naics", "empflag", "emp", "emp_nf", "qp1", "ap", "ap_nf", "est", "censtate"]
    
    print "Year %d" % yr
    data = []
    c = 0
    with open(ROOT + "cbp%02dst.txt" % yr, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            if c == 0:
                labels = row
            else:
                datum = {}
                for i in range(len(row)):
                    datum[labels[i]] = row[i]
                datum['year'] = str(2000 + yr)
                if 'lfo' not in datum:
                    datum['lfo'] = "-"
                if 'emp_nf' not in datum:
                    datum['emp_nf'] = "-"
                if 'ap_nf' not in datum:
                    datum['ap_nf'] = "-"
                
                data.append(datum)
                query = "INSERT INTO states (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) VALUES (%s,%s,'%s','%s','%s',%s,'%s',%s,%s,'%s',%s,%s)" % (tuple(fields + [datum[x] for x in fields]))
                cur.execute(query)
                if c % 25000 == 0:
                    print c
                    conn.commit()                                
            c += 1
    conn.commit()

def place_co(yr):
    fields = ["year", "fips", "naics", "empflag", "emp", "qp1", "ap", "est", "censtate"]
    
    print "Year %d" % yr
    data = []
    c = 0
    with open('/Users/cewilson/Desktop/source/business/cbp/cbp%sco.txt' % yr, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            if c == 0:
                labels = row
            else:
                datum = {}
                for i in range(len(row)):
                    datum[labels[i]] = row[i]
                datum['year'] = str(2000 + yr)
                datum['fips'] = "%02d%03d" % (int(datum['fipstate']), int(datum['fipscty']))
                if 'lfo' not in datum:
                    datum['lfo'] = "-"
                data.append(datum)
                query = "INSERT INTO counties (%s,%s,%s,%s,%s,%s,%s,%s,%s) VALUES (%s,%s,'%s','%s',%s,%s,%s,%s,%s)" % (tuple(fields + [datum[x] for x in fields]))
                cur.execute(query)
                if c % 25000 == 0:
                    print c
                    conn.commit()                                
            c += 1
    conn.commit()


yr = 11
while yr >= 0:
    place_st(yr)
    yr -= 1

conn.commit()
conn.close()
