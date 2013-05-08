import sqlite3, csv

ROOT = "/path/to/database/"
DATA = "/path/to/output/"

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

conn = sqlite3.connect(ROOT + "cbp.sqlite")
conn.row_factory = dict_factory
cur = conn.cursor()

def naics():
    print "query"
    # get data by industry
    query = '''SELECT n.naics,
                    n.description,
                    COUNT(*) as count,
                    SUM(c.emp) as employees,
                    SUM(c.ap) as payroll,
                    SUM(c.est) as establishments
               FROM states as c JOIN naics as n ON c.naics = n.naics
               WHERE c.lfo = "-"
               GROUP BY c.naics'''
    
    keys = None
    codes = {}

    #reduce to unique industries since there are many duplicate codes
    for x in cur.execute(query).fetchall():
        if x["description"] not in codes:
            codes[x["description"]] = x
        else:
            # check to make sure there are not any situations where the same industry has two entries with disagreeing data
            temp = codes[x["description"]]
            if x["employees"] != temp["employees"] or x["establishments"] != temp["establishments"] or x["payroll"] != temp["payroll"]:
                print "COLLISION", x["naics"], temp["naics"], x["description"]
                if len(x["naics"].replace("/", "")) < len(temp["naics"].replace("/", "")):
                    print "replaced %s with %s" % (temp["naics"], x["naics"])
                    codes[x["description"]] = x
                                          

    industries = []

    # weed out very small industries
    for key,code in codes.items():
        if code["employees"] >= 500 and code['establishments'] >= 50 and code['payroll'] >= 10000:
            industries.append([code["naics"], code["description"]])
        else:
            print "Too small", code
            continue
        
        data = cur.execute("SELECT year,fipstate,empflag,emp_nf,emp,ap,ap_nf,est FROM states WHERE naics = '%s' AND lfo='-'" % code["naics"]).fetchall()
        if not keys:
            keys = sorted(data[0].keys())

        print code["naics"], len(data)
        f = open(DATA + 'fips/%s.csv' % code["naics"].replace("/", "-"), 'wb')
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writer.writerow(keys)
        dict_writer.writerows(data)

    print "Writing industry file"

    with open(DATA + 'industries.csv', 'wb') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow(["code", "description"])
        for industry in industries:
            spamwriter.writerow(industry)
 
def combine():
    pass

naics()

conn.close()
