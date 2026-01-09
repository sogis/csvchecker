#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
Name                    : csv checker
Description           : test a csv file
Date                      : 12/03/2014
copyright               : (C) 2014 by Tobias Reber
email                     : tobas.reber (at) bd.so.ch
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
 functions:
 - main
 - 
"""

try:
    import logging
    import os
    import sys
    import re
    from optparse import OptionParser # parsing script options
    import csv

except ImportError as strerror:
    print "A problem occured during Import ({0})".format(strerror)
    exit()


def main():
    # using script as standalone. parsing command line options
    epilog = """
check if the file is ok and return feedback\n
Examples:
1. check csv-file: 
python csv_checker.py -f mycsv.csv-d 'host=myhostname dbname=mydbname user=myusername password=mypassword' -s afu_isboden_devel\n
2. check db: 
python csv_checker.py  -t db -d 'host=myhostname dbname=mydbname user=myusername password=mypassword' -s afu_isboden_devel
\n"""

    # db connection
    try:
        import psycopg2
    except ImportError as strerror:
        print "A problem occured during Import ({0})".format(strerror)
        exit()
        
    OptionParser.format_epilog = lambda self, formatter: self.epilog
    parser = OptionParser(epilog=epilog)
    csvFile = "" # default csv-File-Path

    # input options when using script as standalone
    parser.add_option("-f", "--file", dest="csvFile", default="", help="path to the csv-file to be checked")
    parser.add_option("-d", "--database", dest="dbConnection", default=False, help="db connection: \'host=dbname user=test password=mypassword\'")
    parser.add_option("-s", "--dbschema", dest="dbSchema", default=False, help="afu_isboden")
    parser.add_option("-t", "--type", dest="checkType", default="csv", help="db or csv")

    (options, args) = parser.parse_args()
    
    if not options.dbConnection or not options.dbSchema:
        parser.print_help()
        sys.exit(1)
    
    if not options.csvFile and not options.checkType:
        if options.checkType != "db":
            parser.error('Please add the path to the csv-file concerned -f')
    
    if not options.csvFile and options.checkType == 'csv':
        parser.error('Please add the path to the csv-file concerned -f')

    if options.csvFile != None:
        csvFile = options.csvFile
        
    if not options.dbConnection:
        parser.error('Please add the db connection: \'host=myhostname dbname=mydbname user=test password=mypassword\'')
        
    if options.dbConnection != None:
        dbConnection = options.dbConnection
        
    if not options.dbSchema:
        parser.error('Please add a schema: afu_isboden')
    
    if options.dbSchema != None:
        dbSchema = options.dbSchema
        
    if options.checkType != None:
        if options.checkType == 'db' or options.checkType == 'csv':
            checkType = options.checkType
            
        
    # db: connection
    try: 
        conn = psycopg2.connect(dbConnection)
        conn.set_isolation_level(0)
        cur = conn.cursor()

    except:
        parser.error('The database connection arguments do not seem to be correct!')

    csvchecker = CsvChecker(cur,  dbSchema)

    # output
    for info in csvchecker.check(checkType,  csvFile):
        print ' - ' + info[0]
    

class CsvChecker:
    """
    checks a csvFile and returns an array
    """
    
    def __init__(self, dbObj=None, dbschema='afu_isboden',  dbConnType='psycopg'):
        # vars
        self.cur = dbObj
        self.arrFeedback = []
        self.bolHasError = False
        self.dbschema = dbschema
        
        self.BOL_LOGGING = False # set logging true or false
           
        # allowed values
        self.arr_gelform = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"]
        self.arr_bodentyp = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
        self.arr_gefuegeform = ["Ek","Ko","Kr","Gr","Sp","Po","Pr","Pl","Br","Fr","Klr","Klk","osm","obl","ofi"]
        self.arr_gefueggr = ["1","2","3","4","5","6","7"]
        self.arr_humusform_wa = ["M","Mt","Mf","MHt","MHf","F","Fm","Fa","Fr","Fl","FHm","FHa","FHr","FHl","L","La","La","Lr","LHa","Lhr","A","T"]
        self.arr_kalkgehalt = ["0","1","2","3","4","5"]
        self.arr_koernkl = ["0","1","2","3","4","5","6","7","8","9","10","11","12","13"] + ["35","37","46","56","57","68","73","76","78","79","86","89"]
        self.arr_skelett = ["0","1","2","3","4","5","6","7","8","9"] + ["20","21","22","23","24","25","26"]
        self.arr_skelett_wald = ["0","1","2","4","6","8"] + ["20","21","22","23","24","25","26"]
        self.arr_skelett_lw = ["0","1","2","3","4","5","6","7","8","9"] + ["20","21","22","23","24","25","26"]
        self.arr_untertyp = ["PE","PK","PM","PA","PU","PS","PP","PL","PT","PD","PB","VL","VF","VU","VA","VB","VK","VS","VT","E0","E1","E2","E3","E4","E5","KE","KH","KR","KF","KT","KA","FB","FP","FE","FQ","FM","FK","FG","FN","FR","ZS","ZK","ZT","ZV","ZL","ZP","L1","L2","L3","L4","LM","I1","I2","I3","I4","G1","G2","G3","G4","G5","G6","R1","R2","R3","R4","R5","DD","ML","MF","MA","MM","MH","OM","OS","OA","OF","OT","T1","T2","T3","HD","HA","HU","HB","HT"]
        self.arr_untertyp_e = ["E0","E1","E2","E3","E4","E5"]
        self.arr_untertyp_k = ["KE","KH","KR","KF","KT","KA"]
        self.arr_untertyp_i = ["I1","I2","I3","I4"]
        self.arr_untertyp_g = ["G1","G2","G3","G4","G5","G6"]
        self.arr_untertyp_r = ["R1","R2","R3","R4","R5"]
        self.arr_untertyp_p = ["PE","PK","PM","PA","PU","PS","PP","PL","PT","PD","PB"]
        self.arr_untertyp_div = ["VL","VF","VU","VA","VB","VK","VS","VT","FB","FP","FE","FQ","FM","FK","FG","FN","FR","ZS","ZK","ZT","ZV","ZL","ZP","L1","L2","L3","L4","LM","DD","ML","MF","MA","MM","MH","OM","OS","OA","OF","OT","T1","T2","T3","HD","HA","HU","HB","HT"]
        self.arr_untertyp = self.arr_untertyp_e + self.arr_untertyp_k + self.arr_untertyp_i + self.arr_untertyp_g + \
                            self.arr_untertyp_r + self.arr_untertyp_p + self.arr_untertyp_div
                            
        self.arr_wasserhhgr = ["a","b","c","d","e","f","g","h","i","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"]
        self.arr_boolean = ["t","f"]
        
        # first field: header-title
        # second field: Is the field mandatory?
        self.headers  = [
                ("gemnr", True),
                ("objnr", True),
                ("is_wald", True),
                ("is_hauptauspraegung", True),
                ("gewichtung_auspraegung", True),
                ("wasserhhgr", True),
                ("bodentyp", True),
                ("gelform", True), # geländeform
                ("geologie", True),
                ("untertyp_e", True),
                ("untertyp_k", False),
                ("untertyp_i", False),
                ("untertyp_g", False),
                ("untertyp_r", False),
                ("untertyp_p", False),
                ("untertyp_div", False),
                ("skelett_ob", True),
                ("skelett_ub", False),
                ("koernkl_ob", True),
                ("koernkl_ub", False),
                ("ton_ob", True),
                ("ton_ub", False),
                ("schluff_ob", True),
                ("schluff_ub", False),
                ("karbgrenze", True),
                ("kalkgeh_ob", True),
                ("kalkgeh_ub", False),
                ("ph_ob", True),
                ("ph_ub", False),
                ("maechtigk_ah", True),
                ("humusgeh_ah", True),
                ("humusform_wa", False),
                ("maechtigk_ahh", False),
                ("gefuegeform_ob", True),
                ("gefueggr_ob", False),
                ("gefuegeform_ub", False),
                ("gefueggr_ub", False),
                ("pflngr", True),
                ("bodpktzahl", True),
                ("bemerkungen", False),
                ("los", True),
                ("kartierjahr", True),
                ("kartierer", True),
                ("kartierquartal", False)]
    
    
    def check(self,  check_type="csv",  csvFile=""):
        """
        this ist the main-check function which delegates the check dependent on the arguments db,csv or intern (GUI-Attribute)
        """
        if check_type == "db":
            return self.check_db()
        if check_type == "csv" and csvFile != "":
            return self.run_check(csvFile, False)
        if check_type == "intern" and csvFile != "":
            return self.run_check(csvFile, True)
            
    
    def check_db(self):
        """ 
        to test an entire database
        """
        sql = """
                SELECT  
                gemnr,
                objnr,
                CASE 
                    WHEN is_wald THEN 't'
                    WHEN NOT is_wald THEN 'f'
                    ELSE ''
                END,
                CASE
                    WHEN is_hauptauspraegung THEN 't'  
                    WHEN NOT is_hauptauspraegung THEN 'f'
                    ELSE ''
                END,
                CASE
                    WHEN gewichtung_auspraegung IS NULL THEN ''
                    ELSE gewichtung_auspraegung::text
                END,
                CASE
                    WHEN wasserhhgr IS NULL THEN ''
                    ELSE wasserhhgr::text  
                END,
                CASE 
                    WHEN bodentyp IS NULL THEN ''
                    ELSE bodentyp::text
                END,
                CASE
                    WHEN gelform IS NULL THEN '' 
                    ELSE gelform::text
                END AS gelform,
                CASE
                    WHEN geologie IS NULL THEN ''
                    ELSE geologie::text  
                END,
                CASE
                    WHEN untertyp_e IS NULL THEN ''
                    ELSE untertyp_e::text
                END,
                CASE
                    WHEN untertyp_k IS NULL THEN ''
                    ELSE untertyp_k::text
                END,
                CASE
                    WHEN untertyp_i IS NULL THEN ''
                    ELSE untertyp_i::text
                END,
                CASE
                    WHEN untertyp_g IS NULL THEN ''
                    ELSE untertyp_g::text
                END,
                CASE
                    WHEN untertyp_r IS NULL THEN ''
                    ELSE untertyp_r::text
                END,
                CASE
                    WHEN untertyp_p IS NULL THEN ''
                    ELSE untertyp_p::text
                END,
                CASE
                    WHEN untertyp_div IS NULL THEN ''
                    ELSE untertyp_div::text
                END,
                CASE
                    WHEN skelett_ob IS NULL THEN ''
                    ELSE skelett_ob::text
                END,
                CASE
                    WHEN skelett_ub IS NULL THEN ''
                    ELSE skelett_ub::text
                END,
                CASE
                    WHEN koernkl_ob IS NULL THEN ''
                    ELSE koernkl_ob::text
                END,
                CASE
                    WHEN koernkl_ub IS NULL THEN ''
                    ELSE koernkl_ub::text
                END,
                CASE
                    WHEN ton_ob IS NULL THEN ''
                    ELSE ton_ob::text
                END,
                CASE
                    WHEN ton_ub IS NULL THEN ''
                    ELSE ton_ub::text
                END,
                CASE
                    WHEN schluff_ob IS NULL THEN ''
                    ELSE schluff_ob::text
                END,
                CASE
                    WHEN schluff_ub IS NULL THEN ''
                    ELSE schluff_ub::text
                END,
                CASE
                    WHEN karbgrenze IS NULL THEN ''
                    ELSE karbgrenze::text
                END,
                CASE
                    WHEN kalkgeh_ob IS NULL THEN ''
                    ELSE kalkgeh_ob::text
                END,
                CASE
                    WHEN kalkgeh_ub IS NULL THEN ''
                    ELSE kalkgeh_ub::text
                END,
                CASE
                    WHEN ph_ob IS NULL THEN ''
                    ELSE ph_ob::text
                END,
                CASE
                    WHEN ph_ub IS NULL THEN ''
                    ELSE ph_ub::text
                END,
                CASE
                    WHEN maechtigk_ah IS NULL THEN ''
                    ELSE maechtigk_ah::text
                END,
                CASE
                    WHEN humusgeh_ah IS NULL THEN ''
                    ELSE humusgeh_ah::text
                END,
                CASE
                    WHEN humusform_wa IS NULL THEN ''
                    ELSE humusform_wa::text
                END,
                CASE 
                    WHEN maechtigk_ahh IS NULL THEN ''
                    ELSE maechtigk_ahh::text
                END,
                CASE
                    WHEN gefuegeform_ob IS NULL THEN ''
                    ELSE gefuegeform_ob::text
                END,
                CASE
                    WHEN gefueggr_ob IS NULL THEN ''
                    ELSE gefueggr_ob::text
                END,
                CASE
                    WHEN gefuegeform_ub IS NULL THEN ''
                    ELSE gefuegeform_ub::text
                END,
                CASE
                    WHEN gefueggr_ub IS NULL THEN ''
                    ELSE gefueggr_ub::text
                END,
                CASE
                    WHEN pflngr IS NULL THEN ''
                    ELSE pflngr::text
                END,
                CASE
                    WHEN bodpktzahl IS NULL THEN ''
                    ELSE bodpktzahl::text
                END,
                CASE
                    WHEN bemerkungen IS NULL THEN ''
                    ELSE bemerkungen::text 
                END,
                CASE
                    WHEN los IS NULL THEN ''
                    ELSE los::text
                END,
                CASE
                    WHEN kartierjahr IS NULL THEN ''
                    ELSE kartierjahr::text
                END,
                CASE
                    WHEN kartierer IS NULL THEN ''
                    ELSE kartierer::text
                END,
                CASE
                    WHEN kartierquartal IS NULL THEN ''
                    ELSE kartierquartal::text
                END
                FROM 
                %s.bodeneinheit_datenabgabe_v 
                WHERE archive = 0
                """ % self.dbschema
                
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        mycsv = []
        myobj = {}

        for row in rows:
            i = 0 # reset row counter
            myobj = {}
            for header in self.headers:
                header = header[0] # only header name
                # there is no distinction of untertyp in the datamodell
                myobj.update({ header : str(row[i])})
                i = i + 1 # rise row counter
            mycsv.append(myobj)
        return self.run_check(mycsv, True)            


    def run_check(self, csvFile, bol_not_csv=False):
        """
        The actual check of the csv-object is being done here
        """
        self.arrFeedback = []
        self.bolHasError = False
        if self.BOL_LOGGING:
            logging.basicConfig(filename='/opt/wwwroot/sogis/gifs/qgis_csv_checker.log', level=logging.DEBUG, format='%(asctime)s %(message)s')
            logging.debug('')
            logging.debug('---------------')
            logging.debug('Skript started:')
            logging.debug('---------------')
            logging.debug('')


        if not bol_not_csv:    
            #remove BOM thanks to microsoft and endianness 
            os.system("sed -i '1 s/^\xef\xbb\xbf//' " + csvFile) 
            
            # check filename for cryptic signs like ' or " or whatever 
            if re.match('^[a-zA-Z0-9_\-/\.]+$', csvFile) is None:
                self.arrFeedback.append(["Ein grober Fehler wurde verursacht. Der Dateiname darf nebst - und _ keine Sonderzeichen enthalten. \
                                Die Zeichen '\"?! etc. sind im Dateinamen nicht erlaubt.", 1]) 
                bolNoErrors = False # a test faild
                return self.arrFeedback

            # check utf-8
            ifile = open(csvFile, 'r')
            ifile.seek(0) # reset file
            data = ifile.read()

            self.is_utf8(data)
    

            # check headers
            ifile.seek(0) # reset file
            try:
                reader = csv.reader(ifile, delimiter=';', quotechar='"')
                headers = reader.next()
            except:
                self.arrFeedback.append(["Ein grober Fehler wurde verursacht. Handelt es sich um eine korrekt formatierte CSV-Datei? \
Die Datei muss Tabulatorgetrennt mit einem (\")-Zeichen als Escape-Sequenz sein.", 1]) 
                bolNoErrors = False # a test faild
                return self.arrFeedback
            
            my_count = 1 # counter to find the corresponding line in the csv
            for line in reader:
                my_count += 1
                if len(headers) != len(line):
                    self.arrFeedback.append(["Ein grober Fehler wurde verursacht. Die Anzahl der Zeilen deckt sich nicht mit der Anzahl Überschriften (oder Problem mit Kombination von \";-Zeichen). Siehe Zeile " + str(my_count), 1])
                    bolNoErrors = False
                    return self.arrFeedback
                
        else:
            headers = []
    
        # only csv has to be checked, if all headers/attributes are available
        myswitch = False
        if bol_not_csv: 
            myswitch = bol_not_csv
        else:
            myswitch = self.has_all_headers(headers)

        if myswitch:
    
            # check gemnr
            self.check_gemnr(csvFile if bol_not_csv else self.get_csvDict(ifile))

            # check objnr
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'objnr', 99, 10000)

            # check if obligatory fields are empty
            self.has_all_values(csvFile if bol_not_csv else self.get_csvDict(ifile))

            # check wald
            self.check_codelist(self.arr_boolean, csvFile if bol_not_csv else self.get_csvDict(ifile), 'is_wald', True)
            
            # check wald 4 ziffern, landwirtschaft 3 ziffern
            self.check_len_objnr(csvFile if bol_not_csv else self.get_csvDict(ifile))
            
            # check hauptausprägung
            self.check_codelist(self.arr_boolean, csvFile if bol_not_csv else self.get_csvDict(ifile), 'is_hauptauspraegung', True)
        
            # check gelform
            self.check_codelist(self.arr_gelform, csvFile if bol_not_csv else self.get_csvDict(ifile), 'gelform')
        
            # check bodentyp
            self.check_codelist(self.arr_bodentyp, csvFile if bol_not_csv else self.get_csvDict(ifile), 'bodentyp')
        
            # check gefuegeform_ob
            self.check_codelist(self.arr_gefuegeform, csvFile if bol_not_csv else self.get_csvDict(ifile), 'gefuegeform_ob')
        
            # check gefuegeform_ub
            self.check_codelist(self.arr_gefuegeform, csvFile if bol_not_csv else self.get_csvDict(ifile), 'gefuegeform_ub')
        
            # check gefueggr_ob
            self.check_codelist(self.arr_gefueggr, csvFile if bol_not_csv else self.get_csvDict(ifile), 'gefueggr_ob')
        
            # check gefueggr_ub
            self.check_codelist(self.arr_gefueggr, csvFile if bol_not_csv else self.get_csvDict(ifile), 'gefueggr_ub')
        
            # check humusform_wa
            self.check_only_4_wald(csvFile if bol_not_csv else self.get_csvDict(ifile), 'humusform_wa')
            self.check_codelist(self.arr_humusform_wa, csvFile if bol_not_csv else self.get_csvDict(ifile), 'humusform_wa')
        
            # check kalkgehalt_ob
            self.check_codelist(self.arr_kalkgehalt, csvFile if bol_not_csv else self.get_csvDict(ifile), 'kalkgeh_ob')
        
            # check kalkgehalt_ub
            self.check_codelist(self.arr_kalkgehalt, csvFile if bol_not_csv else self.get_csvDict(ifile), 'kalkgeh_ub')
        
            # check koernkl_ob
            self.check_codelist(self.arr_koernkl, csvFile if bol_not_csv else self.get_csvDict(ifile), 'koernkl_ob')
        
            # check koernkl_ub
            self.check_codelist(self.arr_koernkl, csvFile if bol_not_csv else self.get_csvDict(ifile), 'koernkl_ub')
                
            # check skelett_ob
            self.check_codelist_skelett(self.arr_skelett_lw,  self.arr_skelett_wald, csvFile if bol_not_csv else self.get_csvDict(ifile), 'skelett_ob')
        
            # check skelett_ub
            self.check_codelist_skelett(self.arr_skelett_lw, self.arr_skelett_wald, csvFile if bol_not_csv else self.get_csvDict(ifile), 'skelett_ub')
        
            # check wasserhhgr
            self.check_codelist(self.arr_wasserhhgr, csvFile if bol_not_csv else self.get_csvDict(ifile), 'wasserhhgr')

            # check kartierer
            self.is_known_kartierer(csvFile if bol_not_csv else self.get_csvDict(ifile))
            
            # check untertyp
            self.check_untertyp(csvFile if bol_not_csv else self.get_csvDict(ifile), 'untertyp_e', self.arr_untertyp_e)
            self.check_untertyp(csvFile if bol_not_csv else self.get_csvDict(ifile), 'untertyp_k', self.arr_untertyp_k)
            self.check_untertyp(csvFile if bol_not_csv else self.get_csvDict(ifile), 'untertyp_i', self.arr_untertyp_i)
            self.check_untertyp(csvFile if bol_not_csv else self.get_csvDict(ifile), 'untertyp_g', self.arr_untertyp_g)
            self.check_untertyp(csvFile if bol_not_csv else self.get_csvDict(ifile), 'untertyp_r', self.arr_untertyp_r)
            self.check_untertyp(csvFile if bol_not_csv else self.get_csvDict(ifile), 'untertyp_p', self.arr_untertyp_p)
            self.check_untertyp(csvFile if bol_not_csv else self.get_csvDict(ifile), 'untertyp_div', self.arr_untertyp_div)

            # check bemerkungen
            self.is_within_length_text(csvFile if bol_not_csv else self.get_csvDict(ifile), 'bemerkungen', 300)

            # check los
            self.is_within_length_text(csvFile if bol_not_csv else self.get_csvDict(ifile), 'los', 25)
            
            # check kartierquartal
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'kartierquartal', 1, 5, 'int')
            
            # check kartierjahr
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'kartierjahr', 1899, 2030, 'int')

            # check ton
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'ton_ob', 0, 100, 'int')
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'ton_ub', 0, 100, 'int')

            # check schluff            
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'schluff_ob', 0, 100, 'int')
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'schluff_ub', 0, 100, 'int')

            # check kalkgeh
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'kalkgeh_ob', -1, 6, 'int')
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'kalkgeh_ub', -1, 6, 'int')

            # ph
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'ph_ob', 0, 11, 'float')
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'ph_ub', 0, 11, 'float')

            # maechtigkeit
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'maechtigk_ah', -1, 100)

            # humusgeh
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'humusgeh_ah', -0.001, 80.001, 'float')

            # karbgrenze
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'karbgrenze', -2, 501, 'int')

            # check auspraegung
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'gewichtung_auspraegung', 0.001, 100)

            # check bodenpkzahl
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'bodpktzahl', 0, 101)
            
            # check bflngr
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'pflngr', 0, 251, 'int')

            # check maechtigk_ahh
            self.check_only_4_wald(csvFile if bol_not_csv else self.get_csvDict(ifile),  'maechtigk_ahh')
            self.is_number_within(csvFile if bol_not_csv else self.get_csvDict(ifile), 'maechtigk_ahh', -1, 21, 'float')
    

            # checks with relations between attributes
            # ----------------------------------------

            # check gewichtung total = 100
            self.is_gewichtung_ok(csvFile if bol_not_csv else self.get_csvDict(ifile))

            # check only one hauptauspraegung
            self.is_only_one_hauptauspraegung_ok(csvFile if bol_not_csv else self.get_csvDict(ifile))
            
            # check hauptausprägung has biggest gewichtung
            self.has_hauptauspraegung_biggest_gewichtung(csvFile if bol_not_csv else self.get_csvDict(ifile))
    
            # check wasserhhgr - pflngr
            self.if_wasserhhgr_then_pflngr(csvFile if bol_not_csv else self.get_csvDict(ifile))      

            # check bodentyp - untertyp
            self.if_bodentyp_then_untertyp(csvFile if bol_not_csv else self.get_csvDict(ifile))      

            # check bodentyp - humusgeh_ah
            self.if_bodentyp_then_humusgeh_ah(csvFile if bol_not_csv else self.get_csvDict(ifile))      

            # check only_one_untertyp
            self.only_one_untertyp(csvFile if bol_not_csv else self.get_csvDict(ifile), ['untertyp_g','untertyp_i','untertyp_r', \
                                                                                    'untertyp_e'])      
                                                                                    
            # check koernkl - ton 
            self.if_koernkl_then_ton(csvFile if bol_not_csv else self.get_csvDict(ifile), 'ob') 
            self.if_koernkl_then_ton(csvFile if bol_not_csv else self.get_csvDict(ifile), 'ub') 
            
            # check koernkl - schluff
            self.if_koernkl_then_schluff(csvFile if bol_not_csv else self.get_csvDict(ifile), 'ob') 
            self.if_koernkl_then_schluff(csvFile if bol_not_csv else self.get_csvDict(ifile), 'ub') 
            
            # karbgrenze - kalkgeh_ob
            self.if_karbgrenze_then_kalkgeh_ob(csvFile if bol_not_csv else self.get_csvDict(ifile))
            
            # ph_ub - untertyp
            self.if_ph_ub_then_untertyp_e(csvFile if bol_not_csv else self.get_csvDict(ifile))


        if not bol_not_csv:
            ifile.close()
        
        if self.BOL_LOGGING:
            logging.debug(str(self.arrFeedback))
    
        #for error in arrFeedback:
        #    print '> ' + error
        return self.arrFeedback
    

    def get_csvDict(self, ifile):
        ifile.seek(0) #reset file
        return csv.DictReader(ifile, delimiter=';', quotechar='"')

    
    def is_empty(self, field):
        """return a boolean, whether the field is empty or not"""
        field = field.strip()
        if field == '':
            return False
        else:
            return True


    def is_type_number(self, num_number, type_check):
        """depending on the number type to check, it returns if the number is valid"""
        if type_check == 'int':
            return self.is_int(num_number)
        elif type_check == 'float':
            return self.is_float(num_number) 
        else:
            raise Exception("is_number_type has to be 'int' or 'float'")


    def is_float(self, s):
        """Checks if the input is a float"""
        try:
            float(s)
            return True
        except:
            return False


    def is_int(self, s):
        """check if the intput is an int"""
        try:
            x = int(s)
            if str(x) == str(s):
                return True
            else:
                return False
        except:
            return False 
            

    def check_len_objnr(self, dictContent):
        """"check, if is_wald == t, then objnr has to have a length of 4 numbers. when is_wald == 3 then objnr has to have a length of 3 numbers"""
        bol_ok = True
        str_wrong = ''
        str_wrong_entries = ''
        arr_wrong = []
        
        for line in dictContent:
            str_id = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'])
            
            if ((str(line['is_wald']) == 'f' and len(line['objnr']) != 4) or
                (str(line['is_wald']) == 't' and len(line['objnr']) != 3)):
                bol_ok = False
                str_wrong = str_id
            
            if str_wrong not in arr_wrong and str_wrong != '':
                str_wrong_entries += str_wrong + ', '
                arr_wrong.append(str_wrong)
                
        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["is_wald: Kombination is_wald länge objnr ist falsch: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False
        
            
    def is_gewichtung_ok(self, dictContent):
        """the sum of weighting (Gewichtung) of Haupt- and Nebenausprägungen has to be exactly 100"""
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        arr_obj_gemnr = []
        dict_gewichtung = {}
        str_wrong = ''
        for line in dictContent:
            str_id = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr']) # id for unique ausprägung
            if not str_id in dict_gewichtung:
                try:
                    dict_gewichtung[str_id] = int(line['gewichtung_auspraegung'])
                except:
                    dict_gewichtung[str_id] = 0
            else:
                try:
                    dict_gewichtung[str_id] = dict_gewichtung[str_id] + int(line['gewichtung_auspraegung'])
                except:
                    pass

        for item in dict_gewichtung.items():
            if item[1] != 100:
                bol_ok = False
                str_wrong = str(item[0]) + ' mit Gewichtung ' + str(item[1])

                if str_wrong not in arr_wrong or str_wrong != '':
                            str_wrong_entries += str_wrong + ', '
                            arr_wrong.append(str_wrong)
        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["gewichtung_auspraegung: Die Totale Gewichtung ungleich 100: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False
            

    def is_only_one_hauptauspraegung_ok(self, dictContent):
        """exactly one Hauptausprägung per feature is allowed"""
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        arr_obj_gemnr = []
        dict_is_hauptauspraegung = {}
        str_wrong = ''
        for line in dictContent:
            str_id = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr']) # id for unique ausprägung
            if not str_id in dict_is_hauptauspraegung:
                try:
                    dict_is_hauptauspraegung[str_id] = line['is_hauptauspraegung'] 
                except:
                    dict_is_hauptauspraegung[str_id] = 'f' # dummy value
            else:
                try:
                    dict_is_hauptauspraegung[str_id] = str(dict_is_hauptauspraegung[str_id]) + str(line['is_hauptauspraegung'])
                except:
                    pass

        for item in dict_is_hauptauspraegung.items():
            if str(item[1]).count('t') != 1:
                bol_ok = False
                str_wrong = str(item[0])

                if str_wrong not in arr_wrong or str_wrong != '':
                            str_wrong_entries += str_wrong + ', '
                            arr_wrong.append(str_wrong)
        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["is_hauptauspraegung: Hat nicht genau eine Hauptauspraegung: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False


    def has_hauptauspraegung_biggest_gewichtung(self, dictContent):
        """the Hauptausprägung has to have the stronges weighting (Gewichtung)"""
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        arr_obj_gemnr = []
        dict_auspraegung = {}
        str_wrong = ''

        for line in dictContent:
            str_id = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr']) # id for unique ausprägung
            if not str_id in dict_auspraegung:
                try:
                    dict_auspraegung[str_id] = int(line['gewichtung_auspraegung']), line['is_hauptauspraegung']
                except:
                    dict_auspraegung[str_id] = 0,'f' 
            else:
                try:
                    if int(line['gewichtung_auspraegung']) > int(dict_auspraegung[str_id][0]):
                        dict_auspraegung[str_id] = int(line['gewichtung_auspraegung']), line['is_hauptauspraegung'] 
                except:
                    pass

        for item in dict_auspraegung.items():
            if item[1][1] != 't':
                bol_ok = False
                str_wrong = str(item[0])

                if str_wrong not in arr_wrong or str_wrong != '':
                        str_wrong_entries += str_wrong + ', '
                        arr_wrong.append(str_wrong)
        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["gewichtung_auspraegung: Die Hauptausprägung hat nicht die grösste Gewichtung: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False


    def if_wasserhhgr_then_pflngr(self, dictContent): 
        """check, if right pflngr when wasserhhgr"""
        bol_ok = True
        str_wrong = ''
        str_wrong_entries = ''
        arr_wrong = []
        
        for line in dictContent:
            try:
                str_id = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'] + " (" + str(line['wasserhhgr']) + ", " + str(line['pflngr'])  +")")
                if str(line['wasserhhgr']).strip() == 'a' and int(line['pflngr']) < 100:
                    bol_ok = False
                    str_wrong = str_id

                if str(line['wasserhhgr']).strip() in ['f','s','k'] and int(line['pflngr']) < 70:
                    bol_ok = False
                    str_wrong = str_id
                
                if str(line['wasserhhgr']).strip() in ['b'] and (int(line['pflngr']) < 70 or int(line['pflngr']) > 99):
                    bol_ok = False
                    str_wrong = str_id
                    
                if str(line['wasserhhgr']).strip() in ['o'] and (int(line['pflngr']) < 50 or int(line['pflngr']) > 100):
                    bol_ok = False
                    str_wrong = str_id

                if str(line['wasserhhgr']).strip() in ['c','g','l','t','v'] and (int(line['pflngr']) < 50 or int(line['pflngr']) > 69):
                    bol_ok = False
                    str_wrong = str_id

                if str(line['wasserhhgr']).strip() in ['d','h','m','q','x'] and (int(line['pflngr']) < 30 or int(line['pflngr']) > 49):
                    bol_ok = False
                    str_wrong = str_id

                if str(line['wasserhhgr']).strip() in ['p','u','w'] and (int(line['pflngr']) < 10 or int(line['pflngr']) > 50):
                    bol_ok = False
                    str_wrong = str_id

                if str(line['wasserhhgr']).strip() in ['e','i','n','r','y'] and (int(line['pflngr']) > 30):
                    bol_ok = False
                    str_wrong = str_id

                if str(line['wasserhhgr']).strip() == 'z' and (int(line['pflngr']) > 10):
                    bol_ok = False
                    str_wrong = str_id

                if str_wrong not in arr_wrong and str_wrong != '':
                    str_wrong_entries += str_wrong + ', '
                    arr_wrong.append(str_wrong)
    
            except:
                bol_ok = False
                str_wrong = "Generell falsche Werte, "
                if str_wrong not in arr_wrong and str_wrong != '':
                    str_wrong_entries += str_wrong
                    arr_wrong.append(str_wrong)

        if bol_ok:
            return True

        else:
            self.arrFeedback.append(["wasserhhgr und pflngr: Kombination ist falsch: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False


    def is_number_within(self, dictContent, field_name, int_min, int_max, num_type='float'):
        """checks if it is a quartal. num_type={int|float}"""
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        str_wrong = ''
        for line in dictContent:
            the_number = line[field_name]
            the_number = the_number.strip()
            if the_number != '': # if not empty
                if self.is_type_number(the_number, num_type):
                    if ( float(the_number) > float(int_max) ) or ( float(the_number) < float(int_min) ):
                        bol_ok = False
                        str_wrong = str(the_number)
                else:
                    bol_ok = False
                    str_wrong = str(the_number)
                
                if str_wrong not in arr_wrong and str_wrong != '':
                    str_wrong_entries += str_wrong + ', '
                    arr_wrong.append(str_wrong)
                
        if bol_ok:
            return True
        else:
            self.arrFeedback.append([field_name + ": Folgendes sind keine gültigen Zahlen: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False


    def if_bodentyp_then_untertyp(self, dictContent): 
        """check, if bodentyp then right untertyp, works only for single entries"""
        bol_ok = True
        str_wrong = ''
        str_wrong_entries = ''
        arr_wrong = []
        test_untertypen = ['untertyp_i','untertyp_g']
        
        for line in dictContent:
            for untertyp in test_untertypen:
                bol_build_string = False
                str_untertyp = line[untertyp].replace('{','').replace('}','').replace(' ', '')
                arr_test_untertypen = str_untertyp.split(',')
                str_untertyp = arr_test_untertypen[0] # should only be one (test only_one_untertyp), but to improve stability
                
                if untertyp == 'untertyp_i':
                    if str(line['bodentyp']) == 'I' and str_untertyp not in ['I3','I4']:
                        bol_build_string = True
                    if str(line['bodentyp']) == 'Y' and str_untertyp not in ['I3','I4']:
                        bol_build_string = True
                if untertyp == 'untertyp_g':
                    if str(line['bodentyp']) == 'V' and str_untertyp not in ['G4','G5']:
                        bol_build_string = True
                    if str(line['bodentyp']) == 'W' and str_untertyp not in ['G5','G6']:
                        bol_build_string = True
                    if str(line['bodentyp']) == 'G' and str_untertyp not in ['G5','G6']:
                        bol_build_string = True


                if bol_build_string == True:
                    str_wrong = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'])
                    str_wrong += "(" + str(line['bodentyp']) + ", " + str_untertyp + ")"
                    #print str_wrong
                    if str_wrong not in arr_wrong:
                        str_wrong_entries += str_wrong + ', '
                        arr_wrong.append(str_wrong)
                        bol_ok = False

        if bol_ok:
            return True

        else:
            self.arrFeedback.append(["bodentyp und untertyp: Kombination ist falsch: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False
       
       
    def if_koernkl_then_ton(self,  dictContent, ober_unter):
        """check, if koernkl has correct ton entries"""
        bol_ok = True
        str_wrong = ''
        str_wrong_entries = ''
        arr_wrong = []
        try:
            for line in dictContent:
                    bol_build_string = False
                    if str(line['koernkl_' + ober_unter]) == '1' and (float(line['ton_' + ober_unter]) < 0.0 or float(line['ton_' + ober_unter]) > 5.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '2' and (float(line['ton_' + ober_unter]) < 0.0 or float(line['ton_' + ober_unter]) > 5.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '3' and (float(line['ton_' + ober_unter]) < 5.0 or float(line['ton_' + ober_unter]) > 10.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '4' and (float(line['ton_' + ober_unter]) < 10.0 or float(line['ton_' + ober_unter]) > 15.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '5' and (float(line['ton_' + ober_unter]) < 15.0 or float(line['ton_' + ober_unter]) > 20.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '6' and (float(line['ton_' + ober_unter]) < 20.0 or float(line['ton_' + ober_unter]) > 30.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '7' and (float(line['ton_' + ober_unter]) < 30.0 or float(line['ton_' + ober_unter]) > 40.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '8' and (float(line['ton_' + ober_unter]) < 40.0 or float(line['ton_' + ober_unter]) > 50.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '9' and (float(line['ton_' + ober_unter]) < 50.0 or float(line['ton_' + ober_unter]) > 100.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '10' and (float(line['ton_' + ober_unter]) < 0.0 or float(line['ton_' + ober_unter]) > 10.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '11' and (float(line['ton_' + ober_unter]) < 0.0 or float(line['ton_' + ober_unter]) > 10.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '12' and (float(line['ton_' + ober_unter]) < 10.0 or float(line['ton_' + ober_unter]) > 30.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '13' and (float(line['ton_' + ober_unter]) < 30.0 or float(line['ton_' + ober_unter]) > 70.0):
                        bol_build_string = True
                   
                    if bol_build_string == True:
                        str_wrong = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'])
                        str_wrong += "(" + str(line['koernkl_' + ober_unter]) + ", " + str(line['ton_' + ober_unter])  + ")"
                        #print str_wrong
                        if str_wrong not in arr_wrong:
                            str_wrong_entries += str_wrong + ', '
                            arr_wrong.append(str_wrong)
                            bol_ok = False
                            
        except:
            bol_ok = False
            str_wrong = "Generell falsche Werte, "
            if str_wrong not in arr_wrong and str_wrong != '':
                str_wrong_entries += str_wrong
                arr_wrong.append(str_wrong)
            
        if bol_ok:
            return True

        else:
            self.arrFeedback.append(["koernkl_" + ober_unter + " ton_" + ober_unter + " Kombination ist falsch: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False
        
        
    def if_koernkl_then_schluff(self,  dictContent, ober_unter):
        """check, if koernkl has correct schluff entries"""
        bol_ok = True
        str_wrong = ''
        str_wrong_entries = ''
        arr_wrong = []
        try:
            for line in dictContent:
                    bol_build_string = False
                    if str(line['koernkl_' + ober_unter]) == '1' and (float(line['schluff_' + ober_unter]) < 0.0 or float(line['schluff_' + ober_unter]) > 15.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '2' and (float(line['schluff_' + ober_unter]) < 15.0 or float(line['schluff_' + ober_unter]) > 50.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '3' and (float(line['schluff_' + ober_unter]) < 0.0 or float(line['schluff_' + ober_unter]) > 50.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '4' and (float(line['schluff_' + ober_unter]) < 0.0 or float(line['schluff_' + ober_unter]) > 50.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '5' and (float(line['schluff_' + ober_unter]) < 0.0 or float(line['schluff_' + ober_unter]) > 50.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '6' and (float(line['schluff_' + ober_unter]) < 0.0 or float(line['schluff_' + ober_unter]) > 50.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '7' and (float(line['schluff_' + ober_unter]) < 0.0 or float(line['schluff_' + ober_unter]) > 50.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '8' and (float(line['schluff_' + ober_unter]) < 0.0 or float(line['schluff_' + ober_unter]) > 50.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '9' and (float(line['schluff_' + ober_unter]) < 0.0 or float(line['schluff_' + ober_unter]) > 50.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '10' and (float(line['schluff_' + ober_unter]) < 50.0 or float(line['schluff_' + ober_unter]) > 70.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '11' and (float(line['schluff_' + ober_unter]) < 70.0 or float(line['schluff_' + ober_unter]) > 100.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '12' and (float(line['schluff_' + ober_unter]) < 50.0 or float(line['schluff_' + ober_unter]) > 90.0):
                        bol_build_string = True
                    if str(line['koernkl_' + ober_unter]) == '13' and (float(line['schluff_' + ober_unter]) < 50.0 or float(line['schluff_' + ober_unter]) > 70.0):
                        bol_build_string = True
                   
                    if bol_build_string == True:
                        str_wrong = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'])
                        str_wrong += "(" + str(line['koernkl_' + ober_unter]) + ", " + str(line['schluff_' + ober_unter])  + ")"
                        #print str_wrong
                        if str_wrong not in arr_wrong:
                            str_wrong_entries += str_wrong + ', '
                            arr_wrong.append(str_wrong)
                            bol_ok = False
        except:
            bol_ok = False
            str_wrong = "Generell falsche Werte, "
            if str_wrong not in arr_wrong and str_wrong != '':
                str_wrong_entries += str_wrong
                arr_wrong.append(str_wrong)
            
        if bol_ok:
            return True

        else:
            self.arrFeedback.append(["koernkl_" + ober_unter + " schluff_" + ober_unter + " Kombination ist falsch: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False


    def if_bodentyp_then_humusgeh_ah(self, dictContent): 
        """check, if bodentyp then right humusgeh_ah"""
        bol_ok = True
        str_wrong = ''
        str_wrong_entries = ''
        arr_wrong = []
        
        for line in dictContent:
            try:
                if str(line['bodentyp']).strip() == 'M' and int(line['humusgeh_ah']) < 30:
                    str_wrong = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'])
                    str_wrong += "(" + str(line['bodentyp']) + ", " + str(line['humusgeh_ah'])+ ")"
                    #print str_wrong
                    if str_wrong not in arr_wrong:
                        str_wrong_entries += str_wrong + ', '
                        arr_wrong.append(str_wrong)
                        bol_ok = False
                        
            except:
                bol_ok = False
                str_wrong = "Generell falsche Werte, "
                if str_wrong not in arr_wrong and str_wrong != '':
                    str_wrong_entries += str_wrong
                    arr_wrong.append(str_wrong)
            
        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["bodentyp und humusgeh_ah: Bei bodentyp M muss der humusgeh_ah > 30 sein: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False
            
            
    def if_karbgrenze_then_kalkgeh_ob(self, dictContent): 
        """check, if certain karbgrenze then certain kalkgeh_ob"""
        bol_ok = True
        str_wrong = ''
        str_wrong_entries = ''
        arr_wrong = []
        
        for line in dictContent:
            try:
                if (int(line['karbgrenze']) == 0 and int(line['kalkgeh_ob']) <3) \
                   or (int(line['karbgrenze']) == -1 and int(line['kalkgeh_ob']) >3):
                    str_wrong = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'])
                    str_wrong += "(" + str(line['karbgrenze']) + ", " + str(line['kalkgeh_ob'])+ ")"
                    #print str_wrong
                    if str_wrong not in arr_wrong:
                        str_wrong_entries += str_wrong + ', '
                        arr_wrong.append(str_wrong)
                        bol_ok = False
                        
            except:
                bol_ok = False
                str_wrong = "Generell falsche Werte, "
                if str_wrong not in arr_wrong and str_wrong != '':
                    str_wrong_entries += str_wrong
                    arr_wrong.append(str_wrong)
            
        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["karbgrenze und kalkgeh_ob: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False
            

    def if_ph_ub_then_untertyp_e(self, dictContent): 
        """check, if certain ph_ub then certain untertyp_e"""
        bol_ok = True
        str_wrong = ''
        str_wrong_entries = ''
        arr_wrong = []
        bol_build_string = False
        
        for line in dictContent:
            try:
                if self.is_float(line['ph_ub']):
                    str_untertyp_e = line['untertyp_e'].replace('{','').replace('}','').replace(' ', '')
                    arr_untertypen_e = str_untertyp_e.split(',')
                    bol_build_string = False
                    for untertyp_e in arr_untertypen_e:
                        if float(line['ph_ub']) > 6.7 and untertyp_e.upper() != 'E0':
                            bol_build_string = True
                        if float(line['ph_ub']) >= 6.2 and float(line['ph_ub']) <= 6.7 and untertyp_e.upper() != 'E1':
                            bol_build_string = True
                        if float(line['ph_ub']) >= 5.1 and float(line['ph_ub']) <= 6.1 and untertyp_e.upper() != 'E2':
                            bol_build_string = True
                        if float(line['ph_ub']) >= 4.3 and float(line['ph_ub']) <= 5.0 and untertyp_e.upper() != 'E3':
                            bol_build_string = True
                        if float(line['ph_ub']) >= 3.3 and float(line['ph_ub']) <= 4.2 and untertyp_e.upper() != 'E4':
                            bol_build_string = True
                        if float(line['ph_ub']) < 3.3 and untertyp_e.upper() != 'E5':
                            bol_build_string = True
                        
                        if bol_build_string == True:
                            str_wrong = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'])
                            str_wrong += "(" + str(line['ph_ub']) + ", " + str(line['untertyp_e'])  + ")"
                            #print str_wrong
                            if str_wrong not in arr_wrong:
                                str_wrong_entries += str_wrong + ', '
                                arr_wrong.append(str_wrong)
                                bol_ok = False
                            
            except:
                bol_ok = False
                str_wrong = "Generell falsche Werte, "
                if str_wrong not in arr_wrong and str_wrong != '':
                    str_wrong_entries += str_wrong
                    arr_wrong.append(str_wrong)
            
        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["ph_ub und untertyp_e: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False


    def only_one_untertyp(self, dictContent, arr_untertypen): 
        """check, only one untertyp in array"""
        bol_ok = True
        str_wrong = ''
        str_wrong_entries = ''
        arr_wrong = []
        
        for line in dictContent:
            for untertyp in arr_untertypen:
                str_untertyp = line[untertyp].replace('{','').replace('}','').replace(' ', '')
                arr_test_untertypen = str_untertyp.split(',')
                if len(arr_test_untertypen) > 1:
                    str_wrong = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'])
                    str_wrong += "(untertyp: " + str_untertyp + ")"
                    #print str_wrong
                    if str_wrong not in arr_wrong:
                        str_wrong_entries += str_wrong + ', '
                        arr_wrong.append(str_wrong)
                        bol_ok = False


        if bol_ok:
            return True

        else:
            self.arrFeedback.append(["untertypen: Folgende Untertypen sind mehrfach vorhanden: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False


    def check_only_4_wald(self, dictContent, field):
        """field if objnr is three letters long. Attribute only for forests"""
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        str_wrong = ''
        for line in dictContent:
            field_value = line[field]
            field_value  = field_value .strip()
            objnr = line['objnr']
            objnr = objnr.strip()
            if field_value != '' and field_value != '-' and field_value != '0':
                if len(objnr) > 3:
                    bol_ok = False                    
                    str_wrong = 'objnr ' + str(line['objnr']) + ' ' + 'gemnr ' + str(line['gemnr'])
                    str_wrong += " (" + field_value + ")"

                if str_wrong not in arr_wrong and str_wrong != '':
                    str_wrong_entries += str_wrong + ', '
                    arr_wrong.append(str_wrong)

        if bol_ok:
            return True
        else:
            self.arrFeedback.append([field + " Fehler (Wert nur bei Wald): " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False


    def is_within_length_text(self, dictContent, field_name, length_txt):
        """checks if the length of bemerkungen is conform with the database"""
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        str_wrong = ''

        for line in dictContent:
            str_bemerkungen = line[field_name]
            if len(str_bemerkungen) > length_txt:
                bol_ok = False
                str_wrong = str_bemerkungen[0:20]
                if len(str_bemerkungen) > 21:
                    str_wrong = str_wrong + '... '
                    if str_wrong not in arr_wrong:
                        str_wrong_entries += str_wrong + ', '
                        arr_wrong.append(str_wrong)

        if bol_ok:
            return True
        else:
            self.arrFeedback.append([field_name + ": Text zu lang: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False
 

    def is_known_kartierer(self, dictContent):
        """if kartierer is not yet in db"""
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        str_wrong = ''

        for line in dictContent:
            str_kartierer = line['kartierer'].strip() 
            if str_kartierer != '':
                str_kartierer = str_kartierer.decode('utf-8', 'replace')
                sql = """
                        SELECT COUNT(*) AS anz_kartierer 
                        FROM kartiererin
                        WHERE name LIKE \'%s%%\'
                      """ % (str_kartierer)
                self.cur.execute(sql)
                rows = self.cur.fetchall()
                if rows[0][0] == 0:
                    bol_ok = False
                    str_wrong = str_kartierer
                    if str_wrong not in arr_wrong:
                        str_wrong_entries += str_kartierer + ', '
                        arr_wrong.append(str_wrong)
                    
        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["kartierer: Folgende KartiererInnen wurden noch nicht in der db erfasst: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False

    def check_gemnr(self, dictContent): 
        """checks if valid gemnr"""
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        str_wrong = ''

        for line in dictContent:
            str_gemnr = line['gemnr'].strip() 
            if str_gemnr != '':
                if self.is_float(str_gemnr):
                    sql = """
                            SELECT COUNT(*) AS anz_gemeinden 
                            FROM gemeindegrenzen_gemeinde
                            WHERE bfsnr = %s
                          """ % str_gemnr
                    self.cur.execute(sql)
                    rows = self.cur.fetchall()
                    if rows[0][0] == 0:
                        str_wrong = line['gemnr']
                        bol_ok = False
                        
                else:
                    bol_ok = False
                    str_wrong = line['gemnr']

                if str_wrong not in arr_wrong:
                    str_wrong_entries += str_wrong + ', '
                    arr_wrong.append(str_wrong)

        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["gemnr: Folgende gemnr gibt es aktuell nicht im Kt. SO: " + str_wrong_entries, 1]) 
            bolNoErrors = False # a test faild
            return False

    def is_utf8(self, data):
        """returns true, if the data is unicode"""
        try:
            data.decode('utf-8')
            return True
        except UnicodeDecodeError:
            self.arrFeedback.append(["Das Dokument ist nicht in UTF-8 gespeichert worden", 0]) # add to array with correction messages
            self.bolNoErrors = False # a test faild
            return False
    

    def check_untertyp(self, dictContent, untertyp, arr_untertyp):
        # TODO HERE
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        str_wrong = ''
        for line in dictContent:
            str_untertyp = line[untertyp].replace('{','').replace('}','').replace(' ', '')
            arr_test_untertypen = str_untertyp.split(',')
            for code in arr_test_untertypen:
                if (code.strip() not in arr_untertyp) and code !='':
                    str_wrong = code.strip()
                    #print str_wrong
                    if str_wrong not in arr_wrong:
                        str_wrong_entries += str_wrong + ', '
                        arr_wrong.append(str_wrong)
                        bol_ok = False
                    
        if bol_ok:
            return True
            
        else:
            self.arrFeedback.append([untertyp + ": Folgende Werte sind falsch: " + str_wrong_entries, 1]) # add to array with correction messages
            self.bolNoErrors = False # an test faild
            return False 
                    
                
    def check_codelist(self, codelist, dictContent, header, req=False):
        """check if all codes are defined in the codelist"""
        bol_ok = True
        str_wrong_entries = ''
        arr_wrong = []
        str_wrong = ''
        for line in dictContent:
            if line[header] not in codelist:
                str_wrong = line[header]
                if req and str_wrong == '':
                    str_wrong = 'EMPTY'
                if ((str_wrong not in arr_wrong) and str_wrong !=''):
                    str_wrong_entries += str_wrong + ', '
                    arr_wrong.append(str_wrong)
                    bol_ok = False
    
        if bol_ok:
            return True
        else:
            self.arrFeedback.append([header + ": Folgende Werte sind falsch: " + str_wrong_entries, 1]) # add to array with correction messages
            self.bolNoErrors = False # an test faild
            return False 
            
            
    def check_codelist_skelett(self, codelist_lw, codelist_wald,  dictContent,  header,  req=False):
        """check if all codes are defined in the codelist for attribute skelett"""
        bol_ok = True
        bol_error = False
        str_wrong_entries = ''
        arr_wrong = []
        str_wrong = ''
        for line in dictContent:
            if line['is_wald'] == 't':
                codelist = codelist_wald
            elif line['is_wald'] == 'f':
                codelist = codelist_lw
            else:
                codelist = codelist_wald + codelist_lw
            if line[header].strip() not in codelist and line[header].strip() != '' and not req:
                str_wrong = line[header] + '(wald:' + line[header] + ')'
                bol_error = True
            if line[header].strip() not in codelist and line[header].strip() == '' and req:
                str_wrong = line[header] + '(wald: leer)'
                bol_error = True
            if ((str_wrong not in arr_wrong) and str_wrong !='' and bol_error):
                str_wrong_entries += str_wrong + ', '
                arr_wrong.append(str_wrong)
                bol_ok = False
                bol_error = False
    
        if bol_ok:
            return True
        else:
            self.arrFeedback.append([header + ": Folgende Skelett-Werte sind falsch: " + str_wrong_entries, 1]) # add to array with correction messages
            self.bolNoErrors = False # an test faild
            return False 


    def has_all_values(self, dictContent):
        """check, if all obligatory fields are filled"""
        bol_ok = True
        str_fields_missing = ''
        arr_fields_missing = []
        str_field = ''

        for line in dictContent:
            for header in self.headers:
                    bol_is_obligatory = header[1] 
                    header = header[0] 
                    if bol_is_obligatory:
                        if str(line[header]).strip() == "":
                            str_field = header
                            if not str_field in arr_fields_missing:
                                str_fields_missing += str_field + ', '
                                arr_fields_missing.append(str_field)
                                bol_ok = False

        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["Bei folgenden Attributen hat es fehlende Einträge: " + str_fields_missing, 1]) # add to array with correction messages
            self.bolNoErrors = False # an test faild
            return False 

    def has_all_headers(self, input_headers):
        """check if all headers are available"""
        bol_ok = True
        str_missing_headers = ''
        
        for header in self.headers:
            header = header[0]
            if not (header in input_headers):
                str_missing_headers += header + ', '
                bol_ok = False
    
        if bol_ok:
            return True
        else:
            self.arrFeedback.append(["Folgende Attribute fehlen: " + str_missing_headers, 1]) # add to array with correction messages
            self.bolNoErrors = False # test faild
            return False
    
    
        """
        try:
            return True
        except UnicodeDecodeError:
            arrFeedback.push("Das Dokument ist nicht in UTF-8 gespeichert worden") # add to array with correction messages
            bolNoErrors = False # an test faild
            return False
        """
    
if __name__ == "__main__":
    main()
