import easygui as eg
import os
import time
import sys
import phoenixdb
import re
import threading
import itertools


def check_none(var):
    if var is None:
        sys.exit(0)
    else:
        pass

ret_val = eg.msgbox("Welcome to Phoenix Database Comparator","【D】【B】  Comparator ☺",ok_button="START")
req = eg.msgbox("Make sure queryserver is up and running in both environment","Prerequisite",ok_button="PROCEED")
msg = "Enter Server information"
title = "Connection Details"
fieldNames = ["Hostname of server-1","Hostname of server-2"]
fieldValues = []  # we start with blanks for the values
fieldValues = eg.multenterbox(msg,title, fieldNames)

# make sure that none of the fields was left blank
while 1:
  if fieldValues == None: break
  errmsg = ""
  for i in range(len(fieldNames)):
    if fieldValues[i].strip() == "":
      errmsg = errmsg + ('"%s" is a required field.\n\n' % fieldNames[i])
  if errmsg == "": break # no problems found
  fieldValues = eg.multenterbox(errmsg, title, fieldNames, fieldValues)
#print ("Reply was:", fieldValues)


op_file_path=eg.fileopenbox(msg="Select the Output file for generating results",title="Output file",filetypes= '*.txt')
check_none(op_file_path)

start_time = time.asctime( time.localtime(time.time()))
url_1 = 'http://'+str(fieldValues[0]+':'+'8765/hbase')
url_2 = 'http://'+str(fieldValues[1]+':'+'8765/hbase')
#print(url_1)
#print(url_2)

def dictn(sch,response,m,n):
    d ={}
    for i in sch:
        l = []
        for j in response:
            if j[m] == i:
                l.append(j[n])  
        d[i] = l
    return d


def jdbc_cal(cursor):
        tab_sch = {}
        sql = """select distinct table_name,table_schem from system.catalog where data_table_name is null and table_schem <> 'SYSTEM' and TABLE_TYPE  <> 'i' order by table_schem,TABLE_NAME"""
        cursor.execute(sql)
        resp = cursor.fetchall() # could use .fetchall() or .fetchmany() if needed
        sch_name = [row[1] for row in resp]
        uniq_sch_name = set(sch_name)
        #print (uniq_sch_name)
        for i in uniq_sch_name:
            l = []
            for j in resp:
                if j[1] == i:
                    l.append(j[0])  
            tab_sch[i] = l
        #print(tab_sch)
        return uniq_sch_name,tab_sch


#seeing results from the schema
#tables found in this schema command is below


class base_db_cal:
    schema_1 = []
    schema_2 = []
    table_schema_dic1 = {}
    table_schema_dic2 = {}
    url1 = ''
    url2 = ''
    idx_num_m = []
    idx_m = []
    def __init__(self,url1,url2):

        base_db_cal.url1 = re.findall("[//](\d+.\d+.\d+.\d+)[:]",url1)[0]
        base_db_cal.url2 = re.findall("[//](\d+.\d+.\d+.\d+)[:]",url2)[0]
        

        conn1 = phoenixdb.connect(url1, autocommit=True)
        self.cursor1 = conn1.cursor()
        print('\n-----------------------------------------------------------------------------\n')
        print('\n\n Establishing connection for server 1 ' +  ' @ ' + time.asctime( time.localtime(time.time()) ))
        conn2 = phoenixdb.connect(url2, autocommit=True)
        self.cursor2 = conn2.cursor()

        print('\n\n Establishing connection for server 2' +  ' @ ' + time.asctime( time.localtime(time.time()) )+'\n')
        print('\n------------------------------------------------------------------------------\n')
        base_db_cal.schema_1,base_db_cal.table_schema_dic1 = jdbc_cal(self.cursor1)
        base_db_cal.schema_2,base_db_cal.table_schema_dic2 = jdbc_cal(self.cursor2)


    def comparison(self,op_path):
        sch_m = []
        tabl_m = []
        colm_m =[]
        t_count = 0
        fo = open(op_path,'w')
        fo.write('\n---------------------COMPARISON IS MADE CONSIDERING SERVER -1 AS MASTER -------------------------------\n')
        fo.write('\n NOTE:-In order to find discrepancies in server1 corresponding to server 2,swap hostname entries in UI \n')
        fo.write('\n--------------------------------------SERVER DETAILS---------------------------------------------------\n')
        fo.write('\n Server1 : ' +base_db_cal.url1 +'\n')
        fo.write('\n Server2 : ' +base_db_cal.url2 +'\n')
        fo.write('\n-------------------------------------------------------------------------------------------------------\n')
        for sch_nam in base_db_cal.schema_1:
            if sch_nam not in base_db_cal.schema_2:
                sch_m.append(sch_nam + '\n')
                #fo.write('Missing Schemas in Server-2'+'\n\n'+sch_nam+'\n')
            else:
                print('\n Now Working on Namespace :- '+sch_nam)
                tabl_list_1 = base_db_cal.table_schema_dic1[sch_nam]
                tabl_list_2 = base_db_cal.table_schema_dic2[sch_nam]
                #print(tabl_list_1)
                #print(tabl_list_2)
                for tab_nam in tabl_list_1:
                    if tab_nam not in tabl_list_2:
                        tabl_m.append(sch_nam+'.'+tab_nam+'\n')
                        #fo.write('\n Missing Tables in Server-2'+'\n'+sch_nam+'.'+tab_nam+'\n')
                    else:
                        sql_c = """select COLUMN_NAME from SYSTEM.CATALOG where table_name = '"""+tab_nam+"""' and table_schem ='"""+sch_nam+"""' and COLUMN_NAME is not null order by ordinal_position"""
                        self.cursor1.execute(sql_c)
                        self.cursor2.execute(sql_c)
                        col_list1 = self.cursor1.fetchall()
                        col_list2 = self.cursor2.fetchall()
                        column1 = [row[0] for row in col_list1]
                        column2 = [row[0] for row in col_list2]
                        for col in column1:
                            if col not in column2:
                                #fo.write('\n\n------------------------Comparison of Table :- '+sch_nam+'.'+tab_nam+'--------------------------\n\n')
                                colm_m.append('Column '+col+' is missing in ' + sch_nam+'.'+tab_nam+'\n')
                                #fo.write('Column '+col+' is missing in server-2\n')

        fo.write('\n\n-------------------SCHEMAS MISSING IN SERVER 2------------------------------\n\n')
        for i in sch_m:
            fo.write(i+'\n')
        fo.write('\n\n-------------------TABLES MISSING IN SERVER 2------------------------------\n\n')
        for j in tabl_m:
            fo.write(j+'\n')
        fo.write('\n\n-------------------COLUMNS MISSING IN SERVER 2------------------------------\n\n')
        for z in colm_m:
            fo.write(z+'\n')
                                        
        sql_1="""select b.column_family,a.TABLE_NAME,regexp_replace(array_to_string(first_values(a.COLUMN_NAME,15) within group (order by a.ordinal_position asc), ',') , 'cf:','')
	from system.catalog  a
	join (select table_name,column_family from system.catalog where TABLE_TYPE = 'i'  and column_family like '%:%') b
	on a.table_name = b.table_name
	where
	a.ORDINAL_POSITION IS NOT NULL 
	AND a.COLUMN_NAME NOT IN (':PK','_INDEX_ID') group by b.column_family,a.table_name"""

	
        self.cursor1.execute(sql_1)
        self.cursor2.execute(sql_1)
        
        cf_idx_col1 = self.cursor1.fetchall()
        cf_idx_col2 = self.cursor2.fetchall()

        
        cf_list1 = [row[0] for row in cf_idx_col1]
        cf_list2 = [row[0] for row in cf_idx_col2]

        idx_list1 = [row[1] for row in cf_idx_col1]
        idx_list2 = [row[1] for row in cf_idx_col2]

        idx_cl_dict1 = {str(row[0]+':'+row[1]):row[2] for row in cf_idx_col1}
        idx_cl_dict2 = {str(row[0]+':'+row[2]):row[1] for row in cf_idx_col2}


        for i in set(cf_list1):
            if i not in set(cf_list2):
                base_db_cal.idx_m.append('No Index created for table :-' + i +'\n')


        cf_idx_dic1 = dictn(cf_list1,cf_idx_col1,0,1)
        cf_idx_dic2 = dictn(cf_list2,cf_idx_col2,0,1)
        cf_col_dic1 = dictn(cf_list1,cf_idx_col1,0,2)
        cf_col_dic2 = dictn(cf_list2,cf_idx_col2,0,2)


          
            

        for i,j in cf_idx_dic1.items():
            if i in cf_idx_dic2.keys():
                if len(cf_idx_dic1[i]) != len(cf_idx_dic2[i]):
                    base_db_cal.idx_num_m.append('Total Indexes  for table '+i+ ' is '+str(len(cf_idx_dic1[i]))+' in server 1 and '+str(len(cf_idx_dic2[i]))+' in server 2'+'\n')
                for idx in j:
                    if idx not in cf_idx_dic2[i] and idx_cl_dict1[str(i+':'+idx)] in cf_col_dic2[i]:
                        base_db_cal.idx_num_m.append('Index named '+idx+'('+idx_cl_dict1[str(i+':'+idx)]+') on table '+i+ ' is missing '+'in server 2(but index '+idx_cl_dict2[str(i+':'+str(idx_cl_dict1[str(i+':'+idx)]))]+' has same order of columns)'+'\n')                                        
                    elif idx not in cf_idx_dic2[i] and idx_cl_dict1[str(i+':'+idx)] not in cf_col_dic2[i]:
                        base_db_cal.idx_num_m.append('Index named '+idx+'('+idx_cl_dict1[str(i+':'+idx)]+') on table '+ i + ' is missing '+'in server 2(no other index has same order of columns)'+'\n')
                                        
        fo.write('\n\n-------------------INDEXES MISSING IN SERVER 2------------------------------\n\n')                
        for i in base_db_cal.idx_m:
            fo.write(i + '\n')                                


        for i in base_db_cal.idx_num_m:
            fo.write(i + '\n')
        
        fo.write('\n\n-----------------------------------------------------------------------------\n\n') 
        fo.close()
        print('\n!!!!!!!!!!!!!!!!!!!!!!COMPLETED SUCESSFULLY!!!!!!!!!!!!!!!!!!!\n')

                        
                        


        

c = base_db_cal(url_1,url_2)
c.comparison(op_file_path)

end_time = time.asctime( time.localtime(time.time()))
total_time_msg = "Completed in "+str(int(re.findall("[\s](\d+:\d+)[:]",end_time)[0].replace(":","")) - int(re.findall("[\s](\d+:\d+)[:]",start_time)[0].replace(":","")))+" minutes  :) "

eg.msgbox(total_time_msg,"Done!!!!")
eg.msgbox("Check the output file for the Results of Comparison","Thank You ☺")
eg.msgbox("Application Developed By Seshadri of TCS Optumera Base Product DB Team",title="About",ok_button='Close')



