import csv
import os


#variable
csvFile = "DIEM_DANH_AV.csv"
tableName = "OISP_KET_QUA_MON_HOC"
sqlData = []
maxSizeFile = 100 # may yeu chay ko noi danh phai pass nhieu day file vao 1 lan
mydir = 'result'

# for only OISP_KET_QUA_MON_HOC
maAV = '001231'
maKnm = '001232'
maKnxh = '001233'
maGdtc = '001234'
nhHk = '20191'


# delete old file before tranfer
filelist = [ f for f in os.listdir(mydir) ]
for f in filelist:
    os.remove(os.path.join(mydir, f))


# read file csv and save data into var data

with open(csvFile, newline='') as f:
    reader = csv.reader(f)
    for row in reader:
      print(row)
      sqlData.append(row)
    sqlData = sqlData[1:]

# for function tranfer one line of data into sql
def transferQuery(mssv,maMon,nhHk,diem,soBuoiVang):#variable you must change if different data
#you also change the thing in return for different data
  return f"""MERGE INTO {tableName} USING DUAL ON (MSSV='{mssv}' AND MA_MON='{maMon}' AND NH_HK='{nhHk}')
        WHEN MATCHED THEN UPDATE SET DIEM_TONG_KET='{diem}',SO_BUOI_VANG={soBuoiVang}
        WHEN NOT MATCHED THEN INSERT (MSSV,MA_MON,NH_HK,DIEM_TONG_KET,SO_BUOI_VANG) VALUES ('{mssv}','{maMon}', '{nhHk}','{diem}',{soBuoiVang});"""

# condition
def numbericOrNull(numberTxt):
  try:
    val = int(numberTxt.strip())
    return val
  except ValueError:
    try:
        val = float(numberTxt.strip())
        return val
    except ValueError:
        return 'NULL'
  
def stringOrNull(str):
  return 'NULL' if str.strip() == '' else str.strip()

def stringOrDefault(str,default):
  return default if str.strip() == '' else str.strip()


# return arr of sql from var data
def tranferCSVToSQL(csvFile):
  sqlArr = []
  for row in sqlData:
    if len(row) == 0:
       break
    sql = transferQuery(# in here is the data will pass into tranferQuery, and row in one row in csv file
      row[0],#mssv
      maGdtc,#ma mon
      nhHk,# hoc ky
      stringOrNull(row[1]),
      numbericOrNull(row[2]),
    )
    sqlArr.append(sql)
  return sqlArr
     

def writeToTxt(arr,csvFile):
  f = open(csvFile, "w")
  for item in arr:
    f.write(item+'\n')
  f.close()

def writeAllFile(lstQuery):
  count = 1
  subFile = []
  for query in lstQuery:
    subFile.append(query)
    if len(subFile) >= maxSizeFile or query == lstQuery[-1]: 
      writeToTxt(subFile,f'result/sql{count}.txt')
      subFile = []
      count+=1

writeAllFile(tranferCSVToSQL(csvFile))

writeToTxt(tranferCSVToSQL(csvFile),'sql.txt')

      
    


