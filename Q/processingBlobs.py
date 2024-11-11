#archiving of ingested files from last week: if date is a Monday, archive everything from ingested_files to archived_files within date folder. If updated file is available before Monday then manually archive file from ingested_files and pipeline will pick up new file on next run
import os 
from datetime import datetime, date 

todayDate = datetime.now() 
if todayDate.weekday() == 0:
    try: 
        dateFolder = todayDate.strftime('%Y%m%d')
        rawMount = '/mnt/raw'
        env = os.getenv('env')
        sourcePath =  '{}/quantium/download/{}/inbound/qpromo/ingested_files/'.format(rawMount,env)
        archivePath = '{}/quantium/download/{}/inbound/qpromo/archived_files/{}'.format(rawMount,env, dateFolder)
        files = dbutils.fs.ls(sourcePath)
        print(files)
        try:
            dbutils.fs.ls(archivePath)  # Create the archive directory if it doesn't exist
        except:
            try:
                dbutils.fs.mkdirs(archivePath)
            except Exception as e:
                print(str(e))
            try:
                for f in files:
                    print('\nmoving {}'.format(f.path))
                    archiveFilePath = '{}/{}'.format(archivePath, f.name)
                    dbutils.fs.cp(f.path, archiveFilePath, recurse=True)
                    dbutils.fs.rm(f.path, recurse=True)
            except Exception as e:
                print(str(e))
    except Exception as e:
        print(str(e))
else:
    pass

# COMMAND ----------

%run /3rdparty/quantium/downloadBlobs

# COMMAND ----------

%run /3rdparty/quantium/preProcessingFiles

# COMMAND ----------

##Remove two files not required for ingestion
import os 
env = os.getenv('env') 
fileList = dbutils.fs.ls('/mnt/raw/quantium/download/{}/inbound/qpromo/processed_files'.format(env))
listFilesToRemove = ["PriceSensitivitySegmentation.parquet/", "ValueSegmentation.parquet/"]
for f in fileList:
    if f.name in listFilesToRemove:
        try:
            dbutils.fs.rm(f.path, recurse=True)
        except Exception as e:
            print (str(e))
            pass
