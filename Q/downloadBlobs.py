%pip install requests
%pip install azure-storage-blob

# COMMAND ----------

import requests
import json
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from datetime import datetime, timedelta, date, timezone
import pyspark.sql.functions as F
import os

class DownloadBlobs:
    def __init__(self):
        # set env variables
        self.env = os.getenv('env')
        self.loc = 'uks'
        self.instance = '01'

        # Landing Storage Account
        self.landingStorageAccount = 'saasllnddtadm{}{}{}'.format(self.env, self.loc, self.instance)
        self.rawmount = '/mnt/raw/'
        self.rawContainer = 'sensitive'

        # Files to extract
        self.SunFiles = [
            'CustomerActivity.parquet', 
            'PriceSensitivitySegmentation.parquet',  
            'ValueSegmentation.parquet',
            'qpromo_item_brandoverride.parquet'
        ]
        self.WedsFiles = ['qpromotions.parquet']
        self.AdhocFiles = ['events.parquet']

        ## Key Vault variables
        self.dbScope = 'kv-sa-lnd-{}-{}-{}'.format(self.env, self.loc, self.instance)
        self.secretKey = 'dbr-spn'
        self.clientId = 'spn-AppId'

    def mount_quantium_blob(self):
        # GET SAS TOKEN
        api_url = "https://api.quantium.com/storage/uk/api/v1/token"
        api_key = dbutils.secrets.get(scope='kv-sa-lnd-{}-{}-{}'.format(self.env, self.loc, self.instance), key='quantium-segments-api-prod')
        headers = {"Content-Type": "application/json", "Authorization": "ApiKey {}".format(api_key)}
        response = requests.post(api_url, headers=headers)
        results = response.json()
        self.sasToken = results["sasToken"]
        self.accountUrl = '{}'.format(results["listContentsUrl"].replace(results["listContentsUrl"][51:], ''))
        self.storage_name = self.accountUrl.replace("https://", '')

        # MOUNT QUANTIUM BLOB
        service = BlobServiceClient(account_url=self.accountUrl, credential=self.sasToken)
        container_client = service.get_container_client("segments")
        blobContainerName = "segments"
        blobMount = "/mnt/quantium-segments/"
        
        if not any(mount.mountPoint == blobMount for mount in dbutils.fs.mounts()):
            try:
                dbutils.fs.mount(
                    source="wasbs://{}@{}".format(blobContainerName, self.storage_name),
                    mount_point=blobMount,
                    extra_configs={'fs.azure.sas.' + blobContainerName + '.' + self.storage_name: self.sasToken}
                )
                print("quantium mount succeeded!")
            except Exception as e:
                print("mount exception", e)
        else:
            print("quantium already mounted")

    def mount_landing_zone(self):
        landingZoneMount = "/mnt/lnd/sensitive/quantium"
        applicationId = dbutils.secrets.get(scope=self.dbScope, key=self.clientId)

        if not any(mount.mountPoint == landingZoneMount for mount in dbutils.fs.mounts()):
            try:
                dbutils.fs.mount(
                    source="abfss://{}@{}.dfs.core.windows.net".format(self.rawContainer, self.landingStorageAccount),
                    mount_point=landingZoneMount,
                    extra_configs={
                        "fs.azure.account.auth.type": "OAuth",
                        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                        "fs.azure.account.oauth2.client.id": applicationId,
                        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=self.dbScope,
                                                                                   key=self.secretKey),
                        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b63ee29f-aaa6-4d4e-8267-1d6dca9c0e43/oauth2/token"
                    }
                )
                print("raw mount succeeded!")
            except Exception as e:
                print("mount exception", e)
        else:
            print("raw already mounted")

    def run(self):
        # Mount Quantium Blob
        self.mount_quantium_blob()
        
        # Mount Landing Zone
        self.mount_landing_zone()
        
        # Get latest folders from quantium blob 
        todayDate = date.today()
        weekday = todayDate.weekday()
        diff = 0

        if todayDate.weekday() == 5:  # Sat
            diff = 6
        elif todayDate.weekday() == 6:  # Sunday
            diff = 0
        elif todayDate.weekday() == 0:  # Mon
            diff = 1
        elif todayDate.weekday() == 1:  # Tue
            diff = 2
        elif todayDate.weekday() == 2:  # Weds
            diff = 3
        elif todayDate.weekday() == 3:  # Thurs
            diff = 4
        else:
            diff = 5  # Fri

        # subtracting diff
        maxDate = todayDate 
        minDate = maxDate - timedelta(days=diff)

        minFolder = minDate.strftime('%Y%m%d')
        maxFolder = maxDate.strftime('%Y%m%d')
        print('checking folders:{} {}'.format(minFolder, maxFolder))

        folderExtractionList = []
        for f in dbutils.fs.ls('/mnt/quantium-segments/'):
            folderName = f.name.replace('/', '')
            if minFolder <= folderName <= maxFolder:
                folderExtractionList.append(folderName)

        print('************* START TRANSFER***********************')
        # TRANSFER FILES
        for folderName in folderExtractionList:
            if weekday == 0:
                for f in self.SunFiles:
                    print("checking folder {} for {}".format(folderName, f))
                    try:
                        dbutils.fs.ls('/mnt/raw/quantium/download/{}/inbound/qpromo/ingested_files/{}/'.format(
                                      self.env, f))
                        (print('file already in folder\n'))
                    except:
                        print(('downloading to landing zone...\n'))
                        try:
                            dbutils.fs.cp('/mnt/quantium-segments/{}/{}'.format(folderName, f),
                                  '/mnt/raw/quantium/download/{}/inbound/qpromo/ingested_files/{}'.format(
                                      self.env, f), recurse=True)
                        except Exception as e:
                            print(e)
                            pass
            elif weekday == 2:
                 for f in self.WedsFiles:
                    print("checking folder {} for {}".format(folderName, f))
                    try:
                        dbutils.fs.ls('/mnt/raw/quantium/download/{}/inbound/qpromo/ingested_files/{}/'.format(
                                      self.env, f))
                        (print('file already in folder\n'))
                    except:
                        print(('downloading to landing zone...\n'))
                        try:
                            dbutils.fs.cp('/mnt/quantium-segments/{}/{}'.format(folderName, f),
                                  '/mnt/raw/quantium/download/{}/inbound/qpromo/ingested_files/{}'.format(
                                      self.env, f), recurse=True)
                        except Exception as e:
                            print(e)
                            pass
        #check adhoc 
            for f in self.AdhocFiles:
                print("checking folder {} for {}".format(folderName, f))
                try:
                    dbutils.fs.ls('/mnt/quantium-segments/{}/{}'.format(folderName, f))
                    print(('downloading to landing zone...\n'))
                    try:
                        dbutils.fs.cp('/mnt/quantium-segments/{}/{}'.format(folderName, f),
                                '/mnt/raw/quantium/download/{}/inbound/qpromo/ingested_files/{}'.format(
                                    self.env, f), recurse=True)
                    except:
                        (print('file not available at source\n'))
                except:
                    (print('file not available at source\n'))
                    pass

        print('************* END TRANSFER***********************')

# COMMAND ----------

aa  = DownloadBlobs()
aa.run()

# COMMAND ----------

#dbutils.fs.ls('/mnt/quantium-segments') # for manual checking 

# COMMAND ----------

dbutils.fs.unmount('/mnt/quantium-segments')
