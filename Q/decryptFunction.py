#Install additional libraries
%pip install pycryptodome

# COMMAND ----------

# Importing necessary libraries
import base64
from Crypto.Cipher import AES
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import re
import os

def decrypt_val(text, key):

    key = bytes(key, 'utf-8', "ignore")

    # Initializing the cipher object with AES CBC mode and PKCS7 padding
    cipher = AES.new(key, AES.MODE_ECB)

    # Encrypted message to be decrypted
    encoded = text

    # Decoding the encrypted message
    decoded = base64.b64decode(encoded)

    # Decrypting the message using the cipher object
    decrypted = cipher.decrypt(decoded)

    # Removing the padding from the decrypted message
    decrypted = decrypted.rstrip(b'\0')

    # Converting the decrypted message to string
    decrypt_val = decrypted.decode('ascii')

    decrypt_val = re.sub('[$\x00-\x09\x0b-\x0c\x0e-\x1f]', r'', decrypt_val)
    
    # Printing the decrypted message
    return(str(int(decrypt_val)))

decrypt = F.udf(decrypt_val, StringType())
