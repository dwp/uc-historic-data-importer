#!/usr/bin/env python3

"""Generate sample data for the historic data importer.

Generates encrypted and optionally compressed files which are then
placed in the s3 bucket in localstack to enable integration testing.

Usage: sample_data.py [-h] [-k DATA_KEY_SERVICE] [-c]

optional arguments:
  -h, --help            show this help message and exit
  -k DATA_KEY_SERVICE, --data-key-service DATA_KEY_SERVICE
                        Use the specified data key service.
  -c, --compress        Compress before encryption.
  -e, --encrypt         Encrypt the data.
"""

import argparse
import base64
import binascii
import gzip
import json
import uuid

import requests

from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter

def main():
    """Main entry point."""
    args = command_line_args()
    for i in range(10):
        dks_response = requests.get(args.data_key_service).json()
        encryption_metadata = {
            'encryptionKeyId': dks_response['dataKeyEncryptionKeyId'],
            'encryptedEncryptionKey': dks_response['ciphertextDataKey'],
            'plaintextDatakey': dks_response['plaintextDataKey']
        }
        contents = ""
        for _ in range(100):
            contents = contents + db_object_json() + "\n"

        if args.compress:
            print("Compressing.")
            compressed = gzip.compress(contents.encode())
            [encryption_metadata['iv'], encrypted_contents] = \
                encrypt(encryption_metadata['plaintextDatakey'], compressed,
                        args.encrypt)
        else:
            print("Not compressing.")
            [encryption_metadata['iv'], encrypted_contents] = \
                encrypt(encryption_metadata['plaintextDatakey'],
                        contents.encode("utf8"), args.encrypt)


        metadata_file = f'adb.collection.{i:04d}.json.gz.encryption.json'
        with open(metadata_file, 'w') as metadata:
            json.dump(encryption_metadata, metadata, indent=4)

        data_file = f'adb.collection.{i:04d}.json.gz.enc'
        with open(data_file, 'wb') as data:
            data.write(encrypted_contents)


def encrypt(datakey, unencrypted_bytes, do_encryption):
    """Encrypts the supplied bytes with the supplied key.
    Returns the initialisation vector and the encrypted data as a tuple.
    """
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(datakey.encode("ascii"), AES.MODE_CTR, counter=counter)


    if do_encryption:
        print("Encrypting.")
        ciphertext = aes.encrypt(unencrypted_bytes)
        return (base64.b64encode(initialisation_vector).decode('ascii'),
                base64.b64encode(ciphertext))
    else:
        print("Not encrypting.")
        return (base64.b64encode(initialisation_vector).decode('ascii'),
                unencrypted_bytes)

def db_object_json():
    """Returns a sample dbRecord object with unique ids."""
    record = db_object()
    record['_id']['declarationId'] = guid()
    record['contractId'] = guid()
    record['addressNumber']['cryptoId'] = guid()
    record['townCity']['cryptoId'] = guid()
    record['processId'] = guid()
    return json.dumps(record)

def guid():
    """Generates, returns a guid."""
    return str(uuid.uuid4())

def db_object():
    """Returns a dbObject template to which unique ids can be applied."""
    return {
        "_id": {
            "someId": "RANDOM_GUID"
        },
        "type": "addressDeclaration",
        "contractId": "RANDOM_GUID",
        "addressNumber": {
            "type": "AddressLine",
            "cryptoId": "RANDOM_GUID"
        },
        "addressLine2": None,
        "townCity": {
            "type": "AddressLine",
            "cryptoId": "RANDOM_GUID"
        },
        "postcode": "SM5 2LE",
        "processId": "RANDOM_GUID",
        "effectiveDate": {
            "type": "SPECIFIC_EFFECTIVE_DATE",
            "date": 20150320,
            "knownDate": 20150320
        },
        "paymentEffectiveDate": {
            "type": "SPECIFIC_EFFECTIVE_DATE",
            "date": 20150320,
            "knownDate": 20150320
        },
        "createdDateTime": {
            "$date":"2015-03-20T12:23:25.183Z"
        },
        "_version": 2,
        "_lastModifiedDateTime": {
            "$date": "2018-12-14T15:01:02.000+0000"
        }
    }



def command_line_args():
    """Parses, returns the supplied command line args."""
    parser = argparse.ArgumentParser(description='Generate sample encrypted data.')
    parser.add_argument('-k', '--data-key-service',
                        help='Use the specified data key service.')
    parser.add_argument('-c', '--compress', action='store_true',
                        help='Compress before encryption.')
    parser.add_argument('-e', '--encrypt', action='store_true',
                        help='Encrypt the data.')
    return parser.parse_args()

if __name__ == "__main__":
    main()
