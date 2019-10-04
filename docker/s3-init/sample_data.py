#!/usr/bin/env python3

import argparse
import base64
import binascii
import bz2
import json
import uuid

import requests

from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter

def main():
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
        compressed = bz2.compress(contents.encode())
        [encryption_metadata['iv'], encrypted_contents] = \
            encrypt(encryption_metadata['plaintextDatakey'], compressed)

        metadata_file = f'adb.collection.{i:04d}.json.gz.encryption.json'
        with open(metadata_file, 'w') as metadata:
            json.dump(encryption_metadata, metadata, indent=4)

        data_file = f'adb.collection.{i:04d}.json.gz.enc'
        with open(data_file, 'w') as data:
            data.write(encrypted_contents)


def encrypt(datakey, unencrypted_bytes):
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(datakey.encode("ascii"), AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(unencrypted_bytes)
    return (base64.b64encode(initialisation_vector).decode('ascii'),
            base64.b64encode(ciphertext).decode('ascii'))

def db_object_json():
    record = db_object()
    record['_id']['declarationId'] = guid()
    record['contractId'] = guid()
    record['addressNumber']['cryptoId'] = guid()
    record['townCity']['cryptoId'] = guid()
    record['processId'] = guid()
    return json.dumps(record)

def guid():
    return str(uuid.uuid4())

def db_object():
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
    parser = argparse.ArgumentParser(description='Generate sample encrypted data.')
    parser.add_argument('-k', '--data-key-service',
                        help='Use the specified data key service.')
    return parser.parse_args()

if __name__ == "__main__":
    main()
