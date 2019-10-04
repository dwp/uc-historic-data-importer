#!/usr/bin/env python3

import argparse
import base64
import binascii
import json
import os
import time
import uuid
from pprint import pprint
import requests

from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter

def main():
    args = command_line_args()
    content = requests.get(args.data_key_service).json()
    encryption_key = content['plaintextDataKey']
    encrypted_key = content['ciphertextDataKey']
    master_key_id = content['dataKeyEncryptionKeyId']
    for x in range(100):
        db_object = unique_decrypted_db_object()
        encrypted = encrypt(encryption_key, json.dumps(db_object))
        iv = encrypted[0].decode('ascii')
        encrypted_record = encrypted[1].decode('ascii')
        print(iv)
        print(encrypted_record)


def command_line_args():
    parser = argparse.ArgumentParser(description='Generate sample encrypted data.')
    parser.add_argument('-k', '--data-key-service',
                        help='Use the specified data key service.')
    return parser.parse_args()

def encrypt(key, plaintext):
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(key.encode("utf8"), AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(plaintext.encode("utf8"))
    return (base64.b64encode(initialisation_vector),
            base64.b64encode(ciphertext))

def unique_decrypted_db_object():
    record = decrypted_db_object()
    record['_id']['declarationId'] = guid()
    record['contractId'] = guid()
    record['addressNumber']['cryptoId'] = guid()
    record['townCity']['cryptoId'] = guid()
    record['processId'] = guid()
    return record

def guid():
    return str(uuid.uuid4())

def decrypted_db_object():
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

if __name__ == "__main__":
    main()
