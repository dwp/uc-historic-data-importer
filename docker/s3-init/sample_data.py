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
    data_key_service = args.data_key_service \
        if args.data_key_service \
        else 'http://localhost:8080/datakey'

    print(f'data_key_service: {data_key_service}.')
    batch_nos = {}
    file_count = int(args.file_count if args.file_count else 10)

    for i in range(file_count):
        generate_dump_file(args, batch_nos, data_key_service, i)

    if args.coalesced:
        generate_dump_file(args, batch_nos, data_key_service, file_count + 1, "collection-thirtyone")


def generate_dump_file(args, batch_nos, data_key_service, i, collectionOverride=""):
    dks_response = requests.get(data_key_service).json()
    encryption_metadata = {
        'keyEncryptionKeyId': dks_response['dataKeyEncryptionKeyId'],
        'encryptedEncryptionKey': dks_response['ciphertextDataKey'],
        'plaintextDatakey': dks_response['plaintextDataKey']
    }
    contents = ""
    database = f'database-{(i // 4) + 1}'
    collection = collectionOverride if collectionOverride != "" else f'collection-{(i // 2) + 1}'
    batch = f'{database}.{collection}'
    batch_nos[batch] = batch_nos.get(batch, 0) + 1
    record_count = int(args.batch_size if args.batch_size else 10)
    for j in range(record_count):
        print(f"Making record {j}/{record_count}.")
        contents = contents + \
                   db_object_json(f'{batch}.{batch_nos[batch]:04d}', j) + "\n"
    if args.malformed_input:
        print("Adding corrupted line.")
        record = db_object_json(f'{batch}.{batch_nos[batch]:04d}', j)
        corrupted = record[0:int(len(record) / 2)]
        contents = contents + corrupted + "\n"
    if args.mongo_id:
        print("Adding record with mongo id.")
        record = db_object_json(f'{batch}.{batch_nos[batch]:04d}', j, True)
        contents = contents + record + "\n"
    if args.record_with_no_id:
        print("Adding record with no id.")
        record = db_object_json(f'{batch}.{batch_nos[batch]:04d}', j)
        jso = json.loads(record)
        del jso['_id']
        contents = contents + json.dumps(jso) + "\n"
    if args.id_with_mongo_date_in:
        print("Adding record with id with embedded mongo date.")
        record = db_object_json(f'{batch}.{batch_nos[batch]:04d}', j)
        jso = json.loads(record)
        id = jso['_id']
        id['createdDateTime'] = {"$date": "2010-01-01T00:00:00.000Z"}
        print(jso)
        contents = contents + json.dumps(jso) + "\n"
    if args.removed_record:
        print("Adding a removed record.")
        record = db_object_json(f'{batch}.{batch_nos[batch]:04d}', j)
        jso = json.loads(record)
        jso['_removedDateTime'] = {"$date": "2012-03-04T21:43:56.000Z"}
        print(jso)
        removed_record = {"_removed": jso}
        contents = contents + json.dumps(removed_record) + "\n"
    if args.archived_record:
        print("Adding an archived record.")
        record = db_object_json(f'{batch}.{batch_nos[batch]:04d}', j)
        jso = json.loads(record)
        jso['_archivedDateTime'] = {"$date": "2014-03-02T12:34:56.000Z"}
        archived_record = {"_archived": jso}
        contents = contents + json.dumps(archived_record) + "\n"
    if args.record_with_no_timestamp:
        print("Adding record with no timestamp.")
        record = db_object_json(f'{batch}.{batch_nos[batch]:04d}', j)
        jso = json.loads(record)
        del jso['_lastModifiedDateTime']
        contents = contents + json.dumps(jso) + "\n"
    if args.record_with_no_timestamps:
        print("Adding record with no timestamps.")
        record = db_object_json(f'{batch}.{batch_nos[batch]:04d}', j)
        jso = json.loads(record)
        del jso['_lastModifiedDateTime']
        del jso['createdDateTime']
        contents = contents + json.dumps(jso) + "\n"
    if args.compress:
        print("Compressing.")
        compressed = gzip.compress(contents.encode("ascii"))
        [encryption_metadata['initialisationVector'], encrypted_contents] = \
            encrypt(encryption_metadata['plaintextDatakey'], compressed,
                    args.encrypt)
    else:
        print("Not compressing.")
        [encryption_metadata['initialisationVector'], encrypted_contents] = \
            encrypt(encryption_metadata['plaintextDatakey'],
                    contents.encode("utf8"), args.encrypt)
    metadata_file = f'{batch}.{batch_nos[batch]:04d}.json.encryption.json'
    with open(metadata_file, 'w') as metadata:
        print(f'Writing metadata file {metadata_file}')
        json.dump(encryption_metadata, metadata, indent=4)
    data_file = f'{batch}.{batch_nos[batch]:04d}.json.gz.enc'
    with open(data_file, 'wb') as data:
        print(f'Writing data file {data_file}')
        data.write(encrypted_contents)


def encrypt(datakey, unencrypted_bytes, do_encryption):
    """Encrypts the supplied bytes with the supplied key.
    Returns the initialisation vector and the encrypted data as a tuple.
    """
    initialisation_vector = Random.new().read(AES.block_size)
    iv_int = int(binascii.hexlify(initialisation_vector), 16)
    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(base64.b64decode(datakey), AES.MODE_CTR, counter=counter)

    if do_encryption:
        print("Encrypting.")
        ciphertext = aes.encrypt(unencrypted_bytes)
        return (base64.b64encode(initialisation_vector).decode('ascii'),
                ciphertext)
    else:
        print("Not encrypting.")
        return (base64.b64encode(initialisation_vector).decode('ascii'),
                unencrypted_bytes)


def db_object_json(batch, i, native_id=False):
    """Returns a sample dbRecord object with unique ids."""
    record = db_object(i)

    if native_id:
        record['_id'] = {'$oid': f'{batch}-{(i // 20) + 1}'}
    else:
        record['_id']['declarationId'] = f'{batch}-{(i // 20) + 1}'

    record['contractId'] = guid()
    record['addressNumber']['cryptoId'] = guid()
    record['townCity']['cryptoId'] = guid()
    record['processId'] = guid()
    return json.dumps(record)


def guid():
    """Generates, returns a guid."""
    return str(uuid.uuid4())


def db_object(i):
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
            "$date": "2015-03-20T12:23:25.183Z"
        },
        "_version": 2,
        "_lastModifiedDateTime": {
            "$date": f'2018-12-01T15:01:02.{i:03d}Z'
        }
    }


def command_line_args():
    """Parses, returns the supplied command line args."""
    parser = argparse.ArgumentParser(description='Generate sample encrypted data.')
    parser.add_argument('-a', '--id-with-mongo-date-in', action='store_true',
                        help='Add a record with an id with a mongo created date time.')
    parser.add_argument('-b', '--coalesced', action='store_true',
                        help='Add a file from a coalesced collection.')
    parser.add_argument('-c', '--compress', action='store_true',
                        help='Compress before encryption.')
    parser.add_argument('-d', '--record-with-no-timestamp', action='store_true',
                        help='Add a record with no timestamp to each file.')
    parser.add_argument('-e', '--encrypt', action='store_true',
                        help='Encrypt the data.')
    parser.add_argument('-i', '--record-with-no-id', action='store_true',
                        help='Add a record with no id to each file.')
    parser.add_argument('-k', '--data-key-service',
                        help='Use the specified data key service.')
    parser.add_argument('-m', '--malformed-input', action='store_true',
                        help='Add malformed inputs to each file.')
    parser.add_argument('-n', '--file-count',
                        help='The number of files to create.')
    parser.add_argument('-o', '--mongo-id', action='store_true',
                        help='Add a record with a mongo native id.')
    parser.add_argument('-r', '--removed-record', action='store_true',
                        help='Add a removed record.')
    parser.add_argument('-v', '--archived-record', action='store_true',
                        help='Add an archived record.')
    parser.add_argument('-s', '--batch-size',
                        help='The number of records in each file.')
    parser.add_argument('-t', '--record-with-no-timestamps', action='store_true',
                        help='Add a record with no modified or created timestamp to each file.')
    return parser.parse_args()


if __name__ == "__main__":
    main()
