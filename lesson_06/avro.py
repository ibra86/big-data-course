import json
import os

from fastavro import writer, parse_schema, reader


def write_avro():
    avro_schema = {
        "namespace": "sample.avro",
        "type": "record",
        "name": "Cars",
        "fields": [
            {"name": "model", "type": "string"},
            {"name": "make", "type": ["string", "null"]},
            {"name": "year", "type": ["int", "null"]},
        ]
    }

    records = [
        {"model": "ABC", "make": "ZXC", "year": 2020},
        {"model": "ABC", "year": 2020},
        {"model": "ABC", "make": "ZXC"},
        {"model": "ABasfC", "make": "ZXC", "year": 2020},
        {"model": "ABC", "make": "ZXCf", "year": 2020},
        {"model": "ABC", "make": "ZXfdC", "year": 20220},
    ]

    with open("sample.avro", 'wb') as avro_file:
        writer(avro_file, parse_schema(avro_schema), records)

    with open("sample.json", 'w') as json_file:
        json.dump(records, json_file)


def read_avro():
    with open("sample.avro", 'rb') as avro_file:
        data = reader(avro_file)

        print(type(data))
        print(list(data))


if __name__ == '__main__':
    read_avro()
