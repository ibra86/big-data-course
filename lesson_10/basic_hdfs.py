import json
import os

from hdfs import InsecureClient

def main():
    print('hello')

    client = InsecureClient('http://127.0.0.1:50070/', user='user')
    client.makedirs('/test2')
    ll = client.list('/')
    print(ll)

    data = [{'name':'Anna', 'salary':567000}, {'name':'Vitaliy', 'salary': 12000}]
    with client.write('/test2/sample.json', encoding='utf-8') as json_file:
        json.dump(data, json_file)
    client.download('/test2/sample.json', './file_from_hadoop.json')
    client.upload('/test2/file_from_ubuntu.json', './file_from_hadoop.json')


if __name__ == '__main__':
    main()