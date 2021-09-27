import boto3
import codecs
import functools
import time
from datetime import datetime


client = boto3.client('s3')
resource = boto3.resource("s3")


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        tic = time.perf_counter()
        value = func(*args, **kwargs)
        toc = time.perf_counter()
        elapsed_time = toc - tic
        print(f"Function {func.__name__} elapsed time: {elapsed_time:0.4f} seconds")
        return value
    return wrapper_timer


def get_tags(bucket, key):
    return client.get_object_tagging(Bucket=bucket, Key=key)['TagSet']


@timer
def put_tags(bucket, key, tag_set):
    return client.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={'TagSet': tag_set}
    )


def list_files(bucket, prefix):
    return client.list_objects(Bucket=bucket, Prefix=prefix)


@timer
def proccess(bucket, key, tags={}):

    s3_object = resource.Object(bucket, key)
    line_stream = codecs.getreader("utf-8")

    c = 0
    c_footer = 0
    c_header = 0

    for line in line_stream(s3_object.get()['Body']):
        c = c + 1
        print(f'Linha => {c}')

        if line[0] == '0':
            c_header = c_header + 1

        if line[0] == '9':
            c_footer = c_footer + 1

            if c_footer <= int(tags['processed']):
                continue

            tags['processed'] = str(c_footer)
            tags['timestamp'] = str(time.time())

            put_tags(bucket=bucket, key=file['Key'], tag_set=from_dict_to_list(tags))

    return tags


def from_dict_to_list(dictonary):
    list = []
    for key in dictonary:
        list.append(dict(Key=key, Value=dictonary[key]))
    return list


def from_list_to_dict(list):
    dict = {}
    for item in list:
        dict[item['Key']] = item['Value'];
    return dict


def init_tags(bucket, key):
    raw_tags = get_tags(bucket, key)
    tags = from_list_to_dict(raw_tags)

    if 'status' not in tags:
        tags['status'] = 'READY'

    if 'timestamp' not in tags:
        tags['timestamp'] = str(time.time())

    if 'processed' not in tags:
        tags['processed'] = str(0)

    return tags


def process_file(bucket, key):
    tags = init_tags(bucket, key)

    print(f'Processando => {key}'),
    print(f'Tags => {tags}')

    dt_object = datetime.fromtimestamp(float(tags['timestamp']))
    min_diff = (datetime.now() - dt_object).total_seconds() / 60

    if tags['status'] == 'READY' or (tags['status'] == 'PROCESSANDO' and min_diff > 15):
        tags = proccess(bucket=bucket, key=file['Key'], tags=tags)
        tags['status'] = 'FINALIZADO'
        put_tags(bucket=bucket, key=file['Key'], tag_set=from_dict_to_list(tags))


if __name__ == '__main__':

    bucket = 'alura-reko-site'
    prefix = "s3-lambda-running/"
    files = list_files(bucket=bucket, prefix=prefix)

    for file in files['Contents']:

        if file['Key'] == prefix:
            continue

        process_file(bucket, file['Key'])
