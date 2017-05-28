import argparse
import asyncio
import urllib.parse


DEFAULT_URL = 'https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv'
DEFAULT_AVG = 'tip_amount'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('url', type=str, nargs='?', default=DEFAULT_URL,
                        help=f'defaults to {DEFAULT_URL}')
    parser.add_argument('-a', '--average', dest='average', type=str,
                        default='tip_amount', metavar='COLUMN_NAME',
                        help=f'name of the column to average, defaults to "{DEFAULT_AVG}"')
    return parser.parse_args()

def open_connection(url):
    host, *_port = url.netloc.split(':')
    if url.scheme == 'https':
        port = _port[0] if _port else 443
        ssl = True
    else:
        port = _port[0] if _port else 80
        ssl = False

    return asyncio.open_connection(url.hostname, port, ssl=ssl)

async def stream_lines(_url):
    url = urllib.parse.urlsplit(_url)
    reader, writer = await open_connection(url)

    query = (f'GET {url.path} HTTP/1.0\r\n'
             f'Host: {url.hostname}\r\n'
             '\r\n')
    writer.write(query.encode('latin-1'))

    while True:
        try:
            line = await reader.readuntil()
        except asyncio.IncompleteReadError as e:
            line = e.partial
        if not line:
            # eof!
            break
        yield line

    writer.close()

async def consume_headers(reader):
    async for line in reader:
        if line == b'\r\n':
            break

async def produce_rows(url):
    reader = stream_lines(url)
    await consume_headers(reader)
    async for line in reader:
        yield line.rstrip().split(b',')

async def avg_column_index(avg_field, rows):
    async for row in rows:
        return row.index(bytes(avg_field, 'latin-1'))

async def aggregate(url, avg_field):
    rows = produce_rows(url)
    idx = await avg_column_index(avg_field, rows)

    total_lines = 0
    total_tip_amount = 0
    i = 0
    async for row in rows:
        total_lines += 1
        total_tip_amount += float(row[idx])

    print(f'Total lines: {total_lines}')
    print(f'Average {avg_field}: {total_tip_amount / total_lines}')


if __name__ == '__main__':
    args = parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(aggregate(args.url, args.average))
    loop.close()
