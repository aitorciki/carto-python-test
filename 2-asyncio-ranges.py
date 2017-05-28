import argparse
import asyncio
import itertools
import math
import sys
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
    parser.add_argument('-c', '--concurrency', dest='concurrency', type=int, default=4,
                        help='number of concurrent downloads')
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

async def get_file_size(_url):
    url = urllib.parse.urlsplit(_url)
    reader, writer = await open_connection(url)

    query = (f'HEAD {url.path} HTTP/1.1\r\n'
             f'Host: {url.hostname}\r\n'
             '\r\n')
    writer.write(query.encode('latin-1'))

    while True:
        line = await reader.readline()
        if line == b'\r\n':
            break
        if line.startswith(b'Content-Length'):
            writer.close()
            return int(line.split(b':')[1])

    writer.close()

def parse_content_range(value):
    bytes = value.split()[1]
    range_ = bytes.split('/')[0]
    from_, to = range_.split('-')
    return int(to) - int(from_)

async def stream_lines(_url, byte_range):
    """
    Performs a range HTTP streaming request and produces bytestrings
    + line completion mark for every line in the response body.
    """
    url = urllib.parse.urlsplit(_url)
    reader, writer = await open_connection(url)

    query = (f'GET {url.path} HTTP/1.1\r\n'
             f'Host: {url.hostname}\r\n'
             f'Range: bytes={byte_range[0]}-{byte_range[1]}\r\n'
             '\r\n')
    writer.write(query.encode('latin-1'))

    content_size = 0
    # Consume headers to:
    # - extract the content length needed to find the stream EOF
    # - reach the body!
    while True:
        line = await reader.readline()
        if line.startswith(b'Content-Range'):
            header = line.decode('latin-1')
            content_size = parse_content_range(header.split(':')[1])
        if line == b'\r\n':
            break

    content_read = 0
    while True:
        # This is an ugly hack to work around the egg and chicken
        # mess resulting from combining StreamReader.readuntil() with
        # range requests: readuntil blocks until a newline is found,
        # but because we use ranges that split the file in chunks, it
        # is easy to have chunks end in the middle of the line, thus
        # causing readline to wait for a non-existing newline.
        # We unlock this by manually checking if we're at the end of
        # the chunk: if the pending bytes in the buffer will complete
        # the expected content length but there's no newline to be found,
        # we manually feed an EOF to the stream to force readline to
        # return the remaining bytes.
        if (content_read + len(reader._buffer) >= content_size
                and not b'\n' in reader._buffer):
            reader.feed_eof()

        try:
            line = await reader.readuntil()
            closed = True
        except asyncio.IncompleteReadError as e:
            # We've reached the end of the stream and no newline has
            # been found, return the partial line in the buffer, it
            # will be handled downstream.
            line = e.partial
            closed = False
        yield line, closed

        content_read += len(line)
        if content_read >= content_size:
            reader.feed_eof()
        if reader.at_eof():
            break

    writer.close()

async def produce_rows(url, byte_range):
    """
    Consumes CSV lines as bytestrings from the HTTP stream
    and produces lists of CSV columns + line completion mark.
    """
    reader = stream_lines(url, byte_range)
    first_producer = byte_range[0] == 0

    i = 0
    async for line, closed in reader:
        i += 1
        # First and last rows of each chunk (except first row of first
        # chunk) are potentially cut by the range request, return them
        # raw for downstream to count as partial rows needing reconciliation.
        if (not first_producer and i == 1) or not closed:
            yield line, False
        else:
            yield line.rstrip().split(b','), True

async def avg_column_index(avg_field, producer):
    """
    Consumes the first CSV row in the stream and returns the index
    of the field we want to calculate an average for.
    """
    async for row, _ in producer:
        return row.index(bytes(avg_field, 'latin-1'))

async def aggregate_chunk(producer, avg_idx):
    """
    Consumes CSV rows from the stream and aggregates number of seen
    lines and rolling sum of the field we want to average on.
    """
    # First and last lines from chunks are potentially incomplete, keep
    # them for later reconciliation.
    partial_lines = ['', '']
    n_lines = 0
    avg_sum = 0
    async for row, complete in producer:
        if not complete:
            partial_lines[0 if n_lines == 0 else 1] = row
        else:
            n_lines += 1
            avg_sum += float(row[avg_idx])

    return {
        'n_lines': n_lines,
        'avg_sum': avg_sum,
        'partials': partial_lines
    }

def reconcile_partials(partials, avg_idx):
    """
    Combine potentially incomplete last and first rows from range
    request chunks.
    Since range requests are byte-based, lines are very likely cut in
    the boundaries of each chunk. The consumption pipeline keeps these
    boundary partial lines stored separately for their later merge into
    complete lines here.
    The process is actually very simple: the last last line of each
    chunk is concatenated with the first line of the next chunk. The
    first line of the first chunk is ignored.
    """
    partials = list(itertools.chain.from_iterable(partials))[1:]
    it = iter(partials)
    lines = [a + b for a, b in zip(it, it)]

    n_lines = 0
    avg_sum = 0
    for line in lines:
        row = line.rstrip().split(b',')
        n_lines += 1
        avg_sum += float(row[avg_idx])

    return {
        'n_lines': n_lines,
        'avg_sum': avg_sum
    }

async def execute(url, concurrency, avg_field):
    # Calculate range requests boundaries.
    size = await get_file_size(url)
    if not size:
        sys.stderr.write('No content length found :(')
        sys.exit(1)
    range_width = math.ceil(size / concurrency)
    byte_ranges = [[i, i + range_width] for i in range(0, size, range_width + 1)]

    # Create a lines producer per bytes range.
    producers = [produce_rows(url, byte_range) for byte_range in byte_ranges]

    # Use the first producer (first bytes of the file) to consume
    # CSV header and find the index of the field to average.
    avg_idx = await avg_column_index(avg_field, producers[0])

    # Start per-range stream consumption and aggregation processes
    # concurrently, gather partial results.
    aggregators = [asyncio.ensure_future(aggregate_chunk(p, avg_idx)) for p in producers]
    results = await asyncio.gather(*aggregators)

    # Combine per-range aggregations into the final totals, taking
    # into account partial chunk boundary lines that need to be merged
    # into complete lines and aggregated.
    late_results = reconcile_partials([r['partials'] for r in results], avg_idx)
    n_lines = sum([r['n_lines'] for r in results]) + late_results['n_lines']
    avg_sum = sum([r['avg_sum'] for r in results]) + late_results['avg_sum']

    print(f'Total lines: {n_lines}')
    print(f'Average {avg_field}: {avg_sum / n_lines}')


if __name__ == '__main__':
    args = parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(execute(args.url, args.concurrency, args.average))
    loop.close()
