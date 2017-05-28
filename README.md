# Carto Python code test

See [https://gist.github.com/jorgesancha/2a8027e5a89a2ea1693d63a45afdd8b6](https://gist.github.com/jorgesancha/2a8027e5a89a2ea1693d63a45afdd8b6).

Reproduced here for convenience:

> Build the following and make it run as fast as you possibly can using Python 3 (vanilla). The faster it runs, the more you will impress us!
>
> Your code should:
>
> * Download this 2.2GB file: https://s3.amazonaws.com/carto-1000x/data/yellow\_tripdata_2016-01.csv
> * Count the lines in the file
> * Calculate the average value of the tip_amount field.
>
> All of that in the most efficient way you can come up with.
>
> That's it. Make it fly!

## Approach

The requirements are to use Python 3 with no 3rd party libraries, and to focus on speed.

I wanted to try / combine 3 ideas to build my solution:

1. Download a compressed version of the file, to heavily reduce its size.
2. Use a streaming connection to parse the file as it's downloaded, to avoid having to read it twice (once during the download, another to parse it from disk) and save up resources (reading a huge file into memory isn't usually a good idea).
3. Use HTTP range requests to parallelize the download to better use the available bandwith.

I couldn't use `1`: S3 doesn't return a compressed version of the file when passing the usual `Accept-Encoding` header (no gzip, no deflate).

`2` is a given as long as the client exposes the capability to consume bytes as they're buffered.

I tested if `3` was a valid approach by downloading 4 copies of the file concurrently, and as I expected they all would download at the same rate as a single download: S3 seems to apply bandwith limiting to individual requests. By downloading multiple parts of the file at the same time, I should be able to saturate by download link.

## Implementation

Since only Python 3 and its standard library are to be used, and I want streaming and concurrent downloads, I've chosen `asyncio` for my solution.

A first script, `1-asyncio.py`, implements a streaming client. Lines are consumed from the HTTP stream then produced as bytestrings, consumed by a generator that produces Python lists out of them, finally consumed by an aggregator. This solution beats a naive `wget` + `awk` script but not by much. Still an interesting contender since these tools are super optimized and we're using Python to beat them.

The second script, `2-asyncio-ranges.py`, adds HTTP range requests to the first approach. By default 4 concurrent processors are spawned (this saturates my link, but can be configured to make better use of better networks), they aggregate lines for their file chunk, that are then combined into the final results. This solution is way faster than the naive shell approach or the non-range streaming one.

## To Do

I was mainly interested in the network / concurrency aspect of the problem, and I haven't dug into optimizing the parsing aspect of the scripts. Using profiling to find out where time is consumed might help in squeezing some more seconds. You're free to explore it :wink:.

## Usage

```
usage: 1-asyncio.py [-h] [-a COLUMN_NAME] [url]

positional arguments:
  url                   defaults to https://s3.amazonaws.com/carto-1000x/data/
                        yellow_tripdata_2016-01.csv

optional arguments:
  -h, --help            show this help message and exit
  -a COLUMN_NAME, --average COLUMN_NAME
                        name of the column to average, defaults to
                        "tip_amount"
```

```
usage: 2-asyncio-ranges.py [-h] [-a COLUMN_NAME] [-c CONCURRENCY] [url]

positional arguments:
  url                   defaults to https://s3.amazonaws.com/carto-1000x/data/
                        yellow_tripdata_2016-01.csv

optional arguments:
  -h, --help            show this help message and exit
  -a COLUMN_NAME, --average COLUMN_NAME
                        name of the column to average, defaults to
                        "tip_amount"
  -c CONCURRENCY, --concurrency CONCURRENCY
                        number of concurrent downloads
```

## Results

```bash
$ time ./0-shell.sh
Total lines: 10906858
Average tip_amount: 1.75066
./0-shell.sh  25.52s user 20.27s system 21% cpu 3:28.88 total

$ time python 1-asyncio.py
Total lines: 10906858
Average tip amount: 1.7506631158122512
python 1-asyncio.py  86.95s user 4.76s system 48% cpu 3:08.65 total

$ time python 2-asyncio-ranges.py
Total lines: 10906858
Average tip_amount: 1.7506631158123862
python 2-asyncio-ranges.py  58.66s user 3.29s system 92% cpu 1:07.12 total
```

