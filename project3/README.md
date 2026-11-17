# Cyrpto Trades - Coinbase

**Author:** Evelyn Rosales Mendez
**DS 3022 Data Project 3, Fall 2026**

## Data source

- Coinbase Exchange trades: is an official public market source where people can access digital asessts quickly to buy or sell. Whenever anything is traded the delivery consists of real cryptocurrencies at the actual-current market price (Feed: wss://ws-feed.exchange.coinbase.com)
- This is a stream-live, push based source that is retrieved via WebSocket. You don't need an account or an API key. But you must make sure you have subscribe message within 5 seconds of connecting to the source or else the connection will close. The subscribe message asks for the matches channel for BTC-USD, ETH-USD, SOL-USD, XRP-USD, DOGE-USD and ADA-USD.
- This project uses WebSocket to retrive Coinbase's messages describing trades. This looks like the; type, trade_id, product_is, side (which says whether it was bought or sold), price, size, time, and sequence. Limitations we face consist of messages being dropped, connection closing and reconnection after a pause. 

## How to run

What you will need: 
- Docker Desktop (running)
- Python 3.12+
- Git.

### 1. Setup - Written instructions and code walk through
2. Find this repoistory: https://github.com/evelynxanaxia/ds3022-streaming.git
    a. DO NOT clone it, click on the green button that says 'Code' and download zip.
3. Unzip the the files and ensure the name is 'ds3022-streaming' if it's not =, rename it.
4. Open up 4 terminals. Start up your terminals with a virtual envoirnment. Then install the packages required. In your terminal 1 install prefect maplotlib
    a. python3 -m venv .venv --> source venv/bin/activate
    b. pip install -r requirements.txt
    c. pip install prefect matplotlib
5. Create your '.gitignore' file and add these to it (data/, charts/, *.duckdb, .venv/)
```bash
git clone https://github.com/evelynxanaxia/ds3022-streaming.git
cd ds3022-streaming
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install prefect matplotlib
```

### 2. Start the Kafka cluster (Redpanda)
```bash
docker compose up -d
docker exec -it redpanda-0 rpk cluster info      
docker exec -it redpanda-0 rpk topic create coinbase_trades -p 3 -r 3
```
Console: http://127.0.0.1:8080. If you get "container name already in use",
run `docker rm -f redpanda-0 redpanda-1 redpanda-2 redpanda-console` and repeat.
This removes the cluster if in use and allows you to recreate it. 

### 3. Collect data (full run)
Each command below runs in its own terminal, with the venv activated:
```bash
cd project3
python producer.py      # T1: Coinbase trades -> Kafka 
python lander.py        # T2: Kafka -> data/raw/*.parquet 
python four_numbers.py product_id,trade_id    # T3
```
50,000 raw records takes about 40 minutes to 2 hours (about 23 trades per second,
slower at night). Stop both with Ctrl+C once raw >= 50,000 and the lander has
written its last batch (wait 60 s).

### 4. Start Prefect (own terminal, keep running)
```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect server start     # dashboard: http://127.0.0.1:4200
```

### 5. Run the flow
```bash
cd project3
python flow.py                # full data -> charts/usd_by_hour.png
python flow.py data/sample    # committed sample -> charts/usd_by_hour_sample.png
```
Run the sample twice: the log should show the same four numbers, matching
the sample column in the table below, and end with `Completed()`.

### 6. Stop your docker when done
```bash
docker compose down     
# remember to not use -v because it deletes the topic data
```

## Architecture

Coinbase WebSocket (matches channel, 6 products)
        |
  producer.py   key = product_id, envelope {"action","data"}
        v
  Kafka topic: coinbase_trades (3 partitions, replication 3, Redpanda)
        |
  lander.py     1,000 records or 60 s per file; offsets stored after the file
        v
  data/raw/coinbase_*.parquet   (13 columns, raw JSON kept in `raw`)
        |
  flow.py (Prefect: coinbase-pipeline)
    load_raw -> raw_trades
    stage    -> stg_records   (DISTINCT, malformed and non-trades removed)
    entities -> trades        (one row per product_id + trade_id, event_time)
    marts    -> mart_usd_hour (average USD traded per hour of day, per product)
    chart    -> charts/usd_by_hour.png
        |
  coinbase.duckdb (not committed)

## Design decisions

1. **Key and partitions.** The key is product_id, so all trades of one product go to the same partition and stay in order. This matters because Coinbase's sequence numbers are per product, so a gap means a dropped message. With a random key or no key, trades of one product would spread over partitions and lose their order.
2. **Batch size and time.** A file closes at 1,000 records or 60 seconds. At my measured rate of 22.74 records per second, 1,000 records take about 44 seconds, so the size rule closes most batches, giving about 82 files per hour. My 129,410 raw records landed in 130 files.
3. **Offsets after the file.** The lander stores offsets only after the Parquet file exists. If it crashes in between, the same records are read again and land twice (at-least-once). I accept this because losing data is worse than duplicates, and stage() removes the copies. This was shown in card D1.
4. **Duplicates.** Two records are the same Kafka record if they have the same kafka_partition and kafka_offset; SELECT DISTINCT * EXCLUDE (landed_at) in stage() removes them. They are the same trade if they have the same product_id and trade_id; QUALIFY row_number() OVER (PARTITION BY product_id, trade_id ORDER BY kafka_offset DESC) = 1 in entities() keeps one row. In my data both counts are 128,381, so no trade arrived twice with a new offset.
5. **Windows and time.** I use event time (the time field from Coinbase, cast to a UTC timestamp), never landing time, because a stopped lander creates a gap and then a spike in landing time that never happened at the source. The window is one hour, grouped by hour of day (0-23 UTC), which gives 24 comparable points over the days I collected.
6. **Ten times the volume.** At 10x (about 230 records per second) the lander would write a file every 4 seconds, creating many small files, and load_raw() rebuilds raw_trades from every file on each run, so the flow would slow down first. I would raise the batch size, run one lander per partition in the same consumer group, and load only new files into the database.

## Chaos round

| Card | Prediction | What I did | Numbers before → after | Explanation |
|---|---|---|---|---|
| D1 Crash between file and offsets | The messages will be reread and rewritten. | Wrote`python lander.py crash` which writes one file, prints `CRASH (on purpose)` and exits **before** storing offsets. | 121,000 raw and 1 malformed, unique Kafka records were 119,999. The gap of 1,000 is exactly the crashed batch landed twice. | The command makes the program read the same records again and but this time it writes them to a second file. |
| D2 Poison message | The lander keeps running; malformed goes from 0 to 1. | Used `docker exec -it redpanda-0 rpk topic produce coinbase_trades -k bad`, typed this is not JSON, Enter, Ctrl+C, then waited for the next Landed line with the lander running. |raw 129,410 → 129,411; malformed 1 → 2; unique Kafka records 128,409 → 128,409; unique trades 128,402 → 128,402 | This command `to_row()` caught the JSON error and landed the text as malformed, so the lander did not crash and the partition was not blocked. |
| D3 Broker down  | A short pause at most, nothing will be lost, because every partition has a copy on all 3 brokers. |With producer and lander running, docker stop redpanda-1, watched 60 s and the Console, docker start redpanda-1, stopped the producer, waited 60 s, counted. | raw 131,411 → 138,411; malformed 2 → 2; unique Kafka records 130,409 → 137,409; unique trades 130,402 → 137,402 | Replication factor 3: leaders moved to the other brokers, and 2 of 3 brokers can still accept writes, so nothing was lost and no duplicates appeared. |

The full-data column is from the flow run before the D2 and D3 chaos cards. The chaos rows were counted with four_numbers.py afterwards, so their raw numbers are higher.

## The four numbers

| Number | Full data | Sample (`data/sample`) |
|---|---:|---:|
| Raw records landed | 129,410 | 5000 |
| Malformed (kept in raw) | 1 | 0 |
| Unique Kafka records | 128,381 | 4999 |
| Unique Trades | 128,381 | 4999 |

## Challenges / Obstacles

At first I found setting up this project challenging because I didn't have an understanding of how to use kafka in a project, but with the slides I was able to figure out how exactly to set up my files so that everything worked well together. I think that gathering the data at first was confusing but once I did it and reviewed the numbers and information I was given after running my files, the process started to make sense. I used all of the slideshows for each session and the catch-up files to review what I had done in my pervious sessions and to update this README file. Other challenging things about this project include the necessity of a subscribe message within 5 seconds of connecting, so the producer sends it right after the WebSocket opens. The fact that messages can be dropped and the connection can close, meaning that the producer reconnects after a 5-second pause and skips messages it cannot parse instead of crashing. The tools I used in this project were: Docker, Redpanda, Python, Prefect, DuckDB and matplotlib.


## Analysis

My intial research question was about which hours of the day carry the most traded value in USD, and in viewing my chart I can see that traded value is highest between 13:00 and 15:00 UTC (8-10 am US Eastern, 2-4 pm in Valencia). This is when the European afternoon overlaps with the start of the US trading day, which is a plausible explanation. BTC-USD dominates the value at every hour. We can also see that BTC is the product that dominates the most in USD regardless of the time. This was surprising to me because I expected one product to lead, but not by this much! BTC is far ahead of ETH and all the other products. A limitation that my data is that I only collected a few days of data, so each hour of the day has few observations and the first and last hours I captured are partial. Also, the matches channel can drop messages, so the totals are slight underestimates.

## Plot / Visualization

I changed my chart to show the which hours of the day (UTC) carry the most traded value in USD on Coinbase. I wanted the chart to show if the value is dominated by one product as well.

![usd_by_hour chart title](charts/usd_by_hour.png)

## Reflection

This may sound silly but before this project I did not know that we are able to access any public stream-live data source and analyze it. I thought that each website had a special protection that prevented people outside of their network to see and gather the data that they keep track of. I also learned a lot about Kafka and how it organizes events into topics that is then broken down into partitions and spread across multiple servers. I didn't know that it was an open-source, distributed event streaming platform used to collect, store, and process massive volumes of real-time data streams. I find it fasinating that we have access to tools like this to create our own projects and explore the data that is publicly collected on cites. The chaos round allowed me to truly understand how Kafka organizes events into topics, so one broker can fail without losing data. After the crash test I could see exactly 1,000 duplicated records and then remove them in the flow. If I started again, I would test my lander and flow on a small sample earlier and I would extend the analysis to compare when the top five products trade most to see whether they follow the same daily pattern.

## Acknowledgements

I was unable to test my project with a classmate because my README file was yet to be done in class.
Instructor materials: the Session 1-4 slides and worksheets and the catch-up guide. The producer, lander and flow are adapted from the class scripts (quakes_producer.py, quakes_lander.py, quakes_flow.py), and the WebSocket connection code from probe.py.
AI assistance: I used Claude to help debug errors (Docker container conflict, Prefect connection, NameError in the lander), to explain what the worksheet steps meant, and how to go about creating the SQL for my mart_usd_hour code. I checked the results by running the code, comparing the numbers with the flow log and the worksheet, and rewriting the text in my own words.
