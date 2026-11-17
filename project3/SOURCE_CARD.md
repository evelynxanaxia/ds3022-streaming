# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:** Evelyn Rosales Mendez
**Date:** 11/16/26

## 1. The source
- Source and URL: Coinbase (Feed: wss://ws-feed.exchange.coinbase.com)
- Method (WebSocket push, REST polling or REST history pull): WebSocket
- Account, key or User-Agent needed: No account or key needed
- Limits I know about (rate limits, paging, reconnects):  Messages can be dropped, connection can close and reconnection occur after pauses. 

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json
{
    "action": "live",
    "data": {
        "type": "match",
        "trade_id": 256705690,
        "maker_order_id": "fcc67adc-aee8-431f-88dd-d69f3cf81059",
        "taker_order_id": "dc651c1f-3dd3-472b-a53b-6ddc6829e592",
        "side": "buy",
        "size": "1359.538333",
        "price": "1.0986",
        "product_id": "XRP-USD",
        "sequence": 23286244099,
        "time": "2026-11-09T15:30:26.425765Z"
    }
}
```

## 3. Volume maths
- Measured rate (from the probe):    22.74 per second (1,366 messages in 60 s)
- Live capture needed for 50,000: about 2,199 s, about 37 minutes
- History / backfill available? How many days? No. The WebSocket has no replay because the data can only be collected live. 
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): I plant to run the producer and lander continuously for a few hours so that I am able to reach 50,000 raw records by Thursday. 

## 4. Topic design
- Topic name: coinbase_trades (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: product_id
- Why this key (ordering, spread across partitions): Because all trades of one product go to the same partition, so they stay in order. Coinbase's sequence numbers are per product, which is why order matters. And if there's a gap in them that means a message was dropped. The 6 products spread across the 3 partitions, so the load is shared. 
## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander) 
Same kafka_partition + kafka_offset. This happens if the lander crashes after writing a file but before storing offsets, then re-reads the same records.
- What makes two records **the same thing** (the same trade, post, aircraft position)? Same product_id + trade_id. trade_id alone is not enough because trade IDs are numbered per product. The 'last_match' messages sent right after subscribing (and again after a reconnect) repeat a trade that was already seen, so they also count as duplicates.
- If the same thing can arrive in several versions, which one do I keep? 
Trade messages are never updated, so all copies are identical. I keep the earliest landed copy (lowest offset) and drop the rest. Non-trade messages (type 'subscriptions') are not trades and are filtered out.
 ## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join:
Which hours of the day (UTC) carry the most traded value in USD on Coinbase, and is that value dominated by one product?
## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
