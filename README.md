# TP

[[toc]]

# Client

## Requirements

```bash
 pip install -r requirements.txt
```

## Usage

Use the `run-compose.sh` script to start all nodes, including the client.

```sh
./run-compose.sh [yes/no] [number]
```

`number` represents the percentage that the dataset will be shrinked
in case the `yes` is passed to use the small dataset

Example 1: run without flags is the same as running `python3 generate-compose.py` to generate `docker-compose.yaml`

```sh
./run-compose.sh
```

Example 2: run with `yes` uses small dataset, by default is 10% its equivalent to run `python3 generate-compose.py -s` to generate `docker-compose.yaml`

```sh
./run-compose.sh yes
```

Example 3: run with `yes` and specify percentage

```sh
./run-compose.sh yes 40
```

# Protocol

## Message Structure

| UserId  | MsgType | MsgLen  | ProtobufData    |
| ------- | ------- | ------- | --------------- |
| 4 Bytes | 1 Byte  | 2 Bytes | Variable Length |

> [!Note]
> The 2 Bytes for message len should be enough for a batch of size < 8kb

### Message types

| Id  |      TypeName      |
| :-: | :----------------: |
|  0  |      Unknown       |
|  1  |       Movie        |
|  2  |       Rating       |
|  3  |        Cast        |
|  4  |   MovieSentiment   |
|  5  |   MovieAvgBudget   |
|  6  | MovieBudgetCounter |
|  7  |    MovieRating     |
|  8  |   MovieRatingAvg   |
|  9  |     MovieCast      |
| 10  |     ActorCount     |
| 90  |    ReportResult    |
| 100 |        EOF         |

```
1 Movie:
  1 id
    uint32
  2 title
    string
  3 genres
    list<string>
  4 release_date
    date/string: AAAA-MM-DD
  5 production_countries
    list<string>
  6 budget
    uint32
  7 revenue
    uint32
  8 overview
    string

2 Rating:
  1 movieId
    uint32
  2 rating
    float (4 bytes) / string (3 bytes)

3 Cast:
  1 id
    uint32
  2 cast
    list<string>

SentimentAnalyzer -> AvgBudgetRevenuePositiveAndNegative:
  4 MovieSentiment:
    1 id
      uint32
    2 title
      string
    3 budget
      uint32
    4 revenue
      uint32
    5 sentiment
      string

AvgBudgetRevenuePositiveAndNegative -> ReporterService:
  5 MovieAvgBudget:
    1 id
      uint32
    2 title
      string
    3 avg_budget_revenue
      float
    4 sentiment
      string

BudgetCounter -> Top5BudgetSoloCountry -> ReporterService:
  6 MovieBudgetCounter:
    1 country
      string
    2 total_budget
      uint32

RatingsJoiner -> RatingCounter:
  7 MovieRating:
    1 id
      uint32
    2 title
      string
    3 rating
      float

RatingCounter -> MaxMinAvgRatingArgentina:
  8 MovieRatingAvg:
	  1 id
      uint32
    2 title
      string
    3 average_rating
      float

CastJoiner -> CastSplitter:
  9 MovieCast:
    1 id
      uint32
    2 title
      string
    3 actors_name
      list<string>

CastSplitter -> ActorCounter -> Top10Actors:
  10 ActorCount:
    1 actor_name
      string
    2 count
      uint32

ReporterService -> Gateway:
  // n = 1, 2, 3, 4, 5
  9n:
    91: Q1
    92: Q2
    93: Q3
    94: Q4
    95: Q5

  91 ReportResult:
    // Then inside it has a list of any of the other types written above
    5 MovieAvgBudget
      id, title, avg_budget_revenue, sentiment
    …

Other messages:
100 EOF
200 ACK

```

## EOF treatment

Best case scenario:

1. One node receives EOF for user `a` and becomes the leader, then it notifies other nodes.
2. Node `n` receives the notification from leader node.
3. Node `n` can be in different states:
   1. It is processing a message for user `a`.
      1. Finish processing the message and notify leader that the node is done.
   2. It is processing a message for other user, say `b`.
      1. It is assumed[^1] that the last message for user `a` has already been proceseed, so notify the leader that the node is done.
   3. It is not processing any message.[^1]
      1. Send the leader node that the node is done.

[^1] As messages are delivered in order, the last message before `EOF` for user `a` must have been delivered.

Worst case scenario:

1. One node receives EOF for user `a` and becomes the leader, then it notifies other nodes.
2. Node `n` receives the notification from leader node.
3. Node `n` can be in different states:
   1. It is processing a message for user `a` and fails (sends `nack`).
      1. If other node receives the message it must return `nack` until the message is back to the node `n`.
