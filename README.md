# Client

## Requirements

```bash
 pip install -r requirements.txt
```

## Usage

```bash
python src/client/main.py .data/movies_metadata.csv .data/ratings.csv .data/credits.csv
```

```
Successfully opened files:
  Movies:  .data/movies_metadata.csv
  Ratings: .data/ratings.csv
  Cast:    .data/credits.csv

--- Counting lines in: Movies (.data/movies_metadata.csv) ---
--- Counting ended for Movies. Total lines: 45573 ---

--- Counting lines in: Ratings (.data/ratings.csv) ---
--- Counting ended for Ratings. Total lines: 26024290 ---

--- Counting lines in: Cast (.data/credits.csv) ---
--- Counting ended for Cast. Total lines: 45477 ---

Successfully completed processing all files.
```

# Protocol

## Message Structure

| MsgId  | MsgLen  | ProtobufData    |
| ------ | ------- | --------------- |
| 1 Byte | 2 Bytes | Variable Length |

> [!Note]
> The 2 Bytes for message len should be enough for a batch of size < 8kb

### Message types

| MsgId |      TypeName      |
| :---: | :----------------: |
|   0   |      Unknown       |
|   1   |       Movie        |
|   2   |       Rating       |
|   3   |        Cast        |
|   4   |   MovieSentiment   |
|   5   |   MovieAvgBudget   |
|   6   | MovieBudgetCounter |
|   7   |    MovieRating     |
|   8   |   MovieRatingAvg   |
|   9   |     MovieCast      |
|  10   |     ActorCount     |
|  90   |    ReportResult    |
|  100  |        EOF         |

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

Other messages:
100 EOF

```
