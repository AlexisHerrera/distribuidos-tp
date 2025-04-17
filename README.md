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

| MsgId  | MsgLen  | AttributeId | Data            | AttributeId | Data | ... |
| ------ | ------- | ----------- | --------------- | ----------- | ---- | --- |
| 1 Byte | 2 Bytes | 1 Byte      | Variable length | ...         | ...  | ... |

> [!Note]
> The 2 Bytes for message len should be enough for a batch of size < 8kb

Depending on the AttributeId its type varies. Each type will be decoded in the following way:

|     Type     |                                 How to get                                 |
| :----------: | :------------------------------------------------------------------------: |
|    uint32    |                                Read 4 bytes                                |
|    string    |                              Read until '\0'                               |
| list<string> | Add \[ \] as delimiter of list and each string is separated by a comma ',' |
|    float     |                                Read 4 bytes                                |

Examples:

| MsgId |    MsgLen    | AttributeId | Data | AttributeId |   Data    | AttributeId |       Data        | AttributeId |    Data    | AttributeId | Data | AttributeId |     Data     | AttributeId |   Data    | AttributeId |    Data    |
| :---: | :----------: | :---------: | :--: | :---------: | :-------: | :---------: | :---------------: | :---------: | :--------: | :---------: | :--: | :---------: | :----------: | :---------: | :-------: | :---------: | :--------: |
|   1   | 42+28 = 70\* |      1      | 800  |      2      | Toy Story |      3      | \[comedy,family\] |      4      | 1995-10-30 |      1      | 999  |      2      | Random movie |      3      | \[drama\] |      4      | 2000-10-30 |

\* 42 bytes the first movie, 1 byte for separator (if added), 28 bytes for the second movie

| MsgId | MsgLen | AttributeId | Data | AttributeId | Data | AttributeId | Data | AttributeId | Data |
| :---: | :----: | :---------: | :--: | :---------: | :--: | :---------: | :--: | :---------: | :--: |
|   2   |   20   |      1      | 800  |      2      | 4.5  |      1      | 999  |      2      | 3.0  |

### Message types

| MsgId | TypeName |
| :---: | :------: |
|   1   |  Movie   |

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
      uint32
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
