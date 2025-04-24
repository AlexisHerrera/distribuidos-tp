import logging
from collections import defaultdict

from transformers import pipeline

from src.model.movie import Movie
from src.model.movie_sentiment import MovieSentiment

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    def __init__(self):
        self.__analyzer = pipeline(
            'sentiment-analysis',
            model='distilbert-base-uncased-finetuned-sst-2-english',
        )

    def handle_message(self, movies: list[Movie]):
        sentiments = defaultdict(lambda: 1)
        analyzed_movies = []

        for movie in movies:
            sentiment = self.__analyzer(movie.overview)[0]['label']

            sentiments[sentiment] += 1

            movie_sentiment = MovieSentiment(
                movie.id, movie.title, movie.budget, movie.revenue, sentiment
            )

            analyzed_movies.append(movie_sentiment)

        logger.info('Analyzed sentiments in this batch:')
        for k, v in sentiments.items():
            logger.info(f'- {k}: {v}')
        logger.info('-------------------------')

        return analyzed_movies
