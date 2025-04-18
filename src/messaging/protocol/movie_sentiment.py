# pylint: disable=no-member
from src.messaging.protobuf import movie_sentiments_pb2
from src.messaging.protocol.message_protocol import MessageProtocol
from src.model.movie_sentiment import MovieSentiment


class MovieSentimentProtocol(MessageProtocol):
    def __init__(self):
        super().__init__(item_to_bytes=self.__to_movie_sentiment_pb,
                        encode_all=self.__encode_all,
                        bytes_to_item=self.__to_movie_sentiment,
                        decode_all=self.__decode_all)


    def __to_movie_sentiment_pb(self, movie_sentiment: MovieSentiment):
        movie_sentiment_encoded = movie_sentiments_pb2.MovieSentiments()

        movie_sentiment_encoded.id = movie_sentiment.id
        movie_sentiment_encoded.title = movie_sentiment.title
        movie_sentiment_encoded.budget = movie_sentiment.budget
        movie_sentiment_encoded.revenue = movie_sentiment.revenue
        movie_sentiment_encoded.sentiment = movie_sentiment.sentiment

        return movie_sentiment_encoded

    def __encode_all(self, l):
        return movie_sentiments_pb2.MovieSentiments(list=l).SerializeToString()

    def __to_movie_sentiment(self, movie_sentiment_pb2) -> MovieSentiment:
        movie_id = movie_sentiment_pb2.id
        title = movie_sentiment_pb2.title
        budget = movie_sentiment_pb2.budget
        revenue = movie_sentiment_pb2.revenue
        sentiment = movie_sentiment_pb2.sentiment

        return MovieSentiment(movie_id, title, budget, revenue, sentiment)

    def __decode_all(self, buf: bytes, bytes_amount: int):
        movie_sentiment_pb2_list = movie_sentiments_pb2.MovieSetniments()

        movie_sentiment_pb2_list.ParseFromString(buf[0:bytes_amount])

        return movie_sentiment_pb2_list.list
