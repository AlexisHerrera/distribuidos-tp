# import os

# from src.messaging.protocol.message import Message, MessageType
# from src.model.movie import Movie
# from src.server.sentiment_analyzer.sentiment_analyzer import SentimentAnalyzer
# from src.utils.config import Config


# class TestSentimentAnalyzer:
#     def set_up_config(self) -> Config:
#         os.environ['RABBIT_HOST'] = 'rabbitmq'
#         os.environ['CONSUMER_EXCHANGE'] = 'movies_metadata_clean'
#         os.environ['OUPUT_QUEUE'] = 'sentiment_analyzed'

#         return Config()

#     def test_send_one_movie_returns_positive(self):
#         config = self.set_up_config()

#         sentiment_analyzer = SentimentAnalyzer(config)

#         movie = Movie(
#             movie_id=1,
#             title='Happy movie',
#             budget=1000,
#             revenue=10000,
#             overview='Very happy movie',
#         )

#         message = Message(MessageType.Movie, [movie])

#         sentiment_analyzer.process_message(message)
