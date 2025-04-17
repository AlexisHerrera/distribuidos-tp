class MovieSentiment:
    def __init__(self, movie_id: int, title: str, budget: int, revenue: int, sentiment: str):
        self.id = movie_id
        self.title = title
        self.budget = budget
        self.revenue = revenue
        self.sentiment = sentiment
