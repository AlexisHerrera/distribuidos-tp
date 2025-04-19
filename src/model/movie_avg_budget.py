class MovieAvgBudget:
    def __init__(
        self, movie_id: int, title: str, avg_budget_revenue: float, sentiment: str
    ):
        self.movie_id = movie_id
        self.title = title
        self.avg_budget_revenue = avg_budget_revenue
        self.sentiment = sentiment
