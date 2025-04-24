class MovieRatingCount:
    def __init__(self, movie_id: int, title: str, partial_sum: float, count: int):
        self.movie_id = movie_id
        self.title = title
        self.partial_sum = partial_sum
        self.count = count
