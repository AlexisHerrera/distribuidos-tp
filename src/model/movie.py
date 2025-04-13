class Movie:
    def __init__(self, movie_id, title, genres, release_date, production_countries, budget, revenue, overview): # pylint: disable=too-many-arguments, too-many-positional-arguments
        self.id = movie_id
        self.title = title
        self.genres = genres
        self.release_date = release_date
        self.poduction_countries = production_countries
        self.budget = budget
        self.revenue = revenue
        self.overview = overview
