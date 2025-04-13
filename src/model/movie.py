class Movie:
    def __init__(self, # pylint: disable=too-many-arguments, too-many-positional-arguments
            movie_id: int,
            title: str,
            genres: list[str] = None,
            release_date: str = '1900-01-01',
            production_countries: list[str] = None,
            budget: int = 0,
            revenue: int = 0,
            overview: str = ''):
        self.id = movie_id
        self.title = title
        self.genres = genres
        self.release_date = release_date
        self.poduction_countries = production_countries
        self.budget = budget
        self.revenue = revenue
        self.overview = overview
