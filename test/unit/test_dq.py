from cineflow.dq.checks import validate_ratings, validate_movies

def test_dq_checks_pass() -> None:
    # Asume que ya convertiste u.data/u.item a CSV en data/samples/
    validate_ratings()
    validate_movies()
    print("Data quality checks passed.")