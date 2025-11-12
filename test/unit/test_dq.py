from cineflow.dq.checks import validate_movies, validate_ratings


def test_dq_checks_pass() -> None:
    validate_ratings()
    validate_movies()
    print("Data quality checks passed.")
