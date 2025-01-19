from graphex.model import DAGData
from graphex.document import mermaid_chart, mermaid_link_from_dag


def filter_by_genre(user_genre_preference, all_movies):
    return ['Movie 1', 'Movie 2', 'Movie 3']


def calculate_similarity(filtered_movies):
    return [0.9, 0.85, 0.75]


def rank_movies(similarity_scores):
    return ['Movie 1', 'Movie 2', 'Movie 3']


def adjust_ranking_by_history(user_rating_history, ranked_movies):
    return ['Movie 1', 'Movie 3', 'Movie 2']


def generate_top_n_recommendations(adjusted_ranking, top_n):
    return ['Movie 1', 'Movie 3', 'Movie 2']


def generate_explanation(user_profile, recommendations):
    return 'Recommended because you like action and sci-fi movies, and these match your rating history.'


def process_all():
    dag = DAGData(
        inputs=['user_profile', 'movie_data', 'top_limit'],
        outputs=['recommendations', 'explanation']
    )

    dag.step(
        'filter_movies_by_genre',
        filter_by_genre,
        [],
        {
            'user_genre_preference': 'user_profile',
            'all_movies': 'movie_data'
        },
        ['filtered_movies']
    )

    dag.map(
        name='calculate_similarity',
        function=calculate_similarity,
        arg_var_names=['filtered_movies'],
        string_keyword_args={},
        outputs=['similarity_scores']
    )

    dag.map(
        name='rank_movies',
        function=rank_movies,
        arg_var_names=['similarity_scores'],
        string_keyword_args={},
        outputs=['ranked_movies']
    )

    dag.step(
        name='boost_ratings_based_on_history',
        function=adjust_ranking_by_history,
        arg_var_names=[],
        string_keyword_args={
            'user_rating_history': 'user_profile',
            'ranked_movies': 'ranked_movies'
        },
        outputs=['adjusted_ranking']
    )

    dag.step(
        name='generate_top_recommendations',
        function=generate_top_n_recommendations,
        arg_var_names=[],
        string_keyword_args={
            'adjusted_ranking': 'adjusted_ranking',
            'top_n': 'top_limit'
        },
        outputs=['recommendations']
    )

    dag.step(
        name='generate_explanation',
        function=generate_explanation,
        arg_var_names=[],
        string_keyword_args={
            'user_profile': 'user_profile',
            'recommendations': 'recommendations'
        },
        outputs=['explanation']
    )

    text = mermaid_chart(dag)

    print(text)

    link = mermaid_link_from_dag(dag)

    print(link)


if __name__ == '__main__':
    process_all()
