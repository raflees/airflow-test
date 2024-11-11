import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from scripts.postgres_helper import PostgresHelper

sentiment = SentimentIntensityAnalyzer()
helper = PostgresHelper()

def load_postgres_reviews_table() -> pd.DataFrame:
    engine = helper.create_pg_engine()
    return pd.read_sql('select id, author_id, place_id, review_text from transform.customer_reviews_google', con=engine)

def analyze_text(text: str) -> dict:
    if not text:
        return ""
    result = sentiment.polarity_scores(text)
    return result

def sentiment_analysis():
    df = load_postgres_reviews_table()
    
    series_sentiment_analysis = df.apply(
        lambda row: analyze_text(row["review_text"]),
        axis=1,
    )

    sa_results = pd.json_normalize(series_sentiment_analysis)
    df_join = df.join(sa_results)
    df_join.rename(columns={
        "neg": "negative_score",
        "neu": "neutral_score",
        "pos": "positive_score",
        "compound": "sentiment_polarity",
        "id": "review_id",
        },
        inplace=True
    )

    df_join = df_join[["review_id", "negative_score", "neutral_score", "positive_score", "sentiment_polarity"]]

    helper.upload_overwrite_table(df_join, "reviews_sentiment_analysis", "analysis")