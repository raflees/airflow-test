import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from scripts.postgres_helper import PostgresHelper

class VaderSentimentAnalyst:
    def __init__(self):
        self.model = SentimentIntensityAnalyzer()
        self.helper = PostgresHelper()
        self.analysis_schema = 'analysis'
        self.analysis_table = 'reviews_sentiment_analysis'
        self.method = "append"
    
    def sentiment_analysis(self):
        df = self._load_unanalyzed_reviews()
        
        series_sentiment_analysis = df.apply(
            lambda row: self._analyze_text(row["review_text"]),
            axis=1,
        )

        sa_results = pd.json_normalize(series_sentiment_analysis)
        print(f"Analysed {sa_results.shape[0]} reviews")

        df_join = df.join(sa_results)
        df_join["strongest_sentiment"] = df_join.apply(
            self._select_strongest_sentiment,
            axis=1,
        )
        df_join.rename(columns={
            "neg": "negative_score",
            "neu": "neutral_score",
            "pos": "positive_score",
            "compound": "sentiment_polarity",
            "id": "review_id",
            },
            inplace=True
        )

        df_join = df_join[[
            "review_id",
            "negative_score",
            "neutral_score",
            "positive_score",
            "sentiment_polarity",
            "strongest_sentiment",
        ]]

        self.helper.create_schema_if_not_exists("analysis")
        self.helper.upload_table(
            df=df_join,
            schema=self.analysis_schema,
            table_name=self.analysis_table,
            method=self.method)

    def _load_unanalyzed_reviews(self) -> pd.DataFrame:
        query = self._get_unanalyzed_reviews_query()
        return pd.read_sql(query, con=self.helper.engine)

    def _get_unanalyzed_reviews_query(self):
        if self._sentiment_results_exists():
            print("Detected existing sentiment results. Following incremental approach")
            return self._unanalysed_reviews_query_incremental()
        else:
            print("No existing sentiment results. Recreating table")
            self.method = "replace"
            return self._all_reviews_query()

    def _sentiment_results_exists(self):
        return self.helper.table_exists(self.analysis_schema, self.analysis_table)

    def _unanalysed_reviews_query_incremental(self):
        return """select place_id, review_id, review_text
                from raw.customer_reviews_google reviews
                where not exists (
                    select 1 from analysis.reviews_sentiment_analysis sa
                    where reviews.id = sa.review_id
                )"""

    def _all_reviews_query(self):
        return 'select place_id, review_id, review_text from raw.customer_reviews_google'

    def _analyze_text(self, text: str) -> dict:
        if not text:
            return ""
        result = self.model.polarity_scores(text)
        return result

    def _select_strongest_sentiment(self, row):
        if row["compound"] >= 0:
            return "positive"
        if row["compound"] < 0:  # Don't use else as we have NaN values
            return "negative"