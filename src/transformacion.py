"""Transformaciones de datos para listings, reviews y calendar."""
from __future__ import annotations

from collections import Counter
from typing import Optional, Tuple

import ast
import pandas as pd
import nltk
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
from nltk.sentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

from logs_manage import Logs


class Transformacion:
    """Encapsula el proceso de limpieza y enriquecimiento de los datos."""

    def __init__(
        self,
        df_listings: pd.DataFrame,
        df_reviews_completo: pd.DataFrame,
        df_calendar: pd.DataFrame,
        muestra_reviews: int = 15000,
        logger: Optional[Logs] = None,
    ) -> None:
        self.df_listings = df_listings
        self.df_reviews_completo = df_reviews_completo
        self.df_calendar = df_calendar
        self.muestra_reviews = muestra_reviews
        self.logger = (
            logger.crear_sublogger(header="LOG DE TRANSFORMACIÓN - AIRBNB CDMX")
            if logger
            else Logs(header="LOG DE TRANSFORMACIÓN - AIRBNB CDMX")
        )

        self.df_limpio: Optional[pd.DataFrame] = None
        self.df_reviews_analizado: Optional[pd.DataFrame] = None
        self.df_calendar_limpio: Optional[pd.DataFrame] = None
        self.df_final: Optional[pd.DataFrame] = None

        DetectorFactory.seed = 0
        try:
            nltk.data.find("sentiment/vader_lexicon.zip")
        except LookupError:
            nltk.download("vader_lexicon")
        self._vader = SentimentIntensityAnalyzer()
        self.logger.info("Clase Transformacion inicializada")
        self.logger.info(f"Dimensiones iniciales - listings: {df_listings.shape}, reviews: {df_reviews_completo.shape}, calendar: {df_calendar.shape}")

    # ------------------------------------------------------------------
    def _parse_amenities(self, value) -> list:
        if isinstance(value, list):
            return [str(item).strip().lower() for item in value if str(item).strip()]
        if isinstance(value, str):
            try:
                parsed = ast.literal_eval(value)
                if isinstance(parsed, list):
                    return [str(item).strip().lower() for item in parsed if str(item).strip()]
            except (ValueError, SyntaxError):
                pass
            return [str(item).strip().lower() for item in value.split(",") if str(item).strip()]
        return []

    # ------------------------------------------------------------------
    def _clean_listings(self) -> None:
        self.logger.nueva_entrada("Transformación de listings")
        df = self.df_listings.copy()

        # Normalizar porcentajes del host
        for col in ["host_response_rate", "host_acceptance_rate"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace("%", "", regex=False)
                    .str.strip()
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Mapear velocidad de respuesta
        response_time_map = {
            "within an hour": "Fast",
            "within a few hours": "Fast",
            "within a day": "Moderate",
            "a few days or more": "Slow",
        }
        if "host_response_time" in df.columns:
            df["host_response_speed"] = df["host_response_time"].map(response_time_map)
            df["host_response_speed"].fillna("Unknown", inplace=True)

        # Columnas host_verifications → one hot
        if "host_verifications" in df.columns:
            df["host_verifications"] = df["host_verifications"].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) else x
            )
            df["host_verifications"] = df["host_verifications"].apply(
                lambda x: x if isinstance(x, list) else []
            )
            df_verifications = (
                df["host_verifications"].apply(lambda x: pd.Series(1, index=x))
                .fillna(0)
                .astype(int)
            )
            df_verifications.columns = [f"verif_{col}" for col in df_verifications.columns]
            df = pd.concat([df, df_verifications], axis=1)
            df.drop(columns=["host_verifications"], inplace=True)

        # Amenities top 12 + conteo
        if "amenities" in df.columns:
            df["amenities_list"] = df["amenities"].apply(self._parse_amenities)
            all_amenities = df["amenities_list"].explode()
            top_12 = [amen for amen, _ in Counter(all_amenities).most_common(12)]
            unique_amenities = all_amenities.dropna().unique() if not all_amenities.empty else []
            self.logger.info(f"Amenidades únicas analizadas: {len(unique_amenities)}")
            for amen in top_12:
                safe_name = (
                    amen.replace(" ", "_")
                    .replace("/", "_")
                    .replace("-", "_")
                    .replace(".", "_")
                )
                df[f"amen_{safe_name}"] = df["amenities_list"].apply(lambda x: int(amen in x))
            df["amenities_count"] = df["amenities_list"].apply(len)
            df.drop(columns=["amenities", "amenities_list"], inplace=True)

        # Limpieza de precio
        if "price" in df.columns:
            df["price"] = (
                df["price"]
                .astype(str)
                .str.replace(r"[\$,]", "", regex=True)
                .str.strip()
            )
            df["price"] = pd.to_numeric(df["price"], errors="coerce")
            df.dropna(subset=["price"], inplace=True)

        # Imputaciones
        for col in ["bedrooms", "beds", "bathrooms"]:
            if col in df.columns and df[col].isnull().any():
                df[col].fillna(df[col].median(), inplace=True)

        score_cols = [
            "review_scores_rating",
            "review_scores_accuracy",
            "review_scores_cleanliness",
            "review_scores_checkin",
            "review_scores_communication",
            "review_scores_location",
            "review_scores_value",
            "reviews_per_month",
        ]
        for col in score_cols:
            if col in df.columns:
                df[col].fillna(0, inplace=True)

        if "has_availability" in df.columns:
            df["has_availability"].fillna(df["has_availability"].mode()[0], inplace=True)
            if df["has_availability"].dtype == "object":
                df["has_availability"] = df["has_availability"].apply(lambda x: 1 if x == "t" else 0)
            elif pd.api.types.is_bool_dtype(df["has_availability"]):
                df["has_availability"] = df["has_availability"].astype(int)

        # Limpieza de neighbourhood
        if "neighbourhood" in df.columns:
            df["neighbourhood_cleaned"] = (
                df["neighbourhood"]
                .astype(str)
                .str.split(",")
                .str.get(0)
                .str.normalize("NFKD")
                .str.encode("ascii", "ignore")
                .str.decode("ascii")
                .str.lower()
                .str.strip()
            )

        # Outliers (IQR)
        for col in ["bathrooms", "bedrooms", "beds", "price"]:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                df = df[(df[col] >= lower) & (df[col] <= upper)]

        # Outliers manuales
        limites = {
            "bathrooms": 10,
            "bedrooms": 10,
            "beds": 15,
            "price": 400000,
        }
        for col, limite in limites.items():
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                df = df[df[col] <= limite]

        # Columnas con muchos nulos u obsoletas
        cols_drop = [
            "description",
            "host_name",
            "host_since",
            "host_location",
            "host_response_time",
            "host_response_rate",
            "host_acceptance_rate",
            "host_is_superhost",
            "host_thumbnail_url",
            "host_picture_url",
            "host_listings_count",
            "host_total_listings_count",
            "host_has_profile_pic",
            "host_identity_verified",
            "first_review",
            "last_review",
            "bathrooms_text",
            "minimum_minimum_nights",
            "maximum_minimum_nights",
            "minimum_maximum_nights",
            "maximum_maximum_nights",
        ]
        df.drop(columns=[c for c in cols_drop if c in df.columns], inplace=True)

        self.df_limpio = df.reset_index(drop=True)
        self.logger.info(f"DataFrame listings limpio con forma {self.df_limpio.shape}")

    # ------------------------------------------------------------------
    def _sentiment_score(self, comentario: str) -> Tuple[str, float]:
        if pd.isna(comentario) or not str(comentario).strip():
            return "Neutral", 0.0
        comentario_str = str(comentario).strip()
        try:
            idioma = detect(comentario_str)
        except LangDetectException:
            idioma = "unk"
        try:
            if idioma == "es":
                score = TextBlob(comentario_str).sentiment.polarity
            else:
                score = self._vader.polarity_scores(comentario_str)["compound"]
        except Exception:
            score = 0.0
        if score >= 0.05:
            return "Positivo", score
        if score <= -0.05:
            return "Negativo", score
        return "Neutral", score

    # ------------------------------------------------------------------
    def _clean_reviews(self) -> None:
        self.logger.nueva_entrada("Transformación de reviews")
        df = self.df_reviews_completo.head(self.muestra_reviews).copy()
        if "comments" in df.columns:
            df.dropna(subset=["comments"], inplace=True)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        resultados = df["comments"].apply(lambda x: self._sentiment_score(x))
        df["sentimiento"], df["puntuacion_sentimiento"] = zip(*resultados)
        self.df_reviews_analizado = df.reset_index(drop=True)
        self.logger.info(
            f"Reviews analizados: {self.df_reviews_analizado.shape[0]} filas con columnas {list(self.df_reviews_analizado.columns)}"
        )

    # ------------------------------------------------------------------
    def _clean_calendar(self) -> None:
        self.logger.nueva_entrada("Transformación de calendar")
        df = self.df_calendar.copy()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["year"] = df["date"].dt.year
            df["month"] = df["date"].dt.month
            df["day"] = df["date"].dt.day
        if "available" in df.columns:
            if df["available"].dtype == bool:
                df["available"] = df["available"].astype(int)
            elif set(df["available"].dropna().unique()).issubset({"t", "f"}):
                df["available"] = df["available"].apply(lambda x: 1 if x == "t" else 0)
        df.drop(columns=["minimum_nights", "maximum_nights"], inplace=True, errors="ignore")
        self.df_calendar_limpio = df.reset_index(drop=True)
        self.logger.info(f"Calendar limpio con forma {self.df_calendar_limpio.shape}")

    # ------------------------------------------------------------------
    def _aggregate_and_merge(self) -> None:
        if self.df_limpio is None or self.df_reviews_analizado is None or self.df_calendar_limpio is None:
            raise ValueError("Ejecute los pasos de limpieza antes de agregar y unir.")

        self.logger.nueva_entrada("Agregación y merge final")
        sentiment_agg = (
            self.df_reviews_analizado.groupby("listing_id")["puntuacion_sentimiento"]
            .agg(sentimiento_promedio="mean", numero_de_reviews_sentimiento="count")
            .reset_index()
        )
        sentiment_agg["sentimiento_promedio"] = sentiment_agg["sentimiento_promedio"].round(4)
        sentiment_agg.rename(columns={"listing_id": "id"}, inplace=True)
        self.logger.info(f"Agregación de reviews con forma {sentiment_agg.shape}")

        calendar_agg = (
            self.df_calendar_limpio.groupby("listing_id").agg(
                tasa_disponibilidad_anual=("available", "mean"),
                dias_disponibles_anual=("available", "sum"),
            )
            .reset_index()
        )
        calendar_agg["tasa_disponibilidad_anual"] = (
            calendar_agg["tasa_disponibilidad_anual"] * 100
        ).round(2)
        calendar_agg.rename(columns={"listing_id": "id"}, inplace=True)
        self.logger.info(f"Agregación de calendar con forma {calendar_agg.shape}")

        df_final = self.df_limpio.merge(sentiment_agg, on="id", how="left")
        df_final = df_final.merge(calendar_agg, on="id", how="left")

        for col in [
            "sentimiento_promedio",
            "numero_de_reviews_sentimiento",
            "tasa_disponibilidad_anual",
            "dias_disponibles_anual",
        ]:
            if col in df_final.columns:
                df_final[col].fillna(0, inplace=True)
        if "numero_de_reviews_sentimiento" in df_final.columns:
            df_final["numero_de_reviews_sentimiento"] = df_final[
                "numero_de_reviews_sentimiento"
            ].astype(int)
        if "dias_disponibles_anual" in df_final.columns:
            df_final["dias_disponibles_anual"] = df_final["dias_disponibles_anual"].astype(int)

        self.df_final = df_final
        self.logger.info(f"DataFrame final generado con forma {self.df_final.shape}")

    # ------------------------------------------------------------------
    def ejecutar_transformacion_completa(self) -> pd.DataFrame:
        """Ejecuta el pipeline completo y devuelve el DataFrame final."""
        self._clean_listings()
        self._clean_reviews()
        self._clean_calendar()
        self._aggregate_and_merge()
        if self.df_final is None:
            raise ValueError("No se generó el DataFrame final.")
        self.logger.info("Pipeline de transformación concluido correctamente")
        return self.df_final

    # ------------------------------------------------------------------
    def obtener_resultados(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Devuelve los DataFrames generados en el pipeline."""
        if any(
            obj is None
            for obj in (
                self.df_limpio,
                self.df_reviews_analizado,
                self.df_calendar_limpio,
                self.df_final,
            )
        ):
            raise ValueError("Ejecute 'ejecutar_transformacion_completa' antes de solicitar resultados.")
        return (
            self.df_limpio.copy(),
            self.df_reviews_analizado.copy(),
            self.df_calendar_limpio.copy(),
            self.df_final.copy(),
        )
