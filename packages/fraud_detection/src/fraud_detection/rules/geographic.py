"""
Geographic anomaly detection for insurance fraud analysis.

This module identifies suspicious geographic patterns in claims data that may
indicate fraudulent activity. Geographic analysis is particularly effective at
detecting:

- **Identity theft**: Claims submitted using stolen patient information, where
  the victim lives far from the billing provider.
- **Phantom billing**: Services billed for patients who could not have reasonably
  traveled to the provider location.
- **Fraud rings**: Organized schemes where patients are recruited from specific
  geographic areas to submit false claims.
- **Impossible travel**: Patients appearing at multiple distant locations on the
  same day, indicating identity misuse.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from fraud_detection.detector import DetectionConfig


class GeographicRules:
    """
    Detect geographic anomalies indicative of insurance fraud.

    Analyzes spatial relationships between patients and providers to identify
    claims that are geographically implausible. Supports both precise coordinate-based
    distance calculations and state-level heuristics when coordinates are unavailable.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for distributed processing.
    config : DetectionConfig
        Configuration object containing:

        - ``max_provider_patient_distance_miles``: Maximum reasonable distance
          between patient and provider before flagging.

    Examples
    --------
    >>> geo_rules = GeographicRules(spark, config)
    >>> claims = geo_rules.check_state_mismatch(claims)
    >>> claims = geo_rules.check_provider_patient_distance(claims)
    >>> out_of_area = claims.filter(claims.distance_exceeded)
    """

    def __init__(self, spark: SparkSession, config: DetectionConfig) -> None:
        self.spark = spark
        self.config = config

    def check_provider_patient_distance(self, claims: DataFrame) -> DataFrame:
        """
        Flag claims where patient and provider locations are unusually distant.

        Patients typically receive care from providers within a reasonable travel
        distance. Claims involving distant providers may indicate identity theft
        (someone using a stolen identity far from the victim's home) or phantom
        billing (billing for services never rendered).

        If latitude/longitude coordinates are available, calculates precise
        haversine (great-circle) distance. Otherwise, falls back to state-level
        mismatch detection.

        Parameters
        ----------
        claims : DataFrame
            Input claims. For distance calculation, requires columns:
            ``patient_lat``, ``patient_lon``, ``provider_lat``, ``provider_lon``.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``distance_miles`` : float - Calculated distance (if coordinates available).
            - ``distance_exceeded`` : bool - True if distance exceeds configured maximum.
        """
        has_coordinates = all(col in claims.columns for col in ["patient_lat", "patient_lon", "provider_lat", "provider_lon"])

        if has_coordinates:
            claims = claims.withColumn(
                "distance_miles",
                self._haversine_distance(
                    F.col("patient_lat"),
                    F.col("patient_lon"),
                    F.col("provider_lat"),
                    F.col("provider_lon"),
                ),
            )

            claims = claims.withColumn(
                "distance_exceeded",
                F.col("distance_miles") > F.lit(self.config.max_provider_patient_distance_miles),
            )
        else:
            claims = claims.withColumn("distance_exceeded", F.lit(False))

        return claims

    def _haversine_distance(self, lat1: F.Column, lon1: F.Column, lat2: F.Column, lon2: F.Column) -> F.Column:
        """
        Calculate great-circle distance between two points using the haversine formula.

        The haversine formula determines the shortest distance over the earth's
        surface between two points specified by latitude and longitude, accounting
        for the earth's spherical shape.

        Parameters
        ----------
        lat1 : Column
            Latitude of first point in degrees.
        lon1 : Column
            Longitude of first point in degrees.
        lat2 : Column
            Latitude of second point in degrees.
        lon2 : Column
            Longitude of second point in degrees.

        Returns
        -------
        Column
            Distance in miles between the two points.

        Notes
        -----
        Uses Earth's mean radius of 3,959 miles. Accuracy is typically within
        0.5% for most practical distances.
        """
        earth_radius_miles = 3959.0

        lat1_rad = F.radians(lat1)
        lat2_rad = F.radians(lat2)
        delta_lat = F.radians(lat2 - lat1)
        delta_lon = F.radians(lon2 - lon1)

        a = F.sin(delta_lat / 2) ** 2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(delta_lon / 2) ** 2
        c = 2 * F.asin(F.sqrt(a))

        return earth_radius_miles * c

    def check_state_mismatch(self, claims: DataFrame) -> DataFrame:
        """
        Flag claims where patient and provider are in different states.

        While cross-state healthcare is legitimate in border areas or for
        specialized care, out-of-state claims are statistically more likely
        to be fraudulent and warrant additional scrutiny, especially when
        combined with other risk indicators.

        Parameters
        ----------
        claims : DataFrame
            Input claims with ``patient_state`` and ``provider_state`` columns.

        Returns
        -------
        DataFrame
            Claims with added column:

            - ``state_mismatch`` : bool - True if patient and provider states differ.
        """
        if "patient_state" not in claims.columns or "provider_state" not in claims.columns:
            claims = claims.withColumn("state_mismatch", F.lit(False))
            return claims

        claims = claims.withColumn(
            "state_mismatch",
            (F.col("patient_state") != F.col("provider_state")) & F.col("patient_state").isNotNull() & F.col("provider_state").isNotNull(),
        )

        return claims

    def check_geographic_clustering(self, claims: DataFrame) -> DataFrame:
        """
        Detect suspicious geographic concentration of a provider's patients.

        Legitimate providers typically serve patients from a natural geographic
        distribution reflecting their location and specialty. A provider with
        a high volume of patients concentrated in a single distant state may
        indicate an organized fraud ring recruiting patients from a specific area.

        Flags providers with 100+ patients where all patients come from a single
        state different from the provider's state.

        Parameters
        ----------
        claims : DataFrame
            Input claims with ``provider_id``, ``patient_id``, ``patient_state``,
            and ``provider_state`` columns.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``unique_patient_states`` : int - Number of distinct states patients come from.
            - ``total_patients`` : int - Total patient count for this provider.
            - ``geographic_clustering_flag`` : bool - True if suspicious clustering detected.
        """
        window = Window.partitionBy("provider_id")

        claims = claims.withColumn(
            "unique_patient_states",
            F.size(F.collect_set("patient_state").over(window)),
        )

        claims = claims.withColumn(
            "total_patients",
            F.count("patient_id").over(window),
        )

        claims = claims.withColumn(
            "geographic_clustering_flag",
            (F.col("total_patients") > 100) & (F.col("unique_patient_states") == 1) & (F.col("patient_state") != F.col("provider_state")),
        )

        return claims

    def check_impossible_travel(self, claims: DataFrame) -> DataFrame:
        """
        Detect physically impossible travel patterns for patients.

        Identifies patients who have claims from multiple distant provider
        locations on the same day that would require impossible travel speeds.
        This is a strong indicator of identity theft (multiple people using
        the same patient identity) or systematic billing fraud.

        Parameters
        ----------
        claims : DataFrame
            Input claims with ``patient_id``, ``service_date``, and optionally
            ``provider_lat``, ``provider_lon`` for precise detection.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``provider_locations`` : array<struct> - All provider coordinates visited same day.
            - ``num_providers_same_day`` : int - Count of distinct provider locations.
            - ``impossible_travel_flag`` : bool - True if patient visited >3 providers same day.

        Notes
        -----
        Current implementation uses a simple threshold of >3 provider locations
        per day as a heuristic. A more sophisticated approach would calculate
        pairwise distances and required travel speeds, but this requires a UDF
        for efficient computation.
        """
        required_cols = ["patient_lat", "patient_lon", "provider_lat", "provider_lon"]
        if not all(col in claims.columns for col in required_cols):
            claims = claims.withColumn("impossible_travel_flag", F.lit(False))
            return claims

        window = Window.partitionBy("patient_id", "service_date")

        claims = claims.withColumn(
            "provider_locations",
            F.collect_list(F.struct(F.col("provider_lat").alias("lat"), F.col("provider_lon").alias("lon"))).over(window),
        )

        claims = claims.withColumn(
            "num_providers_same_day",
            F.size("provider_locations"),
        )

        claims = claims.withColumn(
            "impossible_travel_flag",
            F.col("num_providers_same_day") > 3,
        )

        return claims
