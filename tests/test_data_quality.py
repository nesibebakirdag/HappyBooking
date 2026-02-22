"""
HappyBooking — Data Quality Unit Tests
=======================================
Pytest tests for Silver and Gold layer data quality.
Run: pytest tests/test_data_quality.py -v
"""
import pytest


class TestSilverBookingsQuality:
    """Silver Bookings tablosu kalite kontrolleri."""

    @pytest.fixture(autouse=True)
    def setup(self, spark_session):
        """Load silver_bookings once for all tests."""
        self.df = spark_session.read.table("silver_bookings")
        self.total = self.df.count()

    def test_table_not_empty(self):
        """Tablo boş olmamalı."""
        assert self.total > 0, "silver_bookings tablosu boş!"

    def test_minimum_row_count(self):
        """En az 100K satır olmalı."""
        assert self.total >= 100_000, f"Beklenen >=100K, bulunan: {self.total}"

    def test_booking_id_not_null(self):
        """booking_id NULL olmamalı."""
        null_count = self.df.filter("booking_id IS NULL").count()
        assert null_count == 0, f"{null_count} NULL booking_id bulundu"

    def test_booking_id_unique(self):
        """booking_id benzersiz olmalı."""
        distinct = self.df.select("booking_id").distinct().count()
        assert distinct == self.total, f"Duplicate booking: {self.total - distinct}"

    def test_total_amount_non_negative(self):
        """total_amount negatif olmamalı."""
        neg = self.df.filter("total_amount < 0").count()
        assert neg == 0, f"{neg} negatif total_amount bulundu"

    def test_currency_valid_values(self):
        """currency yalnızca beklenen değerleri içermeli."""
        from pyspark.sql.functions import col
        valid = ["EUR", "USD", "GBP", "JPY", "TRY", "AED", "CNY"]
        invalid = self.df.filter(
            ~col("currency").isin(valid) & col("currency").isNotNull()
        ).count()
        assert invalid == 0, f"{invalid} geçersiz currency değeri bulundu"

    def test_schema_has_required_columns(self):
        """Gerekli kolonlar mevcut olmalı."""
        required = ["booking_id", "hotel_id", "booking_date",
                     "total_amount", "currency", "city_clean"]
        missing = [c for c in required if c not in self.df.columns]
        assert len(missing) == 0, f"Eksik kolonlar: {missing}"


class TestBronzeWeatherQuality:
    """Bronze Weather tablosu kalite kontrolleri."""

    @pytest.fixture(autouse=True)
    def setup(self, spark_session):
        self.df = spark_session.read.table("bronze_weather")

    def test_table_not_empty(self):
        assert self.df.count() > 0, "bronze_weather boş!"

    def test_temperature_range(self):
        """Sıcaklık -60 ile +60 arasında olmalı."""
        out_of_range = self.df.filter(
            "(temperature_c < -60 OR temperature_c > 60) AND temperature_c IS NOT NULL"
        ).count()
        assert out_of_range == 0, f"{out_of_range} aşırı sıcaklık değeri"

    def test_city_not_null(self):
        null_count = self.df.filter("city IS NULL").count()
        assert null_count == 0, f"{null_count} NULL city"


class TestBronzeExchangeRatesQuality:
    """Bronze Exchange Rates kalite kontrolleri."""

    @pytest.fixture(autouse=True)
    def setup(self, spark_session):
        self.df = spark_session.read.table("bronze_exchange_rates")

    def test_table_not_empty(self):
        assert self.df.count() > 0, "bronze_exchange_rates boş!"

    def test_rate_positive(self):
        """Döviz kuru pozitif olmalı."""
        neg = self.df.filter("rate <= 0").count()
        assert neg == 0, f"{neg} negatif/sıfır rate"

    def test_expected_currencies_exist(self):
        """Beklenen para birimleri mevcut olmalı."""
        from pyspark.sql.functions import col
        currencies = [row.target_currency for row in
                      self.df.select("target_currency").distinct().collect()]
        expected = ["USD", "GBP", "TRY"]
        missing = [c for c in expected if c not in currencies]
        assert len(missing) == 0, f"Eksik para birimleri: {missing}"


# === Conftest fixture (spark_session) ===
# Bu testler Fabric notebook veya local Spark ortamında çalışır.
# Conftest'te spark_session fixture tanımlanmalı:
#
# @pytest.fixture(scope="session")
# def spark_session():
#     from pyspark.sql import SparkSession
#     return SparkSession.builder.getOrCreate()
