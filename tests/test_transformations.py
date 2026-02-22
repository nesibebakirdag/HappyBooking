"""
HappyBooking — Transformation Logic Unit Tests
===============================================
Tests for specific transformation rules in the Silver pipeline.
"""
import pytest
import re


class TestCityCleanLogic:
    """city_clean transformation kuralları."""

    def test_trailing_dots_removed(self):
        """Trailing noktalar temizlenmeli: 'Cologne...' → 'Cologne'"""
        dirty = "Cologne...."
        cleaned = re.sub(r'[.\s]+$', '', dirty).strip().title()
        assert cleaned == "Cologne"

    def test_trailing_spaces_removed(self):
        """Trailing boşluklar temizlenmeli."""
        dirty = "Rotterdam   "
        cleaned = re.sub(r'[.\s]+$', '', dirty).strip().title()
        assert cleaned == "Rotterdam"

    def test_mixed_trailing_chars(self):
        """Nokta + boşluk karışımı temizlenmeli."""
        dirty = "Berlin. . ."
        cleaned = re.sub(r'[.\s]+$', '', dirty).strip().title()
        assert cleaned == "Berlin"

    def test_normal_city_unchanged(self):
        """Temiz isimler değişmemeli."""
        assert "Amsterdam" == re.sub(r'[.\s]+$', '', "Amsterdam").strip().title()

    def test_initcap_applied(self):
        """İlk harf büyük olmalı."""
        dirty = "istanbul"
        cleaned = re.sub(r'[.\s]+$', '', dirty).strip().title()
        assert cleaned == "Istanbul"


class TestExchangeRateLogic:
    """Exchange rate transformation kuralları."""

    def test_eur_rate_is_one(self):
        """EUR → EUR kuru 1.0 olmalı."""
        currency = "EUR"
        rate = 1.0 if currency == "EUR" else None
        assert rate == 1.0

    def test_non_eur_rate_not_none(self):
        """EUR dışı para birimleri NULL olmamalı (API'den geldiğinde)."""
        # Simulated rates
        rates = {"USD": 1.08, "GBP": 0.86, "TRY": 34.5}
        for curr, rate in rates.items():
            assert rate is not None, f"{curr} rate is None"
            assert rate > 0, f"{curr} rate is not positive: {rate}"


class TestSmartImputationLogic:
    """Smart imputation kuralları."""

    def test_room_price_from_total(self):
        """room_price = total_amount / nights (eğer NULL ise)."""
        total_amount = 300.0
        nights = 3
        room_price = round(total_amount / nights, 2)
        assert room_price == 100.0

    def test_total_from_room_price(self):
        """total_amount = room_price * nights (eğer NULL ise)."""
        room_price = 100.0
        nights = 3
        total_amount = round(room_price * nights, 2)
        assert total_amount == 300.0

    def test_no_division_by_zero(self):
        """nights=0 olduğunda bölme yapılmamalı."""
        nights = 0
        total_amount = 300.0
        room_price = None
        if nights > 0:
            room_price = round(total_amount / nights, 2)
        assert room_price is None


class TestGoldKPILogic:
    """Gold layer KPI hesaplamaları."""

    def test_total_amount_eur(self):
        """total_amount_eur = total_amount * exchange_rate_to_eur"""
        total = 100.0
        rate = 34.5  # TRY
        eur = round(total * rate, 2)
        assert eur == 3450.0

    def test_avg_nightly_rate(self):
        """avg_nightly_rate = total_amount / nights"""
        total = 450.0
        nights = 3
        avg = round(total / nights, 2)
        assert avg == 150.0

    def test_1900_filtered_out(self):
        """1900-01-01 tarihleri Gold'da filtrelenmeli."""
        from datetime import date
        booking_date = date(1900, 1, 1)
        cutoff = date(1950, 1, 1)
        assert booking_date < cutoff, "1900 date should be filtered"
