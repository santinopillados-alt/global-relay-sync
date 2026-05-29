"""
Tests para el pipeline CDC de Global-Relay Sync.
"""
import pytest
from app.wal_cdc import parse_test_decoding
from app.cdc_consumer import detect_conflict, _build_alert_message


class TestWalCDCParser:

    def test_parse_insert(self):
        data = "table public.orders: INSERT: id[integer]:1 customer_id[character varying]:'cust_abc' product[character varying]:'iPhone' quantity[integer]:2 price[numeric]:999.99 status[character varying]:'pending' version[integer]:1"
        result = parse_test_decoding(data)
        assert result is not None
        assert result["op"] == "C"
        assert result["table"] == "orders"
        assert result["after"]["id"] == 1
        assert result["before"] is None

    def test_parse_update(self):
        data = "table public.orders: UPDATE: id[integer]:5 status[character varying]:'completed' version[integer]:2"
        result = parse_test_decoding(data)
        assert result is not None
        assert result["op"] == "U"
        assert result["after"]["id"] == 5

    def test_parse_delete(self):
        data = "table public.orders: DELETE: id[integer]:3"
        result = parse_test_decoding(data)
        assert result is not None
        assert result["op"] == "D"
        assert result["before"]["id"] == 3
        assert result["after"] is None

    def test_ignore_other_tables(self):
        data = "table public.users: INSERT: id[integer]:1 name[varchar]:'test'"
        result = parse_test_decoding(data)
        assert result is None

    def test_ignore_non_dml(self):
        data = "BEGIN 12345"
        result = parse_test_decoding(data)
        assert result is None

    def test_event_has_required_fields(self):
        data = "table public.orders: INSERT: id[integer]:10 version[integer]:1"
        result = parse_test_decoding(data)
        assert "op" in result
        assert "table" in result
        assert "timestamp" in result
        assert "source_db" in result

    def test_parse_numeric_values(self):
        data = "table public.orders: INSERT: id[integer]:42 price[numeric]:1299.99 quantity[integer]:3 version[integer]:1"
        result = parse_test_decoding(data)
        assert result["after"]["id"] == 42
        assert result["after"]["quantity"] == 3


class TestConflictDetection:

    def test_no_conflict_when_versions_match(self):
        before = {"id": 1, "version": 2}
        assert detect_conflict(before, target_version=2) is False

    def test_conflict_when_target_ahead(self):
        before = {"id": 1, "version": 2}
        assert detect_conflict(before, target_version=3) is True

    def test_no_conflict_when_before_is_none(self):
        assert detect_conflict(None, target_version=5) is False

    def test_no_conflict_when_target_version_none(self):
        before = {"id": 1, "version": 2}
        assert detect_conflict(before, target_version=None) is False

    def test_no_conflict_source_ahead(self):
        before = {"id": 1, "version": 5}
        assert detect_conflict(before, target_version=3) is False

    def test_conflict_critical_version_gap(self):
        before = {"id": 1, "version": 1}
        assert detect_conflict(before, target_version=10) is True