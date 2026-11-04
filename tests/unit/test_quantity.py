import pytest

from chronos.api.quantity import (
    QuantityError,
    format_cpu,
    format_memory,
    is_extended,
    parse_cpu,
    parse_extended,
    parse_memory,
    parse_quantity,
    validate_resource_name,
)


@pytest.mark.parametrize(
    ("raw", "millicores"),
    [("250m", 250), ("2", 2000), ("0.5", 500), ("1.5", 1500), (2, 2000), (0.25, 250), ("0", 0)],
)
def test_parse_cpu(raw: object, millicores: int) -> None:
    assert parse_cpu(raw) == millicores


@pytest.mark.parametrize("raw", ["1.5m", "0.0001", "-1", "2x", "", "abc", True, None, [1]])
def test_parse_cpu_rejects(raw: object) -> None:
    with pytest.raises(QuantityError):
        parse_cpu(raw)


@pytest.mark.parametrize(
    ("raw", "nbytes"),
    [
        ("64Mi", 64 * 2**20),
        ("1G", 10**9),
        ("1.5Gi", 3 * 2**29),
        ("512Ki", 512 * 1024),
        ("0.5Ki", 512),
        (1024, 1024),
        ("100", 100),
    ],
)
def test_parse_memory(raw: object, nbytes: int) -> None:
    assert parse_memory(raw) == nbytes


@pytest.mark.parametrize("raw", ["1.5", "64MB", "-1Mi", "Mi", "0.3Ki"])
def test_parse_memory_rejects(raw: object) -> None:
    with pytest.raises(QuantityError):
        parse_memory(raw)


def test_format_examples() -> None:
    assert format_cpu(2000) == "2"
    assert format_cpu(250) == "250m"
    assert format_memory(64 * 2**20) == "64Mi"
    assert format_memory(10**9) == "1G"
    assert format_memory(1000) == "1k"
    assert format_memory(1023) == "1023"
    assert format_memory(0) == "0"


@pytest.mark.parametrize("name", ["chronos.dev/slot", "example.com/widget", "slot", "gpu-like"])
@pytest.mark.parametrize(("raw", "units"), [("2", 2), (2, 2), ("0", 0)])
def test_extended_resources_accept_whole_units(name: str, raw: object, units: int) -> None:
    assert parse_quantity(name, raw) == units


@pytest.mark.parametrize("raw", ["0.5", "500m", "-1", 1.0, 0.5, "1k", "1Ki", True])
def test_extended_resources_reject_fractions_and_suffixes(raw: object) -> None:
    with pytest.raises(QuantityError):
        parse_extended("chronos.dev/slot", raw)


@pytest.mark.parametrize(
    "name", ["cpu", "memory", "slot", "chronos.dev/slot", "a.b-c/d-e", "x" * 63]
)
def test_valid_resource_names(name: str) -> None:
    assert validate_resource_name(name) == name


@pytest.mark.parametrize("name", ["", "Slot", "chronos.dev/", "/slot", "a/b/c", "under_score"])
def test_invalid_resource_names(name: str) -> None:
    with pytest.raises(QuantityError):
        validate_resource_name(name)


def test_only_cpu_and_memory_are_not_extended() -> None:
    assert not is_extended("cpu")
    assert not is_extended("memory")
    assert is_extended("chronos.dev/slot")
