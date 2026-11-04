import pytest
from pydantic import ValidationError

from chronos.api import (
    Deployment,
    Node,
    NodeCondition,
    ObjectMeta,
    PodTemplate,
    ResourceRequirements,
)


def test_requirements_parse_quantities_by_name() -> None:
    req = ResourceRequirements.model_validate(
        {"requests": {"cpu": "250m", "memory": "64Mi", "chronos.dev/slot": 1}}
    )
    assert req.requests == {"cpu": 250, "memory": 64 * 2**20, "chronos.dev/slot": 1}


def test_extended_limits_must_equal_requests() -> None:
    with pytest.raises(ValidationError, match="requests == limits"):
        ResourceRequirements.model_validate(
            {"requests": {"chronos.dev/slot": 1}, "limits": {"chronos.dev/slot": 2}}
        )


def test_extended_limit_without_request_is_rejected() -> None:
    with pytest.raises(ValidationError, match="requests == limits"):
        ResourceRequirements.model_validate({"limits": {"chronos.dev/slot": 1}})


def test_extended_limit_equal_to_request_is_accepted() -> None:
    req = ResourceRequirements.model_validate(
        {"requests": {"chronos.dev/slot": 2}, "limits": {"chronos.dev/slot": "2"}}
    )
    assert req.limits == {"chronos.dev/slot": 2}


def test_memory_limit_below_request_is_rejected() -> None:
    with pytest.raises(ValidationError, match="below request"):
        ResourceRequirements.model_validate(
            {"requests": {"memory": "128Mi"}, "limits": {"memory": "64Mi"}}
        )


def test_fractional_extended_request_is_rejected() -> None:
    with pytest.raises(ValidationError, match="whole number"):
        ResourceRequirements.model_validate({"requests": {"chronos.dev/slot": "0.5"}})


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        PodTemplate.model_validate({"command": ["true"], "comand": ["typo"]})
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Deployment.model_validate(
            {
                "metadata": {"name": "web"},
                "spec": {"replica": 3, "replicas": 3, "template": {"command": ["true"]}},
            }
        )


def test_deployment_rejects_negative_replicas_and_empty_command() -> None:
    with pytest.raises(ValidationError):
        Deployment.model_validate(
            {"metadata": {"name": "web"}, "spec": {"replicas": -1, "template": {"command": ["x"]}}}
        )
    with pytest.raises(ValidationError):
        PodTemplate.model_validate({"command": []})


@pytest.mark.parametrize("name", ["web", "web-1", "a"])
def test_object_names_accept_dns_labels(name: str) -> None:
    assert ObjectMeta(name=name).name == name


@pytest.mark.parametrize("name", ["", "Web", "-web", "web-", "web_1", "x" * 64])
def test_object_names_reject_non_dns_labels(name: str) -> None:
    with pytest.raises(ValidationError):
        ObjectMeta(name=name)


def test_node_capacity_is_a_generic_resource_map() -> None:
    node = Node.model_validate(
        {
            "metadata": {"name": "slot-a"},
            "spec": {"capacity": {"cpu": "4", "memory": "8Gi", "chronos.dev/slot": 1}},
        }
    )
    assert node.spec.capacity == {"cpu": 4000, "memory": 8 * 2**30, "chronos.dev/slot": 1}
    assert node.status.condition is NodeCondition.UNKNOWN


def test_models_round_trip_through_json() -> None:
    node = Node.model_validate(
        {"metadata": {"name": "n1"}, "spec": {"capacity": {"cpu": "2", "chronos.dev/slot": 3}}}
    )
    assert Node.model_validate_json(node.model_dump_json()) == node
    assert Node.model_validate(node.model_dump()) == node
    # Quantities serialize as canonical strings, so cpu can't be misread as cores.
    assert node.model_dump()["spec"]["capacity"] == {"chronos.dev/slot": "3", "cpu": "2"}
