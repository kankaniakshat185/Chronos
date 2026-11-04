"""X-4 WorkloadType registry and X-5 resource providers: registry rules and unit bookkeeping."""

import pytest
from pydantic import ValidationError

from chronos.agent.providers import CountingProvider, ProviderError, Providers
from chronos.api import ObjectMeta, Pod, PodSpec, PodTemplate
from chronos.api.workload_types import Generic, register_workload_type, workload_type


def test_default_type_is_generic_and_runs_the_command_as_given() -> None:
    pod = Pod(metadata=ObjectMeta(name="p"), spec=PodSpec(command=["echo", "hi"]))
    assert pod.spec.workload_type == "generic"
    assert workload_type("generic").prepare(pod) == (["echo", "hi"], {})


def test_unknown_type_is_rejected_when_the_template_is_parsed() -> None:
    with pytest.raises(ValidationError, match="unknown workload_type 'cuda_task'"):
        PodTemplate(command=["x"], workload_type="cuda_task")


def test_a_type_can_veto_a_template() -> None:
    class NeedsTwoArgs(Generic):
        def validate(self, template: PodTemplate) -> None:
            if len(template.command) < 2:
                raise ValueError("needs at least two arguments")

    register_workload_type("test.needs-two-args", NeedsTwoArgs())
    PodTemplate(command=["a", "b"], workload_type="test.needs-two-args")
    with pytest.raises(ValidationError, match="needs at least two arguments"):
        PodTemplate(command=["a"], workload_type="test.needs-two-args")


def test_duplicate_registration_is_an_error() -> None:
    with pytest.raises(ValueError, match="already registered"):
        register_workload_type("generic", Generic())


def test_counting_provider_hands_out_the_lowest_free_units() -> None:
    p = CountingProvider("slot", 3)
    assert p.allocate("a", 2) == ["0", "1"]
    assert p.allocate("b", 1) == ["2"]
    with pytest.raises(ProviderError, match="1 requested, 0 free"):
        p.allocate("c", 1)
    p.release("a")
    assert p.allocate("c", 1) == ["0"]


def test_a_pod_cant_allocate_twice_without_releasing() -> None:
    p = CountingProvider("slot", 3)
    p.allocate("a", 1)
    with pytest.raises(ProviderError, match="already holds"):
        p.allocate("a", 1)


def test_providers_allocate_all_or_nothing() -> None:
    providers = Providers({"cpu": 4000, "a.dev/x": 2, "b.dev/y": 1})
    with pytest.raises(ProviderError):
        providers.allocate("p1", {"a.dev/x": 1, "b.dev/y": 2})
    # a.dev/x's unit was handed back: both are free again
    assigned, _ = providers.allocate("p2", {"a.dev/x": 2})
    assert assigned == {"a.dev/x": ["0", "1"]}


def test_failed_allocation_doesnt_free_what_the_pod_already_held() -> None:
    providers = Providers({"a.dev/x": 2})
    providers.allocate("p1", {"a.dev/x": 1})
    with pytest.raises(ProviderError, match="already holds"):
        providers.allocate("p1", {"a.dev/x": 1})
    assigned, _ = providers.allocate("p2", {"a.dev/x": 1})
    assert assigned == {"a.dev/x": ["1"]}  # unit 0 is still p1's


def test_cpu_memory_and_zero_requests_get_no_units() -> None:
    providers = Providers({"a.dev/x": 1})
    assert providers.allocate("p", {"cpu": 500, "memory": 1, "a.dev/x": 0}) == ({}, {})


def test_resource_the_node_doesnt_advertise() -> None:
    with pytest.raises(ProviderError, match="not advertised"):
        Providers({}).allocate("p", {"a.dev/x": 1})
