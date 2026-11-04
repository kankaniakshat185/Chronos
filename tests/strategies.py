"""Hypothesis strategies shared by property tests."""

from hypothesis import strategies as st

_ALNUM = "abcdefghijklmnopqrstuvwxyz0123456789"


@st.composite
def _resource_name(draw: st.DrawFn) -> str:
    # Built from parts rather than from_regex: deterministic to replay and much faster.
    prefix = ""
    if draw(st.booleans()):
        prefix = draw(st.sampled_from(_ALNUM)) + draw(st.text(_ALNUM + ".-", max_size=10)) + "/"
    return prefix + draw(st.sampled_from(_ALNUM)) + draw(st.text(_ALNUM + "-", max_size=10))


extended_names = _resource_name().filter(lambda n: n not in ("cpu", "memory"))
resource_names = st.one_of(st.sampled_from(["cpu", "memory"]), extended_names)
