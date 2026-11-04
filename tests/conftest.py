"""Shared test configuration.

Hypothesis profiles (TESTING.md section 5): `dev` (default), `ci`, `nightly`,
selected with the HYPOTHESIS_PROFILE environment variable.
"""

import os

from hypothesis import settings

settings.register_profile("dev", max_examples=100)
settings.register_profile("ci", max_examples=1_000, deadline=None)
settings.register_profile("nightly", max_examples=20_000, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
