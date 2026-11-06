"""Deterministic simulation of a whole Chronos cluster (ADR-0016, TESTING.md section 6).

The real `Store` (with its real WAL and recovery), the real controllers and the real
`Agent` run under virtual time and one seeded RNG. Only the network, the processes,
the disk's sync behaviour and time itself are simulated.
"""
