"""Clients for the store: in-process and over HTTP, behind one protocol (ADR-0004)."""

from chronos.client.http import HttpStoreClient
from chronos.client.local import LocalStoreClient
from chronos.client.protocol import StoreClient

__all__ = ["HttpStoreClient", "LocalStoreClient", "StoreClient"]
