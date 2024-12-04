"""Connector utilities."""

from __future__ import annotations


class NotConnectedError(Exception):
    """Raised when trying to use client before connecting."""
