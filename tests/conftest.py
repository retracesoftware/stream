"""Shared test configuration for retracesoftware_stream.

Enables debug builds and assertions for all tests.
"""
import os

os.environ["RETRACE_DEBUG"] = "1"
