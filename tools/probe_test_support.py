# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Load a fresh script module for an offline harness test."""
import importlib.util
from pathlib import Path


def load_probe(filename, module_name=None):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name or path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
