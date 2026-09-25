"""The ASGI entry point: ``uvicorn --factory data_pipeline.api.main:app``."""

from __future__ import annotations

import logging

from fastapi import FastAPI

from data_pipeline.api.app import create_app
from data_pipeline.config import Settings


def app() -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    return create_app(Settings())
