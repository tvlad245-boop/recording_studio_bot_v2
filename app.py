"""
ASGI entrypoint for hosting platforms.

Some platforms (including "domain enabled" bot hosting) run their own uvicorn and expect
an `app` object in a module like `app.py`.

We expose the FastAPI application that handles both YooKassa and Yclients webhooks.
"""

from yookassa_webhook import app  # noqa: F401

