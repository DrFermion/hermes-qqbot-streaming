"""Hermes plugin entry point: QQ Bot C2C native streaming.

``register(ctx)`` runs when Hermes loads the plugin. It registers a ``qqbot`` platform adapter that
subclasses the built-in one with :class:`~hermes_qqbot_streaming.streaming.QQStreamMixin`, and turns
on the platform's streaming display default. Hermes resolves the plugin registry **before** its
built-in adapters, so this entry replaces the built-in QQ adapter for every profile that has the
plugin enabled.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

__version__ = "1.0.2"

_PLUGIN_NAME = "qqbot-streaming"


def _install_streaming_display_default() -> None:
    """Make ``display.platforms.qqbot.streaming`` default to true for this process.

    The gateway only builds a stream consumer when that setting resolves truthy, and the core
    default table ships streaming for WeCom but not for QQ. A plugin may not edit core modules, so
    the default is installed at runtime; an explicit
    ``display.platforms.qqbot.streaming: false`` in ``config.yaml`` still wins (per-platform user
    config is read first).
    """
    try:
        from gateway import display_config
    except Exception as exc:  # pragma: no cover — Hermes always ships this module
        logger.warning("%s: could not import gateway.display_config (%s)", _PLUGIN_NAME, exc)
        return
    defaults = getattr(display_config, "_PLATFORM_DEFAULTS", None)
    if not isinstance(defaults, dict):
        logger.warning("%s: unexpected display_config shape — set display.platforms.qqbot.streaming "
                       "in config.yaml by hand", _PLUGIN_NAME)
        return
    entry = defaults.get("qqbot")
    if entry is None:
        defaults["qqbot"] = {"streaming": True}
        logger.info("%s: display default display.platforms.qqbot.streaming=true installed", _PLUGIN_NAME)
    elif isinstance(entry, dict) and entry.get("streaming") is None:
        entry["streaming"] = True
        logger.info("%s: display default display.platforms.qqbot.streaming=true installed", _PLUGIN_NAME)


def register(ctx) -> None:
    """Register the streaming QQ adapter (called once by the Hermes plugin loader)."""
    from gateway.platforms.qqbot import check_qq_requirements

    from .adapter import StreamingQQAdapter

    _install_streaming_display_default()
    ctx.register_platform(
        "qqbot",
        "QQ Bot (C2C streaming)",
        adapter_factory=StreamingQQAdapter,
        check_fn=check_qq_requirements,
        required_env=["QQ_APP_ID", "QQ_CLIENT_SECRET"],
        install_hint="set QQ_APP_ID / QQ_CLIENT_SECRET in ~/.hermes/.env (or run the QR onboarding)",
    )
    logger.info("%s: registered the streaming QQ adapter", _PLUGIN_NAME)
