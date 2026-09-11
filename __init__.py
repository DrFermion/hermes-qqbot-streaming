"""Drop-in shim for ``~/.hermes/plugins/qqbot-streaming/`` installs.

Hermes loads a plugin directory as a package and calls its ``register(ctx)``, so a directory-based
install needs the entry point at the root. The implementation lives in the
:mod:`hermes_qqbot_streaming` package next to this file, which is also what the pip entry point
(``hermes_agent.plugins``) points at.
"""

from .hermes_qqbot_streaming import __version__, register  # noqa: F401

__all__ = ["register", "__version__"]
