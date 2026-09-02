import logging
import signal

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """Registers SIGTERM/SIGINT handlers and exposes a boolean running state."""

    def __init__(self, name: str):
        self._running = True
        self._name = name
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum, frame):
        logger.info("Shutting down %s...", self._name)
        self._running = False

    @property
    def running(self) -> bool:
        return self._running
