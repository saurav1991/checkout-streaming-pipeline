import os
import time
from collections import defaultdict


def flush_buffer(buffer: dict[str, list[str]], base_path: str) -> int:
    """Write buffered lines to JSONL files. Returns number of lines flushed."""
    total = 0
    for file_key, lines in buffer.items():
        dir_path = os.path.join(base_path, os.path.dirname(file_key))
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(base_path, f"{file_key}.jsonl")
        with open(file_path, "a") as f:
            for line in lines:
                f.write(line + "\n")
        total += len(lines)
    return total


class BatchBuffer:
    """Buffers JSONL lines by file key and tracks when they are due to be written.

    A flush is due once the batch size is reached, or once the flush interval
    has elapsed with anything at all buffered.
    """

    def __init__(self, base_path: str, batch_size: int, interval_secs: float):
        self.base_path = base_path
        self.batch_size = batch_size
        self.interval_secs = interval_secs
        self._lines: dict[str, list[str]] = defaultdict(list)
        self._size = 0
        self._last_flush = time.monotonic()

    def __len__(self) -> int:
        return self._size

    def add(self, file_key: str, line: str) -> None:
        self._lines[file_key].append(line)
        self._size += 1

    def is_flush_due(self) -> bool:
        if self._size >= self.batch_size:
            return True
        elapsed = time.monotonic() - self._last_flush
        return self._size > 0 and elapsed >= self.interval_secs

    def flush(self) -> int:
        """Write buffered lines out, reset the buffer, return lines written."""
        flushed = flush_buffer(self._lines, self.base_path)
        self._lines.clear()
        self._size = 0
        self._last_flush = time.monotonic()
        return flushed
