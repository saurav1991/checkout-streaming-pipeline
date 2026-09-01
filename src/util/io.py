import os


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
