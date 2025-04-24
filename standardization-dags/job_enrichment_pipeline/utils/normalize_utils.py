import re


def normalize_title(title: str) -> str:
    title = re.sub(r"[\n\r\t]", " ", title)
    title = title.replace(",", "")
    title = re.sub(r"\s+", " ", title)
    return title.strip("\"'").strip().lower()
