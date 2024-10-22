import sys
import re

from typing import List
from dataclasses import dataclass
from pathlib import Path
from inspect import cleandoc

from exasol_advanced_analytics_framework.example import generator as example_generator


class ParseException(Exception):
    """
    If input file is not well-formed.
    """


@dataclass
class Template:
    path: str

    def render(self):
        if self.path != "example/generator.py":
            raise ParseException("document_updater.Template currently only"
                                 " supports path example/generator.py")
        return "\n".join([
            "<!-- The example is deliberately tagged as language python since the major",
            "     parts are in python. Formally, however, the python code is embedded into",
            "     an SQL statement, though. -->",
            "```python",
            example_generator.create_script(),
            "",
            example_generator.execute_script() + ";",
            "```",
            ""])


class ChunkReader:
    """
    Enables to replace chunks of a string by generated text, e.g. from
    jinja templates.
    """
    def __init__(self):
        self._generated = None
        self._plain = []
        self._chunks = []

    def _process_plain(self):
        if self._plain:
            self._chunks.append("\n".join(self._plain) + "\n")
        self._plain = []

    def _start_generated(self, line: str, match: re.Match):
        if self._generated:
            raise ParseException(
                f"Found another {line} before {self._generated} was closed."
            )
        self._plain += [
            line,
            "<!-- Do not edit the text from here until /generated! -->",
        ]
        self._process_plain()
        self._generated = line
        self._chunks.append(Template(match.group(2)))

    def _end_generated(self, line: str):
        if not self._generated:
            raise ParseException(
                f"Found {line} before any <!-- generated from/by ... -->."
            )
        self._generated = None
        self._plain.append(line)

    def split(self, content: str) -> List[str|Template]:
        start = re.compile("<!-- +generated +(from|by) +([^ ]+) +-->")
        end = re.compile("<!-- +/generated +-->")
        for line in content.splitlines():
            match = start.match(line)
            if match:
                self._start_generated(line, match)
            elif end.match(line):
                self._end_generated(line)
            elif not self._generated:
                self._plain.append(line)
        self._process_plain()
        return self._chunks

    @classmethod
    def chunks(cls, content: str):
        return cls().split(content)


def update_examples(path: Path):
    content = path.read_text()
    with path.open(mode="w") as f:
        for chunk in ChunkReader.chunks(content):
            f.write(chunk if type(chunk) == str else chunk.render())
