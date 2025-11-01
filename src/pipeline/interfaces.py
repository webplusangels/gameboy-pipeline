from collections.abc import AsyncGenerator
from typing import Any


class Extractor:
    """Extractor 인터페이스."""

    async def extract(self) -> AsyncGenerator[dict[str, Any], None]:
        yield {}
