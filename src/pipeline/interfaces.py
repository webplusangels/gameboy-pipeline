from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Any


class Extractor(ABC):
    """
    Extractor 인터페이스.

    이 인터페이스는 비동기적으로 데이터를 추출하는 메서드를 정의합니다.
    """

    @abstractmethod
    async def extract(self) -> AsyncGenerator[dict[str, Any], None]:
        """
        외부 소스로부터 데이터를 추출합니다.

        Yields:
            dict[str, Any]: 추출된 데이터 항목.
        """
        raise NotImplementedError
        yield
