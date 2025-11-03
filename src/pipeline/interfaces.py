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


class AuthProvider(ABC):
    """
    AuthProvider 인터페이스.

    이 인터페이스는 비동기적으로 유효한 토큰을 반환하는 메서드를 정의합니다.
    """

    @abstractmethod
    async def get_valid_token(self) -> str:
        """
        유효한 액세스 토큰을 비동기적으로 반환합니다.

        Returns:
            str: 유효한 액세스 토큰.
        """
        raise NotImplementedError
        return ""
