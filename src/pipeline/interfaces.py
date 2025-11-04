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


class Loader(ABC):
    """
    Loader 인터페이스.

    이 인터페이스는 비동기적으로 데이터를 로드하는 메서드를 정의합니다.
    """

    @abstractmethod
    async def load(self, data: list[dict[str, Any]], key: str) -> None:
        """
        데이터 배치를 'key'라는 이름으로 Data Lake에 적재합니다.

        Args:
            data (list[dict[str, Any]]): Extractor가 생성한 데이터 배치.
            key (str): S3 등 데이터가 적재될 위치를 나타내는 키.
        """
        raise NotImplementedError
        return None
