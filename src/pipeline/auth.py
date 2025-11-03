from abc import ABC, abstractmethod


class AuthProvider(ABC):
    """
    인증 제공자 추상 클래스.
    유효한 토큰을 반환해야 합니다.
    """

    @abstractmethod
    async def get_valid_token(self) -> str:
        """
        유효한 액세스 토큰을 비동기적으로 반환합니다.
        """
        raise NotImplementedError


class StaticAuthProvider(AuthProvider):
    """
    고정된 토큰을 반환하는 인증 제공자 구현체.
    """

    def __init__(self, token: str) -> None:
        self._token = token
        self.client_id = "static-client-id"

    async def get_valid_token(self) -> str:
        return self._token
