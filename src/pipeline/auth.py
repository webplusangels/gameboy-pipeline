from src.pipeline.interfaces import AuthProvider


class StaticAuthProvider(AuthProvider):
    """
    고정된 토큰을 반환하는 AuthProvider 구현체.
    """

    def __init__(self, token: str) -> None:
        """
        Args:
            token: 반환할 고정된 토큰 값
        """
        self._token = token

    async def get_valid_token(self) -> str:
        """
        고정된 토큰을 반환합니다.

        Returns:
            str: 고정된 액세스 토큰.
        """
        return self._token
