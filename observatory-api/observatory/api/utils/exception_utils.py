from typing import Dict


class APIError(Exception):
    def __init__(self, error: Dict[str, str], status_code: int):
        self.code = error.get("code")
        self.description = error.get("description")
        self.status_code = status_code


# Authentication error handler
class AuthError(APIError):
    def __init__(self, error: Dict[str, str], status_code: int = 403):
        super().__init__(error, status_code)
