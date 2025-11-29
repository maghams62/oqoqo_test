def authenticate(token: str) -> bool:
    return token.startswith("tok_")
