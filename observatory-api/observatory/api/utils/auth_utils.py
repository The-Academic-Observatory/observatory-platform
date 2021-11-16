from flask import request, redirect, session
from observatory.api.utils.exception_utils import AuthError
from jose import jwt
from typing import Optional
from authlib.integrations.flask_client import OAuth
from functools import wraps


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "jwt_payload" not in session:
            # Redirect to Login page here
            return redirect("/")
        return f(*args, **kwargs)

    return decorated


def set_auth_session(auth0: OAuth):
    # Handles response from token endpoint
    token = auth0.authorize_access_token()
    # resp = auth0.get('userinfo')
    # userinfo = resp.json()
    # userinfo['id_token'] = token["id_token"]

    # Store the user information in flask session.
    session["jwt_payload"] = {"access_token": token["access_token"], "scope": token["scope"]}
    # session['profile'] = {
    #     'user_id': userinfo['sub'],
    #     'name': userinfo['name'],
    #     'picture': userinfo['picture'],
    # }


def get_token_auth() -> Optional[str]:
    """Obtains the Access Token from either the Authorization Header or the 'access_token' param in query.

    :return: The access token
    """
    auth_header = request.headers.get("Authorization", None)
    if auth_header:
        parts = auth_header.split()

        if parts[0].lower() != "bearer":
            raise AuthError({"code": "invalid_header", "description": "Authorization header must start with" " Bearer"})
        elif len(parts) == 1:
            raise AuthError({"code": "invalid_header", "description": "Token not found"})
        elif len(parts) > 2:
            raise AuthError({"code": "invalid_header", "description": "Authorization header must be" " Bearer token"})

        token = parts[1]
    else:
        token = request.args.get("access_token")

    if not token:
        raise AuthError(
            {
                "code": "authorization_missing",
                "description": "Access token in authorization header or in query is expected",
            }
        )
    return token


def has_scope(required_scope: str) -> bool:
    """Determines if the required scope is present in the Access Token

    :param required_scope: The scope required to access the resource
    :return: Whether the required scope is in the Access Token
    """
    token = get_token_auth()
    unverified_claims = jwt.get_unverified_claims(token)
    if unverified_claims.get("scope"):
        token_scopes = unverified_claims["scope"].split()
        for token_scope in token_scopes:
            if token_scope == required_scope:
                return True
    return False
