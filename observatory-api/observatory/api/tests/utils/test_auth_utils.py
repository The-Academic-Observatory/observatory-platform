# Copyright 2022 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Aniek Roelofs

import unittest
from unittest.mock import MagicMock, patch

from connexion import App
from flask import session
from jose import jwt
from observatory.api.utils.auth_utils import get_token_auth, has_scope, requires_auth, set_auth_session
from observatory.api.utils.exception_utils import AuthError


class TestAuthUtils(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super().__init__(*args, **kwargs)

        self.app = App(__name__).app
        self.app.config["SECRET_KEY"] = "sekrit!"

    @patch("observatory.api.utils.auth_utils.redirect")
    def test_requires_auth(self, mock_redirect):
        @requires_auth
        def my_function() -> bool:
            return True

        with self.app.test_request_context():
            with self.app.test_client() as c:
                with c.session_transaction() as sess:
                    # Test with empty unauthorized session, should redirect to root
                    with patch.dict("observatory.api.utils.auth_utils.session", sess):
                        result = my_function()

                    mock_redirect.assert_called_once_with("/")
                    self.assertEqual(mock_redirect(), result)

                    # Test with authorized session, should return result of 'my_function'
                    mock_redirect.reset_mock()
                    sess["jwt_payload"] = {"access_token": "access_token", "scope": "scope"}
                    with patch.dict("observatory.api.utils.auth_utils.session", sess):
                        result = my_function()

                    mock_redirect.assert_not_called()
                    self.assertEqual(True, result)

    def test_set_auth_session(self):
        auth0 = MagicMock()
        auth0.authorize_access_token.return_value = {"access_token": "token", "scope": "scope"}

        with self.app.test_request_context():
            set_auth_session(auth0)

            self.assertDictEqual(auth0.authorize_access_token.return_value, session.get("jwt_payload"))

    def test_get_token_auth(self):
        # Test token in valid header
        with self.app.test_request_context("?access_token=from_args", headers={"Authorization": "Bearer from_header"}):
            token = get_token_auth()
        self.assertEqual("from_header", token)

        # Test invalid header that does not start with 'Bearer'
        with self.app.test_request_context(headers={"Authorization": "NotBearer from_header"}):
            with self.assertRaises(AuthError):
                get_token_auth()

        # Test invalid header in incorrect format
        with self.app.test_request_context(headers={"Authorization": "Bearer from_header something"}):
            with self.assertRaises(AuthError):
                get_token_auth()

        # Test invalid header without token
        with self.app.test_request_context(headers={"Authorization": "Bearer"}):
            with self.assertRaises(AuthError):
                get_token_auth()

        # Tests for token in args only
        with self.app.test_request_context("?access_token=from_args"):
            token = get_token_auth()
        self.assertEqual("from_args", token)

        # Test without any token
        with self.app.test_request_context():
            with self.assertRaises(AuthError):
                get_token_auth()

    @patch("observatory.api.utils.auth_utils.get_token_auth")
    def test_has_scope(self, mock_token):
        claims = {"scope": "scope1 scope2"}
        no_claims = {}

        token = jwt.encode(claims, "secret")
        token_no_claims = jwt.encode(no_claims, "secret")

        mock_token.side_effect = [token, token, token_no_claims]

        # Test with scope that is not included in token
        result = has_scope("scope3")
        self.assertFalse(result)

        # Test with scope that is included in token
        result = has_scope("scope1")
        self.assertTrue(result)

        # Test when the claims do not contain any scopes
        result = has_scope("scope")
        self.assertFalse(result)
