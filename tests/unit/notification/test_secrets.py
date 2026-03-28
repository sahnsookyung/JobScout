from __future__ import annotations

import os

import pytest
from cryptography.fernet import Fernet

from notification.exceptions import NotificationConfigurationError
from notification.secrets import FernetSecretEncryptionProvider


@pytest.fixture(autouse=True)
def restore_env():
    original = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(original)


class TestFernetSecretEncryptionProvider:
    def test_encrypt_and_decrypt_round_trip(self):
        os.environ["NOTIFICATION_SECRET_KEY"] = Fernet.generate_key().decode("utf-8")
        os.environ["NOTIFICATION_SECRET_KEY_VERSION"] = "v42"
        provider = FernetSecretEncryptionProvider()

        encrypted = provider.encrypt("super-secret")

        assert encrypted.key_version == "v42"
        assert provider.decrypt(encrypted.ciphertext, encrypted.key_version) == "super-secret"

    def test_missing_key_raises_configuration_error(self):
        provider = FernetSecretEncryptionProvider()

        with pytest.raises(NotificationConfigurationError, match="NOTIFICATION_SECRET_KEY"):
            provider.encrypt("super-secret")

    def test_invalid_key_raises_configuration_error(self):
        os.environ["NOTIFICATION_SECRET_KEY"] = "not-a-fernet-key"
        provider = FernetSecretEncryptionProvider()

        with pytest.raises(NotificationConfigurationError, match="valid Fernet key"):
            provider.encrypt("super-secret")

    def test_invalid_ciphertext_raises_configuration_error(self):
        os.environ["NOTIFICATION_SECRET_KEY"] = Fernet.generate_key().decode("utf-8")
        provider = FernetSecretEncryptionProvider()

        with pytest.raises(NotificationConfigurationError, match="could not be decrypted"):
            provider.decrypt("not-a-token", "v1")
