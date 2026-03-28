"""Encryption helpers for per-user notification secrets."""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass

from cryptography.fernet import Fernet, InvalidToken

from notification.exceptions import NotificationConfigurationError

DEFAULT_SECRET_KEY_ENV = "NOTIFICATION_SECRET_KEY"
DEFAULT_SECRET_KEY_VERSION_ENV = "NOTIFICATION_SECRET_KEY_VERSION"


@dataclass(frozen=True)
class EncryptedSecret:
    """Encrypted secret material plus its key version."""

    ciphertext: str
    key_version: str


class SecretEncryptionProvider(ABC):
    """Abstraction for encrypting and decrypting stored channel secrets."""

    @abstractmethod
    def encrypt(self, plaintext: str) -> EncryptedSecret:
        raise NotImplementedError

    @abstractmethod
    def decrypt(self, ciphertext: str, key_version: str | None) -> str:
        raise NotImplementedError


class FernetSecretEncryptionProvider(SecretEncryptionProvider):
    """OSS default secret provider backed by a single env-managed Fernet key."""

    def __init__(
        self,
        *,
        key_env: str = DEFAULT_SECRET_KEY_ENV,
        key_version_env: str = DEFAULT_SECRET_KEY_VERSION_ENV,
    ):
        self.key_env = key_env
        self.key_version_env = key_version_env
        self._fernet: Fernet | None = None

    def _get_fernet(self) -> Fernet:
        if self._fernet is not None:
            return self._fernet

        key = os.environ.get(self.key_env, "").strip()
        if not key:
            raise NotificationConfigurationError(
                f"{self.key_env} must be configured to save notification secrets",
                failure_class="secret_provider_missing",
            )

        try:
            self._fernet = Fernet(key.encode("utf-8"))
        except Exception as exc:  # pragma: no cover - defensive
            raise NotificationConfigurationError(
                f"{self.key_env} is not a valid Fernet key",
                failure_class="secret_provider_invalid",
            ) from exc
        return self._fernet

    def _key_version(self) -> str:
        return os.environ.get(self.key_version_env, "v1").strip() or "v1"

    def encrypt(self, plaintext: str) -> EncryptedSecret:
        token = self._get_fernet().encrypt(plaintext.encode("utf-8"))
        return EncryptedSecret(
            ciphertext=token.decode("utf-8"),
            key_version=self._key_version(),
        )

    def decrypt(self, ciphertext: str, key_version: str | None) -> str:
        del key_version
        try:
            return self._get_fernet().decrypt(ciphertext.encode("utf-8")).decode("utf-8")
        except InvalidToken as exc:
            raise NotificationConfigurationError(
                "Stored notification secret could not be decrypted",
                failure_class="secret_decrypt_failed",
            ) from exc
