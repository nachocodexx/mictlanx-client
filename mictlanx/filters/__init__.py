import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import mictlanx.errors as EX
from mictlanx.utils.compression import CompressionAlgorithm, CompressionX

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

_AESGCM_NONCE_SIZE      = 12
_AESGCM_VALID_KEY_SIZES = (16, 24, 32)


class IOFilter(ABC):
    """A single byte-transforming step in an :class:`~mictlanx.asyncx.AsyncClient`
    ``put()``/``get()`` filter pipeline.

    ``put(filters=[f1, f2])`` runs ``f2.filter(f1.filter(value))`` before
    chunking; ``get(filters=[g1, g2])`` runs ``g2.filter(g1.filter(data))``
    on the reassembled bytes. The caller is responsible for supplying the
    correctly-ordered inverse chain on the ``get()`` side — the SDK never
    auto-reverses a filter list.

    Two filters that are inverses of each other (e.g. :class:`CompressFilter`
    and :class:`DecompressFilter`) MUST share the same ``name``. ``name``
    identifies *which transform* was applied, not its direction — direction
    is implicit in which class is instantiated. ``AsyncClient`` uses this to
    record a provenance tag at put time and validate it at get time (via
    :meth:`names_match`), catching a wrong/missing/misordered filter list
    with a clear error instead of silently returning garbage bytes.
    """

    name: str

    @abstractmethod
    def filter(self, data: bytes) -> bytes:
        """Transform ``data`` and return the result.

        Args:
            data: Input bytes.

        Returns:
            Transformed bytes.

        Raises:
            EX.FilterExecutionError: If the transform fails (bad key,
                tampered/corrupt input, missing optional dependency, ...).
        """
        raise NotImplementedError

    @staticmethod
    def names_match(stored: List[str], get_filters: List["IOFilter"]) -> bool:
        """Check that ``get_filters`` is the correct reverse of a put-side chain.

        Args:
            stored: Filter names in put order, as recorded in the
                ``mictlanx_filters`` tag (empty list when no filters were
                used).
            get_filters: The filters passed to ``get()``.

        Returns:
            ``True`` when ``list(reversed([f.name for f in get_filters]))``
            equals ``stored``.
        """
        return list(reversed([f.name for f in get_filters])) == stored


class CompressFilter(IOFilter):
    """Compresses bytes using :class:`mictlanx.utils.compression.CompressionX`."""

    name = "compress"

    def __init__(self, algorithm: CompressionAlgorithm = CompressionAlgorithm.ZLIB, params: Dict[str, Any] = {}):
        """
        Args:
            algorithm: Compression algorithm to use. Defaults to ``ZLIB``.
            params: Extra keyword arguments forwarded to the underlying
                compressor (see ``CompressionX.compress_stream``).
        """
        self.algorithm = algorithm
        self.params    = params

    def filter(self, data: bytes) -> bytes:
        result = CompressionX.compress_stream(algorithm=self.algorithm, data=data, params=self.params)
        if result.is_err:
            e = result.unwrap_err()
            raise EX.FilterExecutionError(f"{self.name} filter raised {type(e).__name__}: {e}") from e
        return result.unwrap()


class DecompressFilter(IOFilter):
    """Reverses :class:`CompressFilter` using the same underlying algorithm."""

    name = "compress"

    def __init__(self, algorithm: CompressionAlgorithm = CompressionAlgorithm.ZLIB, chunk_size: str = "1MB"):
        """
        Args:
            algorithm: Compression algorithm the data was compressed with.
                Defaults to ``ZLIB``.
            chunk_size: Internal streaming buffer size (humanfriendly
                string) used while decompressing. Defaults to ``"1MB"``.
        """
        self.algorithm  = algorithm
        self.chunk_size = chunk_size

    def filter(self, data: bytes) -> bytes:
        result = CompressionX.decompress_stream(algorithm=self.algorithm, data=data, chunk_size=self.chunk_size)
        if result.is_err:
            e = result.unwrap_err()
            raise EX.FilterExecutionError(f"{self.name} filter raised {type(e).__name__}: {e}") from e
        return result.unwrap()


class EncryptFilter(IOFilter):
    """Encrypts bytes with AES-GCM (authenticated encryption).

    Each call generates a fresh random 12-byte nonce and prepends it to the
    output (``nonce + ciphertext``, ciphertext already includes the AEAD
    tag) so :class:`DecryptFilter` can recover it without out-of-band state.
    """

    name = "encrypt"

    def __init__(self, key: bytes):
        """
        Args:
            key: AES key, 16/24/32 bytes for AES-128/192/256-GCM.

        Raises:
            ValueError: If ``key`` is not a valid AES-GCM key length.
        """
        if len(key) not in _AESGCM_VALID_KEY_SIZES:
            raise ValueError(
                f"EncryptFilter key must be 16, 24, or 32 bytes long (AES-128/192/256-GCM), got {len(key)}"
            )
        self.key = key

    def filter(self, data: bytes) -> bytes:
        if not CRYPTOGRAPHY_AVAILABLE:
            raise EX.FilterExecutionError(
                "cryptography is not installed. Install with `pip install cryptography` or `pip install mictlanx[crypto]`"
            )
        nonce = os.urandom(_AESGCM_NONCE_SIZE)
        try:
            ciphertext = AESGCM(self.key).encrypt(nonce, data, None)
        except Exception as e:
            raise EX.FilterExecutionError(f"{self.name} filter raised {type(e).__name__}: {e}") from e
        return nonce + ciphertext


class DecryptFilter(IOFilter):
    """Reverses :class:`EncryptFilter`.

    Raises :class:`~mictlanx.errors.FilterExecutionError` on a wrong key or
    tampered ciphertext alike — AES-GCM's authentication tag makes the two
    indistinguishable from the caller's point of view, which is the desired
    behavior (both mean "this data cannot be trusted").
    """

    name = "encrypt"

    def __init__(self, key: bytes):
        """
        Args:
            key: AES key, 16/24/32 bytes for AES-128/192/256-GCM. Must match
                the key used by the corresponding :class:`EncryptFilter`.

        Raises:
            ValueError: If ``key`` is not a valid AES-GCM key length.
        """
        if len(key) not in _AESGCM_VALID_KEY_SIZES:
            raise ValueError(
                f"DecryptFilter key must be 16, 24, or 32 bytes long (AES-128/192/256-GCM), got {len(key)}"
            )
        self.key = key

    def filter(self, data: bytes) -> bytes:
        if not CRYPTOGRAPHY_AVAILABLE:
            raise EX.FilterExecutionError(
                "cryptography is not installed. Install with `pip install cryptography` or `pip install mictlanx[crypto]`"
            )
        nonce, ciphertext = data[:_AESGCM_NONCE_SIZE], data[_AESGCM_NONCE_SIZE:]
        try:
            return AESGCM(self.key).decrypt(nonce, ciphertext, None)
        except Exception as e:
            raise EX.FilterExecutionError(f"{self.name} filter raised {type(e).__name__}: {e}") from e
