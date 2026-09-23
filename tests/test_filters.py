import pytest
import mictlanx.errors as EX
from mictlanx.filters import (
    IOFilter,
    CompressFilter,
    DecompressFilter,
    EncryptFilter,
    DecryptFilter,
    CRYPTOGRAPHY_AVAILABLE,
)
from mictlanx.utils.compression import CompressionAlgorithm, LZ4_AVAILABLE

# --- Fixtures ---

@pytest.fixture
def sample_data():
    """Returns a repetitive string that compresses well."""
    return b"MictlanX" * 1000

@pytest.fixture
def aes_key():
    """Fixed 32-byte (AES-256-GCM) test key."""
    return b"0" * 32

# --- IOFilter ABC ---

def test_iofilter_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        IOFilter()

# --- CompressFilter / DecompressFilter ---

@pytest.mark.parametrize("algo", [
    CompressionAlgorithm.ZLIB,
    CompressionAlgorithm.GZIP,
    pytest.param(CompressionAlgorithm.LZ4, marks=pytest.mark.skipif(not LZ4_AVAILABLE, reason="lz4 not installed")),
])
def test_compress_decompress_roundtrip(algo, sample_data):
    compressed = CompressFilter(algo).filter(sample_data)
    assert compressed != sample_data
    assert DecompressFilter(algo).filter(compressed) == sample_data

def test_compress_filter_name_matches_decompress_filter_name():
    assert CompressFilter().name == DecompressFilter().name == "compress"

# --- EncryptFilter / DecryptFilter ---

@pytest.mark.skipif(not CRYPTOGRAPHY_AVAILABLE, reason="cryptography not installed")
def test_encrypt_decrypt_roundtrip(sample_data, aes_key):
    ciphertext = EncryptFilter(aes_key).filter(sample_data)
    assert ciphertext != sample_data
    assert DecryptFilter(aes_key).filter(ciphertext) == sample_data

@pytest.mark.skipif(not CRYPTOGRAPHY_AVAILABLE, reason="cryptography not installed")
def test_encrypt_filter_nonce_is_randomized(sample_data, aes_key):
    f = EncryptFilter(aes_key)
    assert f.filter(sample_data) != f.filter(sample_data)

@pytest.mark.skipif(not CRYPTOGRAPHY_AVAILABLE, reason="cryptography not installed")
def test_decrypt_filter_detects_tampering(sample_data, aes_key):
    ciphertext = EncryptFilter(aes_key).filter(sample_data)
    # Flip one byte past the 12-byte nonce prefix, inside the ciphertext/tag.
    tampered = bytearray(ciphertext)
    tampered[20] ^= 0xFF
    with pytest.raises(EX.FilterExecutionError):
        DecryptFilter(aes_key).filter(bytes(tampered))

@pytest.mark.skipif(not CRYPTOGRAPHY_AVAILABLE, reason="cryptography not installed")
def test_decrypt_filter_wrong_key_fails(sample_data, aes_key):
    ciphertext = EncryptFilter(aes_key).filter(sample_data)
    other_key = b"1" * 32
    with pytest.raises(EX.FilterExecutionError):
        DecryptFilter(other_key).filter(ciphertext)

def test_encrypt_filter_rejects_invalid_key_length():
    with pytest.raises(ValueError):
        EncryptFilter(b"too-short")

def test_decrypt_filter_rejects_invalid_key_length():
    with pytest.raises(ValueError):
        DecryptFilter(b"too-short")

def test_encrypt_filter_name_matches_decrypt_filter_name(aes_key):
    assert EncryptFilter(aes_key).name == DecryptFilter(aes_key).name == "encrypt"

# --- Full pipeline (filter classes only, no AsyncClient/live infra) ---

@pytest.mark.skipif(not CRYPTOGRAPHY_AVAILABLE, reason="cryptography not installed")
def test_filter_pipeline_forward_then_reverse(sample_data, aes_key):
    put_filters = [EncryptFilter(aes_key), CompressFilter()]
    get_filters = [DecompressFilter(), DecryptFilter(aes_key)]

    data = sample_data
    for f in put_filters:
        data = f.filter(data)
    assert data != sample_data

    for f in get_filters:
        data = f.filter(data)
    assert data == sample_data

# --- Provenance-tag name-matching logic (IOFilter.names_match) ---

@pytest.mark.skipif(not CRYPTOGRAPHY_AVAILABLE, reason="cryptography not installed")
@pytest.mark.parametrize("stored, get_filters_factory, expected", [
    ([], lambda key: [], True),
    (["encrypt", "compress"], lambda key: [DecompressFilter(), DecryptFilter(key)], True),
    (["encrypt", "compress"], lambda key: [DecryptFilter(key), DecompressFilter()], False),
    (["encrypt", "compress"], lambda key: [], False),
    ([], lambda key: [DecompressFilter()], False),
    (["compress"], lambda key: [DecompressFilter()], True),
])
def test_filter_name_matching_logic(stored, get_filters_factory, expected, aes_key):
    assert IOFilter.names_match(stored=stored, get_filters=get_filters_factory(aes_key)) is expected
