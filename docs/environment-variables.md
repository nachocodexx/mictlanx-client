# Environment Variables

All variables are optional unless marked **required**. Boolean variables accept `1`, `true`, or `yes` (case-insensitive).

---

## AsyncClient (`MICTLANX_CLIENT_*`)

These variables configure an `AsyncClient` instance when the corresponding constructor parameter is not passed in code. They are resolved at construction time, not at import time.

| Variable | Default | Configures |
|---|---|---|
| `MICTLANX_CLIENT_URI` | **required** | `uri` — `mictlanx://` router connection string. Must be set if `uri` is not passed to the constructor. |
| `MICTLANX_CLIENT_ID` | random hex | `client_id` — unique name for this instance; used as logger name and `producer_id`. |
| `MICTLANX_CLIENT_DEBUG` | `1` | `debug` — when `1`, log records are echoed to the console. |
| `MICTLANX_CLIENT_MAX_WORKERS` | `12` | `max_workers` — thread-pool upper bound (capped at `os.cpu_count()`). |
| `MICTLANX_CLIENT_EVICTION_POLICY` | `LFU` | `eviction_policy` — in-memory cache strategy (`LFU` with decay, or `LRU`). |
| `MICTLANX_CLIENT_CAPACITY_STORAGE` | `1GB` | `capacity_storage` — cache size as a humanfriendly string (e.g. `512MB`, `2GB`). |
| `MICTLANX_CLIENT_CACHE_DEFAULT` | `0` | `cache_default` — whether `get()` uses the cache when `cache=` is not given. `mictlanx.objects` handles always use it. |
| `MICTLANX_CLIENT_HALF_LIFE` | `10m` | `half_life` — decay half-life of the access frequency (`client.stats`, LFU eviction), as a humanfriendly timespan. |
| `MICTLANX_CLIENT_VERIFY` | `0` | `verify` — SSL certificate verification. `0` = off, `1` = system CAs. For a CA-bundle path or `SSLContext`, pass `verify=` directly in code. |

```bash
export MICTLANX_CLIENT_URI=mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0
export MICTLANX_CLIENT_ID=my-client
export MICTLANX_CLIENT_DEBUG=0
export MICTLANX_CLIENT_MAX_WORKERS=8
export MICTLANX_CLIENT_EVICTION_POLICY=LFU
export MICTLANX_CLIENT_CAPACITY_STORAGE=512MB
```

> **Logging parameters** (`log_output_path`, `log_when`, `log_interval`, `enable_logging`, `use_rich`, `log_level`) are controlled by the `MICTLANX_LOG_*` variables below. Pass `None` (or omit them) to let `Log` read the env vars directly.

---

## Logging (`MICTLANX_LOG_*`)

These variables configure the `Log` class. They are read at `Log` / `AsyncClient` construction time.
Level variables accept `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.

| Variable | Default | Configures |
|---|---|---|
| `MICTLANX_LOG_PATH` | `.mictlanx/log` | Log directory for all rotating files. Created automatically if absent. |
| `MICTLANX_LOG_DISABLED` | `0` | Set to `1` to suppress all log output globally. Maps to the `disabled` / `enable_logging` constructor parameter. |
| `MICTLANX_LOG_LEVEL` | `DEBUG` | Minimum level written to all handlers. Maps to the `log_level` constructor parameter. |
| `MICTLANX_LOG_RICH` | `0` | Set to `1` to use `RichHandler` for syntax-coloured console output. Requires `pip install mictlanx[rich]`. Maps to the `use_rich` constructor parameter. |
| `MICTLANX_LOG_JSON_INDENT` | `0` | Console JSON indentation. `0` = compact single-line; any positive integer = pretty-printed with that many spaces. |
| `MICTLANX_LOG_TO_FILE` | `0` | Set to `1` to enable the rotating `.log` file. Disabled by default — console only. |
| `MICTLANX_LOG_ERROR_FILE` | `0` | Set to `1` to write a separate `.error.log` file (ERROR and CRITICAL only). |
| `MICTLANX_LOG_ROTATION_WHEN` | `m` | Rotation time unit passed to `TimedRotatingFileHandler` (`s`, `m`, `h`, `d`). |
| `MICTLANX_LOG_ROTATION_INTERVAL` | `10` | Rotation interval (integer, interpreted in units of `MICTLANX_LOG_ROTATION_WHEN`). |
| `MICTLANX_LOG_CONSOLE_LEVEL` | `DEBUG` | Minimum level for the console handler. |
| `MICTLANX_LOG_FILE_LEVEL` | `INFO` | Minimum level for the main rotating file handler. |

```bash
export MICTLANX_LOG_PATH=/var/log/mictlanx
export MICTLANX_LOG_DISABLED=1
export MICTLANX_LOG_RICH=1
export MICTLANX_LOG_LEVEL=INFO
export MICTLANX_LOG_JSON_INDENT=4
```

> **Note:** `MICTLANX_LOG_DISABLED` and `MICTLANX_LOG_RICH` replace the old `MICTLANX_DISABLE_LOGGING` and `MICTLANX_USE_RICH_LOGGER` names. The old names are no longer read.

> **Note:** All variables are resolved at `Log` / `AsyncClient` construction time. Load your `.env` file with `dotenv.load_dotenv()` *before* constructing any client or logger to ensure the variables are visible.

---

## Integration tests

These variables are only used by the test suite. Copy `.env.test.example` to `.env.test` and adjust the values for your local VSS.

| Variable | Default | Description |
|---|---|---|
| `MICTLANX_ENV_FILE` | `.env.test` | Path to the dotenv file loaded before the test session. Set this if your env file lives somewhere other than the project root. |
| `MICTLANX_URI` | `mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0` | Full router URI used by `AsyncClient` in tests. |
| `CLIENT_ID` | `client-0` | Client identity string passed to `AsyncClient`. |
| `MICTLANX_LOG_PATH` | `/mictlanx/client` | Log directory (same as the runtime variable above). |
| `MICTLANX_ROUTER_PORT` | `60666` | Port used when constructing `AsyncRouter` directly in router tests. |
| `MICTLANX_TEST_PEER_PORT` | `25000` | Port used when connecting directly to a single peer in peer tests. |
| `MICTLANX_TEST_BUCKET` | `pytest-bucket` | Bucket name used by peer-level tests. |
| `MICTLANX_SUMMONER_PORT` | `15000` | Port for the Summoner service (reserved, not yet active). |

Minimal `.env.test` for a local single-router VSS:

```bash
MICTLANX_URI=mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0
CLIENT_ID=client-0
MICTLANX_ROUTER_PORT=60666
MICTLANX_TEST_PEER_PORT=25000
```
