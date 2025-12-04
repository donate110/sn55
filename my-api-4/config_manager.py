import asyncio
import json
import logging
import os
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

_DEFAULT_SCALE_INFO: Dict[str, Dict[str, float]] = {
    "btc": {"base_point": 0.0, "ratio": 1.0},
    "eth": {"base_point": 0.0, "ratio": 1.0},
    "tao_bittensor": {"base_point": 0.0, "ratio": 1.0},
    "default": {"base_point": 0.0, "ratio": 1.0},
}


def _project_root() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _abs_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(_project_root(), path)


class ConfigManager:
    """
    Hot-reloads config.json, exposing helper accessors for runtime-configurable values.
    """

    def __init__(self, config_filename: str = "config.json") -> None:
        self._config_filename = _abs_path(config_filename)
        self._lock = asyncio.Lock()
        self._config_cache: Optional[Dict[str, Any]] = None
        self._config_mtime: Optional[float] = None
        self._logging_level: Optional[str] = None

    async def get_config(self) -> Dict[str, Any]:
        async with self._lock:
            try:
                st = os.stat(self._config_filename)
            except FileNotFoundError as exc:
                raise FileNotFoundError(f"Missing config file: {self._config_filename}") from exc

            if self._config_cache is not None and self._config_mtime == st.st_mtime:
                return self._config_cache

            with open(self._config_filename, "r", encoding="utf-8") as f:
                cfg = json.load(f)

            self._config_cache = cfg
            self._config_mtime = st.st_mtime
            log.info("Loaded config.json (mtime=%s)", self._config_mtime)
            self._apply_logging_level_unlocked(cfg)
            return cfg

    def _apply_logging_level_unlocked(self, cfg: Dict[str, Any]) -> None:
        level_name = str(cfg.get("logging_level", "INFO")).upper()
        if level_name == self._logging_level:
            return
        level_value = getattr(logging, level_name, logging.INFO)
        logging.getLogger().setLevel(level_value)
        self._logging_level = level_name
        log.info("Logging level set to %s", level_name)

    async def get_logging_level(self) -> str:
        cfg = await self.get_config()
        return str(cfg.get("logging_level", self._logging_level or "INFO")).upper()

    def _normalize_scale_info(self, raw: Any) -> Dict[str, Dict[str, float]]:
        normalized: Dict[str, Dict[str, float]] = {k: dict(v) for k, v in _DEFAULT_SCALE_INFO.items()}
        if not isinstance(raw, dict):
            if raw not in (None, {}):
                log.warning("scale_ratio section must be an object; using defaults")
            return normalized

        for key, value in raw.items():
            if not isinstance(key, str):
                log.warning("Ignoring non-string scale ratio key: %s", key)
                continue
            if not isinstance(value, dict):
                log.warning("Scale ratio for '%s' must be an object; skipping", key)
                continue
            try:
                base_point = float(value.get("base_point", 0.0))
                ratio = float(value.get("ratio", 1.0))
            except (TypeError, ValueError):
                log.warning("Invalid scale ratio entry for '%s'; skipping", key)
                continue
            normalized[key.strip().lower()] = {"base_point": base_point, "ratio": ratio}
        return normalized

    async def get_scale_ratio_info(self) -> Dict[str, Dict[str, float]]:
        cfg = await self.get_config()
        return self._normalize_scale_info(cfg.get("scale_ratio"))

    async def get_ip_blacklist(self) -> set[str]:
        cfg = await self.get_config()
        raw_list = cfg.get("ip_blacklist", [])
        if isinstance(raw_list, list):
            return {str(ip).strip() for ip in raw_list if str(ip).strip()}
        if raw_list not in (None, {}):
            log.warning("ip_blacklist must be a list; ignoring")
        return set()
