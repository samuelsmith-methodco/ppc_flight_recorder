"""
IDeaS SFTP/FTPS client — list and download .tar.gz archives from SFTPCloud.
"""

from __future__ import annotations

import ftplib
import io
import logging
import re
import time
import urllib.request
from typing import Callable, Protocol, TypeVar

try:
    import paramiko
except ImportError:
    paramiko = None

from config import (
    IDEAS_FTPS_PORT,
    IDEAS_SFTP_HOST,
    IDEAS_SFTP_MAX_RETRIES,
    IDEAS_SFTP_PASSWORD,
    IDEAS_SFTP_PORT,
    IDEAS_SFTP_PROTOCOL,
    IDEAS_SFTP_REMOTE_DIR,
    IDEAS_SFTP_RETRY_BACKOFF_SEC,
    IDEAS_SFTP_USERNAME,
)

logger = logging.getLogger(__name__)

# ftplib.all_errors is already a tuple; concatenate instead of nesting in except.
_FTPLIB_RETRY_EXCEPTIONS = (ConnectionError, OSError, TimeoutError) + ftplib.all_errors

FILE_DATE_RE = re.compile(r"^\d{4}_(\d{8})_\d{4}\.tar\.gz$")

T = TypeVar("T")


class IdeasSftpError(Exception):
    """SFTP/FTPS failed after retries; caller should abort sync gracefully."""


class RemoteClient(Protocol):
    def list_names(self, remote_dir: str) -> list[str]: ...
    def download_bytes(self, remote_path: str) -> bytes: ...
    def close(self) -> None: ...


def normalize_date(value: str) -> str:
    cleaned = value.strip().replace("-", "")
    if not re.fullmatch(r"\d{8}", cleaned):
        raise ValueError(f"Invalid date '{value}'. Use YYYY-MM-DD or YYYYMMDD.")
    return cleaned


def extract_file_date(filename: str) -> str | None:
    match = FILE_DATE_RE.match(filename)
    return match.group(1) if match else None


def normalize_remote_dir(remote_dir: str) -> str:
    remote_dir = remote_dir.strip() or "/"
    if not remote_dir.startswith("/"):
        remote_dir = f"/{remote_dir}"
    return remote_dir.rstrip("/") or "/"


def join_remote(remote_dir: str, filename: str) -> str:
    remote_dir = normalize_remote_dir(remote_dir)
    if remote_dir == "/":
        return f"/{filename}"
    return f"{remote_dir}/{filename}"


def filter_files(
    filenames: list[str],
    *,
    date_filter: str | None = None,
    from_date: str | None = None,
    date_filters: list[str] | None = None,
) -> list[str]:
    matched: list[str] = []
    for name in filenames:
        file_date = extract_file_date(name)
        if file_date is None:
            continue
        if date_filters is not None:
            if file_date in date_filters:
                matched.append(name)
        elif date_filter is not None:
            if file_date == date_filter:
                matched.append(name)
        elif from_date is not None:
            if file_date >= from_date:
                matched.append(name)
        else:
            matched.append(name)
    return sorted(matched)


def public_ip() -> str:
    try:
        with urllib.request.urlopen("https://api.ipify.org", timeout=5) as resp:
            return resp.read().decode("utf-8").strip()
    except OSError:
        return "unknown"


def _stat_is_dir(attr: paramiko.SFTPAttributes) -> bool:
    import stat

    return stat.S_ISDIR(attr.st_mode)


def _call_with_retries(label: str, fn: Callable[[], T]) -> T:
    """Retry SFTP operations up to IDEAS_SFTP_MAX_RETRIES times, then raise IdeasSftpError."""
    last_exc: Exception | None = None
    max_attempts = max(1, IDEAS_SFTP_MAX_RETRIES)

    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except IdeasSftpError:
            raise
        except _FTPLIB_RETRY_EXCEPTIONS as exc:
            last_exc = exc
            logger.warning(
                "IDeaS SFTP %s failed (attempt %d/%d): %s",
                label,
                attempt,
                max_attempts,
                exc,
            )
            if attempt < max_attempts:
                time.sleep(IDEAS_SFTP_RETRY_BACKOFF_SEC * attempt)

    raise IdeasSftpError(f"{label} failed after {max_attempts} attempts: {last_exc}") from last_exc


class SftpRemoteClient:
    def __init__(self, sftp: paramiko.SFTPClient, ssh: paramiko.SSHClient) -> None:
        self._sftp = sftp
        self._ssh = ssh

    def list_names(self, remote_dir: str) -> list[str]:
        remote_dir = normalize_remote_dir(remote_dir)
        entries = self._sftp.listdir_attr(remote_dir)
        return sorted(
            attr.filename
            for attr in entries
            if not attr.filename.startswith(".") and not _stat_is_dir(attr)
        )

    def download_bytes(self, remote_path: str) -> bytes:
        with self._sftp.open(remote_path, "rb") as handle:
            return handle.read()

    def close(self) -> None:
        self._sftp.close()
        self._ssh.close()


class FtpsRemoteClient:
    def __init__(self, ftp: ftplib.FTP_TLS) -> None:
        self._ftp = ftp

    def list_names(self, remote_dir: str) -> list[str]:
        remote_dir = normalize_remote_dir(remote_dir)
        if remote_dir == "/":
            names = self._ftp.nlst()
        else:
            names = self._ftp.nlst(remote_dir)
        return sorted(name for name in names if not name.endswith("/"))

    def download_bytes(self, remote_path: str) -> bytes:
        buffer = io.BytesIO()
        self._ftp.retrbinary(f"RETR {remote_path}", buffer.write)
        return buffer.getvalue()

    def close(self) -> None:
        try:
            self._ftp.quit()
        except ftplib.all_errors:
            self._ftp.close()


def connect_sftp() -> SftpRemoteClient:
    if paramiko is None:
        raise ConnectionError("paramiko is required. Install with: pip install paramiko")
    if not IDEAS_SFTP_USERNAME or not IDEAS_SFTP_PASSWORD:
        raise ConnectionError("IDEAS_SFTP_USERNAME and IDEAS_SFTP_PASSWORD must be set in .env")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(
            hostname=IDEAS_SFTP_HOST,
            port=IDEAS_SFTP_PORT,
            username=IDEAS_SFTP_USERNAME,
            password=IDEAS_SFTP_PASSWORD,
            look_for_keys=False,
            allow_agent=False,
            timeout=30,
        )
    except paramiko.AuthenticationException as exc:
        raise ConnectionError(
            "SFTP authentication failed. Confirm IDEAS_SFTP_* credentials in .env. "
            f"If correct, whitelist public IP ({public_ip()}) in SFTPCloud."
        ) from exc
    except (paramiko.SSHException, OSError, TimeoutError) as exc:
        raise ConnectionError(f"SFTP connection error: {exc}") from exc

    return SftpRemoteClient(ssh.open_sftp(), ssh)


def connect_ftps() -> FtpsRemoteClient:
    if not IDEAS_SFTP_USERNAME or not IDEAS_SFTP_PASSWORD:
        raise ConnectionError("IDEAS_SFTP_USERNAME and IDEAS_SFTP_PASSWORD must be set in .env")

    ftp = ftplib.FTP_TLS(timeout=30)
    try:
        ftp.connect(IDEAS_SFTP_HOST, IDEAS_FTPS_PORT)
        ftp.login(IDEAS_SFTP_USERNAME, IDEAS_SFTP_PASSWORD)
        ftp.prot_p()
    except ftplib.error_perm as exc:
        raise ConnectionError(f"FTPS authentication failed: {exc}") from exc
    except _FTPLIB_RETRY_EXCEPTIONS as exc:
        raise ConnectionError(f"FTPS connection error: {exc}") from exc

    return FtpsRemoteClient(ftp)


def connect_client(protocol: str | None = None) -> RemoteClient:
    protocol = (protocol or IDEAS_SFTP_PROTOCOL).lower()
    if protocol == "sftp":
        return connect_sftp()
    if protocol == "ftps":
        return connect_ftps()
    if protocol != "auto":
        raise ValueError(f"Unknown protocol '{protocol}'. Use sftp, ftps, or auto.")

    errors: list[str] = []
    for candidate in ("sftp", "ftps"):
        try:
            if candidate == "sftp":
                return connect_sftp()
            return connect_ftps()
        except ConnectionError as exc:
            errors.append(f"{candidate.upper()}: {exc}")

    raise ConnectionError("Could not connect using SFTP or FTPS.\n" + "\n".join(errors))


def connect_client_with_retries(protocol: str | None = None) -> RemoteClient:
    """Connect with up to IDEAS_SFTP_MAX_RETRIES attempts."""
    return _call_with_retries("connect", lambda: connect_client(protocol))


def list_remote_archives(
    *,
    remote_dir: str | None = None,
    date_filter: str | None = None,
    from_date: str | None = None,
    date_filters: list[str] | None = None,
    protocol: str | None = None,
) -> list[str]:
    def _do() -> list[str]:
        remote_dir_norm = normalize_remote_dir(remote_dir or IDEAS_SFTP_REMOTE_DIR)
        client = connect_client(protocol)
        try:
            all_names = client.list_names(remote_dir_norm)
            return filter_files(
                all_names,
                date_filter=date_filter,
                from_date=from_date,
                date_filters=date_filters,
            )
        finally:
            client.close()

    return _call_with_retries("list archives", _do)


def download_archive_bytes(
    *,
    archive_name: str,
    remote_dir: str | None = None,
    protocol: str | None = None,
) -> bytes:
    def _do() -> bytes:
        remote_dir_norm = normalize_remote_dir(remote_dir or IDEAS_SFTP_REMOTE_DIR)
        remote_path = join_remote(remote_dir_norm, archive_name)
        client = connect_client(protocol)
        try:
            return client.download_bytes(remote_path)
        finally:
            client.close()

    return _call_with_retries(f"download {archive_name}", _do)
