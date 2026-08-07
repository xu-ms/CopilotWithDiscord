from __future__ import annotations

import errno
import os
import stat
import sys
from collections.abc import Mapping
from pathlib import Path
from types import TracebackType


class GatewayAlreadyRunning(RuntimeError):
    def __init__(self) -> None:
        super().__init__("another copilotD gateway is already running for this OS user")


def gateway_lock_path(
    platform_name: str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    home: Path | None = None,
) -> Path:
    del environ
    effective_platform = sys.platform if platform_name is None else platform_name
    effective_home = (
        _authoritative_user_home(effective_platform) if home is None else home.resolve()
    )
    if effective_platform == "darwin":
        cache = effective_home / "Library" / "Caches" / "copilotd"
    elif effective_platform == "win32":
        cache = effective_home / "AppData" / "Local" / "copilotd" / "cache"
    else:
        cache = effective_home / ".cache" / "copilotd"
    return cache / "gateway.lock"


def _authoritative_user_home(platform_name: str) -> Path:
    if platform_name == "win32":
        return _windows_profile_directory()

    import pwd

    return Path(pwd.getpwuid(os.geteuid()).pw_dir).resolve()


def _windows_profile_directory() -> Path:
    import ctypes
    from ctypes import wintypes

    token_query = 0x0008
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    userenv = ctypes.WinDLL("userenv", use_last_error=True)
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    advapi32.OpenProcessToken.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    ]
    advapi32.OpenProcessToken.restype = wintypes.BOOL
    userenv.GetUserProfileDirectoryW.argtypes = [
        wintypes.HANDLE,
        wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD),
    ]
    userenv.GetUserProfileDirectoryW.restype = wintypes.BOOL
    token = wintypes.HANDLE()
    if not advapi32.OpenProcessToken(
        kernel32.GetCurrentProcess(),
        token_query,
        ctypes.byref(token),
    ):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        size = wintypes.DWORD()
        userenv.GetUserProfileDirectoryW(token, None, ctypes.byref(size))
        if size.value == 0:
            raise ctypes.WinError(ctypes.get_last_error())
        buffer = ctypes.create_unicode_buffer(size.value)
        if not userenv.GetUserProfileDirectoryW(token, buffer, ctypes.byref(size)):
            raise ctypes.WinError(ctypes.get_last_error())
        return Path(buffer.value).resolve()
    finally:
        kernel32.CloseHandle(token)


class GatewayInstanceLock:
    """Non-blocking per-user gateway lock held for the process gateway lifetime."""

    def __init__(
        self,
        *,
        path: Path | None = None,
        platform_name: str | None = None,
    ) -> None:
        self.path = gateway_lock_path(platform_name) if path is None else path
        self._windows = (
            sys.platform == "win32" if platform_name is None else platform_name == "win32"
        )
        self._descriptor: int | None = None

    @property
    def acquired(self) -> bool:
        return self._descriptor is not None

    def acquire(self) -> None:
        if self._descriptor is not None:
            raise RuntimeError("gateway instance lock is already acquired")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if os.name == "posix":
            self.path.parent.chmod(0o700)
        flags = os.O_RDWR | os.O_CREAT
        if self._windows and hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        descriptor = os.open(self.path, flags, 0o600)
        try:
            if os.name == "posix":
                os.fchmod(descriptor, stat.S_IRUSR | stat.S_IWUSR)
            if self._windows:
                self._acquire_windows(descriptor)
            else:
                self._acquire_posix(descriptor)
        except BaseException:
            os.close(descriptor)
            raise
        self._descriptor = descriptor

    def release(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        self._descriptor = None
        try:
            if self._windows:
                self._release_windows(descriptor)
            else:
                self._release_posix(descriptor)
        finally:
            os.close(descriptor)

    def __enter__(self) -> GatewayInstanceLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.release()

    @staticmethod
    def _acquire_posix(descriptor: int) -> None:
        import fcntl

        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN}:
                raise GatewayAlreadyRunning() from None
            raise

    @staticmethod
    def _release_posix(descriptor: int) -> None:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_UN)

    @staticmethod
    def _acquire_windows(descriptor: int) -> None:
        import msvcrt

        if os.fstat(descriptor).st_size == 0:
            os.write(descriptor, b"\0")
        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        except OSError as error:
            if (
                error.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}
                or getattr(
                    error,
                    "winerror",
                    None,
                )
                == 33
            ):
                raise GatewayAlreadyRunning() from None
            raise

    @staticmethod
    def _release_windows(descriptor: int) -> None:
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
