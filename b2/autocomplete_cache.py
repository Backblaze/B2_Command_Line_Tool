######################################################################
#
# File: b2/autocomplete_cache.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import abc
import argparse
import hashlib
import os
import pathlib
import pickle
from typing import Callable, Iterable

import argcomplete


def identity(x):
    return x


class StateTracker(abc.ABC):
    @abc.abstractmethod
    def current_state_identifier(self) -> str:
        raise NotImplementedError()


class PickleStore(abc.ABC):
    @abc.abstractmethod
    def get_pickle(self, identifier: str) -> bytes | None:
        raise NotImplementedError()

    @abc.abstractmethod
    def set_pickle(self, identifier: str, data: bytes) -> None:
        raise NotImplementedError()


class FileSetStateTrakcer(StateTracker):
    _files: list[pathlib.Path]

    def __init__(self, files: Iterable[pathlib.Path]) -> None:
        self._files = list(files)

    def _one_file_hash(self, file: pathlib.Path) -> str:
        with open(file, 'rb') as f:
            return hashlib.md5(str(file.absolute).encode('utf-8') + f.read()).hexdigest()

    def current_state_identifier(self) -> str:
        return hashlib.md5(
            b''.join(self._one_file_hash(file).encode('ascii') for file in self._files)
        ).hexdigest()


class HomeCachePickleStore(PickleStore):
    _dir: pathlib.Path

    def __init__(self, dir: pathlib.Path | None = None) -> None:
        self._dir = dir

    def _dir_or_default(self) -> pathlib.Path:
        if self._dir is not None:
            return self._dir
        cache_home = os.environ.get('XDG_CACHE_HOME')
        if cache_home:
            return pathlib.Path(cache_home) / 'b2' / 'autocomplete'
        home = os.environ.get('HOME')
        if not home:
            raise RuntimeError(
                'Neither $HOME not $XDG_CACHE_HOME is set, cannot determine cache directory'
            )
        return pathlib.Path(home) / '.cache' / 'b2' / 'autocomplete'

    def _fname(self, identifier: str) -> str:
        return f"b2-autocomplete-cache-{identifier}.pickle"

    def get_pickle(self, identifier: str) -> bytes | None:
        path = self._dir_or_default() / self._fname(identifier)
        if path.exists():
            with open(path, 'rb') as f:
                return f.read()

    def set_pickle(self, identifier: str, data: bytes) -> None:
        """Sets the pickle for identifier if it doesn't exist.
        When a new pickle is added, old ones are removed."""

        dir = self._dir_or_default()
        os.makedirs(dir, exist_ok=True)
        path = dir / self._fname(identifier)
        for file in dir.glob('b2-autocomplete-cache-*.pickle'):
            file.unlink()
        with open(path, 'wb') as f:
            f.write(data)


class AutocompleteCache:
    _tracker: StateTracker
    _store: PickleStore
    _unpickle: Callable[[bytes], argparse.ArgumentParser]

    def __init__(
        self,
        tracker: StateTracker,
        store: PickleStore,
        unpickle: Callable[[bytes], argparse.ArgumentParser] | None = None
    ):
        self._tracker = tracker
        self._store = store
        self._unpickle = unpickle or pickle.loads

    def _is_autocomplete_run(self) -> bool:
        return '_ARGCOMPLETE' in os.environ

    def autocomplete_from_cache(self, uncached_args: dict | None = None) -> None:
        if not self._is_autocomplete_run():
            return

        try:
            identifier = self._tracker.current_state_identifier()
            pickle_data = self._store.get_pickle(identifier)
            if pickle_data:
                parser = self._unpickle(pickle_data)
                argcomplete.autocomplete(parser, **(uncached_args or {}))
        except Exception:
            # Autocomplete from cache failed but maybe we can autocomplete from scratch
            return

    def _clean_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.register('type', None, identity)
        for action in parser._actions:
            if action.type not in [str, int]:
                action.type = None
        for action in parser._action_groups:
            for key in parser._defaults:
                action.set_defaults(**{key: None})
        parser.description = None
        if parser._subparsers:
            for group_action in parser._subparsers._group_actions:
                for parser in group_action.choices.values():
                    self._clean_parser(parser)

    def cache_and_autocomplete(
        self, parser: argparse.ArgumentParser, uncached_args: dict | None = None
    ) -> None:
        if not self._is_autocomplete_run():
            return

        try:
            identifier = self._tracker.current_state_identifier()
            self._clean_parser(parser)
            self._store.set_pickle(identifier, pickle.dumps(parser))
        finally:
            argcomplete.autocomplete(parser, **(uncached_args or {}))


AUTOCOMPLETE = AutocompleteCache(
    tracker=FileSetStateTrakcer(pathlib.Path(__file__).parent.glob('**/*.py')),
    store=HomeCachePickleStore()
)
