######################################################################
#
# File: b2/_internal/_cli/autocomplete_cache.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import abc
import argparse
import itertools
import os
import pathlib
import pickle
from typing import Callable

import argcomplete
import platformdirs

from b2._internal.arg_parser import DeprecatedActionMarker
from b2._internal.version import VERSION


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


class VersionTracker(StateTracker):
    def current_state_identifier(self) -> str:
        return VERSION


class HomeCachePickleStore(PickleStore):
    _dir: pathlib.Path

    def __init__(self, dir_path: pathlib.Path | None = None) -> None:
        self._dir = dir_path

    def _cache_dir(self) -> pathlib.Path:
        if not self._dir:
            self._dir = pathlib.Path(
                platformdirs.user_cache_dir(appname='b2', appauthor='backblaze')
            ) / 'autocomplete'
        return self._dir

    def _fname(self, identifier: str) -> str:
        return f"b2-autocomplete-cache-{identifier}.pickle"

    def get_pickle(self, identifier: str) -> bytes | None:
        path = self._cache_dir() / self._fname(identifier)
        if path.exists():
            with open(path, 'rb') as f:
                return f.read()

    def set_pickle(self, identifier: str, data: bytes) -> None:
        """Sets the pickle for identifier if it doesn't exist.
        When a new pickle is added, old ones are removed."""

        dir_path = self._cache_dir()
        dir_path.mkdir(parents=True, exist_ok=True)
        path = dir_path / self._fname(identifier)
        for file in dir_path.glob('b2-autocomplete-cache-*.pickle'):
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

    def autocomplete_from_cache(
        self, uncached_args: dict | None = None, raise_exc: bool = False
    ) -> None:
        if not self._is_autocomplete_run():
            return

        try:
            identifier = self._tracker.current_state_identifier()
            pickle_data = self._store.get_pickle(identifier)
            if pickle_data:
                parser = self._unpickle(pickle_data)
                argcomplete.autocomplete(parser, **(uncached_args or {}))
        except Exception:
            if raise_exc:
                raise
            # Autocomplete from cache failed but maybe we can autocomplete from scratch
            return

    def _clean_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.register('type', None, identity)

        def _get_deprecated_actions(actions):
            return [action for action in actions if isinstance(action, DeprecatedActionMarker)]

        for action in _get_deprecated_actions(parser._actions):
            parser._actions.remove(action)
            for option_string in action.option_strings:
                del parser._option_string_actions[option_string]

        for action in parser._actions:
            if action.type not in [str, int]:
                action.type = None

        for group in itertools.chain(parser._action_groups, parser._mutually_exclusive_groups):
            for action in _get_deprecated_actions(group._group_actions):
                group._group_actions.remove(action)

            for key in parser._defaults:
                group.set_defaults(**{key: None})

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


AUTOCOMPLETE = AutocompleteCache(tracker=VersionTracker(), store=HomeCachePickleStore())
