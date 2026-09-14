######################################################################
#
# File: b2/_internal/class_registry.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Hashable, Iterable, Iterator
from typing import Any


class RegistryKeyError(KeyError):
    """Raised when a registry lookup fails."""


class ClassRegistry:
    """Registry with decorator-based class registration and instantiation."""

    def __init__(self, attr_name: str | None = None, unique: bool = False) -> None:
        self.attr_name = attr_name
        self.unique = unique
        self._registry: OrderedDict[Hashable, type] = OrderedDict()

    def __contains__(self, key: Hashable) -> bool:
        try:
            self.get_class(key)
        except RegistryKeyError:
            return False
        return True

    def __getitem__(self, key: Hashable) -> object:
        return self.get(key)

    def __iter__(self) -> Iterator[Hashable]:
        return self.keys()

    def __len__(self) -> int:
        return len(self._registry)

    def __repr__(self) -> str:
        return f'{type(self).__name__}(attr_name={self.attr_name!r}, unique={self.unique!r})'

    def copy(self) -> ClassRegistry:
        """Return a registry with the same configuration and registrations."""
        copied_registry = ClassRegistry(attr_name=self.attr_name, unique=self.unique)
        copied_registry._registry = self._registry.copy()
        return copied_registry

    def __setitem__(self, key: Hashable, class_: type) -> None:
        self._register(key, class_)

    def __delitem__(self, key: Hashable) -> None:
        self._unregister(key)

    def __missing__(self, key: Hashable) -> object:
        raise RegistryKeyError(key)

    @staticmethod
    def create_instance(class_: type, *args: Any, **kwargs: Any) -> object:
        return class_(*args, **kwargs)

    def get_class(self, key: Hashable) -> type:
        try:
            return self._registry[key]
        except KeyError:
            return self.__missing__(key)

    def get(self, key: Hashable, *args: Any, **kwargs: Any) -> object:
        return self.create_instance(self.get_class(key), *args, **kwargs)

    def items(self) -> Iterable[tuple[Hashable, type]]:
        return self._registry.items()

    def keys(self) -> Iterable[Hashable]:
        return self._registry.keys()

    def values(self) -> Iterable[type]:
        return self._registry.values()

    def register(self, key: Hashable | type) -> Callable[[type], type] | type:
        if isinstance(key, type):
            if not self.attr_name:
                raise ValueError(
                    f'Attempting to register {key.__name__} via decorator, but attr_name is not set.'
                )
            attr_key = getattr(key, self.attr_name)
            self._register(attr_key, key)
            return key

        def _decorator(cls: type) -> type:
            self._register(key, cls)
            return cls

        return _decorator

    def unregister(self, key: Hashable) -> type:
        return self._unregister(key)

    def _register(self, key: Hashable, class_: type) -> None:
        if key in ['', None]:
            raise ValueError(
                f'Attempting to register class {class_.__name__} with empty registry key {key!r}.'
            )
        if self.unique and key in self._registry:
            raise RegistryKeyError(f'{class_.__name__} with key {key!r} is already registered.')
        self._registry[key] = class_

    def _unregister(self, key: Hashable) -> type:
        return self._registry.pop(key) if key in self._registry else self.__missing__(key)
