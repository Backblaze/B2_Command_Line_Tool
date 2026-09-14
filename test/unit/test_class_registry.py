######################################################################
#
# File: test/unit/test_class_registry.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from b2._internal.class_registry import ClassRegistry, RegistryKeyError


def test_register_with_key_and_get_instance():
    registry = ClassRegistry()

    @registry.register('shell')
    class ShellInstaller:
        def __init__(self, prog: str) -> None:
            self.prog = prog

    instance = registry.get('shell', 'b2')

    assert isinstance(instance, ShellInstaller)
    assert instance.prog == 'b2'

    instance_kwargs = registry.get('shell', prog='b2-kw')
    assert isinstance(instance_kwargs, ShellInstaller)
    assert instance_kwargs.prog == 'b2-kw'


def test_register_with_attr_name():
    registry = ClassRegistry(attr_name='COMMAND_NAME')

    @registry.register
    class ListBuckets:
        COMMAND_NAME = 'list-buckets'

    assert 'list-buckets' in registry
    assert isinstance(registry.get('list-buckets'), ListBuckets)

    with pytest.raises(AttributeError):

        @registry.register
        class MissingKey:
            pass


def test_register_requires_key_without_attr_name():
    registry = ClassRegistry()

    with pytest.raises(ValueError):

        @registry.register
        class MissingKey:
            pass


def test_duplicate_key_overrides():
    registry = ClassRegistry()

    @registry.register('dup')
    class First:
        pass

    @registry.register('dup')
    class Second:
        pass

    assert registry.get('dup').__class__ is Second


def test_duplicate_key_raises_when_unique():
    registry = ClassRegistry(unique=True)

    @registry.register('dup')
    class First:
        pass

    with pytest.raises(RegistryKeyError):

        @registry.register('dup')
        class Second:
            pass


def test_missing_key_raises_registry_key_error():
    registry = ClassRegistry()

    with pytest.raises(RegistryKeyError):
        registry.get('missing')


def test_empty_key_is_rejected():
    registry = ClassRegistry()

    with pytest.raises(ValueError):

        @registry.register('')
        class EmptyKey:
            pass


def test_copy_has_independent_registrations_and_same_configuration():
    registry = ClassRegistry(attr_name='COMMAND_NAME', unique=True)

    @registry.register
    class First:
        COMMAND_NAME = 'first'

    copied_registry = registry.copy()

    assert copied_registry is not registry
    assert list(copied_registry.items()) == list(registry.items())

    @copied_registry.register
    class Second:
        COMMAND_NAME = 'second'

    copied_registry.unregister('first')

    assert 'first' in registry
    assert 'first' not in copied_registry
    assert 'second' in copied_registry
    assert 'second' not in registry
