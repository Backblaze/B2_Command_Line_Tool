import warnings


def deprecation_message(module_name):
    sdk_module = module_name.replace('b2.', 'b2sdk.')
    return 'Module {0} is deprecated, please use {1} instead'.format(module_name, sdk_module)


def deprecate_module(module_name):
    if not module_name.startswith('b2.'):
        return

    warnings.warn(deprecation_message(module_name), DeprecationWarning, stacklevel=2)
