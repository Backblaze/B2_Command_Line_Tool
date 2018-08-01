######################################################################
#
# File: b2/parse_args.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging

import six

from .utils import repr_dict_deterministically

logger = logging.getLogger(__name__)


class Arguments(object):
    """
    An object to stick attributes on.
    """

    def __repr__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            repr_dict_deterministically(self.__dict__),
        )


def check_for_duplicate_args(args_dict):
    """
    Checks that no argument name is listed in multiple places.

    Raises a ValueError if there is a problem.

    This args_dict has a problem because 'required' and 'optional'
    both contain 'a':

       {
          'option_args': ['b', 'c'],
          'required': ['a', 'd']
          'optional': ['a', 'e']
       }
    """
    categories = sorted(six.iterkeys(args_dict))
    for index_a, category_a in enumerate(categories):
        for category_b in categories[index_a + 1:]:
            names_a = args_dict[category_a]
            names_b = args_dict[category_b]
            for common_name in set(names_a) & set(names_b):
                raise ValueError(
                    "argument '%s' is in both '%s' an '%s'" % (common_name, category_a, category_b)
                )


def parse_arg_list(
    arg_list, option_flags, option_args, list_args, optional_before, required, optional, arg_parser
):
    """
    Converts a list of string arguments to an Arguments object, with
    one attribute per parameter.

    The value of every parameter is set in the returned Arguments object,
    even for parameters not specified on the command line.

    Option Flags set boolean values that default to False.  When the
    option is present on the command line, the value is set to True.

    Option Args have values provided on the command line.  The default
    if not present is None.

    List Args act like Option Args, but can be specified more than
    once, and their values are collected into a list.  Default is [].

    Required positional parameters must be present, and do not have
    a double-dash name preceding them.

    Optional positional parameters are just like required parameters,
    but don't have to be there and default to None.

    Arg Parser is a dict that maps from a parameter name to a function
    tha converts the string argument into the value needed by the
    program.  These parameters can be Option Args, List Args, Required,
    or Optional.

    :param arg_list sys.argv[1:], or equivalent
    :param option_flags: Names of options that are boolean flags.
    :param option_args: Names of options that have values.
    :param list_args: Names of options whose values are collected into a list.
    :param optional_before: Names of option positional params that come before the required ones.
    :param required: Names of positional params that must be there.
    :param optional: Names of optional params.
    :param arg_parser: Map from param name to parser for values.
    :return: An Argument object, or None if there was any error parsing.
    """

    # Sanity check the inputs.
    check_for_duplicate_args(
        {
            'option_flags': option_flags,
            'option_args': option_args,
            'optional_before': optional_before,
            'required': required,
            'optional': optional
        }
    )

    # Create an object to hold the arguments.
    result = Arguments()

    # Set the default value for everything that has a default value.
    for name in option_flags:
        setattr(result, name, False)
    for name in option_args:
        setattr(result, name, None)
    for name in list_args:
        setattr(result, name, [])
    for name in optional:
        setattr(result, name, None)

    # Make a function for parsing argument values
    def parse_arg(name, arg_list):
        value = arg_list.pop(0)
        if name in arg_parser:
            value = arg_parser[name](value)
        return value

    # Parse the '--' options
    while len(arg_list) != 0 and arg_list[0].startswith('--'):
        option = arg_list.pop(0)[2:]
        if option in option_flags:
            logger.debug('option %s is properly recognized as OPTION_FLAGS', option)
            setattr(result, option, True)
        elif option in option_args:
            if len(arg_list) == 0:
                logger.debug(
                    'option %s is recognized as OPTION_ARGS and there are no more arguments on arg_list to parse',
                    option
                )
                return None
            else:
                logger.debug('option %s is properly recognized as OPTION_ARGS', option)
                setattr(result, option, parse_arg(option, arg_list))
        elif option in list_args:
            if len(arg_list) == 0:
                logger.debug(
                    'option %s is recognized as LIST_ARGS and there are no more arguments on arg_list to parse',
                    option
                )
                return None
            else:
                logger.debug('option %s is properly recognized as LIST_ARGS', option)
                getattr(result, option).append(parse_arg(option, arg_list))
        else:
            logger.error('option %s is of unknown type!', option)
            return None

    # Handle optional positional parameters that come first.
    # We assume that if there are optional parameters, the
    # ones that come before take precedence over the ones
    # that come after the required arguments.
    for arg_name in optional_before:
        if len(required) < len(arg_list):
            setattr(result, arg_name, parse_arg(arg_name, arg_list))
        else:
            setattr(result, arg_name, None)

    # Parse the positional parameters
    for arg_name in required:
        if len(arg_list) == 0:
            logger.debug('lack of required positional argument: %s', arg_name)
            return None
        setattr(result, arg_name, parse_arg(arg_name, arg_list))
    for arg_name in optional:
        if len(arg_list) != 0:
            setattr(result, arg_name, parse_arg(arg_name, arg_list))

    # Anything left is a problem
    if len(arg_list) != 0:
        logger.debug('option parser failed to consume this: %s', arg_list)
        return None

    # Return the Arguments object
    return result
