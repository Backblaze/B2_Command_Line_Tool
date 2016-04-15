######################################################################
#
# File: b2/parse_args.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


class Arguments(object):
    """
    An object to stick attributes on.
    """


def parse_arg_list(arg_list, option_flags, option_args, list_args, required, optional, arg_parser):
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
    once, and their values are collected into a liste.  Default is [].

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
    :param required: Names of positional params that must be there.
    :param optional: Names of optional params.
    :param arg_parser: Map from param name to parser for values.
    :return: An Argument object, or None if there was any error parsing.
    """

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
            setattr(result, option, True)
        elif option in option_args:
            if len(arg_list) == 0:
                return None
            else:
                setattr(result, option, parse_arg(option, arg_list))
        elif option in list_args:
            if len(arg_list) == 0:
                return None
            else:
                getattr(result, option).append(parse_arg(option, arg_list))
        else:
            return None

    # Parse the positional parameters
    for arg_name in required:
        if len(arg_list) == 0:
            return None
        setattr(result, arg_name, parse_arg(arg_name, arg_list))
    for arg_name in optional:
        if len(arg_list) != 0:
            setattr(result, arg_name, parse_arg(arg_name, arg_list))

    # Anything left is a problem
    if len(arg_list) != 0:
        return None

    # Return the Arguments object
    return result
