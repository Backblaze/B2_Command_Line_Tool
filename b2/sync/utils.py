######################################################################
#
# File: b2/sync/utils.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from ..exception import CommandError, DestFileNewer


def files_are_different(source_file, dest_file, args):
    """
    Compare two files and determine if the the destination file
    should be replaced by the source file.
    """

    # Compare using modification time by default
    compareVersions = args.compareVersions or 'modTime'

    # Compare using file name only
    if compareVersions == 'none':
        return False

    # Compare using modification time
    elif compareVersions == 'modTime':
        # Get the modification time of the latest versions
        source_mod_time = source_file.latest_version().mod_time
        dest_mod_time = dest_file.latest_version().mod_time

        # Source is newer
        if dest_mod_time < source_mod_time:
            return True

        # Source is older
        elif source_mod_time < dest_mod_time:
            if args.replaceNewer:
                return True
            elif args.skipNewer:
                return False
            else:
                raise DestFileNewer(dest_file.name,)

    # Compare using file size
    elif compareVersions == 'size':
        # Get file size of the latest versions
        source_size = source_file.latest_version().size
        dest_size = dest_file.latest_version().size

        # Replace if sizes are different
        return source_size != dest_size

    else:
        raise CommandError('Invalid option for --compareVersions')
