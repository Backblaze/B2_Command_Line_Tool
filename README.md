# B2 Command Line Tool

| CI type  | Status |
| :------------ | :------------ |
| Travis CI  | [![Build Status](https://travis-ci.org/Backblaze/B2_Command_Line_Tool.svg?branch=master)](https://travis-ci.org/Backblaze/B2_Command_Line_Tool)  |

The command-line tool that gives easy access to all of the capabilities of B2 Cloud Storage.

This program provides command-line access to the B2 service.

Version 0.3.13

# Usage

    b2 authorize_account [--dev | --staging | --production] [accountId] [applicationKey]

        Prompts for Backblaze accountID and applicationKey (unless they are given
        on the command line).

        The account ID is a 12-digit hex number that you can get from
        your account page on backblaze.com.

        The application key is a 40-digit hex number that you can get from
        your account page on backblaze.com.

        Stores an account auth token in ~/.b2_account_info.  This can be overridden using the
        B2_ACCOUNT_INFO environment variable.

    b2 clear_account

        Erases everything in ~/.b2_account_info
        
    b2 clear_bucket [--force] <bucketName>

        Deletes all files in the named bucket.  The force argument will
        suppress a prompt asking if the user is sure they wish to delete
        all files.

    b2 create_bucket <bucketName> <bucketType>

        Creates a new bucket.  Prints the ID of the bucket created.

    b2 delete_bucket <bucketName>

        Deletes the bucket with the given name.

    b2 delete_file_version <fileName> <fileId>

        Permanently and irrevocably deletes one version of a file.

    b2 download_file_by_id <fileId> <localFileName>

        Downloads the given file, and stores it in the given local file.

    b2 download_file_by_name <bucketName> <fileName> <localFileName>

        Downloads the given file, and stores it in the given local file.

    b2 get_file_info <fileId>

        Prints all of the information about the file, but not its contents.

    b2 hide_file <bucketName> <fileName>

        Uploads a new, hidden, version of the given file.

    b2 list_buckets

        Lists all of the buckets in the current account.

    b2 list_file_names <bucketName> [<startingName>] [<numberToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.

    b2 list_file_versions <bucketName> [<startingName>] [<startingFileId>] [<numberToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.

    b2 ls [--long] [--versions] <bucketName> [<folderName>]

        Using the file naming convention that "/" separates folder
        names from their contents, returns a list of the files
        and folders in a given folder.  If no folder name is given,
        lists all files at the top level.

        The --long option produces very wide multi-column output
        showing the upload date/time, file size, file id, whether it
        is an uploaded file or the hiding of a file, and the file
        name.  Folders don't really exist in B2, so folders are
        shown with "-" in each of the fields other than the name.

        The --version option shows all of versions of each file, not
        just the most recent.

    b2 make_url <fileId>

        Prints an URL that can be used to download the given file, if
        it is public.

    b2 sync [--delete] [--hide] <source> <destination>

        UNDER DEVELOPMENT -- there may be changes coming to this command

        Uploads or downloads multiple files from source to destination.
        One of the paths must be a local file path, and the other must be
        a B2 bucket path. Use "b2:<bucketName>/<prefix>" for B2 paths, e.g.
        "b2:my-bucket-name/a/path/prefix/".

        If the --delete or --hide flags are specified, destination files
        are deleted or hidden if not present in the source path. Note that
        files are matched only by name and size.

    b2 update_bucket <bucketName> <bucketType>

        Updates the bucketType of an existing bucket.  Prints the ID
        of the bucket updated.

    b2 upload_file [--sha1 <sha1sum>] [--contentType <contentType>] [--info <key>=<value>]* <bucketName> <localFilePath> <b2FileName>

        Uploads one file to the given bucket.  Uploads the contents
        of the local file, and assigns the given name to the B2 file.

        By default, upload_file will compute the sha1 checksum of the file
        to be uploaded.  But, you you already have it, you can provide it
        on the command line to save a little time.

        Content type is optional.  If not set, it will be set based on the
        file extension.

        If `tqdm` library is installed, progress bar is displayed on stderr.
        (use pip install tqdm to install it)

        Each fileInfo is of the form "a=b".

    b2 version

        Echos the version number of this program.


## Contrib

You can find a [bash completion](https://www.gnu.org/software/bash/manual/html_node/Programmable-Completion.html#Programmable-Completion)
script in the `contrib` directory. You can install it for example with

     [ ! -z $BASH_COMPLETION_DIR ] && sudo cp contrib/b2 $BASH_COMPLETION_DIR

Once installed, you will be able to enter both commands and parameters just
typing some (unambiguous) prefix of them and having the shell complete the
rest by pressing the `tab` key.
