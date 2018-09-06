######################################################################
#
# File: progress.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod
import six
import sys
import time
import hashlib

from .utils import raise_if_shutting_down

# tqdm doesn't work on 2.6 with at least some encodings
# on sys.stderr.  See: https://github.com/Backblaze/B2_Command_Line_Tool/issues/272
if sys.version_info < (2, 7):
    tqdm = None  # will fall back to simple progress reporting
else:
    try:
        from tqdm import tqdm  # displays a nice progress bar
    except ImportError:
        tqdm = None  # noqa


@six.add_metaclass(ABCMeta)
class AbstractProgressListener(object):
    """
    Interface expected by B2Api upload and download methods to report
    on progress.

    This interface just accepts the number of bytes transferred so far.
    Subclasses will need to know the total size if they want to report
    a percent done.
    """

    def __init__(self):
        self._closed = False

    @abstractmethod
    def set_total_bytes(self, total_byte_count):
        """
        Always called before __enter__ to set the expected total number of bytes.

        May be called more than once if an upload is retried.
        """

    @abstractmethod
    def bytes_completed(self, byte_count):
        """
        Reports that the given number of bytes have been transferred
        so far.  This is not a delta, it is the total number of bytes
        transferred so far.
        """

    @abstractmethod
    def close(self):
        """
        Must be called when you're done with the listener.
        In well-structured code, should be called only once.
        """
        #import traceback, sys; traceback.print_stack(file=sys.stdout)
        assert self._closed is False, 'progress listener was closed twice! uncomment the line above to debug this'
        self._closed = True

    def __enter__(self):
        """
        standard context manager method
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        standard context manager method
        """
        self.close()


class TqdmProgressListener(AbstractProgressListener):
    def __init__(self, description, *args, **kwargs):
        self.description = description
        self.tqdm = None  # set in set_total_bytes()
        self.prev_value = 0
        super(TqdmProgressListener, self).__init__(*args, **kwargs)

    def set_total_bytes(self, total_byte_count):
        raise_if_shutting_down()
        if self.tqdm is None:
            self.tqdm = tqdm(
                desc=self.description,
                total=total_byte_count,
                unit='B',
                unit_scale=True,
                leave=True,
                miniters=1
            )

    def bytes_completed(self, byte_count):
        # tqdm doesn't support running the progress bar backwards,
        # so on an upload retry, it just won't move until it gets
        # past the point where it failed.
        raise_if_shutting_down()
        if self.prev_value < byte_count:
            self.tqdm.update(byte_count - self.prev_value)
            self.prev_value = byte_count

    def close(self):
        if self.tqdm is not None:
            self.tqdm.close()
        super(TqdmProgressListener, self).close()


class SimpleProgressListener(AbstractProgressListener):
    def __init__(self, description, *args, **kwargs):
        self.desc = description
        self.complete = 0
        self.last_time = time.time()
        self.any_printed = False
        super(SimpleProgressListener, self).__init__(*args, **kwargs)

    def set_total_bytes(self, total_byte_count):
        raise_if_shutting_down()
        self.total = total_byte_count

    def bytes_completed(self, byte_count):
        raise_if_shutting_down()
        now = time.time()
        elapsed = now - self.last_time
        if 3 <= elapsed and self.total != 0:
            if not self.any_printed:
                print(self.desc)
            print('     %d%%' % int(100.0 * byte_count / self.total))
            self.last_time = now
            self.any_printed = True

    def close(self):
        raise_if_shutting_down()
        if self.any_printed:
            print('    DONE.')
        super(SimpleProgressListener, self).close()


class DoNothingProgressListener(AbstractProgressListener):
    def set_total_bytes(self, total_byte_count):
        raise_if_shutting_down()

    def bytes_completed(self, byte_count):
        raise_if_shutting_down()

    def close(self):
        super(DoNothingProgressListener, self).close()


class ProgressListenerForTest(AbstractProgressListener):
    """
    Captures all of the calls so they can be checked.
    """

    def __init__(self, *args, **kwargs):
        self.calls = []
        super(ProgressListenerForTest, self).__init__(*args, **kwargs)

    def set_total_bytes(self, total_byte_count):
        self.calls.append('set_total_bytes(%d)' % (total_byte_count,))

    def bytes_completed(self, byte_count):
        self.calls.append('bytes_completed(%d)' % (byte_count,))

    def close(self):
        self.calls.append('close()')
        super(ProgressListenerForTest, self).close()

    def get_calls(self):
        return self.calls


def make_progress_listener(description, quiet):
    if quiet:
        return DoNothingProgressListener()
    elif tqdm is not None:
        return TqdmProgressListener(description)
    else:
        return SimpleProgressListener(description)


class RangeOfInputStream(object):
    """
    Wraps a file-like object (read only) and reads the selected
    range of the file.
    """

    def __init__(self, stream, offset, length):
        self.stream = stream
        self.offset = offset
        self.remaining = length

    def __enter__(self):
        self.stream.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.stream.__exit__(exc_type, exc_val, exc_tb)

    def seek(self, pos):
        self.stream.seek(self.offset + pos)

    def read(self, size=None):
        if size is None:
            to_read = self.remaining
        else:
            to_read = min(size, self.remaining)
        data = self.stream.read(to_read)
        self.remaining -= len(data)
        return data


class AbstractStreamWithProgress(object):
    """
    Wraps a file-like object and updates a ProgressListener
    as data is read / written.
    In the abstract class, read and write methods do not update
    the progress - child classes shall do it
    """

    def __init__(self, stream, progress_listener, offset=0):
        """

        :param stream: the stream to read from or write to
        :param progress_listener: the listener that we tell about progress
        :param offset: the starting byte offset in the file
        :return: None
        """
        assert progress_listener is not None
        self.stream = stream
        self.progress_listener = progress_listener
        self.bytes_completed = 0
        self.offset = offset

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.stream.__exit__(exc_type, exc_val, exc_tb)

    def seek(self, pos):
        return self.stream.seek(pos)

    def tell(self):
        return self.stream.tell()

    def flush(self):
        self.stream.flush()

    def read(self, size=None):
        if size is None:
            data = self.stream.read()
        else:
            data = self.stream.read(size)
        return data

    def write(self, data):
        self.stream.write(data)

    def _update(self, delta):
        self.bytes_completed += delta
        self.progress_listener.bytes_completed(self.bytes_completed + self.offset)


class ReadingStreamWithProgress(AbstractStreamWithProgress):
    def read(self, size=None):
        data = super(ReadingStreamWithProgress, self).read(size)
        self._update(len(data))
        return data


class WritingStreamWithProgress(AbstractStreamWithProgress):
    def write(self, data):
        self._update(len(data))
        super(WritingStreamWithProgress, self).write(data)


class StreamWithHash(object):
    """
    Wraps a file-like object, calculates SHA1 while reading
    and appends hash at the end
    """

    def __init__(self, stream):
        """
        :param stream: the stream to read from
        :return: None
        """
        self.stream = stream
        self.digest = hashlib.sha1()
        self.hash = None
        self.hash_read = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.stream.__exit__(exc_type, exc_val, exc_tb)

    def seek(self, pos):
        assert pos == 0
        self.stream.seek(0)
        self.digest = hashlib.sha1()
        self.hash = None
        self.hash_read = 0

    def read(self, size=None):
        data = b''
        if self.hash is None:
            # Read some bytes from stream
            if size is None:
                data = self.stream.read()
            else:
                data = self.stream.read(size)

            # Update hash
            self.digest.update(data)

            # Check for end of stream
            if size is None or len(data) < size:
                self.hash = self.digest.hexdigest()
                if size is not None:
                    size -= len(data)

        if self.hash is not None:
            # The end of stream was reached, return hash now
            size = size or len(self.hash)
            data += str.encode(self.hash[self.hash_read:self.hash_read + size])
            self.hash_read += size

        return data

    def hash_size(self):
        return self.digest.digest_size * 2
