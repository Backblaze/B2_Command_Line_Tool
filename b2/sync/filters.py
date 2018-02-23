import re
import six
from abc import ABCMeta, abstractmethod
from collections import defaultdict


@six.add_metaclass(ABCMeta)
class FileFilter(object):

    @abstractmethod
    def is_exclude_filter(self):
        pass


@six.add_metaclass(ABCMeta)
class ExcludeFileFilter(FileFilter):
    """
    Policy object that decides which files should be excluded in a scan.
    Exclude filters never force file. Matched files are dropped.
    """
    @abstractmethod
    def should_exclude_file(self, file_path):
        """
        Given the full path of a file, should it be excluded from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff excluded.
        """

    def is_exclude_filter(self):
        return True


@six.add_metaclass(ABCMeta)
class IncludeFileFilter(FileFilter):
    """
    Policy object that decides which files should be included in a scan.
    Include filters always force matched file. Once forced file is always in scan.
    """
    @abstractmethod
    def should_include_file(self, file_path):
        """
        Given the full path of a file, should it be included from the scan?

        :param file_path: The path of the file, relative to the root directory
                          being scanned.
        :return: True iff included.
        """

    def is_exclude_filter(self):
        return False


class ExcludeDirRegexFilter(ExcludeFileFilter):
    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex + '.*')

    def __hash__(self):
        return hash(self.regex_str)

    def __eq__(self, other):
        return self.regex_str == other.regex_str

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return 'ExcludeDirRegexFilter(%s)' % (self.regex_str,)

    def should_exclude_file(self, file_path):
        return self.regex.match(file_path)


class ExcludeFileRegexFilter(ExcludeFileFilter):
    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __hash__(self):
        return hash(self.regex_str)

    def __eq__(self, other):
        return self.regex_str == other.regex_str

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return 'ExcludeFileRegexFilter(%s)' % (self.regex_str,)

    def should_exclude_file(self, file_path):
        return self.regex.match(file_path)


class IncludeFileRegexFilter(IncludeFileFilter):
    def __init__(self, regex):
        self.regex_str = regex
        self.regex = re.compile(regex)

    def __hash__(self):
        return hash(self.regex_str)

    def __eq__(self, other):
        return self.regex_str == other.regex_str

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return 'IncludeFileRegexFilter(%s)' % (self.regex_str,)

    def should_include_file(self, file_path):
        return self.regex.match(file_path)


class FilterManager(object):

    FILTERS_PRIORITES = {
        ExcludeDirRegexFilter: 100,
        IncludeFileRegexFilter: 50,
        ExcludeFileRegexFilter: 0,
    }

    def __init__(self, filters):
        self._filters = filters
        # Sorting filters by priorites
        self._filters.sort(key=lambda f: self.FILTERS_PRIORITES[f.__class__], reverse=True)
        self._excluded_files = defaultdict(list)
        self._included_files = defaultdict(list)

    def exclude(self, file_path):
        print(self._filters)
        for filter_obj in self._filters:
            if filter_obj.is_exclude_filter():
                if filter_obj.should_exclude_file(file_path):
                    self._excluded_files[filter_obj].append(file_path)
                    return True
            else:
                if filter_obj.should_include_file(file_path):
                    self._included_files[filter_obj].append(file_path)
                    return False
        return False

    @classmethod
    def _yield_collected_files(cls, collector):
        for excluded_files in six.itervalues(collector):
            for excluded_file in excluded_files:
                yield excluded_file

    def excluded_files(self):
        for excluded_file in self._yield_collected_files(self._excluded_files):
            yield excluded_file

    def included_files(self):
        for included_file in self._yield_collected_files(self._included_files):
            yield included_file
