import re


def strip_prefix(name):
    return re.sub(r'^\d+\.\s*', '', name)


def convert_czech_or_slovak(value):
    return 1 if value.lower() == 'e' else 0


def truncate_string(value, max_length):
    if len(value) > max_length:
        return value[:max_length]
    return value
