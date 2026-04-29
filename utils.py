import hashlib


def generate_row_hash(row) -> str:
    """
    This function generates MD5 hash for a row.
    """
    row_string = "|".join(map(str, row.values))
    return hashlib.md5(row_string.encode()).hexdigest()