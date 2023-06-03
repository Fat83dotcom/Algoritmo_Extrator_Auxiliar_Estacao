import confidentials


def dbCredentials(database: int) -> dict:
    CONFIG = confidentials.bataBaseChoice(database)
    return CONFIG
