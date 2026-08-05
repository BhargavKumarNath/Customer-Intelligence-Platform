class UserNotFoundError(Exception):
    """Raised when a user_id has no matching row in the requested table."""


class InsufficientHistoryError(Exception):
    """Raised when a user has no October activity to build propensity features from."""


class SegmentNotFoundError(Exception):
    """Raised when an RFM segment name doesn't exist or is too small to simulate."""
