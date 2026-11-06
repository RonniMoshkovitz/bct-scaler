"""Errors the application layer expects its ports to raise, so it can react to them."""


class ObjectUnavailableError(Exception):
    """Raised when the object can never be read 
    (it doesn't exist, or access is denied), so retrying it is pointless."""
