import requests as req


class ResponseNotOkError(req.exceptions.RequestException):
    pass

# Service temporarily unavailable, e.g. server is overloaded
class ServiceUnavailableError(ResponseNotOkError):
    pass

