class BatchManagerException(Exception):
    """
    This exception is used to handle any exceptions that may occur during the execution of the script.
    """

    def __init__(self, message: str):
        super().__init__(message)