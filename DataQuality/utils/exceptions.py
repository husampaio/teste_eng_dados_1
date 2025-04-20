class NullValueError(Exception):
    """Exception raised when a column has null values

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class NegativeValueError(Exception):
    """Exception raised when a column has negative values

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class UniqueError(Exception):
    """Exception raised when a column doesn't have unique values

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class OutdatedDatasetError(Exception):
    """Exception raised when dataset is outdated.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
