class UninitializedAttributeError(Exception):
    """
    Error indicating that a method of a class accessed an attribute that has not been
    initialized dynamically before, e.g. in method run().
    """


class IllegalParametersError(Exception):
    """
    Indicating an error  when called a function, method or initializing a class with an illegal
    combination of argument values.
    """
