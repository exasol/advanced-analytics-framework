
class UninitializedAttributeError(Exception):
    """
    A method of a class accesses an attribute that has not been
    initialized dynamically before, e.g. in method run().
    """

class IllegalParametersError(Exception):
    """
    Calling a function, method or iniinitializing a class with an illegal
    combination of argument values.
    """
