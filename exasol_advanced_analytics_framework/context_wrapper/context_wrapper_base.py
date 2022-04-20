from abc import ABC


class ContextWrapperBase(ABC):
    def __init__(self, ctx):
        self.ctx = ctx

