
class CreateEventHandlerUDF:
    def __init__(self, exa):
        self.exa = exa

    def run(self, ctx) -> None:
        ctx.emit("return query")
        ctx.emit("status")
        ctx.emit("query 1")
        ctx.emit("query 2")
