from exasol.analytics.query_handler.udf.runner.udf     import QueryHandlerRunnerUDF

udf = QueryHandlerRunnerUDF(exa)


def run(ctx):
    return udf.run(ctx)
