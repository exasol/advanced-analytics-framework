from exasol_advanced_analytics_framework.udf_framework.query_handler_runner_udf \
    import CreateQueryHandlerUDF

udf = CreateQueryHandlerUDF(exa)


def run(ctx):
    return udf.run(ctx)
