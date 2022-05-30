from exasol_advanced_analytics_framework.udf_framework.create_event_handler_udf \
    import CreateEventHandlerUDF

udf = CreateEventHandlerUDF(exa)


def run(ctx):
    return udf.run(ctx)
