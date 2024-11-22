from typing import Any, Callable

import pandas as pd


def iterate_trough_dataset(
    ctx,
    batch_size: int,
    map_function: Callable[[pd.DataFrame], Any],
    init_function: Callable[[], Any],
    aggregate_function: Callable[[Any, Any], Any],
    reset_function: Callable[[], Any],
):
    reset_function()
    number_of_tuples_left = ctx.size()
    state = init_function()
    while True:
        if number_of_tuples_left < batch_size:
            if number_of_tuples_left > 0:
                df = ctx.get_dataframe(number_of_tuples_left)
                number_of_tuples_left = 0
            else:
                reset_function()
                break
        else:
            df = ctx.get_dataframe(batch_size)
            number_of_tuples_left = number_of_tuples_left - batch_size
        result = map_function(df)
        state = aggregate_function(state, result)
    return state


def ctx_iterator(ctx, batch_size: int, reset_function: Callable[[], Any]):
    reset_function()
    number_of_tuples_left = ctx.size()
    while True:
        if number_of_tuples_left < batch_size:
            if number_of_tuples_left > 0:
                df = ctx.get_dataframe(num_rows=number_of_tuples_left)
                yield df
                number_of_tuples_left = 0
            else:
                reset_function()
                break
        else:
            df = ctx.get_dataframe(num_rows=batch_size)
            yield df
            number_of_tuples_left = max(0, number_of_tuples_left - batch_size)
