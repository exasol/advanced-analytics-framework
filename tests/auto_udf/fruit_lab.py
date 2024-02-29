import os
from math import pi
from decimal import Decimal
from collections import defaultdict
import numpy as np

import pyexasol
from exasol_advanced_analytics_framework.auto_udf import ExaQue, udf


def get_fruit_volume(length: Decimal, diameter: Decimal, shape: str) -> Decimal:

    # Get the volume of one fruit in cubic centimeters
    cuboid = length * diameter * diameter
    if shape == 'cuboid':
        return cuboid
    elif shape == 'cylinder':
        return cuboid * Decimal(pi) / 4
    elif shape == 'ellipsoid':
        return cuboid * Decimal(pi) / 6
    elif shape == 'pyramid':
        return cuboid / 3
    else:
        raise ValueError(f'Unknown shape {shape}')


@udf
def fruit_price_converter(exa_input: ExaQue,
                          exa_output: ExaQue,
                          shape: str,
                          length_to_diameter_ratio: Decimal = Decimal(2),
                          density: Decimal = Decimal(1000)) -> None:

    for _, row in exa_input:
        fruit, length, price = row

        # Get the weight of one fruit in kg
        length = Decimal(length)
        diameter = length / length_to_diameter_ratio
        volume = get_fruit_volume(length, diameter, shape)
        weight = volume * density / Decimal(1.e6)

        # Get the price per kg
        price_kg = Decimal(price) / weight

        exa_output.emit(fruit, length, price_kg)


def get_stat_function(stat: str):

    stat = stat.lower()
    if stat == 'mean' or stat == 'average':
        return np.mean
    elif stat == 'median':
        return np.median
    elif stat == 'std' or stat == 'standard deviation':
        return np.std
    elif stat == 'var' or stat == 'variance':
        return np.var
    else:
        raise ValueError('Unknown statistical function')


@udf
def fruit_price_stat1(exa_input: ExaQue, exa_output: ExaQue, stat: str) -> None:

    # Expected input data format:
    # SIZE_CM, (FRUIT, SIZE_CM, PRICE)

    stat_func = get_stat_function(stat)

    # Load and group the data
    groups = defaultdict(list)
    for group, row in exa_input:
        groups[group].append(Decimal(row[-1]))

    for group, vals in groups.items():
        exa_output.emit(Decimal(group), Decimal(stat_func(vals)))


@udf
def fruit_price_stat2(exa_input: ExaQue, exa_output: ExaQue, stat: str) -> None:

    # Expected input data format:
    # None, (SIZE_CM, [(FRUIT, SIZE_CM, PRICE), ...])

    stat_func = get_stat_function(stat)

    for _, group_of_rows in exa_input:
        prices = [Decimal(row[-1]) for row in group_of_rows[1]]
        exa_output.emit(group_of_rows[0], stat_func(prices))


def supermarket_inspection():

    pwd = os.environ.get('DB_PASSWORD')
    if not pwd:
        pwd = input("Enter password:")
    conn_params = {
        "dsn": "demodb.exasol.com:8563",
        "user": "EXASOL_MIBE",
        "password": pwd,
        "encryption": True,
        "schema": "EXASOL_MIBE"
    }

    with pyexasol.connect(**conn_params) as conn:
        # Convert price per item to price per kilogram.
        statement = conn.execute('select * from fruits')
        table_format = ['FRUIT:VARCHAR(50)', 'SIZE_CM:DECIMAL(10,2)', 'PRICE:DECIMAL(10,2)']
        input_data = ExaQue(table_format, statement=statement)
        converted_data = ExaQue(table_format, group_by='SIZE_CM')
        fruit_price_converter(input_data, converted_data, 'pyramid',
                              length_to_diameter_ratio=Decimal('1.5'))

        # Group fruits by their size and compute price statistics.
        converted_data.group()
        output_format = ['SIZE_CM:DECIMAL(10,2)', 'PRICE:DECIMAL(10,2)']
        output_data = ExaQue(output_format)
        fruit_price_stat2(converted_data, output_data, 'mean')

        for _, row in output_data:
            print(row)


if __name__ == '__main__':
    supermarket_inspection()
