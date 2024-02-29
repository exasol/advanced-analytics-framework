from inspect import signature, Parameter, getmodule
import re
from itertools import chain
import functools
from textwrap import dedent
from pyexasol import ExaStatement, ExaConnection

from .code_collector import CodeExtractor
from .exa_que import ExaQue, split_column_def, ExaGroupFormat, ExaRowFormat
from .exa_que_udf import ExaQue as ExaQueUdf


def get_parameter_vector(func, *args, **kwargs) -> list:
    """
    Creates a parameter value vector.
    The elements are taken either from *args, **kwargs or from the default values.
    """

    sig = signature(func)
    sig_parameters = sig.parameters
    values = [None] * len(sig_parameters)

    for i, name in enumerate(sig_parameters.keys()):
        if i < len(args):
            values[i] = args[i]
        elif name in kwargs.keys():
            values[i] = kwargs[name]
        else:
            param_def = sig_parameters[name]
            if param_def.default == Parameter.empty:
                raise TypeError(f'No value for the parameter {name}')
            values[i] = param_def.default

    return values


def quoted_name(name: str) -> str:
    return name if name.startswith('"') else f'"{name}"'


def get_original_group_by(exa_input: ExaQue) -> ExaGroupFormat:
    """Returns the group_by as it was specified in the constractor."""
    if not exa_input.grouped:
        return exa_input.group_by
    elif len(exa_input.row_format) == 2:
        return exa_input.row_format[0]
    return exa_input.row_format[:-1]


def get_original_row_format(exa_input: ExaQue) -> ExaRowFormat:
    """Returns the row_format as it was specified in the constractor."""
    if exa_input.grouped:
        return exa_input.row_format[-1]
    return exa_input.row_format


def interpret_script_type(script_type: str, group_by: ExaGroupFormat) -> tuple[str, bool]:
    script_type = script_type.lower()
    if script_type == 'auto':
        is_scalar = not group_by
        return 'scalar' if is_scalar else 'set', is_scalar
    elif script_type == 'scalar':
        return script_type, True
    elif script_type == 'set':
        return script_type, False
    else:
        raise ValueError(f'Unknown script type {script_type}')


def build_udf_header(udf_name: str, script_type: str, schema: str | None,
                     output_format: ExaRowFormat) -> str:

    def format_column_def(column_def: str) -> str:
        column_name, column_data_type = split_column_def(column_def)
        if column_data_type:
            return f'{quoted_name(column_name)} {column_data_type}'
        return quoted_name(column_def)

    schema_str = f'{quoted_name(schema)}.' if schema else ''
    emit_format = ', '.join(format_column_def(column_def) for column_def in output_format)
    return dedent(f"""
        CREATE OR REPLACE PYTHON3 {script_type.upper()} SCRIPT {schema_str}{quoted_name(udf_name)}(...)
        EMITS ({emit_format}) AS
    """)


def build_udf_run_function(func_name: str, is_scalar: bool, param_values: list,
                           input_format: ExaRowFormat, output_format: ExaRowFormat,
                           group_by: ExaGroupFormat, grouped: bool) -> str:

    def row_format_to_string(row_format: list[str]) -> str:
        return '[' + ', '.join(f"'{column_def}'" for column_def in row_format) + ']'

    if group_by is None:
        group_by_str = None
        num_group_params = 0
    elif isinstance(group_by, str):
        group_by_str = f"'{group_by}'"
        num_group_params = 1
    else:
        group_by_str = row_format_to_string(group_by)
        num_group_params = len(group_by)

    group_start = len(param_values)
    input_start = group_start + num_group_params
    arg_types = [type(val).__name__ for val in param_values]
    type_imports = [f'from {module_name} import {class_name}' for module_name, class_name in
                    [('decimal', 'Decimal'), ('datetime', 'date'), ('datetime', 'datetime')]
                    if class_name in arg_types]
    args_list = ', '.join(f'{arg_type}(ctx[{i}])' for i, arg_type in enumerate(arg_types))
    type_imports = '\n'.join(type_imports)
    group_call = 'inputs.group()\n' if grouped else ''

    return dedent(f"""
        def run(ctx):
            {type_imports}

            input_format = {row_format_to_string(input_format)}
            group_by = {group_by_str}
            output_format = {row_format_to_string(output_format)}
            inputs = ExaQue(ctx, {is_scalar}, input_format, group_by, {group_start}, {input_start})
            outputs = ExaQue(ctx, {is_scalar}, output_format)
            {group_call}
            args = [{args_list}]
            {func_name}(inputs, outputs, *args)
    """)


def build_udf_call(udf_name: str, query: str, param_values: list,
                   input_format: ExaRowFormat, group_by: ExaGroupFormat) -> str:
    """Generates an SQL command that calls the UDF"""

    if group_by:
        if isinstance(group_by, str):
            grouping = [quoted_name(group_by)]
        else:
            grouping = [quoted_name(column_name) for column_name in group_by]
        group_by_clause = f" GROUP BY {', '.join(grouping)}"
    else:
        grouping = []
        group_by_clause = ''

    args = (f"'{arg}'" if isinstance(arg, str) else str(arg) for arg in param_values)
    inputs = (quoted_name(split_column_def(column_def)[0]) for column_def in input_format)
    return dedent(f"""
        WITH data_source AS ({query}) 
        SELECT {quoted_name(udf_name)}({", ".join(chain(args, grouping, inputs))})
        FROM data_source{group_by_clause};
    """)


def execute_udf(func, func_code: str | None, script_type: str, schema: str | None,
                *args, **kwargs):
    """
    Creates the UDF if the function code is provided.

    Builds an SQL command calling the UDF, executes it and assigns the ExaStatement
    to the output data object.
    """

    param_values = get_parameter_vector(func, *args, **kwargs)
    if ((len(param_values) < 2) or (not isinstance(param_values[0], ExaQue)) or
            (not isinstance(param_values[1], ExaQue))):
        raise ValueError('Invalid function signature. '
                         f'The first two parameters must be instances of the {ExaQue.__name__} class')

    exa_input: ExaQue = param_values[0]
    exa_output: ExaQue = param_values[1]
    param_values = param_values[2:]
    input_statement: ExaStatement = exa_input._statement
    connection: ExaConnection = input_statement.connection
    if input_statement is None:
        raise ValueError('The inputs parameter must be linked to an ExaStatement')

    group_by = get_original_group_by(exa_input)
    input_format = get_original_row_format(exa_input)
    func_name = func.__name__
    udf_name = func_name + '_udf'

    if func_code:
        # Create the UDF
        output_format = exa_output.row_format
        script_type, is_scalar = interpret_script_type(script_type, group_by)

        create_query = '\n'.join([
            build_udf_header(udf_name, script_type, schema, output_format),
            func_code,
            build_udf_run_function(func_name, is_scalar, param_values, input_format, output_format,
                                   group_by, exa_input.grouped),
            "/"
        ])

        connection.execute(create_query)

    # Call the UDF
    call_query = build_udf_call(udf_name, input_statement.query, param_values, input_format,
                                group_by)
    exa_output._statement = connection.execute(call_query)


def load_exa_que_udf_code() -> str:

    file_name = getmodule(ExaQueUdf).__file__
    with open(file_name) as f:
        code = f.read()

    # Remove the base class
    pattern = rf'class\s+{ExaQueUdf.__name__}\s*\(\w+\)\s*:'
    simple_class_def = f'class {ExaQueUdf.__name__}:'
    code = re.sub(pattern, simple_class_def, code)

    # Trim everything preceding the class definition
    code = code[code.index(simple_class_def):]
    return code


def remove_decorator_from_code(func, func_code: str) -> str:

    # TODO: Consider using pyparsing or a similar library instead of the regular expression.
    pattern = rf'@{udf.__name__}\s*(?:\(|\n).*(def\s+{func.__name__})'
    match = re.search(pattern, func_code, re.M | re.DOTALL)
    if match is not None:
        cutout_start = match.start(0)
        cutout_end = match.start(1)
        # Check if there is another decorator after this one.
        # In that case move the cut end to the start of the next decorator.
        pattern = rf'@\w+\s*(?:\(|\n)'
        match = re.search(func_code[cutout_start:cutout_end], pattern, re.M)
        if match is not None:
            cutout_end = cutout_start + match.start(0)
        return func_code[:cutout_start] + func_code[cutout_end:]
    return func_code


def get_func_code(func, include_modules: str | None) -> str:

    # Block imports of this module and the ExaQue
    exclude_modules = f'{getmodule(ExaQue).__name__}|{__name__}'
    func_module = getmodule(func)
    extractor = CodeExtractor(func_module.__name__, include_modules=include_modules,
                              exclude_modules=exclude_modules)
    # Add the UDF side of the ExaQue, loading the code from the file.
    extractor.code_blocks.append(load_exa_que_udf_code())

    extractor.extract_object(func, func_module)
    func_code = extractor.get_complete_code()

    # Remove the udf decorator.
    func_code = remove_decorator_from_code(func, func_code)
    return func_code


def udf(f=None, *,
        script_type: str = 'auto',
        schema: str | None = None,
        modules: str | None = None):

    def udf_decorator(func):
        processed = set()

        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            if func not in processed:
                func_code = get_func_code(func, modules)
                processed.add(func)
            else:
                func_code = None

            execute_udf(func, func_code, script_type, schema, *args, **kwargs)
        return wrapper

    if f is None:
        return udf_decorator
    return udf_decorator(f)
