import functools
import inspect

from graphex.analysis import parse
from graphex.lib_logger import logger
from graphex.model import DAGData, DagStepData
from graphex.execution import ThreadExecutor, StateManager
from pprint import pprint
from typing import TypeVar

current_dag: DAGData = None


class VarPlaceHolder:
    def __init__(self, name):
        self.name = name


class Step:
    def __init__(self, func, output_count=1):
        self.func = func
        self.output_count = output_count

    def __call__(self, *args, **kwargs):
        if any(isinstance(v, VarPlaceHolder) for v in (list(args) + list(kwargs.values()))):
            return None
        result = self.func(*args, **kwargs)
        return result


class DAG:
    def __init__(self, func):
        self.func = func

        # TODO: may inspect the function to derive names. outside "transparent", in "inspect_flow"
        """
        signature = inspect.signature(func)
        print(signature)
        lineno = inspect.getsourcelines(func)[1]
        print('declared at line', lineno)
        source_code = inspect.getsource(func)
        print(source_code)
        """

    def __call__(self, *args, **kwargs):
        result = self.func(*args, **kwargs)
        return result

    def build_dag(self):

        inputs = list(inspect.signature(self.func).parameters)
        outputs = []
        global current_dag
        current_dag = DAGData(inputs, outputs)

        # we create a placeholder version of args and kwargs
        placeholders_args = [VarPlaceHolder(iname) for iname in inputs]
        placeholder_outputs = self.func(*placeholders_args)
        if isinstance(placeholder_outputs, VarPlaceHolder):
            placeholder_outputs = tuple(placeholder_outputs)
        elif not isinstance(placeholder_outputs, list) and not isinstance(placeholder_outputs, tuple):
            raise ValueError(f'Unsupported return type for {self.func.__name__}')

        for output_var_placeholder in placeholder_outputs:
            current_dag.outputs.append(output_var_placeholder.name)

        try:
            named_dag = add_names_from_ast(self.func, current_dag)
        except Exception as e:
            logger.exception('excpetion when calling add_names_from_ast')
            logger.exception(e)
            named_dag = current_dag

        current_dag = None
        return named_dag

    def parallelize(self, *args, **kwargs):
        dag = self.build_dag()
        # pprint(current_dag.data())
        state = StateManager()
        executor = ThreadExecutor(dag, state)
        result = executor.run_with(*args, **kwargs)
        return result

def new_placeholders(func, step_args, step_kwargs, output_count):
    # TODO use step_args parameter to support positional arguments
    # TODO: look for a better way. singleton, context?
    assert current_dag, 'no DAG being built'

    step_name = f'{len(current_dag.steps)}_{func.__name__}'
    string_args = [v.name for v in step_args]
    string_kwargs = {k: v.name for k, v in step_kwargs.items()}
    output_names = [f'{step_name}-output_{i+1}' for i in range(output_count)]
    outputs = [VarPlaceHolder(name) for name in output_names]
    current_dag.step(
        name=step_name,
        function=func,
        arg_var_names=string_args,
        string_keyword_args=string_kwargs,
        outputs=output_names
    )

    if output_count == 1:
        return outputs[0]
    return outputs


def new_map_placeholders(func, iterable_placeholder: VarPlaceHolder):
    # TODO: look for a better way. singleton, context?
    assert current_dag, 'no DAG being built'

    step_name = f'{len(current_dag.steps)}_{func.__name__}'
    output_name = f'{step_name}-output_1'
    output = VarPlaceHolder(output_name)
    current_dag.map(
        name=step_name,
        function=func,
        arg_var_names=[iterable_placeholder.name],
        string_keyword_args={},
        outputs=[output_name]
    )
    # print(f'added step map {step_name}')

    return output


# For a detailed explanation of this pattern see: 
# https://realpython.com/primer-on-python-decorators/#creating-decorators-with-optional-arguments
def step(_func=None, output_count=1):

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            all_args = [isinstance(v, VarPlaceHolder) for v in list(args) + list(kwargs.values())]
            if all(all_args):
                return new_placeholders(func, args, kwargs, output_count)
            elif not any(all_args):
                return func(*args, **kwargs)
            else:
                return ValueError('mixed run mode')
        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def dag(_func=None):

    def decorator(func):
        return DAG(func)

    if _func is None:
        return decorator
    else:
        return decorator(_func)


# TODO typing hints
def gxmap(func, iterable):
    if isinstance(iterable, VarPlaceHolder):
        return new_map_placeholders(func, iterable)
    else:
        return map(func, iterable)


def add_names_from_ast(func, logical_dag: DAGData) -> DAGData:
    problem, inputs, assignments, returning = parse(func)

    if problem:
        raise RuntimeError(f'parse error {problem}')
    assert len(inputs) == len(logical_dag.inputs), 'length of detected inputs do not match'
    assert len(assignments) == len(logical_dag.steps), 'length of detected steps do not match'
    assert len(returning) == len(logical_dag.outputs), 'length of detected outputs do not match'
    name_map = {}

    dag = DAGData(inputs=inputs, outputs=returning)

    # TODO
    step_data: DagStepData
    for parsed_line, step_data, i in zip(assignments, logical_dag.steps.values(), range(len(assignments))):
        logical = step_data.data()
        # print(parsed_line)
        # print(step_data.data(), '\n\n')

        if step_data.is_map:
            parsed_function_name = parsed_line['args'][0]
            pos_args = parsed_line['args'][1:]
        else:
            parsed_function_name = parsed_line['function']
            pos_args = parsed_line['args']

        assert logical['function'].endswith(parsed_function_name), (
            f'function names do not match at pos {i} lineno {parsed_line["lineno"]}: '
            f'{logical["function"]} vs {parsed_function_name}'
        )

        assert len(logical['arg_var_names']) == len(pos_args), 'Length of positional arguments do not match'
        for larg, parg in zip(logical['arg_var_names'], pos_args):
            if larg in name_map and name_map[larg] != parg:
                raise RuntimeError(f'Could not match arguments at {parsed_line["lineno"]}')
            if larg not in name_map:
                name_map[larg] = parg

        for lpair, ppair in zip(logical['string_keyword_args'].items(), parsed_line['kwargs'].items()):
            lkey, larg = lpair
            pkey, parg = ppair
            if lkey != pkey:
                # TODO: use specific exception
                raise RuntimeError(f'Argument keys do not match at {parsed_line["lineno"]}: {lkey} vs {pkey}')
            if larg in name_map and name_map[larg] != parg:
                raise RuntimeError(f'Could not match arguments at {parsed_line["lineno"]}')
            if larg not in name_map:
                name_map[larg] = parg

        assert len(logical['outputs']) == len(parsed_line['assigned']), 'step output length does not match'
        for logical_output_var, parsed_output_var in zip(logical['outputs'], parsed_line['assigned']):
            if logical_output_var in name_map and name_map[logical_output_var] != parsed_output_var:
                raise RuntimeError(f'Could not match arguments at {parsed_line["lineno"]}')
            if logical_output_var not in name_map:
                name_map[logical_output_var] = parsed_output_var

        if step_data.is_map:
            dag.map(
                name=step_data.name,
                function=step_data.function,
                arg_var_names=pos_args,
                string_keyword_args=parsed_line['kwargs'],
                outputs=parsed_line['assigned']
            )
        else:
            dag.step(
                name=step_data.name,
                function=step_data.function,
                arg_var_names=pos_args,
                string_keyword_args=parsed_line['kwargs'],
                outputs=parsed_line['assigned']
            )

    return dag
