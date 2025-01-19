import ast
import inspect

from dataclasses import dataclass


@dataclass(frozen=True)
class Problem:
    lineno: int
    message: str


def parse(func):
    inputs = inspect.signature(func).parameters
    starting_lineno = inspect.getsourcelines(func)[1]
    tree = ast.parse(inspect.getsource(func))
    # TODO: change return signature to: error, assignments, returning
    # in error report the line number and a description

    assert isinstance(tree.body[0], ast.FunctionDef)

    assignments = []
    for node in tree.body[0].body:
        lineno = starting_lineno + node.lineno - 1
        name_of_assigned_var = None
        name_of_called_function = None
        name_of_arguments = None
        kwargs = None
        if isinstance(node, ast.Assign):

            for target in node.targets:
                if isinstance(target, ast.Name):
                    name_of_assigned_var = [target.id]
                elif isinstance(target, ast.Tuple):
                    name_of_assigned_var = [var.id for var in target.elts]
                else:
                    problem = Problem(lineno, 'unknown left side of assignment')
                    return problem, None, None, None

                if isinstance(node.value, ast.Call):
                    if isinstance(node.value.func, ast.Name):
                        name_of_called_function = node.value.func.id
                    elif isinstance(node.value.func, ast.Attribute):
                        name_of_called_function = f'{node.value.func.value.id}.{node.value.func.attr}'
                        if name_of_called_function != 'gx.gxmap':
                            problem = Problem(lineno, f'unsupported syntax for function {name_of_called_function}')
                            return problem, None, None, None
                    else:
                        name_of_called_function = 'idk'
                    name_of_arguments = []
                    for arg in node.value.args:
                        assert isinstance(arg, ast.Name)
                        name_of_arguments.append(arg.id)

                    kwargs = {}
                    for kw in node.value.keywords:
                        kwargs[kw.arg] = kw.value.id

                    assignment = {
                        'lineno': starting_lineno + node.lineno - 1,
                        'assigned': name_of_assigned_var,
                        'function': name_of_called_function,
                        'args': name_of_arguments,
                        'kwargs': kwargs
                    }

                    assignments.append(assignment)

        elif isinstance(node, ast.Return):
            returning = []
            if isinstance(node.value, ast.Tuple):
                for var in node.value.elts:
                    returning.append(var.id)

            elif isinstance(node.value, ast.Name):
                returning.append(node.value.id)
        else:
            raise ValueError(type(node))

    return None, inputs, assignments, returning
