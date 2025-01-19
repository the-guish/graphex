from typing import Dict, List


class DagStepData:
    def __init__(self, name: str, function, is_map: bool, arg_var_names: List[str], string_keyword_args: Dict[str, str], outputs: List[str]):
        self.name = name
        self.function = function
        self.is_map = is_map
        self.arg_var_names = arg_var_names
        self.string_keyword_args = string_keyword_args
        self.outputs = outputs

    def data(self):
        my_data = self.__dict__.copy()
        my_data['function'] = f'{self.function.__module__}.{self.function.__name__}'
        return my_data


class DAGData:

    def __init__(self, inputs: List[str], outputs: List[str]):
        self.inputs = inputs
        self.outputs = outputs
        self.steps: Dict[str, DagStepData] = {}

    def step(self, name, function, arg_var_names, string_keyword_args, outputs):
        if name in self.steps:
            raise ValueError(f'A step with name {name} already exists')
        self.steps[name] = DagStepData(name, function, False, arg_var_names, string_keyword_args, outputs)

    def map(self, name, function, arg_var_names, string_keyword_args, outputs):
        if name in self.steps:
            raise ValueError(f'A step with name {name} already exists')
        self.steps[name] = DagStepData(name, function, True, arg_var_names, string_keyword_args,
                                       outputs)

    def data(self):
        props = self.__dict__.copy()
        props['steps'] = {step_name: step.data() for step_name, step in self.steps.items()}
        return props
