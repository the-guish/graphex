import concurrent.futures
import multiprocessing as mp
import random
import sys
import time

from collections import defaultdict, namedtuple
from threading import Thread
from typing import Dict, List, Set

StepDefinition = namedtuple('StepDefinition', 'step_name inputs func outputs is_map func_args func_kwargs')

VarReadySignal = namedtuple('VarReadySignal', 'var_name index')
StepFailedSignal = namedtuple('StepFailedSignal', 'step_name error_message')

StepInstance = namedtuple('StepInstance', 'step_name index')


class ProcessingCollection:
    def __init__(self, size: int):
        self.vars = [None] * size
        self.ready = [False] * size

    def is_ready(self, index):
        return self.ready[index]

    def put(self, index, val):
        self.vars[index] = val
        self.ready[index] = True

    def __getitem__(self, index):
        if not self.ready[index]:
            raise RuntimeError('element not ready')
        return self.vars[index]

    def are_all_ready(self):
        return all(self.ready)

    def to_list(self):
        return self.vars

    def __len__(self):
        return len(self.vars)


def validate_list_of(variable_name, variable, expected_type):
    assert isinstance(variable, list), f'{variable_name} should be a list'
    assert all(isinstance(e, expected_type) for e in variable), 'dag_inputs should be a list of strings'

class MiniDag():
    def __init__(self, dag_inputs, dag_outputs):
        validate_list_of('dag_inputs', dag_inputs, str)
        validate_list_of('dag_outputs', dag_outputs, str)

        self.dag_inputs = dag_inputs
        self.dag_outputs = dag_outputs
        self.map_inputs_to_tasks = defaultdict(list)
        self.steps: Dict[str, StepDefinition] = {}
        self.vars = {}
        self.collections = {}
        self.started_steps: Set[StepInstance] = set()

    def add_step(self, name, func, outputs, fn_args, fn_kwargs):
        validate_list_of('outputs', outputs, str)
        if name in self.steps:
            raise ValueError('a step with name {name} was already added')
        
        # TODO we could inspect the function and validate the parameters names

        inputs = list(set(fn_args + list(fn_kwargs.values())))
        step = StepDefinition(name, inputs, func, outputs, False, fn_args, fn_kwargs)
        self.steps[name] = step
        for one_param in inputs:
            self.map_inputs_to_tasks[one_param].append(step)

    def add_mapping_step(self, name, inputs, func, outputs):
        if name in self.steps:
            raise ValueError('a step with name {name} was already added')
        # TODO we could inspect the function and validate the parameters names
        step = StepDefinition(name, inputs, func, outputs, True, [], {})
        self.steps[name] = step
        for one_param in inputs:
            self.map_inputs_to_tasks[one_param].append(step)

    def run_for(self, **kwargs):
        self.walk_from_inputs()
        # create queue
        self.main_queue = mp.Queue()

        # put inputs in queue
        for k, v in kwargs.items():
            self.vars[k] = v
            self.main_queue.put(VarReadySignal(k, None))

        # TODO: start steps without args

        while True:
            new_signal = self.main_queue.get()
            print('received', new_signal)

            if isinstance(new_signal, StepFailedSignal):
                raise RuntimeError(f'{new_signal.step_name} failed {new_signal.error_message}')

            var_ready: VarReadySignal = new_signal
            # TODO receive errors from threads in addition to VarReadySignal

            if var_ready.var_name in self.collections and self.collections[var_ready.var_name].are_all_ready():
                self.vars[var_ready.var_name] = self.collections[var_ready.var_name].to_list()

            # are outputs ready ? break
            if all(o in self.vars for o in self.dag_outputs):
                break

            dependant_steps = self.map_inputs_to_tasks[var_ready.var_name]

            for step in dependant_steps:

                self.evaluate_step_start(step, var_ready)

        outputs = [self.vars[e] for e in self.dag_outputs]
        if len(outputs) == 1:
            return outputs[0]
        return outputs

    def evaluate_step_start(self, step: StepDefinition, var_ready: VarReadySignal):

        for one_arg in step.inputs:
            if one_arg not in self.vars and one_arg not in self.collections and not step.is_map:
                return False

        # does step started ? skip
        print(f'evaluate start {step.step_name}')
        if step.is_map:
            self.evaluate_step_start_map(step, var_ready)
        else:
            self.evaluate_step_start_single(step)

    def evaluate_step_start_single(self, step: StepDefinition):
        step_instance = StepInstance(step.step_name, None)
        for one_arg in step.inputs:
            if one_arg not in self.vars:
                return False
        if step_instance in self.started_steps:
            return False
        self.submit_step_instance_single(step_instance)
        self.started_steps.add(step_instance)

    def evaluate_step_start_map(self, step: StepDefinition, var_ready: VarReadySignal):
        for one_arg in step.inputs:
            if one_arg not in self.vars and one_arg not in self.collections:
                return False
        
        if step.inputs[0] in self.vars:
            input_list = self.vars[step.inputs[0]]
        elif step.inputs[0] in self.collections:
            input_list = self.collections[step.inputs[0]]
        else:
            raise RuntimeError()

        if var_ready.index is None:
            eval_indexes = range(len(input_list))
        else:
            eval_indexes = [var_ready.index]

        for index in eval_indexes:
            step_instance = StepInstance(step.step_name, index)
            if step_instance in self.started_steps:
                continue

            if isinstance(input_list, ProcessingCollection):
                if not input_list.is_ready(index):
                    continue

            self.submit_step_instance_map(step_instance)
            self.started_steps.add(step_instance)

    def submit_step_instance_single(self, step_instance: StepInstance):
        step = self.steps[step_instance.step_name]

        def run_step():
            try:
                args = [self.vars[one_arg] for one_arg in step.func_args]
                kwargs = {arg_name: self.vars[fill_var] for arg_name, fill_var in step.func_kwargs.items()}
                raw_results = step.func(*args, **kwargs)
                if len(step.outputs) == 1:
                    raw_results = [raw_results]

                assert len(step.outputs) == len(raw_results)
                for name, val in zip(step.outputs, raw_results):
                    self.vars[name] = val
                    self.main_queue.put(VarReadySignal(name, None))
            except Exception as e:
                self.main_queue.put(StepFailedSignal(step_instance.step_name, str(e)))

        t = Thread(target=run_step)
        t.start()
        print(f'submitted {step_instance}')

    def submit_step_instance_map(self, step_instance: StepInstance):
        print('starting submit_step_instance_map', step_instance)
        step = self.steps[step_instance.step_name]

        if step.outputs[0] not in self.collections:
            if step.inputs[0] in self.vars:
                input_list = self.vars[step.inputs[0]]
                new_size = len(input_list)
            else:
                input_list = self.collections[step.inputs[0]]
                new_size = len(input_list)
            self.collections[step.outputs[0]] = ProcessingCollection(new_size)

        def run_step():
            input_list_name = step.inputs[0]
            if input_list_name in self.vars:
                arg0 = self.vars[input_list_name][step_instance.index]
            else:
                arg0 = self.collections[input_list_name][step_instance.index]
            # TODO expand to add non mapping args
            args = [arg0]
            kwargs = {one_arg: self.vars[one_arg] for one_arg in step.inputs[1:]}
            raw_result = step.func(*args, **kwargs)
            output_var_name = step.outputs[0]
            self.collections[output_var_name].put(step_instance.index, raw_result)
            self.main_queue.put(VarReadySignal(output_var_name, step_instance.index))

        t = Thread(target=run_step)
        t.start()
        print(f'submitted map {step_instance}')

    def walk_from_inputs(self):
        seen = set()
        complete_steps = set()
        seen.update(self.dag_inputs)
        found_new = True
        while found_new:
            found_new = False
            for step in self.steps.values():
                if step.step_name in complete_steps:
                    continue
                if all(var_name in seen for var_name in step.inputs):
                    complete_steps.add(step.step_name)
                    seen.update(step.outputs)
                    found_new = True

        for step in self.steps.values():
            if not all(var_name in seen for var_name in step.inputs):
                print(f'step "{step.step_name}" is not reachable')
                print(step)
                print('seen', seen)
                raise ValueError()
        for output in self.dag_outputs:
            if output not in seen:
                print(f'output "{output}" is not reachable')
                raise ValueError()
# SAMPLE FUNCTIONS


def add_two(input_a):
    temp_b = input_a + 2
    return temp_b


def fan_out(temp_b):
    if temp_b == 3:
        fanout_factor = 3
    else:
        fanout_factor = random.randint(1, 6)

    list_of_temp_c = [temp_b + 1] * fanout_factor

    return list_of_temp_c


def minus_one(temp_c):
    temp_d = temp_c - 1
    return temp_d


def by_ten(a_parameter):
    temp_e = a_parameter * 10
    temp_h = a_parameter * 10 * 10
    return temp_e, temp_h


def sum_all(list_of_temp_g):
    final_f = sum(list_of_temp_g)
    time.sleep(3)
    print('sum_all', list_of_temp_g, ' => ', final_f)
    return final_f


def compute_score(temp_e, temp_f, temp_h):
    score = temp_e + temp_f + temp_h
    explanation = f'score components E: {temp_e} F:{temp_f} G:{temp_h}'
    return score, explanation


def sync_processing():
    input_a = 1
    temp_b = add_two(input_a)
    list_of_temp_c = fan_out(temp_b)
    list_of_temp_d = map(minus_one, list_of_temp_c)
    list_of_temp_g = map(minus_one, list_of_temp_d)
    temp_e, temp_h = by_ten(temp_b)
    temp_f = sum_all(list_of_temp_g)
    score, explanation = compute_score(temp_e, temp_f, temp_h)
    print(score, explanation)


def explicit_api():
    mini_dag = MiniDag(
        dag_inputs=['input_a'],
        dag_outputs=['score', 'explanation']
    )

    mini_dag.add_step(
        'a_plus_two',
        add_two,
        outputs=['temp_b'],
        fn_args=[],
        fn_kwargs={
            'input_a': 'input_a'
        }
    )

    mini_dag.add_step('trifurcate', fan_out, ['list_of_temp_c'], [], {'temp_b': 'temp_b'})

    mini_dag.add_mapping_step('list_minus_one', ['list_of_temp_c'], minus_one, ['list_of_temp_d'])

    mini_dag.add_mapping_step('list_minus_one_again', ['list_of_temp_d'], minus_one, ['list_of_temp_g'])

    mini_dag.add_step('by_ten', by_ten, ['temp_e', 'temp_h'], [], {'a_parameter': 'temp_b'})

    mini_dag.add_step('sum_all', sum_all, ['temp_f'], [], {'list_of_temp_g': 'list_of_temp_g'})

    mini_dag.add_step('compute_score', compute_score, ['score', 'explanation'], ['temp_e', 'temp_f', 'temp_h'], {})

    score, explanation = mini_dag.run_for(input_a=1)
    print(score, explanation)


class AnonVar:
    def __init__(self, name):
        self.name = name


class AnonInput:
    def __init__(self, name, value):
        self.name = name
        self.value = value


class AnonStep:
    def __init__(self, dag, name, func, is_map):
        self.dag = dag
        self.name = name
        self.func = func
        self.is_map = is_map
        self.output_count = 1

    def returning(self, output_count):
        self.output_count = output_count
        return self

    def apply_to(self, *args, **kwargs):

        self.func_args = []
        for arg in args:
            if not isinstance(arg, AnonVar) and not isinstance(arg, AnonInput):
                new_input = self.dag.add_input(arg)
                self.func_args.append(new_input)
            else:
                self.func_args.append(arg)

        self.func_kwargs = {}
        for arg_name, arg_value in kwargs.items():
            if not isinstance(arg_value, AnonVar) and not isinstance(arg_value, AnonInput):
                new_input = self.dag.add_input(arg_value)
                self.func_kwargs[arg_name] = new_input
            else:
                self.func_kwargs[arg_name] = arg_value

        self.output_refs = [AnonVar(f'{self.name}_output_{i+1}') for i in range(self.output_count)]

        if self.output_count == 1:
            return self.output_refs[0]
        return self.output_refs

    def get_output_names(self):
        return [e.name for e in self.output_refs]

    def get_func_args(self):
        return [e.name for e in self.func_args]

    def get_func_kwargs(self):
        return {k: v.name for k, v in self.func_kwargs.items()}


class AnonymousDag:
    def __init__(self):
        self.steps: List[AnonStep] = []
        self.inputs: List[AnonInput] = []

    def step(self, func) -> AnonStep:
        auto_name = f'step_{len(self.steps)+1}'
        step = AnonStep(self, auto_name, func, False)
        self.steps.append(step)
        return step

    def map(self, func) -> AnonStep:
        auto_name = f'map_step_{len(self.steps)+1}'
        step = AnonStep(self, auto_name, func, True)
        self.steps.append(step)
        return step

    def add_input(self, value):
        auto_name = f'input_{len(self.inputs)+1}'
        new_input = AnonInput(auto_name, value)
        self.inputs.append(new_input)
        return new_input

    def compute(self, *args):
        input_names = [e.name for e in self.inputs]
        output_names = [var.name for var in args]
        # TODO: do we need declare dag outputs?
        self.exec_dag = MiniDag(input_names, output_names)

        for step in self.steps:
            if step.is_map:
                inputs = [var.name for var in step.func_kwargs.values()]
                print('map inputs', inputs)

                self.exec_dag.add_mapping_step(step.name, inputs, step.func, step.get_output_names() )
            else:
                self.exec_dag.add_step(step.name, step.func, step.get_output_names(), step.get_func_args(), step.get_func_kwargs())

        self.exec_dag.walk_from_inputs()
        dag_inputs = {e.name: e.value for e in self.inputs}
        raw_output = self.exec_dag.run_for(**dag_inputs)

        if len(raw_output) == 1:
            return raw_output[0]

        return raw_output


def fluent_api2():
    dag = AnonymousDag()

    input_a = 1
    temp_b = dag.step(add_two).apply_to(input_a)
    list_of_temp_c = dag.step(fan_out).apply_to(temp_b)
    list_of_temp_d = dag.map(minus_one).apply_to(list_of_temp_c=list_of_temp_c)
    list_of_temp_g = dag.map(minus_one).apply_to(list_of_temp_d=list_of_temp_d)
    temp_e, temp_h = dag.step(by_ten).returning(2).apply_to(temp_b)
    temp_f = dag.step(sum_all).apply_to(list_of_temp_g)
    output_score, output_explanation = dag.step(compute_score).returning(2).apply_to(temp_e, temp_f, temp_h)

    score, explanation = dag.compute(output_score, output_explanation)
    print(score, explanation)


def using_futures():
    input_a = 1

    # Create a ThreadPoolExecutor to run functions concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:

        future_vars = {}
        d_futures = []
        g_futures = []
        final_stage_queue = mp.Queue()

        future_vars['B'] = executor.submit(add_two, input_a)

        def b_ready(future_b):
            future_vars['C'] = executor.submit(fan_out, future_b.result())
            future_vars['E_H'] = executor.submit(by_ten, future_b.result())
            final_stage_queue.put('processing e_h')

        future_vars['B'].add_done_callback(b_ready)

        def c_ready(future_c):

            def run_first_minus_submit_second(c_element):
                d = minus_one(c_element)
                g_element_future = executor.submit(minus_one, d)
                g_futures.append(g_element_future)

            for c_element in future_c.result():
                d_element_future = executor.submit(run_first_minus_submit_second, c_element)
                d_futures.append(d_element_future)

            concurrent.futures.wait(g_futures)
            concurrent.futures.wait(d_futures)

            list_of_g = list(g.result() for g in g_futures)
            future_vars['F'] = executor.submit(sum_all, list_of_g)
            final_stage_queue.put('processing f')

        future_vars['C'].add_done_callback(c_ready)

        while 'F' not in future_vars or 'E_H' not in future_vars:
            final_stage_queue.get()

        temp_e, temp_h = future_vars['E_H'].result()
        temp_f = future_vars['F'].result()

        score, explanation = compute_score(temp_e, temp_f, temp_h)
        print(score, '|', explanation)




if __name__ == '__main__':
    # sync_processing()
    #explicit_api()
    # fluent_api2()
    #using_futures()


