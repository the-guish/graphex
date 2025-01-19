import datetime
import multiprocessing as mp
import queue


from dataclasses import dataclass
from graphex.model import DAGData, DagStepData
from graphex.lib_logger import logger
from typing import Dict, List, Tuple, Optional
from threading import Thread


# TODO: chose how to setup config
CONFIG_REPORT_TERMINATING_THREADS_EVERY_X_SECONDS = 3


def current_timestamp() -> float:
    return datetime.datetime.now(datetime.timezone.utc).timestamp()


@dataclass(frozen=True)
class StepID():
    step_name: str
    index: int


@dataclass
class StepInstance():
    step_id: StepID
    thread: Thread
    start_timestamp: Optional[float] = None
    end_timestamp: Optional[float] = None
    success: Optional[bool] = None


class RunTimeoutException(Exception):
    pass


class StateManager:
    def __init__(self):
        self.__completeness_dict = {}
        self.__values_dict = {}
        self.__collections = {}
        self.__items__completeness_dict = {}

    def is_complete(self, var_name: str):
        return var_name in self.__completeness_dict

    def all_complete(self, var_names: List[str]) -> bool:
        return all([self.is_complete(v) for v in var_names])

    def put(self, var_name: str, value):
        self.__values_dict[var_name] = value
        self.__completeness_dict[var_name] = True

    def get(self, var_name: str):
        if not self.is_complete(var_name):
            raise RuntimeError(f'variable {var_name} was not set')
        return self.__values_dict[var_name]

    def init_collection(self, var_name: str, size: int):
        self.__collections[var_name] = [None] * size
        self.__items__completeness_dict[var_name] = [False] * size

    def collection_in_progress(self, var_name: str):
        return var_name in self.__items__completeness_dict

    def item_is_complete(self, var_name: str, index: int):
        if self.is_complete(var_name):
            return True

        if var_name not in self.__items__completeness_dict:
            return False
        return self.__items__completeness_dict[var_name][index]

    def put_item(self, var_name: str, index: int, value):
        self.__collections[var_name][index] = value
        self.__items__completeness_dict[var_name][index] = True
        if all(self.__items__completeness_dict[var_name]):
            self.put(var_name, self.__collections[var_name])

    def get_item(self, var_name: str, index: int):
        if not self.item_is_complete(var_name, index):
            raise RuntimeError(f'collection {var_name} item {index} was not set')
        if self.is_complete(var_name):
            return self.__values_dict[var_name][index]
        return self.__collections[var_name][index]

    def get_len(self, var_name) -> int:
        if self.is_complete(var_name):
            return len(self.get(var_name))

        if self.collection_in_progress(var_name):
            return len(self.__collections[var_name])

        raise ValueError('no list or collection')


class ThreadExecutor:
    def __init__(self, dag: DAGData, state: StateManager, timeout: Optional[float] = None):
        self.dag = dag
        self.timeout = timeout
        self.state = state

    def run_with(self, **kwargs):
        self.main_queue = mp.Queue()
        self.pending_steps = set([StepID(step_name, None) for step_name, step_data in self.dag.steps.items() if not step_data.is_map])
        self.pending_map_ops = {step_data.name: step_data for step_data in self.dag.steps.values() if step_data.is_map}
        self.step_instances: Dict[StepID, StepInstance] = {}
        self.start_time = current_timestamp()
        self.timeout_reached = False

        for k, v in kwargs.items():
            self.state.put(k, v)

        self.find_and_submit()

        finished = False
        while not finished:

            new_msg = self.wait_for_new_message()

            finished = self.process_step_complete(new_msg)

        results = [self.state.get(var_name) for var_name in self.dag.outputs]

        if len(results) == 1:
            return results[0]
        return results

    def process_step_complete(self, message):
        step_id = StepID(message['step_name'], message['step_index'])

        si: StepInstance = self.step_instances[step_id]

        si.start_timestamp = message['start_timestamp']
        si.end_timestamp = message['end_timestamp']
        si.success = message['success']

        if not si.success:

            message = f'step {step_id} ended with exception "{message.get("exception")}"'
            logger.exception(message)
            self.wait_for_inprogress_to_finish()
            raise RuntimeError(message)

        if self.state.all_complete(self.dag.outputs):
            return True

        if self.timeout_reached:
            raise RunTimeoutException()

        self.find_and_submit()

        return False

    def find_and_submit(self):
        ready_steps = self.find_ready_to_run_steps()

        for step_id in ready_steps:
            self.submit_step(step_id)

    def find_ready_to_run_steps(self) -> List[Tuple[str, int]]:

        steps_to_expand = []
        map_op: DagStepData
        for map_op in self.pending_map_ops.values():
            # TODO support multiple map inputs
            single_map_input = map_op.arg_var_names[0]
            if self.state.is_complete(single_map_input) or self.state.collection_in_progress(single_map_input):
                new_size = self.state.get_len(single_map_input)
                steps_to_expand.append((map_op.name, new_size))


        for step_name, new_size in steps_to_expand:
            for i in range(new_size):
                self.pending_steps.add(StepID(step_name, i))
            self.pending_map_ops.pop(step_name)
            for output_name in self.dag.steps[step_name].outputs:
                self.state.init_collection(output_name, new_size)

        ready_steps = []
        for step_id in self.pending_steps:
            step: DagStepData = self.dag.steps[step_id.step_name]
            input_names = step.arg_var_names + list(step.string_keyword_args.values())
            if not step.is_map:
                if self.state.all_complete(input_names):
                    ready_steps.append(step_id)
                continue

            if all([self.state.item_is_complete(v, step_id.index) for v in step.arg_var_names]):
                ready_steps.append(step_id)

        return ready_steps

    def submit_step(self, step_id: StepID):
        step: DagStepData = self.dag.steps[step_id.step_name]

        def execute_step():
            step_args = []
            step_kwargs = {}
            if step.is_map:
                step_args = [self.state.get_item(a, step_id.index) for a in step.arg_var_names]
            else:
                step_args = [self.state.get(a) for a in step.arg_var_names]

            for param, var_name in step.string_keyword_args.items():
                step_kwargs[param] = self.state.get(var_name)
            # TODO: handle exceptions
            start_time = current_timestamp()
            try:
                run_outputs_raw = step.function(*step_args, **step_kwargs)
                end_time = current_timestamp()
            except Exception as e:
                end_time = current_timestamp()
                logger.exception(e)
                self.main_queue.put({
                    'step_name': step.name,
                    'step_index': step_id.index,
                    'success': False,
                    'start_timestamp': start_time,
                    'end_timestamp': end_time,
                    'exception': str(e)
                })
                return

            if len(step.outputs) == 1:
                run_outputs = [run_outputs_raw]
            else:
                run_outputs = run_outputs_raw

            assert len(run_outputs) == len(step.outputs), 'step output len mismatch'

            for var_name, val in zip(step.outputs, run_outputs):
                if step_id.index is not None:
                    self.state.put_item(var_name, step_id.index, val)
                else:
                    self.state.put(var_name, val)

            self.main_queue.put({
                'step_name': step.name,
                'step_index': step_id.index,
                'success': True,
                'start_timestamp': start_time,
                'end_timestamp': end_time
            })

        t = Thread(target=execute_step)
        t.start()
        si = StepInstance(
            step_id=step_id,
            thread=t
        )
        self.step_instances[step_id] = si

        self.pending_steps.remove(step_id)

    def active_threads_count(self):
        return sum(int(si.thread.is_alive()) for si in self.step_instances.values())

    def wait_for_new_message(self):
        try:
            return self.main_queue.get(timeout=0.01)
        except queue.Empty:
            pass

        while self.timeout is None or not self.check_timeout_is_reached():
            active_threads_before = self.active_threads_count()

            # if timeout requested by the user is lower than 1 second we need to use that in queue.get
            time_to_wait_for_queue = 1
            if self.timeout and self.timeout < time_to_wait_for_queue:
                time_to_wait_for_queue = self.timeout

            try:
                message = self.main_queue.get(timeout=time_to_wait_for_queue)
                self.check_timeout_is_reached()
                return message
            except queue.Empty:
                if active_threads_before == 0:
                    raise RuntimeError('No active threads running and no message to process. '
                                       f'This is likely a bug in {self.__module__}')

        self.check_timeout_is_reached()
        self.wait_for_inprogress_to_finish()
        try:
            return self.main_queue.get(timeout=0.01)
        except queue.Empty:
            raise RuntimeError('No new messages after current threads finished. '
                               f'This is likely a bug in {self.__module__}')

    def elapsed_time(self):
        seconds = current_timestamp() - self.start_time
        return seconds

    def check_timeout_is_reached(self):
        if self.timeout is None:
            return False

        check = self.elapsed_time() >= self.timeout
        if check and not self.timeout_reached:
            logger.info('timeout reached')
            self.timeout_reached = True

        return check

    def wait_for_inprogress_to_finish(self):
        logger.info('waiting for %s threads to finish', self.active_threads_count())
        still_running = True
        while still_running:
            still_running = False
            for si in self.step_instances.values():
                si.thread.join(CONFIG_REPORT_TERMINATING_THREADS_EVERY_X_SECONDS)
                if si.thread.is_alive():
                    logger.info('step with id %s is still running', si.step_id)
                    still_running = True
