import pytest
import time

from graphex.model import DAGData
from graphex.execution import StateManager, ThreadExecutor


def test_simple_exception():
    def good_actor(good_input):
        for i in range(1, 11):
            time.sleep(0.01)
            print(f'the good actor progress is {i*10}')
        return 'I work as expected for ' + good_input

    def bad_actor(good_input):
        if good_input:
            raise RuntimeError('I do not')
        return 'I work as expected for ' + good_input

    dag = DAGData(['some_input'], ['good_behavior', 'bad_behavior'])

    dag.step('good', good_actor, ['some_input'], {}, ['good_behavior'])
    dag.step('bad', bad_actor, ['some_input'], {}, ['bad_behavior'])

    state = StateManager()
    executor = ThreadExecutor(dag, state)

    with pytest.raises(Exception) as exc_info:
        _ = executor.run_with(some_input='some good input')

    assert str(exc_info.value) == "step StepID(step_name='bad', index=None) ended with exception \"I do not\""
