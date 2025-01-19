import datetime
import logging
import pytest
import sys
import time

from graphex.model import DAGData
from graphex.execution import StateManager, ThreadExecutor, current_timestamp, RunTimeoutException

logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.getLogger().setLevel(logging.INFO)


def good_actor(good_input):
    return 'good fast work'


def slow_actor(good_input):
    time.sleep(0.2)
    return 'good slow work'


def test_wait_for_in_progress_at_timeout():
    dag = DAGData(['some_input'], ['good_behavior', 'bad_behavior'])

    dag.step('good', good_actor, ['some_input'], {}, ['good_behavior'])
    dag.step('slow', slow_actor, ['some_input'], {}, ['bad_behavior'])

    state = StateManager()
    executor = ThreadExecutor(dag, state, timeout=0.1)

    fast_output, slow_output = executor.run_with(some_input='some good input')

    # we assert all steps where allowed to complete by the gentle timeout
    assert fast_output == 'good fast work'
    assert slow_output == 'good slow work'
    assert executor.timeout_reached

    # we repeat with a higher timeout
    state2 = StateManager()
    executor2 = ThreadExecutor(dag, state2, timeout=0.3)

    fast_output, slow_output = executor2.run_with(some_input='some good input')

    assert fast_output == 'good fast work'
    assert slow_output == 'good slow work'
    assert executor2.timeout_reached is False


def test_run_is_halted_by_timeout():
    dag = DAGData(['some_input'], ['good_behavior', 'bad_behavior'])

    dag.step('slow1', slow_actor, ['some_input'], {}, ['temp1'])
    dag.step('slow2', slow_actor, ['temp1'], {}, ['temp2'])
    dag.step('slow3', slow_actor, ['temp2'], {}, ['temp3'])
    dag.step('slow4', slow_actor, ['temp3'], {}, ['temp4'])
    dag.step('slow5', slow_actor, ['temp4'], {}, ['temp5'])

    state = StateManager()
    executor = ThreadExecutor(dag, state, timeout=0.25)

    with pytest.raises(Exception) as exc_info:
        _ = executor.run_with(some_input='some good input')

    assert isinstance(exc_info.value, RunTimeoutException), 'run should raise timeout exception'

    # timeout will happen when waiting in the queue for slow2 result
    assert state.is_complete('temp2') is True
    # slow3 should not be submitted
    assert state.is_complete('temp3') is False

    assert executor.timeout_reached
