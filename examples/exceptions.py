import logging
import sys
import time

from graphex.model import DAGData
from graphex.execution import StateManager, ThreadExecutor

logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.getLogger().setLevel(logging.INFO)


def good_actor(good_input):
    for i in range(1, 11):
        time.sleep(1)
        print(f'a good actor was working by {i} seconds')
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


output = executor.run_with(some_input='some good input')
