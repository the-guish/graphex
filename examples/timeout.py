import logging
import sys
import time

from graphex.model import DAGData
from graphex.execution import StateManager, ThreadExecutor
from graphex.document import mermaid_link_from_dag

logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.getLogger().setLevel(logging.INFO)


def good_actor(good_input):
    return 'good work'


def slow_actor(good_input):
    for i in range(1, 11):
        time.sleep(1)
        print(f'the slow actor progress is  {i*10}%')
    return 'good work'


dag = DAGData(['some_input'], ['good_behavior', 'bad_behavior'])

dag.step('good', good_actor, ['some_input'], {}, ['good_behavior'])
dag.step('bad', slow_actor, ['some_input'], {}, ['bad_behavior'])

print(mermaid_link_from_dag(dag))

state = StateManager()
executor = ThreadExecutor(dag, state, timeout=3)


output = executor.run_with(some_input='some good input')

print(output)
