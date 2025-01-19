import time
from graphex.execution import DAGData, StateManager, ThreadExecutor


def fanout(input_a):
    return list(range(4))


def slow_for_1(arg1):
    if arg1 == 1:
        time.sleep(0.2)
    print('slow_for_1', arg1)
    return arg1


def slow_for_3(arg1):
    if arg1 == 3:
        time.sleep(0.2)
    print('slow_for_3', arg1)
    return arg1


def sum_all(list_val):
    print('sum_all begins', list_val)
    return sum(list_val)


def test_mapping_stages():
    """
    This test ensures the second map operation starts executing each one of their steps as soon as the needed item for 
    that specific step is available.

    It aims to prevent that map operations are serialized one after the other as a whole, eg. make map2 wait until all
    items in map1 are processed.

    """
    dag = DAGData(
        inputs=['input_a'],
        outputs=['total']
    )

    dag.step('fanout', fanout, [], {'input_a': 'input_a'}, ['list_of_b'])

    dag.map('map1', slow_for_1,  ['list_of_b'], {}, ['list_of_c'])

    dag.map('map2', slow_for_3, ['list_of_c'], {}, ['list_of_d'])

    dag.step('sum_all', sum_all, [], {'list_val': 'list_of_d'}, ['total'])

    state = StateManager()
    executor = ThreadExecutor(dag, state)

    start_time = time.perf_counter()
    total = executor.run_with(
        input_a=0
    )

    elapsed_time = time.perf_counter() - start_time
    print(total)

    assert total == 6
    assert elapsed_time >= 0.20
    assert elapsed_time < 0.23
