from graphex.model import DAGData
from graphex.execution import ThreadExecutor, StateManager
from pprint import pprint

# SAMPLE FUNCTIONS


def add_two(input_a):
    temp_b = input_a + 2
    return temp_b


def fan_out(value_to_repeat):

    fanout_factor = 3

    list_of_temp_c = [value_to_repeat] * fanout_factor

    return list_of_temp_c


def minus_one(temp_c):
    temp_d = temp_c - 1
    return temp_d


def by_ten(a_parameter):
    temp_e = a_parameter * 10
    temp_h = a_parameter * 10 * 10
    return temp_e, temp_h


def sum_all(input_list):
    final_f = sum(input_list)
    return final_f


def compute_score(e, f, h):
    score = e + f + h
    explanation = f'score components E: {e} F:{f} G:{h}'
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


def test_base_case():
    dag = DAGData(
        inputs=['main_dag_input'],
        outputs=['score', 'explanation']
    )

    dag.step(
        'increment_two',
        add_two,
        ['main_dag_input'],
        {},
        ['temp_b']
    )

    dag.step(
        'explode',
        fan_out,
        ['temp_b'],
        {},
        ['temp_c']
    )

    dag.map(
        name='first_substraction',
        function=minus_one,
        arg_var_names=['temp_c'],
        string_keyword_args={},
        outputs=['temp_d']
    )

    dag.map(
        name='second_substraction',
        function=minus_one,
        arg_var_names=['temp_d'],
        string_keyword_args={},
        outputs=['temp_g']
    )

    #  temp_e, temp_h = by_ten(temp_b)

    dag.step(
        name='ten_product',
        function=by_ten,
        arg_var_names=[],
        string_keyword_args={
            'a_parameter': 'temp_b'
        },
        outputs=['temp_e', 'temp_h']
    )

    # temp_f = sum_all(list_of_temp_g)

    dag.step(
        name='sum_it_all',
        function=sum_all,
        arg_var_names=[],
        string_keyword_args={
            'input_list': 'temp_g'
        },
        outputs=['temp_f']
    )

    #  score, explanation = compute_score(temp_e, temp_f, temp_h)

    dag.step(
        name='final_effort',
        function=compute_score,
        arg_var_names=[],
        string_keyword_args={
            'e': 'temp_e',
            'f': 'temp_f',
            'h': 'temp_h'
        },
        outputs=['score', 'explanation']
    )

    pprint(dag.data())

    state = StateManager()
    executor = ThreadExecutor(dag, state)

    score, explanation = executor.run_with(
        main_dag_input=1
    )

    assert score == 333
    assert explanation == 'score components E: 30 F:3 G:300'
