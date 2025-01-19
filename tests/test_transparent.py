import graphex.transparent as gx


# SAMPLE FUNCTIONS

@gx.step
def add_two(input_a):
    temp_b = input_a + 2
    return temp_b


@gx.step
def fan_out(value_to_repeat):
    print('fanout args', value_to_repeat)

    fanout_factor = 3

    list_of_temp_c = [value_to_repeat] * fanout_factor

    return list_of_temp_c


@gx.step
def minus_one(temp_c):
    print(temp_c)
    temp_d = temp_c - 1
    return temp_d


@gx.step(output_count=2)
def by_ten(a_parameter):
    temp_e = a_parameter * 10
    temp_h = a_parameter * 10 * 10
    return temp_e, temp_h


@gx.step
def sum_all(list_of_temp_g):
    final_f = sum(list_of_temp_g)
    return final_f


@gx.step(output_count=2)
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


@gx.DAG
def process_fluent(input_a: int):
    temp_b = add_two(input_a)

    temp_list_c = fan_out(temp_b)

    temp_list_d = gx.gxmap(minus_one, temp_list_c)
    temp_list_g = gx.gxmap(minus_one, temp_list_d)
    temp_e, temp_h = by_ten(temp_b)
    temp_f = sum_all(temp_list_g)
    score, explanation = compute_score(e=temp_e, f=temp_f, h=temp_h)
    return score, explanation


def test_base_transparent_api():
    score2, explanation2 = process_fluent.parallelize(input_a=1)  # should parallelize when possible
    assert score2 == 333
    assert explanation2 == 'score components E: 30 F:3 G:300'
