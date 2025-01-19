import graphex.transparent as gx
import time

from graphex.execution import current_timestamp
from graphex.document import mermaid_link_from_dag


@gx.step
def add_one(n):
    time.sleep(0.5)
    return n + 1


@gx.step
def make_a_list_of_ten(element):
    return [element] * 10


@gx.step
def add_two(n):
    time.sleep(0.5)
    return n + 2


@gx.step
def sum_all(list_of_numbers):
    return sum(list_of_numbers)


@gx.dag
def process_all(some_number):
    temp_list_b = make_a_list_of_ten(some_number)
    next_n = add_one(some_number)
    temp_list_c = gx.gxmap(add_two, temp_list_b)
    sum_d = sum_all(temp_list_c)

    return next_n, sum_d


if __name__ == '__main__':

    start = current_timestamp()
    print('running it single-threaded')
    result1, result2 = process_all(some_number=3)
    print(f'results: {result1}, {result2} - in {round(current_timestamp()-start,2)} seconds\n')

    start = current_timestamp()
    print('running it in parallel')
    result1, result2 = process_all.parallelize(some_number=3)
    print(f'results: {result1}, {result2} - in {round(current_timestamp()-start,2)} seconds')

    link = mermaid_link_from_dag(process_all.build_dag())
    print('See execution graph at:')
    print(link)
