import ast
import inspect

from graphex.transparent import dag
from graphex.analysis import parse

def add_two(arg1, arg2):
    return arg1 + arg2


def minus_one(arg1):
    return arg1 - 1


@dag
def process_all(val1, val2):
    temp_a = add_two(val1, arg2=val2)
    # just a comment
    temp_b = minus_one(temp_a)
    return temp_b


if __name__ == '__main__':
    print(parse(process_all))
