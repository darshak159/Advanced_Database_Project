
from ra_operator import union
from ra_operator import union2
from ra_operator import intersection
from ra_operator import set_difference
from ra_operator import selection
from ra_operator import project
from ra_operator import crossproduct


def test_union_0():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]
    d2 = [[4, "pat", "ullrich"], [5, "chahat", "sharma"], [6, "micheal", "mendes"]]

    result =  union(d1, d2)
    expected = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"], [4, "pat", "ullrich"],
            [5, "chahat", "sharma"], [6, "micheal", "mendes"]]

    assert sorted(result) == sorted(expected)

def test_union_1():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]
    d2 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]

    result =  union(d1, d2)
    expected = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"], [1, "john", "smith"],
                [2, "tine", "hert"], [3, "seth", "singh"]]

    assert sorted(result) == sorted(expected)

def test_union2_0():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"]]
    d2 = [[3, "chahat", "sharma"], [1, "john", "smith"], [4, "micheal", "mendes"]]

    result =  union2(d1, d2)
    expected = [[1, "john", "smith"], [2, "tine", "hert"], [3, "chahat", "sharma"], [4, "micheal", "mendes"]]

    assert sorted(result) == sorted(expected)

def test_union2_1():
    d1 = [[3, "chahat", "sharma"], [1, "john", "smith"], [4, "micheal", "mendes"]]
    d2 = [[3, "chahat", "sharma"], [1, "john", "smith"], [4, "micheal", "mendes"]]

    result =  union2(d1, d2)
    expected = [[3, "chahat", "sharma"], [1, "john", "smith"], [4, "micheal", "mendes"]]

    assert sorted(result) == sorted(expected)

def test_union2_2():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]
    d2 = [[4, "pat", "ullrich"], [5, "chahat", "sharma"], [6, "micheal", "mendes"]]

    result =  union2(d1, d2)
    expected = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"], [4, "pat", "ullrich"],
                [5, "chahat", "sharma"], [6, "micheal", "mendes"]]

    assert sorted(result) == sorted(expected)

def test_intersection_0():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]
    d2 = [[4, "pat", "ullrich"], [5, "chahat", "sharma"], [6, "micheal", "mendes"]]

    result =  intersection(d1, d2)

    assert 0 == len(result)

def test_intersection_1():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]
    d2 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]

    result =  intersection(d1, d2)
    expected = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]

    assert 3 == len(result)
    assert sorted(result) == sorted(expected)

def test_intersection_2():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]
    d2 = [[4, "pat", "ullrich"], [1, "john", "smith"], [5, "chahat", "bhamra"]]

    result =  intersection(d1, d2)
    expected = [[1, "john", "smith"]]

    assert 1 == len(result)
    assert sorted(result) == sorted(expected)

def test_set_diff_0():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]
    d2 = [[4, "pat", "ullrich"], [5, "chahat", "sharma"], [6, "micheal", "mendes"]]

    result =  set_difference(d1, d2)
    expected = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]

    assert sorted(result) == sorted(expected)

def test_set_diff_1():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"], [4, "pat", "ullrich"],
            [5, "chahat", "sharma"], [6, "micheal", "mendes"]]
    d2 = [[4, "pat", "ullrich"], [5, "chahat", "sharma"], [6, "micheal", "mendes"]]

    result =  set_difference(d1, d2)
    expected = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]

    assert sorted(result) == sorted(expected)

def test_set_diff_2():
    d1 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]
    d2 = [[1, "john", "smith"], [2, "tine", "hert"], [3, "seth", "singh"]]

    result =  set_difference(d1, d2)
    expected = []

    assert sorted(result) == sorted(expected)

def test_project_0():
    lst = [[1, "john", "smith"], [2, "seth", "singh"], [3, "pat", "ullrich"]]

    result =  project(lst, [0])
    expected = [[1], [2], [3]]

    assert sorted(result) == sorted(expected)

def test_project_1():
    lst = [[1, "john", "smith"], [2, "seth", "singh"], [3, "pat", "ullrich"]]

    result = project(lst, [0, 1])
    expected = [[1, "john"], [2, "seth"], [3, "pat"]]

    assert sorted(result) == sorted(expected)

def test_project_2():
    lst = [[1, "john", "smith"], [2, "seth", "singh"], [3, "pat", "ullrich"]]

    result =  project(lst, [0, 2])
    expected = [[1, "smith"], [2, "singh"], [3, "ullrich"]]

    assert sorted(result) == sorted(expected)

def test_selection_0():
    lst = [[1, "john", "smith"], [2, "seth", "singh"], [3, "pat", "ullrich"]]

    result =  selection(lst, 1, "john", "==")
    print(result)
    expected = [[1, "john", "smith"]]

    assert sorted(result) == sorted(expected)