
#########################################################
###  Implementing Relational Algebra Operators        ###
#########################################################


# Implement an operator to merge two lists with duplicated allowed
def union(lst1, lst2):
    """Relational algebra project set union
    """ 
    return lst1 + [elem for elem in lst2 if elem not in lst1]
     ## IMPLEMENT HERE THE UNION OF TWO LISTS

    return 

# mplement an operator to merge two lists with no duplicated allowed
def union2(lst1, lst2):
    """Relational algebra project set union with no duplicates
    """ 
    return list(set(lst1 + lst2))
    ###IMPLEMENT HERE THE INTERSECTION OF TWO LISTS
    return 

# Implement an operator to perform the intersection of two lists
def intersection(lst1, lst2):
    """Relational algebra project set intersection
    """ 
    ###IMPLEMENT HERE THE INTERSECTION OF TWO LISTS
    return list(set(lst1) & set(lst2))


# Implement an operator to perform the set difference of two lists
def set_difference(lst1, lst2):
    """Relational algebra project set difference
    """ 
    ## IMPLEMENT HERE THE SET DIFFERENT BETWEEN TWO LISTS
    return  list(set(lst1) - set(lst2))

## RA SELECTIOn

def selection(relation, column, predicate, mode):
    """Relational algebra project selection
    """ 
    operators = {'<': lambda x, y: x < y,
                 '<=': lambda x, y: x <= y,
                 '>': lambda x, y: x > y,
                 '>=': lambda x, y: x >= y,
                 '==': lambda x, y: x == y,
                 '!=': lambda x, y: x != y}
    
    index = relation[0].index(column)
    return [row for row in relation if operators[mode](row[index], predicate)]

#RA PROJECTION

def project(relation, columns):
    """Relational algebra project operator
    
       relation: list of rows
       columns: list with columns index
    """
    # IMPLEMENT HERE THE PROJECT OPERATOR. THE FUNCTION CCEPTS A LIST OF COLUMNS INDEXES AND RETURN THE VALUES OF THOSE COLUMNS 
    
    return  [[row[col] for col in columns] for row in relation]


## CROSS PRODUCT
# The cross product pairs each row of a relation with every row of another relation to create 
# a new relation that contains every possible combination of the input relations tuples.

def crossproduct(relation_a, relation_b):
    """Relational algebra cross product operator
    """
    ## IMPLEMENT CROSS PRODUCT HERE
    return [row_a + row_b for row_a in relation_a for row_b in relation_b]