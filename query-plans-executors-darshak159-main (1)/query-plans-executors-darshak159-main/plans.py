# Query Plans
#Query plans are modeled as trees, and each plan node can have a variable number of children.
#This file contains all the different types of plan nodes such as SeqScan, NestedLoopJoin, and Aggregation

# setup_logger.py
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PlanNode:
    """
    This abstract PlanNode represents all the possible types of plan nodes in our system.
    Plan nodes can be of SeqScan, NestedLoopJoin, Aggregation types (later we will add IndexScan, Insert, Update, Delete,...)
    Plan nodes are modeled as trees, so each plan node can have a variable number of children.
    Following the pipeline query processing model, the plan node receives the tuples of its children.
    
    Attributes
    ----------
        outputSchema: list
            the schema for the output of this plan node
      
    Methods
    ------
        getOutputSchema():
            return the schema for the output of this plan node
        getType():
            return the type of this plan node
    """
    
    def __init__(self, outputSchema):
        self.outputSchema = outputSchema
    
    def getOutputSchema(self):
        return self.outputSchema
    
    def getType(self):
        pass

class SeqScanPlanNode(PlanNode):
    """
    The SeqScanPlanNode represents a sequential table scan operation.
    It identifies a table to be scanned and an optional predicate.
    
    Attributes
    ----------
        table_id: int
            table ID
        predicate: Predicate
            optional predicate that all returned tuples must satisfy
      
    Methods
    ------
        getPredicate():
            returns the predicate that all returned tuples must satisfy
        getType():
            returns the type of this plan node
        getTableID()
            returns the table ID
    """
    def __init__(self, outputSchema, table_id, predicate=None):
        super().__init__(outputSchema)
        self.predicate = predicate
        self.table_id = table_id

    def getType(self):
        return "SeqScan"
    
    def getPredicate(self):
        return self.predicate
    
    def getTableID(self):
        return self.table_id

class NestedLoopJoinPlanNode(PlanNode):
    """
    NestedLoopJoinPlanNode joins tuples from two child plan nodes.
    
    Attributes
    ----------
        left_child_plan: PlanNode
            sequential scan child plan
        right_child_plan: PlanNode
            sequential scan child plan
        predicate: Predicate
            predicate to be used in the nested loop join 
            
      
    Methods
    ------
        getPredicate():
            returns the predicate to be used in the nested loop join 
        getType():
            returns the type of this plan node
        getLeftPLan()
            returns left child plan
        getRightPLan()
            returns right child plan
    """
    
    def __init__(self, outputSchema, left_child_plan, right_child_plan, predicate=None):
        super().__init__(outputSchema)
        self.predicate = predicate
        self.left_child_plan = left_child_plan
        self.right_child_plan = right_child_plan

    def getType(self):
        return "NestedLoopJoin"
    
    def getPredicate(self):
        return self.predicate
    
    def getLeftPlan(self):
        return self.left_child_plan
    
    def getRightPlan(self):
        return self.right_child_plan


class AggregationPlanNode(PlanNode):
    """
    AggregationPlanNode represents the various SQL aggregation functions.
    For example, COUNT(), SUM(), MIN() and MAX().
    
    Attributes
    ----------
        child_plan: PlanNode
            child plan
        group_by: str
            group by expression
        agg_at: str
            aggregation attribute
        agg_type: str
            aggregation type such as COUNT, SUM, MIN
      
    Methods
    ------
        
        getType():
            returns the type of this plan node
        getChildPLan()
            returns child plan
        getGroupByAt()
            returns group by attribute
        getAggregateAt():
            return aggregation attribute
        getAggregateType():
            returns aggreation type
    """
    def __init__(self, outputSchema, child_plan, group_by, agg_at, agg_type):
        super().__init__(outputSchema)
        self.child_plan = child_plan
        self.group_by = group_by
        self.agg_at = agg_at
        self.agg_type = agg_type

    def getType(self):
        return "Aggregation"
    
    def getChildPlan(self):
        return self.child_plan
    
    def getGroupByAt(self):
        return self.group_by
    
    def getAggregateAt(self):
        return self.agg_at
    
    def getAggregateType(self):
        return self.agg_type


import operator 
class Predicate:
    
    def __init__(self, field, operator, operand = None):
        self.field = field
        self.operator = operator
        self.operand = operand
        
    def evaluatePredicate(self, schema, row):
        col_idx = schema.getColunmIdx(self.field)
        value = row[col_idx]
        return self.compare(value, self.operand)
        
    def evaluateJoin(self, schema_left, row_left, schema_right, row_right):
        col_idx = schema_left.getColunmIdx(self.field)
        value_left = row_left[col_idx]
        col_idx = schema_right.getColunmIdx(self.field)
        value_right = row_right[col_idx]
        return self.compare(value_right, value_left)
        
    def compare(self,value1, value2):
        mode = self.operator
        valid_modes = ['<', '<=', '>', '>=', '==', '!=']
        if mode not in valid_modes:
            logging.info(f'Error!')
            return
        
        if mode == '<':
            return operator.lt(value1,value2)
        elif mode == '<=':
            return operator.le(value1,value2)
        elif mode == '>':
            return operator.gt(value1,value2)
        elif mode == '>=':
            return operator.ge(value1,value2)
        elif mode == '==':
            return operator.eq(value1,value2)
        elif mode == '!=':
            operator.ne
        else:
            print('Error!')
            return None
