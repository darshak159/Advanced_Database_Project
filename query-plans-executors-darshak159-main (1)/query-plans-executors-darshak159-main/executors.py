# Query Operators - Pipeline Query Processing Model
# This file contains all the operators of our system.
# You will need to add your code for some of the functions in each operator

# setup_logger.py
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Executor:
    """
    A class to  represent all executors in our system
    This abstract Executor implements the pipelining tuple-at-a-time query processing model.
    This is the base class from which all executors in the system engine inherit, and defines the minimal interface that all executors support.

    Methods
    -------
    init():
        initializes operator state and sets parameters
    openOp():
        opens the operator
    getNext()
        calls fetchNext() on its inputs processes and produces output tuple(s) 
        returns None when done
    fetchNext()
        Yield the next tuple from operator
    closeOp():
        cleans up (if any)
    """
    def __init__(self, tx_context):
        self.tx_context = tx_context
    
    def openOp(self):
        self.open = True
    
    def closeOp(self):
        self.open = False
        
    def getNext(self):
        return self.fetchNext()

    def reset(self):
        pass
    
    def fetchNext(self):
        pass

class SeqScanExecutor(Executor):
    """
    The SeqScanExecutor executor executes a sequential table scan.
    The sequential scan executor iterates over a table and return its tuples one-at-a-time. 
    A sequential scan is specified by a SeqScanPlanNode. 
    The plan node specifies which table to iterate over. 
    The plan node may also contain a predicate; if a tuple does not satisfy the predicate, it is skipped over.

    Attributes
    ----------
        plan: PlanNode
            plan to be executed
        tx_context: Context
            context of the transaction executing the query
    
    Methods
    -------
    init():
        initializes the sequential scan
    openOp():
        opens the operator
    getNext()
        calls fetchNext() on its inputs processes and produces output tuple(s) 
        returns None when done
    fetchNext()
        Yield the next tuple from the sequential scan.
    closeOp():
        cleans up (if any)
    """
    
    def __init__(self, tx_context, plan): 
        super().__init__(tx_context)
        self.plan = plan
        self.table_iterator = self.tx_context.getCatalog().getTable(self.plan.getTableID()).getIterator()
        
    def openOp(self):
        super().openOp()
        self.table_iterator.openIt()
        self.next_tuple = self.table_iterator.getNext()
        
    def closeOp(self):
        super().closeOp()
        self.table_iterator.closeIt()

    def reset(self):
        self.table_iterator.resetIt()
        self.next_tuple = self.table_iterator.getNext()
    
    
    def fetchNext(self):
        #This function returns the next tuple from the scan, or None when it is done
        #You will want to make use of the predicate in the sequential scan plan node. 

        
        #The ouput of sequential scan should be the matched tuples. Please make sure you understand the getNext function in the base Executor class'
        next_tuple = None
        found = False
        while not found:
            if(self.next_tuple == None):
                found  = True
            else:
                predicate = self.plan.getPredicate()
                if(predicate != None):
                    schema = self.plan.getOutputSchema()
                    found = self.plan.getPredicate().evaluatePredicate(schema, self.next_tuple)
                    if found:
                        next_tuple = self.next_tuple

                else:
                    found = True
                    next_tuple =  self.next_tuple

                self.next_tuple = self.table_iterator.getNext()

            return next_tuple
        
class NestedLoopJoinExecutor(Executor):
    """
    NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
    The nested loop join executor iterates over the two children table and return its tuples one-at-a-time. 
    A sequential scan is specified by a NestedLoopJoinPlanNode. 
    The plan node specifies which tables to iterate over. 
    The plan node may also contain a predicate; if a tuple does not satisfy the join predicate, it is skipped over.

    Attributes
    ----------
        plan: PlanNode
            plan to be executed
        tx_context: Context
            context of the transaction executing the query
        left_executor: Executor
            the child executor that produces tuple for the left side of join
        right_executor: Executor
            the child executor that produces tuple for the right side of join
    
    Methods
    -------
    init():
        initializes the sequential scan
    openOp():
        opens the operator
    getNext()
        calls fetchNext() on its inputs processes and produces output tuple(s) 
        returns None when done
    fetchNext()
        Yield the next tuple from the join.
    closeOp():
        cleans up (if any)
    """
    def __init__(self, tx_context, plan, left_executor, right_executor):
        super().__init__(tx_context)
        self.left_executor = left_executor
        self.right_executor = right_executor
        self.plan = plan
        
    def openOp(self):
        super().openOp()
        self.left_executor.openOp()
        self.right_executor.openOp()
        
        self.right_tuple =  self.right_executor.getNext()
        self.left_tuple = self.left_executor.getNext()
        
    def closeOp(self):
        super().closeOp()
        self.left_executor.closeOp()
        self.right_executor.closeOp()
        
    def fetchNext(self):
            found = False
            while not found:
                if(self.left_tuple != None):
                    if(self.right_tuple != None):
                        left_schema = self.left_executor.plan.getOutputSchema()
                        right_schema = self.right_executor.plan.getOutputSchema()
                        predicate = self.plan.getPredicate()
                        if(predicate == None):
                            next_tuple = self.left_tuple +self.right_tuple
                            self.right_tuple = self.right_executor.getNext()
                            found = True
                        else:
                            match = self.plan.predicate.evaluateJoin(left_schema, self.left_tuple, right_schema, self.right_tuple)
                            if match:
                                next_tuple = self.left_tuple +self.right_tuple
                                self.getNextLeftTuple()
                                found =True
                            else:
                                self.right_tuple = self.right_executor.getNext()
                    else:
                        self.resetRightTuple()
                        self.getNextLeftTuple()
                else:
                    found = True

            return next_tuple
    
    def resetRightTuple(self): 
        self.right_executor.reset()
        self.right_tuple =  self.right_executor.getNext()
    
    def getNextLeftTuple(self):
        self.left_tuple = self.left_executor.getNext()
                
import sys

#Class to keep the aggregates of each "group_by" map
class AggregationInfo():
    def __init__(self):
        self.cnt = 0
        self.sum = 0
        self.max = -sys.maxsize -1
        self.min = sys.maxsize
        
    def getCount(self):
        return self.cnt
    
    def getSum(self):
        return self.sum
    
    def getMax(self):
        return self.max
    
    def getMin(self):
        return self.min
    
    def getAvg(self):
        return self.sum/self.cnt
    
    def setCount(self, cnt):
        self.cnt = cnt
    
    def setSum(self, sum):
        self.sum = sum
    
    def setMax(self, max):
        self.max = max
    
    def setMin(self, min):
        self.min = min
    

class AggregationExecutor(Executor):
    """
    AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX) over the tuples produced by a child executor.

    Attributes
    ----------
        plan: PlanNode
            plan to be executed
        tx_context: Context
            context of the transaction executing the query
        group_map: dict
            maps the aggregation value for each group by attribute
    Methods
    -------
    init():
        initializes the sequential scan
    openOp():
        opens the operator
    getNext()
        calls fetchNext() on its inputs processes and produces output tuple(s) 
        returns None when done
    fetchNext()
        return the results of the aggregate
    closeOp():
        cleans up (if any)
    """
    def __init__(self, tx_context, plan, child_executor):
        super().__init__(tx_context)
        self.child_executor = child_executor
        self.plan = plan 
        self.group_map= {} #key,value     
       
    def openOp(self):
        self.child_executor.openOp()
        super().openOp()
        self.done = False
        
    def closeOp(self):
        super().closeOp()
        self.child_executor.closeOp()
        
    def fetchNext(self):
        
        ## ADD YOUR CODE
        if self.done:
            return None
        else:
            next_tuple = self.child_executor.getnext()
            while(next_tuple != None):
                key = self.getKey(next_tuple)
                value= self.getValue(next_tuple)
                self.insert(key, value)
                next_tuple = self.child_executor.getNext()

                self.done = True

        results = self.parseResults()
        return results
    
    def getKey(self, row):
        group_by_at = self.plan.getGroupByAt() 
        schema = self.child_executor.plan.getOutputSchema()
        col_idx = schema.getColunmIdx(group_by_at)
        value = row[col_idx]
        return  value
    
    def getValue(self, row):
        agg_at = self.plan.getAggregateAt()
        schema = self.child_executor.plan.getOutputSchema()
        col_idx = schema.getColunmIdx(agg_at)
        value = row[col_idx]
        return  value

    def aggregate(self, key, value):
        if (key != None):
            agg_info = self.group_map.get(key)
            agg_type = self.plan.getAggregateType()
            if (agg_type =="MIN"):
                min_value = min(agg_info.getMin(), value)
                agg_info.setMin(min_value)
            if (agg_type =="MAX"):
                max_value = max(agg_info.getMax(), value)
                agg_info.setMax(max_value)
            if (agg_type =="SUM"):
                sum_value = agg_info.getSum() + value
                agg_info.setSum(sum_value)
            if (agg_type =="AVG"):
                sum_value = agg_info.getSum() + value
                agg_info.setSum(sum_value)
                cnt_value = agg_info.getCount() + 1
                agg_info.setCount(cnt_value)
            if (agg_type =="COUNT"):
                cnt_value = agg_info.getCount() + 1
                agg_info.setCount(cnt_value)
        
    def insert(self, key, value):
        if (key not in self.group_map):
            aggInfo = AggregationInfo()
            self.group_map[key] = aggInfo
            
        self.aggregate(key, value)
        return
  
    def parseResults(self):
        result_map= {} #key,value 
        for key in self.group_map:
            val = self.group_map[key]
            value = 0
            agg_type = self.plan.getAggregateType()
            if (agg_type =="MIN"):
                value = val.getMin()
            if (agg_type =="MAX"):
                value = val.getMax()
            if (agg_type =="SUM"):
                value = val.getSum()
            if (agg_type =="AVG"):
                value = val.getAvg()
            if (agg_type =="COUNT"):
                value = val.getCount()
            
            result_map[key] = value
        return result_map
            