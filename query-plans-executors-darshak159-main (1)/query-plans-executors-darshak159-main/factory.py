# ExecutorFactory
# The executor factory creates a new executor given the executor 
# context and plan node.

import executors as ex
import plans as pl

class ExecutorFactory(): 
    """
    ExecutorFactory creates executors for arbitrary plan nodes
    
    Attributes
    ----------
        tx_context: Context

    Methods
    ------
        createExecution():
            returns the execution plan of query
    """
    def __init__(self,tx_context):
        self.tx_context = tx_context
        
    def createExecution(self, plan):
        plan_type = plan.getType()
        if (plan_type == "SeqScan"):
            print("SeqScan")
            plan.__class__ = pl.SeqScanPlanNode
            return ex.SeqScanExecutor(self.tx_context, plan)
        elif (plan_type == "NestedLoopJoin"):
            print("NestedLoopJoin")
            plan.__class__ = pl.NestedLoopJoinPlanNode
            left = ExecutorFactory(self.tx_context).createExecution(plan.getLeftPlan())
            right = ExecutorFactory(self.tx_context).createExecution(plan.getRightPlan())
            return ex.NestedLoopJoinExecutor(self.tx_context, plan, left,right)
        elif (plan_type =="Aggregation"):
            print("Aggregation")
            plan.__class__ = pl.AggregationPlanNode
            child_executor = ExecutorFactory(self.tx_context).createExecution(plan.getChildPlan())
            return ex.AggregationExecutor(self.tx_context, plan, child_executor)
        
              
        print("Error - Operator not found")
        return                                                   