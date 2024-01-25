# ExecutionEngine
# The ExecutionEngine receives a query plan, creates its execution plan with 
# the query operators and execute the plan.

import factory as exf

class ExecutionEngine:
    """
    The ExecutionEngine class executes query plans.
    
    Attributes
    ----------
        tx_context: Context

    Methods
    ------
        execute():
            returns the results of a query
    """
    def __init__(self, tx_context):
        self.tx_context = tx_context
        
    def execute(self,plan):
        """Execute a query plan.

        Parameters
        ----------
        plan : Plan
            Query plan

        """
        
        factory = exf.ExecutorFactory(self.tx_context)
        root_operator = factory.createExecution(plan)
        
        root_operator.openOp()
        result = []
        next_tuple = root_operator.getNext()
        while(next_tuple != None):
            result.append(next_tuple)
            next_tuple = root_operator.getNext()
            
        root_operator.closeOp()
        return result