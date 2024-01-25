import metadata
import plans
import context as cont
import engine as eng
import executors as ex
import disk_manager
import access_methods
import factory as exf 


def createObjs():
    #create a DB catalog
    catalog = metadata.Catalog()
    schema1 = metadata.Schema('actors', ['actor_id','first_name','last_name','gender'] )
    db_file = disk_manager.DBFile(access_methods.DBFileIterator('IMDb_sample/actors.csv'))
    table1 = catalog.createTable(schema1, db_file)

    schema2= metadata.Schema('roles',['actor_id','movie_id','role'])
    db_file = disk_manager.DBFile(access_methods.DBFileIterator('IMDb_sample/roles.csv'))
    table2 = catalog.createTable(schema2,db_file)


    #create a query engine to parse the query plan into an execution plan
    context = cont.Context(catalog)
    return context, schema1, schema2, table1, table2

def testSeqScan():  
    context, schema, _, table, _ = createObjs()
    plan = plans.SeqScanPlanNode(schema, table)
    executor = ex.SeqScanExecutor(context, plan)
    executor.openOp()
    tuple = executor.getNext()
    assert tuple == ['10963', 'Chris', 'Anastasio', 'M']
    tuple = executor.getNext()
    assert tuple == ['32638', 'Michael', 'Beach', 'M']
    while(tuple != None):
        tuple = executor.getNext()
    assert tuple == None

def testSeqScanWithPredicate():  
    context, schema, _, table, _  = createObjs()
    predicate = plans.Predicate("gender", "==", "F")
    plan = plans.SeqScanPlanNode(schema, table, predicate)
    executor = ex.SeqScanExecutor(context, plan)
    executor.openOp()
    tuple = executor.getNext()
    assert tuple == ['589318', 'Paula', 'Cross', 'F']
    tuple = executor.getNext()
    assert tuple == ['636502', 'Wendy', 'Gordon', 'F']
    while(tuple != None):
        tuple = executor.getNext()
    assert tuple == None


def testJoin():  
    context, schema1, schema2, table1, table2  = createObjs()
    predicate = plans.Predicate("gender", "==", "F")
    plan1 = plans.SeqScanPlanNode(schema1, table1, predicate)
    plan2 = plans.SeqScanPlanNode(schema2, table2)
    schema = schema1.columns + schema2.columns
    join_schema = metadata.Schema('test', schema)
    plan = plans.NestedLoopJoinPlanNode(join_schema, plan1, plan2)
    
    left = exf.ExecutorFactory(context).createExecution(plan.getLeftPlan())
    right = exf.ExecutorFactory(context).createExecution(plan.getRightPlan())
    executor = ex.NestedLoopJoinExecutor(context, plan, left,right)
    executor.openOp()
    tuple = executor.getNext()
    assert tuple == ['589318', 'Paula', 'Cross', 'F', '10963', '5306', 'Truck Driver']
    tuple = executor.getNext()
    assert tuple == ['589318', 'Paula', 'Cross', 'F', '32638', '5306', 'Barnes']
    while(tuple != None):
        tuple = executor.getNext()
    assert tuple == None

def testJoinWithPredicate():  
    context, schema1, schema2, table1, table2  = createObjs()
    predicate = plans.Predicate("gender", "==", "F")
    plan1 = plans.SeqScanPlanNode(schema1, table1, predicate)
    plan2 = plans.SeqScanPlanNode(schema2, table2)
    schema = schema1.columns + schema2.columns
    join_schema = metadata.Schema('test', schema)
    predicate = plans.Predicate("actor_id", "==")
    plan = plans.NestedLoopJoinPlanNode(join_schema, plan1, plan2, predicate)
    
    left = exf.ExecutorFactory(context).createExecution(plan.getLeftPlan())
    right = exf.ExecutorFactory(context).createExecution(plan.getRightPlan())
    executor = ex.NestedLoopJoinExecutor(context, plan, left,right)
    executor.openOp()
    tuple = executor.getNext()
    assert tuple == ['589318', 'Paula', 'Cross', 'F', '589318', '5306', 'Young Woman']
    tuple = executor.getNext()
    assert tuple == ['636502', 'Wendy', 'Gordon', 'F', '636502', '5306', 'Anchorwoman']
    while(tuple != None):
        tuple = executor.getNext()
    assert tuple == None


def testAggregate():  
    context, schema1, schema2, table1, table2  = createObjs()
    predicate = plans.Predicate("gender", "==", "F")
    plan1 = plans.SeqScanPlanNode(schema1, table1, predicate)
    plan2 = plans.SeqScanPlanNode(schema2, table2)
    schema = schema1.columns + schema2.columns
    join_schema = metadata.Schema('test', schema)
    predicate = plans.Predicate("actor_id", "==")
    plan = plans.NestedLoopJoinPlanNode(join_schema, plan1, plan2, predicate)

    agg_schema = ['max', 'actor_id' ]
    agg_plan = plans.AggregationPlanNode(agg_schema, plan, 'actor_id', 'movie_id', 'COUNT')
    
    child_executor = exf.ExecutorFactory(context).createExecution(agg_plan.getChildPlan())
    executor =  ex.AggregationExecutor(context, agg_plan, child_executor)
    
    executor.openOp()
    tuple = executor.getNext()
    tuple = list(tuple.keys())[0]
    assert tuple == '589318'
    tuple = executor.getNext()
    assert tuple == None

