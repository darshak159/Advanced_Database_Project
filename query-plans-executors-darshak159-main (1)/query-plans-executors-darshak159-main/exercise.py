import metadata
import disk_manager
import access_methods
import plans
import context as cont
import engine as eng


def createTable(catalog, fileName, schema):
    #create table 'actors' with corresponding schema and disk location
    db_file = disk_manager.DBFile(access_methods.DBFileIterator(fileName))
    table = catalog.createTable(schema, db_file)
    return table

def createQueryPlanSeqMaleActors(schema, table):
    predicate = plans.Predicate(schema.getColunmIdx('gender'),'=','M')
    plan = plans.SeqScanPlanNode(schema, table, predicate)
    #create SeqScanPlanNode for table actors with predicate gender==M
    ## ADD YOUR CODE
    return plan

def createQueryPlanSeqRoles(schema, table):
    #create SeqScanPlanNode for table roles
    plan = plans.SeqScanPlanNode(schema, table)
    ## ADD YOUR CODE
    return plan

def createQueryPlanJoinMaleActorsAndRoles(schema_actors, schema_roles,plan_actor, plan_roles):
    #create NestedLoopPlanNode with join on actor_id
    join_predicate = plans.Predicate(schema_actors.getColunmIdx('actor_id'), '=', schema_roles.getColunmIdx('actor_id'))
    plan = plans.NestedLoopJoinPlanNode(plan_actor, plan_roles, join_predicate)
    return plan
    ## ADD YOUR CODE
    return plan

def createQueryPlanCountActorsroles(join_plan):
    #create AggregationPlanNode with child NestedLoopPLanNode
    agg_schema = ['max','actor_id']
    plan = plans.AggregationPlanNode(agg_schema,join_plan,'actor_id','movie_id','COUNT')
    ## ADD YOUR CODE
    
    return plan

def main():
    #create a DB catalog
    catalog = metadata.Catalog()

    schema_actors = metadata.Schema('actors', ['actor_id','first_name','last_name','gender'] )
    schema_roles = metadata.Schema('roles',['actor_id','movie_id','role'])
    table_id_actors = createTable(catalog,'IMDb_sample/actors.csv', schema_actors)
    table_id_roles = createTable(catalog,'IMDb_sample/roles.csv', schema_roles )

    #create a query engine to parse the query plan into an execution plan
    context = cont.Context(catalog)
    engine = eng.ExecutionEngine(context)

    plan_actor = createQueryPlanSeqMaleActors(schema_actors, table_id_actors)
    plan_roles = createQueryPlanSeqRoles(schema_roles, table_id_roles)
    plan_join = createQueryPlanJoinMaleActorsAndRoles(schema_actors, schema_roles, plan_actor, plan_roles)
    plan_agg = createQueryPlanCountActorsroles(plan_join)

    #Execute Plans
    print('Executing query plan male actors:')
    query = engine.execute(plan_actor)
    print('Result size:', len(query))
    print('First 5 answers', query[:5])

    print('Executing query plan roles:')
    query = engine.execute(plan_roles)
    print('Result size:', len(query))
    print('First 5 answers',query[:5])

    print('Executing query plan male actors with roles:')
    query = engine.execute(plan_join)
    print('Result size:', len(query))
    print('First 5 answers',query[:5])

    print('Executing query plan count roles of male actors:')
    query = engine.execute(plan_agg)
    print('Result size:', len(query[0].keys()))
    print('First 5 answers',dict(list(query[0].items())[0:5]))


main()