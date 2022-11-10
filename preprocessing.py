import psycopg2
import json
import queue

default_seqpage_cost = 1.0
default_randompage_cost = 4.0

class DBConnection:
    # Open connection to DB, enter your database name and password
    # Change this accordingly
    def __init__(self, host="localhost", port = 5432, database="TPC-H", user="postgres", password="password") -> None:
        self.conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        self.cur = self.conn.cursor()

    def execute(self, query):
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        return query_results

    def getmainQEP(self, query):
        self.cur.execute("SET enable_hashjoin TO 1")
        self.cur.execute("SET enable_mergejoin TO 1")
        self.cur.execute("SET enable_nestloop TO 1")
        self.cur.execute("SET enable_bitmapscan TO 1")
        self.cur.execute("SET enable_indexscan TO 1")
        self.cur.execute("SET enable_seqscan TO 1")

        self.cur.execute(self.cur.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query))
        query_plan = self.cur.fetchall()
        return query_plan

    def getALTQEP(self, query, operator):
        # self.cur.execute("SET seq_page_cost TO " + str(0))
        if operator == 0:   self.cur.execute("SET enable_hashagg TO 0")
        elif operator == 1: self.cur.execute("SET enable_hashjoin TO 0")
        elif operator == 2: self.cur.execute("SET enable_mergejoin TO 0")
        elif operator == 3: self.cur.execute("SET enable_nestloop TO 0")
        elif operator == 4: self.cur.execute("SET enable_bitmapscan TO 0")
        elif operator == 5: self.cur.execute("SET enable_indexscan TO 0")
        else:		    self.cur.execute("SET enable_seqscan TO 0")
        self.cur.execute(self.cur.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query))
        query_plan = self.cur.fetchall()
        # self.cur.execute("SET seq_page_cost TO " + str(default_seqpage_cost))
        if operator == 0:   self.cur.execute("SET enable_hashagg TO 1")
        elif operator == 1: self.cur.execute("SET enable_hashjoin TO 1")
        elif operator == 2: self.cur.execute("SET enable_mergejoin TO 1")
        elif operator == 3: self.cur.execute("SET enable_nestloop TO 1")
        elif operator == 4: self.cur.execute("SET enable_bitmapscan TO 1")
        elif operator == 5: self.cur.execute("SET enable_indexscan TO 1")
        else:		        self.cur.execute("SET enable_seqscan TO 1")
        return query_plan

    def getAQP(self, query, enable_hashjoin=True, enable_mergejoin=True, enable_nestloop=True,
        enable_bitmapscan=True, enable_indexscan=True, enable_seqscan=True):
        
        self.cur.execute("SET enable_hashjoin TO 1") if enable_hashjoin else self.cur.execute("SET enable_hashjoin TO 0")
        self.cur.execute("SET enable_mergejoin TO 1") if enable_mergejoin else self.cur.execute("SET enable_mergejoin TO 0")
        self.cur.execute("SET enable_nestloop TO 1") if enable_nestloop else self.cur.execute("SET enable_nestloop TO 0")
        self.cur.execute("SET enable_bitmapscan TO 1") if enable_bitmapscan else self.cur.execute("SET enable_bitmapscan TO 0")
        self.cur.execute("SET enable_indexscan TO 1") if enable_indexscan else self.cur.execute("SET enable_indexscan TO 0")
        self.cur.execute("SET enable_seqscan TO 1") if enable_seqscan else self.cur.execute("SET enable_seqscan TO 0")
        
        self.cur.execute(self.cur.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query))
        query_plan = self.cur.fetchall()
        return query_plan

    def close(self):
        self.cur.close()
        self.conn.close()

connection = DBConnection()
print("PostgreSQL server information")

record = connection.execute("SELECT version();")
print("You are connected to - ", record, "\n")

# User input query
#input_query = input("Enter your SQL query: ")
# Query for testing
input_query = """
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%pending%packages%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc
limit 1;"""

# Execute statement
# query_result = connection.execute(input_query)
# for row in query_result:
#     print("Region = ", row[1])
# # Get execution plan from PostgreSQL
# query_analysis = connection.execute("EXPLAIN (ANALYZE, COSTS, BUFFERS, FORMAT JSON ) " + input_query)
# plan = query_analysis[0][0][0]['Plan']
# q = queue.Queue()
# q.put(plan)
# while not q.empty():
#     plan1 = q.get()
#     print(plan1)
#     print("==========================================================================")
#     if "Plans" in plan1:
#         for plan in plan1["Plans"]:
#             q.put(plan)

#cursor.close()
#connection.close()
