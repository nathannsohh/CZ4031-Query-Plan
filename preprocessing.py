import psycopg2
import json
import queue

class DBConnection:
    # Open connection to DB, enter your database name and password
    # Change this accordingly
    def __init__(self, host="localhost", port = 5432, database="TPC-H", user="postgres", password="postgres") -> None:
        self.conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        self.cur = self.conn.cursor()

    def execute(self, query):
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        return query_results

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