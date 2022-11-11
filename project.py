## Just testing, will change to the actual project.py once everything is done
import preprocessing
from annotation import build_qep_tree
import queue
import streamlit as st
import interface


interface.running()
# connection = preprocessing.DBConnection()

# input_query = """
# select
# 	c_count,
# 	count(*) as custdist
# from
# 	(
# 		select
# 			c_custkey,
# 			count(o_orderkey)
# 		from
# 			customer left outer join orders on
# 				c_custkey = o_custkey
# 				and o_comment not like '%pending%packages%'
# 		group by
# 			c_custkey
# 	) as c_orders (c_custkey, c_count)
# group by
# 	c_count
# order by
# 	custdist desc,
# 	c_count desc
# limit 1;"""

# result = connection.execute(input_query)
# print("The result of the query is:")
# print(result)

# query_analysis = connection.execute("EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON ) " + input_query)
# print("Initial Query Analysis JSON:")
# print(query_analysis)

# qep_root_node = build_qep_tree(query_analysis)
# qep_root_node.set_step(0)

# # BFS just to see how the nodes look like
# q = queue.Queue()
# q.put(qep_root_node)

# while not q.empty():
#     cur_node = q.get()
#     print(cur_node.node_type)
#     print("Level: " + str(cur_node.step))
#     print("===========================================")
#     print("\n")
#     for node in cur_node.children:
#         node.set_step(cur_node.step + 1)
#         q.put(node)