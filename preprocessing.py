import psycopg2
import json
import queue

default_seqpage_cost = 1.0
default_randompage_cost = 4.0

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

class QueryPlanGenerator:
    def __init__(self) -> None:
        self.connection = DBConnection()

    def getAQP(self, query, enable_hashjoin=True, enable_mergejoin=True, enable_nestloop=True,
        enable_bitmapscan=True, enable_indexscan=True, enable_seqscan=True, enable_indexonlyscan=True):
        cursor = self.connection.cur
        cursor.execute("SET enable_hashjoin TO 1") if enable_hashjoin else cursor.execute("SET enable_hashjoin TO 0")
        cursor.execute("SET enable_mergejoin TO 1") if enable_mergejoin else cursor.execute("SET enable_mergejoin TO 0")
        cursor.execute("SET enable_nestloop TO 1") if enable_nestloop else cursor.execute("SET enable_nestloop TO 0")
        cursor.execute("SET enable_bitmapscan TO 1") if enable_bitmapscan else cursor.execute("SET enable_bitmapscan TO 0")
        cursor.execute("SET enable_indexscan TO 1") if enable_indexscan else cursor.execute("SET enable_indexscan TO 0")
        cursor.execute("SET enable_seqscan TO 1") if enable_seqscan else cursor.execute("SET enable_seqscan TO 0")
        cursor.execute("SET enable_indexonlyscan TO 1") if enable_indexonlyscan else cursor.execute("SET enable_indexonlyscan TO 0")

        cursor.execute(cursor.mogrify("EXPLAIN (ANALYZE, FORMAT JSON) " + query))
        query_plan = cursor.fetchall()
        return query_plan

    def generateNoJoinAQPsList(self, query):
        noJoinList = []
        noJoinList.append(self.getAQP(query, enable_mergejoin=False))
        noJoinList.append(self.getAQP(query, enable_hashjoin=False))
        return noJoinList

    def generateNoScanAQPsList(self, query):
        noScanList = []
        noScanList.append(self.getAQP(query, enable_bitmapscan=False))
        noScanList.append(self.getAQP(query, enable_indexscan=False))
        noScanList.append(self.getAQP(query, enable_indexonlyscan=False))
        noScanList.append(self.getAQP(query, enable_bitmapscan=False, enable_indexscan=False))
        noScanList.append(self.getAQP(query, enable_bitmapscan=False, enable_indexonlyscan=False))
        noScanList.append(self.getAQP(query, enable_bitmapscan=False, enable_indexscan=False, enable_indexonlyscan=False))
        return noScanList

    def getQueryResult(self, query):
        result = self.connection.execute(query)
        return result

connection = DBConnection()
print("PostgreSQL server information")

record = connection.execute("SELECT version();")
print("You are connected to - ", record, "\n")