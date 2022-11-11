import psycopg2
import json
import queue
from preprocessing import QueryPlanGenerator

class Node(object):
    """
    The Node class represents an individual node on a QEP Tree
    """
    def __init__(self, node_type, node_cost, row_number, relation_name, 
                group_key, sort_method, sort_key, index_name, index_condition,
                hash_condition, merge_condition, rows_filtered, recheck_condition):
        self.node_type = node_type
        self.node_cost = node_cost
        self.row_number = row_number
        self.relation_name = relation_name
        self.group_key = group_key
        self.sort_method = sort_method
        self.sort_key = sort_key
        self.index_name = index_name
        self.index_condition = index_condition
        self.hash_condition = hash_condition
        self.merge_condition = merge_condition
        self.rows_filtered = rows_filtered
        self.recheck_condition = recheck_condition
        self.annotation = None
        self.children = []

    def add_child(self, child):
        """
        Takes a child node as input and adds it to the list of children
        """
        self.children.append(child)

    def set_annotation(self, annotation):
        """
        Adds relevant annotation to the node
        """
        self.annotation = annotation

    def num_children(self):
        """
        Returns the number of children for this node
        """
        return len(self.children)

    def print_tree(self, enable_print=True):
        """
        Prints out the tree structure from this node
        """
        node_list = [self, None]
        child_num_list = [1]
        output_string = ""

        while len(node_list) != 0:
            node = node_list.pop(0)
            
            # If branch has no children
            if node == 0: 

                # If it is not the end of the tree; length is 1 if only 'None' is remaining
                if len(node_list) > 1: 
                    output_string += "-"

                # If there are branches on the same level
                if node_list[0] != None: 
                    output_string += " | "

                continue
        
            # If end of tree level
            if node == None: 
                output_string += "\n"

                if len(child_num_list) != 0:
                    child_num_list.pop(0)
                    
                # If not the end of the tree; there are remaining nodes in the tree
                if len(node_list) != 0: 
                    node_list.append(None)
                continue
            
            # If this is a node
            else: 
                output_string += node.node_type # Print the node type
                child_num_list[0] -= 1
                
                # If this node has children
                if node.num_children() != 0: 
                    for child in node.children:
                        node_list.append(child) # Add children to list
                    child_num_list.append(node.num_children()) # Add number of children to the list
                
                # Otherwise indicate no children
                else: 
                    node_list.append(0)
            
            # If there is a sibling node
            if child_num_list[0] != 0: 
                output_string += " : " # Print sibling separator
            
            # If there is a different branch on the same level
            elif node_list[0] != None: 
                child_num_list.pop(0) # Remove children count for the current branch
                output_string += " | " # Print branch separator

        if enable_print:
            print(output_string)

        return output_string

    def print_qep_steps(self, enable_print=True):
        """
        Generates the QEP steps in order
        """
        node_list = [self]
        step_list = []

        # Go through the tree
        while len(node_list) != 0:

            node = node_list.pop(0) # Remove this node from the step list
            step_list.append(node) # Add this node to the step list

            # Add the children of this node to the node list in reverse order
            for child in node.children: 
                node_list.insert(0, child)

        # Reverse the step list
        step_list = step_list[::-1]

        if enable_print:
            for step in step_list:
                print(step.node_type)
            print("")
        
        return step_list

def build_qep_tree(json_qep_data):
    """
    Takes QEP in json format as input and generates a tree structure for the QEP
    """
    child_plans = queue.Queue() # List of Plans
    parent_nodes = queue.Queue() # List of Nodes

    plan = json_qep_data[0][0][0]['Plan'] # Get first Plan of the QEP

    child_plans.put(plan)
    parent_nodes.put(None)

    # Get all Nodes for the QEP Tree
    while not child_plans.empty(): 
        cur_plan = child_plans.get() # Current json Plan
        par_node = parent_nodes.get() # Parent Node
  
        # Set Node attributes
        ## General Node Info
        node_type = cur_plan['Node Type']
        node_cost = cur_plan['Actual Total Time'] - cur_plan['Actual Startup Time']
        row_number = cur_plan['Plan Rows']
        relation_name = cur_plan['Relation Name'] if ('Relation Name' in cur_plan) else None
        ## Groupings
        group_key = cur_plan['Group Key'] if ('Group Key' in cur_plan) else None
        ## Sorts
        sort_method = cur_plan['Sort Method'] if ('Sort Method' in cur_plan) else None
        sort_key = cur_plan['Sort Key'] if ('Sort Key' in cur_plan) else None
        ## Joins
        ### Index Join
        index_name = cur_plan['Index Name'] if ('Index Name' in cur_plan) else None
        index_condition = cur_plan['Index Cond'] if ('Index Cond' in cur_plan) else None
        ### Hash Join
        hash_condition = cur_plan['Hash Cond'] if ('Hash Cond' in cur_plan) else None
        ### Merge Join
        merge_condition = cur_plan['Merge Cond'] if ('Merge Cond' in cur_plan) else None
        ## Filters
        rows_filtered = cur_plan['Rows Removed by Filter'] if ('Rows Removed by Filter' in cur_plan) else None
        ## Rechecks
        recheck_condition = cur_plan['Recheck Cond'] if ('Recheck Cond' in cur_plan) else None

        # Build the Node
        cur_node = Node(node_type, node_cost, row_number, relation_name, 
                        group_key, sort_method, sort_key, index_name, index_condition,
                        hash_condition, merge_condition, rows_filtered, recheck_condition)

        # Add the newly built Node as a child of its parent Node
        if par_node != None:
            par_node.add_child(cur_node)
        # Otherwise set the new Node as the root Node
        else:
            root_node = cur_node

        # Add futher Plans to the list
        if 'Plans' in cur_plan:
            for plan in cur_plan['Plans']:
                child_plans.put(plan) # Put child Plans in the list
                parent_nodes.put(cur_node) # Put the parent Nodes for each child Node into the list

    return root_node

def build_initial_QEP_tree(qep):
    return build_qep_tree(qep).print_qep_steps(enable_print=False)

def build_nojoin_AQPs_tree_list(no_join_list):
    nojoin_AQPs = []
    for aqp in no_join_list:
        nojoin_AQPs.append(build_qep_tree(aqp).print_qep_steps(enable_print=False))
    return nojoin_AQPs

def build_noscan_AQPs_tree_list(no_scan_list):
    noscan_AQPs = []
    for aqp in no_scan_list:
        noscan_AQPs.append(build_qep_tree(aqp).print_qep_steps(enable_print=False))
    return noscan_AQPs

def generate_qep_reasons(QEP, nojoin_AQPs, noscan_AQPs, log=False):
    anno_list = []
    step_count = 1
    join_count = 0

    # Create joins list
    joins_list = []
    for step in QEP:
        if "Join" in step.node_type:
            joins_list.append(step)
    nojoin_steplist = []
    for AQP in nojoin_AQPs:
        ajoins_list = []
        for step in AQP:
            if "Join" in step.node_type:
                ajoins_list.append(step)
        nojoin_steplist.append(ajoins_list)
    
    # Review each step in the QEP
    for step in QEP: 
        output_string = f"Step {step_count:<2}: "
        step_count += 1

        # Join
        if "Join" in step.node_type:
            ## Track if faster joins have been compared already
            hash_join = False
            merge_join = False
            nestedloop_join = False
            partwise_join = False
            explained = False

            ## Log
            if log: print(f"QEP {step.node_type} costs {step.node_cost}")

            ## Print the name of the Join
            if "Nest" in step.node_type:
                output_string += f"Nested Loop Join\n" + \
                    "         This join is implemented using nested loop join because the cost of the nested loop is low.\n"
                step.set_annotation(f"This join is implemented using nested loop join because the cost of the nested loop is low.")
            else:
                output_string += step.node_type + "\n"

            ## Compare to other potential Joins
            for ajoins_list in nojoin_steplist:
                astep = None

                ### Check if the AQP has the same number of joins
                if len(ajoins_list) != len(joins_list):
                    continue
                
                ### Find AQP join with the same complete relations and join separation
                if log: print("Finding AQP join with the same complete relations and join separation")
                relations_QEP = find_common_relations(step, QEP)
                AQP = nojoin_AQPs[nojoin_steplist.index(ajoins_list)]
                for ajoin in ajoins_list:
                    relations_AQP = find_common_relations(ajoin, AQP)
                    if relations_AQP == relations_QEP and ajoin.node_type != step.node_type:
                        astep = ajoin
                        if log: print("AQP join found")
                        break

                ### Otherwise find AQP join with the same half relations and join separation
                if astep == None:
                    if log: print("Finding AQP join with the same half relations and join separation")
                    relations_QEP = find_common_relations(step, QEP)
                    AQP = nojoin_AQPs[nojoin_steplist.index(ajoins_list)]
                    for ajoin in ajoins_list:
                        relations_AQP = find_common_relations(ajoin, AQP)
                        if relations_AQP[:2] == relations_QEP[:2] and ajoin.node_type != step.node_type:
                            astep = ajoin
                            if log: print("AQP join found")
                            break

                ### Otherwise find AQP join with the same relations
                if astep == None:
                    if log: print("Finding AQP join with the same relations")
                    relations_QEP = find_common_relations(step, QEP)
                    AQP = nojoin_AQPs[nojoin_steplist.index(ajoins_list)]
                    for ajoin in ajoins_list:
                        relations_AQP = find_common_relations(ajoin, AQP)
                        if relations_AQP[0] == relations_QEP[0] and relations_AQP[2] == relations_QEP[2] and ajoin.node_type != step.node_type:
                            astep = ajoin
                            if log: print("AQP join found")
                            break

                ### Otherwise the AQP has a different structure
                if astep == None:
                    #### Use the AQP join in the same position as QEP
                    if log: print("Using AQP join in the same position as QEP")
                    astep = ajoins_list[join_count]
                    if "Hash" in astep.node_type and hash_join: continue
                    if "Merge" in astep.node_type and merge_join: continue
                    if "Nest" in astep.node_type and nestedloop_join: continue
                    if "Partition" in astep.node_type and partwise_join: continue
                    #### If AQP join is the same type as QEP join, skip this AQP
                    if astep.node_type == step.node_type: 
                        if log: print("AQP join not found")
                        continue

                ### Log
                if log: print(f"AQP {astep.node_type} costs {astep.node_cost}")

                ### Check if QEP step is faster than AQP step
                if step.node_cost < astep.node_cost:
                    cost_ratio = astep.node_cost / step.node_cost
                    ratio_2dp = round(cost_ratio * 100) / 100
                    output_string += f"         {step.node_type} is {ratio_2dp} times faster than {astep.node_type}.\n"
                    step.set_annotation(f"{step.node_type} is {ratio_2dp} times faster than {astep.node_type}.")
                    if "Hash" in astep.node_type: hash_join = True
                    if "Merge" in astep.node_type: merge_join = True
                    if "Nest" in astep.node_type: nestedloop_join = True
                    if "Partition" in astep.node_type: partwise_join = True
                    explained = True

            ## Additional Explanations
            if not explained and "Nest" not in step.node_type:
                output_string += f"         {step.node_type} is faster than other join operations.\n"
                step.set_annotation(f"{step.node_type} is faster than other join operations.")

            ## Log
            if log: print("")

            ## Increment join count for tracking
            join_count += 1

        # Scan
        elif "Scan" in step.node_type:
            ## Track if faster scans have been compared already
            bitmap_scan = False
            index_scan = False
            indexonly_scan = False
            seq_scan = False
            tid_scan = False
            explained = False

            ## Log
            if log: print(f"QEP {step.node_type} costs {step.node_cost}")

            ## Print the name of the Scan
            if "Seq" in step.node_type:
                output_string += f"Sequential Scan\n" + \
                    f"         Relation {step.relation_name} is read using Sequential Scan because no index is created on the tables.\n"
                step.set_annotation(f"Relation {step.relation_name} is read using Sequential Scan because no index is created on the tables.")
            else:
                output_string += step.node_type + "\n"
            
            ## Compare to other potential Scans
            for AQP in noscan_AQPs:
                astep = None
                for anode in AQP:
                    if "Scan" in anode.node_type and anode.relation_name == step.relation_name and anode.node_type != step.node_type:
                        if "Bitmap" in anode.node_type and bitmap_scan: continue
                        if "Index Scan" in anode.node_type and index_scan: continue
                        if "Index Only Scan" in anode.node_type and indexonly_scan: continue
                        if "Seq" in anode.node_type and seq_scan: continue
                        if "TID" in anode.node_type and tid_scan: continue
                        astep = anode
                        break

                ### Log
                if log: print(f"AQP {astep.node_type} costs {astep.node_cost}") if astep else print("No AQP scan node found")

                ### Check if QEP step is faster than AQP step
                if astep and step.node_cost < astep.node_cost and step.node_cost > 0:
                    cost_ratio = astep.node_cost / step.node_cost
                    ratio_2dp = round(cost_ratio * 100) / 100
                    output_string += f"         {step.node_type} is used for Relation {step.relation_name} as it is {ratio_2dp} times faster than " +\
                        f"{'Sequential Scan' if 'Seq' in astep.node_type else astep.node_type}.\n"
                    step.set_annotation(f"{'Sequential Scan' if 'Seq' in astep.node_type else astep.node_type}.")
                    if "Bitmap" in astep.node_type: bitmap_scan = True
                    if "Index Scan" in astep.node_type: index_scan = True
                    if "Index Only Scan" in astep.node_type: indexonly_scan = True
                    if "Seq" in astep.node_type: seq_scan = True
                    if "TID" in astep.node_type: tid_scan = True
                    explained = True
                
            ## Additional Explanations
            if not explained and "Seq" not in step.node_type:
                output_string += f"         {step.node_type} is used for Relation {step.relation_name} is faster than other scan operations.\n"
                step.set_annotation(f"{step.node_type} is used for Relation {step.relation_name} is faster than other scan operations.")

            ## Log
            if log: print("")

        # Sort
        elif "Sort" in step.node_type:
            output_string += step.node_type + "\n"

        # Others
        else:
            output_string += step.node_type + "\n"

        anno_list.append(output_string)

    return anno_list

def find_common_relations(join, step_list):
    # List containing 2 scan relations and the number of joins between them and the input join
    relation_list = [None, 0, None, 0]
    relation_num = 0

    idx = step_list.index(join) - 1

    # Find scan relations in the children of the join node
    while idx >= 0:
        step = step_list[idx]
        if "Join" in step.node_type:
            relation_list[relation_num * 2 + 1] += 1
        if "Scan" in step.node_type:
            relation_list[relation_num * 2] = step.relation_name
            relation_num += 1
            if relation_num > 1:
                break
        idx -= 1
        
    return relation_list

def print_annotations(anno_list):
    """
    Takes an array of annotation strings and prints all annotations
    """
    for anno in anno_list:
        print(anno, end="")
    return

#################### Testing ####################

query = """
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 314
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 1;"""

if __name__ == "__main__":
    queryPlanGenerator = QueryPlanGenerator()
    QEP = build_initial_QEP_tree(queryPlanGenerator.getAQP(query))
    no_join_aqps_list = queryPlanGenerator.generateNoJoinAQPsList(query)
    no_scan_aqps_list = queryPlanGenerator.generateNoScanAQPsList(query)
    nojoin_AQPs = build_nojoin_AQPs_tree_list(no_join_aqps_list)
    noscan_AQPs = build_noscan_AQPs_tree_list(no_scan_aqps_list)
    anno_list = generate_qep_reasons(QEP, nojoin_AQPs, noscan_AQPs, log=False)
    print_annotations(anno_list)