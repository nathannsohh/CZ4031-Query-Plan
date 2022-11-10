import psycopg2
import json
import queue
import preprocessing

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
        self.children = []

    def add_child(self, child):
        """
        Takes a child node as input and adds it to the list of children
        """
        self.children.append(child)

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

def generate_QEP(query):
    print("generating main QEP")
    connection = preprocessing.DBConnection()
    QEP = build_qep_tree(connection.getAQP(query, enable_seqscan=False)).print_qep_steps(enable_print=False)
    connection.close()
    print("finished generating main QEP\n")
    return QEP

def generate_nojoin_AQPs(query):
    nojoin_AQPs = []
    print("generating join disabled AQPs")
    connection = preprocessing.DBConnection()
    print("generating no_mergejoin_AQP")
    nojoin_AQPs.append(build_qep_tree(connection.getAQP(query, enable_mergejoin=False)).print_qep_steps(enable_print=False))
    print("generating no_hashjoin_AQP")
    nojoin_AQPs.append(build_qep_tree(connection.getAQP(query, enable_hashjoin=False)).print_qep_steps(enable_print=False))
    # print("generating no_mj_hj_AQP")
    # nojoin_AQPs.append(build_qep_tree(connection.getAQP(query, enable_mergejoin=False, enable_hashjoin=False)).print_qep_steps(enable_print=False))
    connection.close()
    print("finished generating join disabled AQPs\n")
    return nojoin_AQPs

def generate_noscan_AQPs(query):
    noscan_AQPs = []
    print("generating scan disabled AQPs")
    connection = preprocessing.DBConnection()
    print("generating no_bitmapscan_AQP")
    noscan_AQPs.append(build_qep_tree(connection.getAQP(query, enable_bitmapscan=False)).print_qep_steps(enable_print=False))
    print("generating no_indexscan_AQP")
    noscan_AQPs.append(build_qep_tree(connection.getAQP(query, enable_indexscan=False)).print_qep_steps(enable_print=False))
    print("generating no_idxonlyscan_AQP")
    noscan_AQPs.append(build_qep_tree(connection.getAQP(query, enable_indexonlyscan=False)).print_qep_steps(enable_print=False))
    print("generating no_bmp_idx_AQP")
    noscan_AQPs.append(build_qep_tree(connection.getAQP(query, enable_bitmapscan=False, enable_indexscan=False)).print_qep_steps(enable_print=False))
    print("generating no_bmp_idxo_AQP")
    noscan_AQPs.append(build_qep_tree(connection.getAQP(query, enable_bitmapscan=False, enable_indexonlyscan=False)).print_qep_steps(enable_print=False))
    print("generating no_bmp_idx_idxo_AQP")
    noscan_AQPs.append(build_qep_tree(connection.getAQP(query, enable_bitmapscan=False, enable_indexscan=False, enable_indexonlyscan=False)).print_qep_steps(enable_print=False))
    connection.close()
    print("finished generating scan disabled AQPs\n")
    return noscan_AQPs

def generate_qep_reasons(QEP, nojoin_AQPs, noscan_AQPs, log=False):
    anno_list = []
    step_count = 1
    join_count = 0
    
    # Review each step in the QEP
    for step in QEP: 
        output_string = f"Step {step_count:<2}: "
        step_count += 1

        # Join
        if "Join" in step.node_type:
            ## Nested Loop Join
            if "Nest" in step.node_type:
                output_string += f"Nested Loop Join\n"
                output_string += \
                    "         This join is implemented using nested loop join because the cost of the nested loop is low.\n"
            ## Other Join
            else:
                output_string += step.node_type + "\n"
                ### Compare with Merge Join
                ### Compare with Hash Join
                ### Compare with Nested Loop Join

            ## Increment join count for tracking
            join_count += 1

        # Scan
        elif "Scan" in step.node_type:
            bitmap_scan = False
            index_scan = False
            indexonly_scan = False
            seq_scan = False
            tid_scan = False

            ## Log
            if log: print(f"QEP {step.node_type} costs {step.node_cost}")

            ## Print the name of the Scan
            if "Seq" in step.node_type:
                output_string += f"Sequential Scan\n" + \
                    "         Tables are read using sequential scan because no index is created on the tables.\n"
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
                if astep and step.node_cost < astep.node_cost:
                    cost_ratio = astep.node_cost / step.node_cost
                    ratio_2dp = round(cost_ratio * 100) / 100
                    output_string += f"         {step.node_type} is {ratio_2dp} faster than " +\
                        f"{'Sequential Scan' if 'Seq' in astep.node_type else astep.node_type}.\n"
                    bitmap_scan = True if "Bitmap" in astep.node_type else False
                    index_scan = True if "Index Scan" in astep.node_type else False
                    indexonly_scan = True if "Index Only Scan" in astep.node_type else False
                    seq_scan = True if "Seq" in astep.node_type else False
                    tid_scan = True if "TID" in astep.node_type else False

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

if __name__ == "__main__":
    QEP = generate_QEP(query)
    nojoin_AQPs = generate_nojoin_AQPs(query)
    noscan_AQPs = generate_noscan_AQPs(query)
    anno_list = generate_qep_reasons(QEP, nojoin_AQPs, noscan_AQPs)
    print_annotations(anno_list)