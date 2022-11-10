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

    def print_tree(self, prints=True):
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

        if prints:
            print(output_string)

        return output_string

    def print_qep_steps(self, prints=True):
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

        if prints:
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
        node_cost = cur_plan['Total Cost'] - cur_plan['Startup Cost']
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

def generate_qep_reasons(root):
    step_list = root.print_qep_steps(prints=False) # Generate QEP steps

    output_string = ""
    step_count = 1

    # Go through the step list
    for step in step_list: 
        output_string += f"Step {step_count:<2}: "
        step_count += 1

        # Join
        if "Join" in step.node_type:
            output_string += step.node_type + "\n"

        # Scan
        elif "Scan" in step.node_type:
            ## Sequential Scan
            if "Seq" in step.node_type:
                output_string += f"Sequential Scan\n"
                output_string += \
                "         Tables are read using sequential scan.\n" + \
                "         This is because no index is created on the tables.\n"
            else:
                output_string += step.node_type + "\n"

        # Sort
        elif "Sort" in step.node_type:
            output_string += step.node_type + "\n"

        # Others
        else:
            output_string += step.node_type + "\n"

    return output_string

##############################################################################
def generate_qep_conditions(op_name, conditions, table_subquery_name_pair):
    """
    Generates string conditions for a node operation
    Used in generate_qep_text
    """

    if isinstance(conditions, str):
        if "::" in conditions:
            return conditions.replace("::", " ")[1:-1]
        return conditions[1:-1]
    cond = ""
    for i in range(len(conditions)):
        cond = cond + conditions[i]
        if (not (i == len(conditions) - 1)):
            cond = cond + "and"
    return cond

def generate_qep_text(node, skip=False):
    """
    Generates text string for a QEP node
    Uses generate_qep_conditions to get string conditions for this node
    """

    global steps, cur_step, cur_table_name
    increment = True
    # skip the child if merge it with current node

    if node.node_type in ["Unique", "Aggregate"] and len(node.children) == 1 \
            and ("Scan" in node.children[0].node_type or node.children[0].node_type == "Sort"):
        children_skip = True
    elif node.node_type == "Bitmap Heap Scan" and node.children[0].node_type == "Bitmap Index Scan":
        children_skip = True
    else:
        children_skip = False

    # recursive
    for child in node.children:
        if node.node_type == "Aggregate" and len(node.children) > 1 and child.node_type == "Sort":
            generate_qep_text(child, True)
        else:
            generate_qep_text(child, children_skip)

    if node.node_type in ["Hash"] or skip:
        return

    step=""
    # Extract textual translation of QEP.
    # Combining hash with hash join under certain condition which can be extracted. 
    if "Join" in node.node_type:
        if node.join_type == "Semi":
            # Add 'semi' to 'join' incase of semi join 
            node_type_list = node.node_type.split()
            node_type_list.insert(-1, node.join_type)
            node.node_type = " ".join(node_type_list)
        else:
            pass

        if "Hash" in node.node_type:
            step += "and perform {} on ".format(node.node_type.lower())
            for i, child in enumerate(node.children):
                if child.node_type == "Hash":
                    child.write_qep_output_name(child.children[0].read_qep_output_name())
                    hashed_table = child.read_qep_output_name()
                if i < len(node.children) - 1:
                    step += ("table {} ".format(child.read_qep_output_name()))
                else:
                    step+= (" and table {}".format(child.read_qep_output_name()))
            step = "hash table {} {} under condition {}".format(hashed_table, step, generate_qep_conditions("Hash Cond", node.hash_condition, table_subquery_name_pair))
    


        elif "Merge" in node.node_type:
            step += "perform {} on ".format(node.node_type.lower())
            any_sort = False  # Flag indicated if sort has been performed on relation
            for i, child in enumerate(node.children):
                if child.node_type == "Sort":
                    child.write_qep_output_name(child.children[0].read_qep_output_name())
                    any_sort = True
                if i < len(node.children) - 1:
                    step += ("table {} ".format(child.read_qep_output_name()))
                else:
                    step += (" and table {} ".format(child.read_qep_output_name()))
            # combining sort with merge if table has been sorted
            if any_sort:
                sort_step = "sort "
                for child in node.children:
                    if child.node_type == "Sort":
                        if i < len(node.children) - 1:
                            sort_step += ("table {} ".format(child.read_qep_output_name()))
                        else:
                            sort_step += (" and table {} ".format(child.read_qep_output_name()))

                step = "{} and {}".format(sort_step, step)

    elif node.node_type == "Bitmap Heap Scan":
        # combine bitmap heap scan and bitmap index scan
        if "Bitmap Index Scan" in node.children[0].node_type:
            node.children[0].write_qep_output_name(node.relation_name)
            step = " with index condition {} ".format(generate_qep_conditions("Recheck Cond", \
                node.recheck_condition,table_subquery_name_pair))

        step = "perform bitmap heap scan on table {} {} ".format(node.children[0].read_qep_output_name(), step)


    elif "Scan" in node.node_type:
        if node.node_type == "Seq Scan":
            step += "perform sequential scan on table "
        else:
            step += "perform {} on table ".format(node.node_type.lower())

        step += node.read_qep_output_name()

        if not node.table_filter:
            increment = False

    elif node.node_type == "Unique":
        # combine unique and sort
        if "Sort" in node.children[0].node_type:
            node.children[0].write_qep_output_name(
                node.children[0].children[0].read_qep_output_name())
            step = "sort {} ".format(node.children[0].read_qep_output_name())
            if node.children[0].sort_key:
                step += "with attribute {} and ".format(generate_qep_conditions\
                    ("Sort Key", node.children[0].sort_key, table_subquery_name_pair))

            else:
                step += " and "

        step += "perform unique on table {} ".format(node.children[0].read_qep_output_name())

    elif node.node_type == "Aggregate":
        for child in node.children:
            # combine aggregate and sort
            if "Sort" in child.node_type:
                child.write_qep_output_name(child.children[0].read_qep_output_name())
                step = "sort {} and ".format(child.read_qep_output_name())
            # combine aggregate and scan
            if "Scan" in child.node_type:
                if child.node_type == "Seq Scan":
                    step = "perform sequential scan on {} and ".format(child.read_qep_output_name())
                else:
                    step = "perform {} on {} and ".format(child.node_type.lower(), child.read_qep_output_name())

        step += "perform aggregate on table {}".format(node.children[0].read_qep_output_name())

        if len(node.children) == 2:
            step += " and table {} ".format(node.children[1].read_qep_output_name())

    elif node.node_type == "Sort":
        step+= "perform sort on table {} with {}".format(node.children[0].read_qep_output_name(), generate_qep_conditions("Sort Key", node.sort_key, table_subquery_name_pair))

    elif node.node_type == "Limit":
        step += "limit the result from table {} to {} record(s)".format(node.children[0].read_qep_output_name(), node.plan_rows)
    
    else:
        step += "perform {} on ".format(node.node_type.lower())

        if len(node.children) > 1:
            for i, child in enumerate(node.children):
                if i < len(node.children) - 1:
                    step += (" table {},".format(child.read_qep_output_name()))
                else:
                    step += (" and table {} ".format(child.read_qep_output_name()))
        else:
            step+= " table {}".format(node.children[0].read_qep_output_name())
    
    if node.group_key:
        step += " with grouping on attribute {}".format(generate_qep_conditions("Group Key", node.group_key, table_subquery_name_pair))
    if node.table_filter:
        step += " and filtering on {}".format(generate_qep_conditions("Table Filter", node.table_filter, table_subquery_name_pair))
    if node.join_filter:
        step += " while filtering on {}".format(generate_qep_conditions("Join Filter", node.join_filter, table_subquery_name_pair))

    if increment:
        node.write_qep_output_name("T" + str(cur_table_name))
        step += " to get intermediate table " + node.read_qep_output_name()
        cur_table_name += 1
    if node.subplan_name:
        table_subquery_name_pair[node.subplan_name] = node.read_qep_output_name()

    node.update_desc(step)
    step = "\nStep {}, {}.".format(cur_step, step)
    node.set_step(cur_step)
    cur_step += 1

    steps.append(step)

def generate_reasons(node_a, node_b, diff_idx):
    """
    Compares two nodes and generates a string containing the reasons for their differences
    Used in generate_text_arrays
    """
    text = ""
    # Index Scan vs Seq Scan
    if node_a.node_type =="Index Scan" and node_b.node_type == "Seq Scan":
        text = "Difference {} Reasoning: ".format(diff_idx)
        text += "{} in Plan 1 on relation {} has now transformed to Sequential Scan in Plan 2 on relation {}. This can be attributed to "\
            .format(node_a.node_type, node_a.relation_name, node_b.relation_name)
        # check conditions for transformation from end condition. Here seq scan doesn't use index
        if node_b.index_name is None:
            text += "Plan 1 uses the index attribute {} for selection, which is not used by Plan 2".format(node_a.index_name)
        if int(node_a.actual_rows) < int(node_b.actual_rows):
            text += "and due to this, the actual row count returned increases from {} to {}. ".format(node_a.actual_rows, node_b.actual_rows)

        if node_a.index_condition != node_b.table_filter and int(node_a.actual_rows) < int(node_b.actual_rows):
            text += "This behavior is generally consistent with the change in the selection predicates from {} to {}."\
                .format(node_a.index_condition if node_a.index_condition is not None else "None", node_b.table_filter if node_b.table_filter is not None else "None")
        
    elif node_b.node_type =="Index Scan" and node_a.node_type == "Seq Scan":
        text = "Difference {} Reasoning: ".format(diff_idx)
        text += "Sequential Scan in Plan 1 on relation {} has now transformed to {} in Plan 2 on relation {}. This can be attributed to "\
            .format(node_a.relation_name, node_b.node_type, node_b.relation_name)
        if  node_a.index_name is None:  
            text += "Plan 2 uses the index attribute {} for selection, which is not used by Plan 1.".format(node_b.index_name)
        elif node_a.index_name is not None:
            text += "Both Plan 1 and Plan 2 use their index attributes for selection, which are {} and {} respectively.".format(node_a.index_name, node_b.index_name)
        if int(node_a.actual_rows) > int(node_b.actual_rows):
           text += "Due to this, the actual row count returned decreases from {} to {}. ".format(node_a.actual_rows, node_b.actual_rows)
        if node_a.table_filter != node_b.index_condition and int(node_a.actual_rows) > int(node_b.actual_rows):
            text += "This behavior is generally consistent with the change in the selection predicates from {} to {}."\
                .format(node_a.table_filter if node_a.table_filter is not None else "None", node_b.index_condition if node_b.index_condition is not None else "None")

    # Joins
    elif node_a.node_type and node_b.node_type in ['Merge Join', "Hash Join", "Nested Loop"]:
        text = "Difference {} Reasoning: ".format(diff_idx)
        if node_a.node_type == "Nested Loop" and node_b.node_type == "Merge Join":
            text += "{} in Plan 1 on relation {} has now transformed to {} in Plan 2 on relation {}. This can be attributed to "\
            .format(node_a.node_type, node_a.relation_name, node_b.node_type, node_b.relation_name)
            if int(node_a.actual_rows) < int(node_b.actual_rows):
                text += "the actual row count returned increases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
            if "=" in node_b.node_type:
                text += "The join condition is performed with an equality operator."
            text += "Both sides of the Join operator in Plan 2 can be sorted on the join condition efficiently."

        if node_a.node_type == "Nested Loop" and node_b.node_type == "Hash Join":
            text += "{} in Plan 1 on relation {} has now transformed to {} in Plan 2 on relation {}. This can be attributed to "\
            .format(node_a.node_type, node_a.relation_name, node_b.node_type, node_b.relation_name)
            if int(node_a.actual_rows) < int(node_b.actual_rows):
                text += "the actual row count returned increases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
            if "=" in node_b.node_type:
                text += "The join condition is performed with an equality operator."
                
        if node_a.node_type == "Merge Join" and node_b.node_type == "Nested Loop":
            text += "{} in Plan 1 on relation {} has now transformed to {} in Plan 2 on relation {}. This can be attributed to "\
            .format(node_a.node_type, node_a.relation_name, node_b.node_type, node_b.relation_name)
            if int(node_a.actual_rows) > int(node_b.actual_rows):
                text += "the actual row count returned decreases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
            elif int(node_a.actual_rows) < int(node_b.actual_rows):
                text += "the actual row count returned increases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
                text += "{} joins are used in the scenario where the join conditions are not performed with the equality operator".format(node_b.node_type)
            
        if node_a.node_type == "Merge Join" and node_b.node_type == "Hash Join":
            text += "{} in Plan 1 on relation {} has now transformed to {} in Plan 2 on relation {}. This can be attributed to "\
                .format(node_a.node_type, node_a.relation_name, node_b.node_type, node_b.relation_name)

            if int(node_a.actual_rows) < int(node_b.actual_rows):
                text += "the actual row count returned increases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
            if int(node_a.actual_rows) > int(node_b.actual_rows):
                text += "the actual row count returned decreases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
            text += "Both sides of the Join operator in Plan 2 can be sorted on the join condition efficiently. "

        if node_a.node_type == "Hash Join" and node_b.node_type == "Nested Loop":
            text += "{} in Plan 1 on relation {} has now transformed to {} in Plan 2 on relation {}. This can be attributed to "\
            .format(node_a.node_type, node_a.relation_name, node_b.node_type, node_b.relation_name)
            if int(node_a.actual_rows) > int(node_b.actual_rows):
                text += "the actual row count returned decreases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
            elif int(node_a.actual_rows) < int(node_b.actual_rows):
                text += "the actual row count returned increases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
                text += "{} joins are used in the scenario where the join conditions are not performed with the equality operator".format(node_b.node_type)

        if node_a.node_type == "Hash Join" and node_b.node_type == "Merge Join":
            text += "{} in Plan 1 on relation {} has now transformed to {} in Plan 2 on relation {}. This can be attributed to "\
            .format(node_a.node_type, node_a.relation_name, node_b.node_type, node_b.relation_name)
            if int(node_a.actual_rows) < int(node_b.actual_rows):
                text += "the actual row count returned increases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
            if int(node_a.actual_rows) > int(node_b.actual_rows):
                text += "the actual row count returned decreases from {} to {}.".format(node_a.actual_rows, node_b.actual_rows)
            text += "Both sides of the Join operator in Plan 2 can be sorted on the join condition efficiently. "

    return text

def generate_text_arrays(nodeA, nodeB, difference, reasons):
    """
    Recursively compares the 2 trees
    Generates comparison text from 2 nodes (generate_reasons)
    Appends the differences and reasons to arrays of text
    Returns the arrays of reasons and differences
    Used in generate_comparison
    """
    global diff_idx
    childrenA = nodeA.children
    childrenB = nodeB.children
    children_no_A = len(childrenA)
    children_no_B = len(childrenB)

    if nodeA.node_type == nodeB.node_type and children_no_A == children_no_B:
        if children_no_A != 0:
            for i in range(len(childrenA)):
                generate_text_arrays(childrenA[i], childrenB[i],  difference, reasons)

    else:
        if nodeA.node_type == 'Hash' or nodeA.node_type == 'Sort':
            text = "Difference: {} - {} has been transformed to {}".format(diff_idx, nodeA.children[0].description, nodeB.description)

            text = modify_text(text)
            difference.append(text)
            reason = generate_reasons(nodeA.children[0], nodeB, diff_idx)
            reasons.append(reason)
            diff_idx += 1

        elif nodeB.node_type == 'Hash' or nodeB.node_type == 'Sort':
            text = "Difference: {} - {} has been transformed to {}".format(diff_idx, nodeA.description, nodeB.children[0].description)

            text = modify_text(text)
            difference.append(text)
            reason = generate_reasons(nodeA, nodeB.children[0], diff_idx)
            reasons.append(reason)
            diff_idx += 1

        elif 'Gather' in nodeA.node_type:
            generate_text_arrays(childrenA[0], nodeB, difference, reasons)

        elif 'Gather' in nodeB.node_type:
            generate_text_arrays(nodeA, childrenB[0],  difference, reasons)
        else:
            text = "Difference: {} - {} has been transformed to {}".format(diff_idx, nodeA.description, nodeB.description)

            text = modify_text(text)
            difference.append(text)
            reason = generate_reasons(nodeA, nodeB, diff_idx)
            reasons.append(reason)
            diff_idx += 1

        if children_no_A == children_no_B:
            if children_no_A == 1:
                generate_text_arrays(childrenA[0], childrenB[0], difference, reasons)
            if children_no_A == 2:
                generate_text_arrays(childrenA[0], childrenB[0], difference, reasons)
                generate_text_arrays(childrenA[1], childrenB[1],  difference, reasons)

def modify_text(str):
    """
    Used in generate_text_arrays
    """
    str = str.replace('perform ', '')
    return str

def generate_comparison(json_obj_A, json_obj_B):
    """
    Takes 2 QEP in JSON as input
    Converts the QEP in JSON to a Tree (generate_qep_tree)
    Converts the Nodes in the Tree to text (generate_qep_text)
    Converts the Tree into 2 arrays of text (generate_text_arrays)
    Concatenates the 2 arrays of text into a single string
    Returns this string as output
    """
    global diff_idx
    root_node_a = build_qep_tree(json_obj_A)
    reset_vars()
    generate_qep_text(root_node_a)

    root_node_b = build_qep_tree(json_obj_B)
    reset_vars()
    generate_qep_text(root_node_b)

    diff_idx=1
    difference = []
    reasons = []
    generate_text_arrays(root_node_a, root_node_b, difference, reasons)
    diff_str = ""
    for i in range (len(reasons)):

        diff_str = diff_str + difference[i] + "\n"
        if reasons[i] != "":
            diff_str = diff_str + reasons[i] + "\n"

    return diff_str

def reset_vars():
    """
    Used in generate_comparison()
    """
    global steps, cur_step, cur_table_name, table_subquery_name_pair
    steps = []
    cur_step = 1
    cur_table_name = 1
    table_subquery_name_pair = {}
##############################################################################

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
    connection = preprocessing.DBConnection()
    # QEP_json = connection.getmainQEP(query)
    QEP_json = connection.getALTQEP(query, 1)
    # AQP_json = connection.getALTQEP(query, 0)
    connection.close()
    # comparison = generate_comparison(QEP, AQP)
    QEP = build_qep_tree(QEP_json)
    QEP.print_tree()
    QEP.print_qep_steps()
    print(generate_qep_reasons(QEP))
    # print(QEP_tree)