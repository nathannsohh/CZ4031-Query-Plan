import annotation
import streamlit as st
import queue
import time
import graphviz
import preprocessing

@st.cache
def queryProcessing(code):
    QEP = annotation.generate_QEP(code)
    nojoin_AQPs = annotation.generate_nojoin_AQPs(code)
    noscan_AQPs = annotation.generate_noscan_AQPs(code)
    anno_list = annotation.generate_qep_reasons(QEP, nojoin_AQPs, noscan_AQPs)
    return anno_list


def getresultMain(query):
    plans=[]
    connection = preprocessing.DBConnection()
    plans.append(connection.execute(query))
    plans.append(connection.getmainQEP(query))
    connection.close()
    return plans

#will be changed - according to project.py
def processQEPTree(json , anno_list):
    qep_root_node = annotation.build_qep_tree(json)
    step_list = qep_root_node.print_qep_steps()

    graph = graphviz.Digraph()
    graph.attr(rankdir='BT')
    graph.attr('node', shape='rect')
        
    q = queue.Queue()
    q.put(qep_root_node)
    
    parent=None
    j=1
    while not q.empty():
        cur_node = q.get()
        graph.node(name=str(cur_node) , label=cur_node.node_type)
        index = step_list.index(cur_node)
        
        #getting annotation and linking to relevant nodes
        string = getAnnotation(index , anno_list)
        if(string is not None):
            graph.node(name=str(string) , label=string , color='red' )
            graph.edge(str(string) , str(cur_node) , color='red' , rankdir='LR')

        print(cur_node.node_type)
        if(parent!=None):
            graph.edge(str(cur_node) , parent , dir='none')
        #print("Level: " + str(j))
        #print("===========================================")
        #print("Annotation: " + str(cur_node.annotation))
        #print("\n")
        for node in cur_node.children:
            parent = str(cur_node)
            q.put(node)
    
    st.graphviz_chart(graph)

#find index node in step_list , which will then be used by anno_list 
def getAnnotation(index , anno_list):
    val=anno_list[index].split("\n")
    if(len(val)==3):
        return val[1]
    return None
    

#interface page
st.title("Query Plan Application")

#col1, col2 = st.columns(2)
#plan_options = ["Select QEP" , "Main QEP" , "Alternate QEP 1" , "Alternate QEP 2"]

if 'btn_clicked' not in st.session_state:
    st.session_state['btn_clicked'] = False


def callback():
    # change state value
    st.session_state['btn_clicked'] = True
    

col1, col2, = st.columns(2)

with st.form(key="query field"):
    code = st.text_area("Enter Query:" , height=400)
    submit_code = st.form_submit_button("Execute" , on_click=callback)


if submit_code or st.session_state['btn_clicked']:
    
    anno_list= queryProcessing(code)
    
    val = getresultMain(code)
    st.write("Query result:" )
    st.write(val[0])
    st.write("\n")

    agree = st.checkbox('Display Main Query Execution Plan')

    if agree:
        processQEPTree(val[1] , anno_list)
    


