import preprocessing
import project
import annotation
import streamlit as st
import queue
import time

#returns json of [result , mainQEP, ALTQEP1 , ALTAEP2]
@st.cache
def fetchResultQuery(query):
    plans=[]
    connection = preprocessing.DBConnection()
    plans.append(connection.execute(query))
    plans.append(connection.getmainQEP(query))
    plans.append(connection.getALTQEP1(query))
    plans.append(connection.getALTQEP2(query))
    connection.close()
    return plans

#will be changed - according to project.py
def processQEPTree(annotate):
    qep_root_node = annotation.build_qep_tree(annotate)
    qep_root_node.set_step(0)

    # BFS just to see how the nodes look like
    q = queue.Queue()
    q.put(qep_root_node)

    while not q.empty():
        cur_node = q.get()
        print(cur_node.node_type)
        st.write(cur_node.node_type)
        print("Level: " + str(cur_node.step))
        st.write("Level " + str(cur_node.step) )
        print("===========================================")
        st.write("===========================================\n")
        print("\n")
        for node in cur_node.children:
            node.set_step(cur_node.step + 1)
            q.put(node)


#interface page
st.title("Query Plan Application")

col1, col2 = st.columns(2)
plan_options = ["Select QEP" , "Main QEP" , "Alternate QEP 1" , "Alternate QEP 2"]

if 'btn_clicked' not in st.session_state:
    st.session_state['btn_clicked'] = False


def callback():
    # change state value
    st.session_state['btn_clicked'] = True
    
def time_consuming_func():
    time.sleep(3)
    return

with st.form(key="query field"):
    code = st.text_area("Enter Query:" , height=400)
    submit_code = st.form_submit_button("Execute" , on_click=callback)


if submit_code or st.session_state['btn_clicked']:
    time_consuming_func()
    #with st.spinner("Processing query"):
    result = fetchResultQuery(code)

    st.success("Results Obtained")
    st.write("Result is:", result[0])

    choice = st.selectbox("Select Choice of QEP", options=plan_options)
    
    if(choice=="Main QEP"):
        processQEPTree(result[1])
       
    elif(choice=="Alternate QEP 1"):
        processQEPTree(result[2])
        
    elif(choice=="Alternate QEP 2"):
        processQEPTree(result[3])
        
