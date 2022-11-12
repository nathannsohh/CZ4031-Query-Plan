import annotation
import streamlit as st
import queue
import graphviz
import preprocessing

@st.cache
def queryProcessing(code):
    queryPlanGenerator = preprocessing.QueryPlanGenerator()
    json = queryPlanGenerator.getAQP(code)
    QEP = annotation.build_initial_QEP_tree(json)
    no_join_aqps_list = queryPlanGenerator.generateNoJoinAQPsList(code)
    no_scan_aqps_list = queryPlanGenerator.generateNoScanAQPsList(code)
    nojoin_AQPs = annotation.build_nojoin_AQPs_tree_list(no_join_aqps_list)
    noscan_AQPs = annotation.build_noscan_AQPs_tree_list(no_scan_aqps_list)
    anno_list = annotation.generate_qep_reasons(QEP, nojoin_AQPs, noscan_AQPs, log=False)

    return anno_list


def getresultMain(query):
    plans=[]
    connection = preprocessing.DBConnection()
    queryPlanGenerator = preprocessing.QueryPlanGenerator()
    plans.append(connection.execute(query))
    plans.append(queryPlanGenerator.getAQP(query))
    connection.close()
    return plans

def processQEPTree(json , anno_list):
    graph = graphviz.Digraph()
    graph.attr(rankdir='BT' , bgcolor='lightblue' , margin='0.0 , 0.0')
    graph.attr('node', shape='rect')

    qep_root_node = annotation.build_qep_tree(json)
    step_list = qep_root_node.print_qep_steps()

    q = queue.Queue()
    q.put(qep_root_node)
    
    parent=None
    while not q.empty():
        cur_node = q.get()
        labelling = cur_node.node_type + "\n"
        if(cur_node.relation_name is not None):
            labelling+=str(cur_node.relation_name)
        graph.node(name=str(cur_node) , label=labelling)
        index = step_list.index(cur_node)
        
        #getting annotation and linking to relevant nodes
        string = getAnnotation(index , anno_list)
        if(string is not None):
            graph.node(name=str(string) , label=string, color='red' )
            graph.edge(str(string) , str(cur_node) , color='red')

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
    
    st.graphviz_chart(graph , use_container_width=True)

#find index node in step_list , which will then be used by anno_list 
def getAnnotation(index , anno_list):
    val=anno_list[index].split("\n")
    if(len(val)==3):
        return val[1]
    return None
    

def callback():
    # change state value
    st.session_state['btn_clicked'] = True
    
def running():
#interface page
    st.set_page_config(layout="wide")
    st.title("Query Plan Application")

#plan_options = ["Select QEP" , "Main QEP" , "Alternate QEP 1" , "Alternate QEP 2"]

    if 'btn_clicked' not in st.session_state:
        st.session_state['btn_clicked'] = False
    with st.form(key="query field"):
        code = st.text_area("Enter Query:" , height=400 )
        submit_code = st.form_submit_button("Execute" , on_click=callback)


    if submit_code or st.session_state['btn_clicked']:
    
        anno_list= queryProcessing(code)
    
        val = getresultMain(code)
        st.write("Query result:" )
        st.write(val[0])

        st.markdown("""
        <style>
                .css-6awftf{
                    right:0rem;
                }
                .css-pe32b6 svg{
                    max-height:100%;
                }

        <style>
        """ , unsafe_allow_html=True)
      
        agree = st.checkbox('Display Main Query Execution Plan')
        
        if(agree):
            processQEPTree(val[1] , anno_list)
            
            annotation.print_annotations(anno_list)
            for anno in anno_list:
                val=anno.split("\n")
                st.write(val[0])
                st.write(val[1])
                if(len(val)==3):
                    st.write(val[2])
    
    

