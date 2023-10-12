import openai
import re
import pyperclip
import streamlit as st
from prompts import get_system_prompt
from snowflake.snowpark import Session
from config import conn_params
import plotly.express as px
import altair as alt
from snowflake.snowpark.functions import col
from streamlit_option_menu import option_menu
import pandas as pd
from test_price import test_price
st.set_page_config(layout = 'wide' , initial_sidebar_state = 'expanded')
st.sidebar.write("")
st.sidebar.write("")

st.markdown(
    """
    <div style="display: flex; justify-content: center; margin-top: -75px;">
        <img src="https://yshah1505.blob.core.windows.net/logo/SnowPilot%20Logo.png" width="500" />
    </div>
    """,
    unsafe_allow_html=True
)


page_selected = option_menu(
    menu_title=None,
    options=['Home', 'Insurance Price Quotation'],
    default_index=0,
    icons=None,
    menu_icon=None,
    orientation='horizontal',
    styles={
        "container": {
            "padding": "0!important",
            "background-color": "#fafafa",
            "width": "480px",
            "margin-left": "200",
                # Adjust the width as needed
        },
        "icon": {"display": "none"},
        "nav": {"background-color": "#f2f5f9"},
        "nav-link": {
            "font-size": "14px",
            "font-weight": "bold",
            "color": "#00568D",
            "border-right": "1.5px solid #00568D",
            "border-left": "1.5px solid #00568D",
            "border-top": "1.5px solid #00568D",
            "border-bottom": "1.5px solid #00568D",
            "padding": "10px",
            "text-transform": "uppercase",
            "border-radius": "0px",
            "margin": "5px",
            "--hover-color": "#e1e1e1",
        },
        "nav-link-selected": {"background-color": "#00568d", "color": "#ffffff"},
    }
)
faq_dict = {
            "gmf": [
                "Show me Insurance Price for ''Vehicle Number'' ",
                "Show me Risky Vehicle based on Driving pattern",
                "Tell me the VIN numbers of vehicles with an age greater than 5 years.",
            ],
        }
faq = faq_dict["gmf"]



option = st.sidebar.selectbox("**FAQs**", faq)
st.sidebar.success(option)






show_result = st.sidebar.checkbox("Show Result", True)
# df=message["results"].toPandas()  
show_graph = st.sidebar.checkbox("Show Graph", False)
graph_type = st.sidebar.multiselect("Select graph type:", ["Bar chart","Double Bar Chart", "Line chart", "3D Scatter Plot","Scatter Plot", "Pie chart"],default="Bar chart")


if page_selected == 'Insurance Price Quotation':
	test_price()
if page_selected == 'Home':
    if "error" not in st.session_state:
        st.session_state.error = 0
    if "show_result" not in st.session_state:
        st.session_state.show_result = 0
    if "data_query" not in st.session_state:
        st.session_state.data_query = ""
        
    openai.api_key = conn_params["OPENAI_API_KEY"]
    # print("generate prompt part ")
    if "messages" not in st.session_state:
        # system prompt includes table information, rules, and prompts the LLM to produce
        # a welcome message to the user.
        st.session_state.messages = [{"role": "system", "content": get_system_prompt()}]
    if "intt" not in st.session_state:
        st.session_state.intt=0

    # st.sidebar.title("Know Your Premium")
    # VIN = st.sidebar.text_input("VIN:", placeholder= "Please provide the Vehicle Number:")

    # if st.sidebar.button("Check"):
    #     session = Session.builder.configs(conn_params).create()
    #     table_name = "TRAINING.GOLD_MODIFIED"
    #     data = session.table(table_name).filter(col("VIN")==VIN)
    #     df = pd.DataFrame(data.to_pandas())
    #     df=df["ML_PRICE"]
        
    #     pattern = r'^[A-Z0-9]{17}$'
    #     if len(df) >=1:
    #         formatted_number = "{:.2f}".format(df[0])
    #         st.sidebar.success(f" Premium Price : {formatted_number}")

    #     elif (re.match(pattern, VIN)) :
    #         st.sidebar.error("You have entered valid VIN but not available in the database. Go for the New Tab to find the Basic Price")
            
    #     else:
    #         st.sidebar.error("You have entered Invalid VIN")
    
    # Add checkboxes for showing/hiding SQL and result
    # show_sql = st.sidebar.checkbox("Show SQL Query",    )

    

    sql_displayed = False   
    # Prompt for user input and save
    
    if prompt := st.chat_input():
        st.session_state.intt=1
        st.session_state.messages.append({"role": "user", "content": prompt})

     # print("for message loop")
    #display the existing chat messages
    for message in st.session_state.messages:
        if message["role"] == "system":
            continue

        with st.chat_message(message["role"],avatar=("https://yshah1505.blob.core.windows.net/logo/Assistant.png" if message["role"] == "assistant" else "🧑")):

            # if message["content"][0]=="#":
            #     x=0
            # else:
            st.write(f'<span style="font-size: 22px;">{message["content"]}</span>', unsafe_allow_html=True)

            # if "results" in message:
            #     st.dataframe(message["results"])
    sql=""
    query_found = 0
    # If last message is not from assistant, we need to generate a new response
    if st.session_state.messages[-1]["role"] != "assistant":
        with st.chat_message("assistant",avatar='https://yshah1505.blob.core.windows.net/logo/Assistant.png'):
            response = ""
            resp_container = st.empty()
            query_found = 0
            for delta in openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
                stream=True,
            ):
                # print(delta)
                response += delta.choices[0].delta.get("content", "")       
                resp_container.markdown(f"""<div style="
                        font-size: 20px;
                        ">{response}</div>""",unsafe_allow_html=True)
                # print([{"role": m["role"], "content": m["content"]} for m in st.session_state.messages])
            # print(type(response))
            sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)

            
            message = {"role": "assistant", "content": response}
            
            sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)

            message["results"]=[]

            if sql_match:
                query_found = 1
                st.session_state.data_query=sql_match.group(1)
                sql = sql_match.group(1)
                st.session_state.show_result=1
                st.session_state.error = 0
                try:
                    session = Session.builder.configs(conn_params).create()
                    message["results"] = session.sql(sql).collect()
                    
                except Exception as e:
                    st.session_state.error = 1
                    # Handle the error gracefully and display a custom message
                    custom_error_message = "An error has occurred: " + str(e)
                    
                    st.error(f'{custom_error_message}')
                    st.write(f'<span style="font-size: 20px;">Please Try Again.</span>', unsafe_allow_html=True)
                    

                # session = Session.builder.configs(conn_params).create()
                # message["results"] = session.sql(sql).collect()
                # print(message["results"])
                
            st.session_state.messages.append(message)
    

    if show_result:
        vin=None
        if(st.session_state.show_result==0):
            st.write("")
        elif message["results"]== []:  
                
                # pattern = r"SELECT FINAL_PREMIUM_PRICE FROM .* VIN = '(\w+)' .*"
                pattern = r"SELECT\s+FINAL_PREMIUM_PRICE\s+FROM\s+.*\s+VIN\s*=\s*'(\w+)'\s+.*"
                pattern2= r"SELECT\s+FINAL_PREMIUM_PRICE\s+FROM\s+.*\s+VIN\s*=\s*'(\w+)'(\s*;)?$"
                pattern3=r"SELECT\s+FINAL_PREMIUM_PRICE\s+FROM\s+[^;]*\s+WHERE\s+VIN\s+(?i)(?:ILIKE|LIKE)\s+'%?([^%']+)%?'"
                match = re.search(pattern, sql, flags=re.DOTALL)
                match2= re.search(pattern2, sql, flags=re.DOTALL)
                match3= re.search(pattern3, sql, flags=re.DOTALL)
                # st.write(match)
                if match:
                    # st.write(sql)
                    vin = match.group(1)
                    pattern_vin = r'^[A-Z0-9]{17}$'                  
                    # st.write(vin)
                    if (re.match(pattern_vin, vin)) :
                        st.error("Valid VIN, not in the database. Open a New Tab to find the Basic Price.")
                        
                    else:
                        st.error("You have entered Invalid VIN")
                                        
                elif match2:
                    # st.write(sql)
                    vin = match2.group(1)

                    pattern_vin = r'^[A-Z0-9]{17}$'
                    # st.write(vin)
                    print(type(vin))
                    if(re.match(pattern_vin, vin)) :
                        st.error("Valid VIN, not in the database. Open a New Tab to find the Basic Price.")
                        
                    else:
                        st.error("You have entered Invalid VIN")
                elif match3:
                    # st.write(sql)
                    vin = match3.group(1)
                    pattern_vin = r'^[A-Z0-9]{17}$'
                    # st.write(vin)
                    print(type(vin))
                    if(re.match(pattern_vin, vin)) :
                        st.error("Valid VIN, not in the database. Open a New Tab to find the Basic Price.")
                        
                    else:
                        st.error("You have entered Invalid VIN")
                else:
                    if st.session_state.error == 0:
                        if query_found == 0:
                            pass
                        else:
                            st.write(f'<span style="font-size: 20px;">We didn''t find any result regarding you prompt</span>', unsafe_allow_html=True)
            # st.dataframe(message["results"])   
        else:
            data_frame = message["results"]   
            column_names = data_frame[0].asDict().keys()
            num_rows=2
            num_rows = len(data_frame)
            num_columns = len(column_names)
            # Get the column names from the DataFrame
            # Print or display the column names
            column_names_list = list(column_names)
            # st.write(column_names_list[0]) # gets the column naem
            # st.dataframe(message["results"])
            
            if(column_names_list[0].upper() == "FINAL_PREMIUM_PRICE"):
                # if table has multiple values
                #   then simpley print the table
                # else
                formatted_number = "{:.2f}".format(message["results"][0][0])
                # print(num_rows)
                # st.write(num_rows)
                if(num_rows > 1): #if the data has more than 1 rown print table else print the custom message for premium price
                    st.dataframe(message["results"])
                else:

                    st.markdown(f"""<div style="                        
                            text-align: center;                     
                            font-size: 22px;
                            ">The Premium Price for this vehicle is ${formatted_number}</div>""",unsafe_allow_html=True)
                    #  st.write(f'<span style="font-size: 50px;">{centered_markdown}</span>', unsafe_allow_html=True)
                    # st.write(f"The Premium Price for this vehicle is {message['results'][0][0]}")
                
            else:  
                st.dataframe(message["results"])
    else:
        st.write("")

    # if show_sql:
    #         # Display st.session_state.data_query with Markdown decoration
    #     if st.session_state.data_query:
    #         # f'<span style="font-size: 24px;">{var}</span>', unsafe_allow_html=True
    #         centered_markdown = f'<div style="text-align: center;">\n\n**SQL Query:**\n\n```sql\n{st.session_state.data_query}\n```\n\n</div>'
    #         st.markdown(centered_markdown, unsafe_allow_html=True)
    #         # st.markdown(f"**SQL Query:**\n```sql\n{st.session_state.data_query}\n```")


    if show_graph and st.session_state.data_query:
        df = pd.DataFrame(message["results"])
        

        for i in graph_type:
            if i == "Bar chart":
                left_columns, right_columns , more_columns = st.columns(3)

    # Create a dropdown menu in the left column
                with left_columns:    
                    x_column_bar = st.selectbox("# **Select x-axis column for Bar chart**", df.columns)

    # Create a dropdown menu in the right column
                with right_columns:    
                    y_column_bar = st.selectbox("# **Select y-axis column for Bar chart**", df.columns)
                fig = px.bar(df.head(10), x=x_column_bar, y=y_column_bar)
                fig.update_layout(
                    xaxis_title= x_column_bar,
                    yaxis_title= y_column_bar,
                    title=f"Bar Chart of {x_column_bar} vs {y_column_bar}"
                )
                st.plotly_chart(fig)
            elif i == "Double Bar Chart":

                left_columns, right_columns ,more_columns = st.columns(3)
                
    # Create a dropdown menu in the left column
                with left_columns:    
                    x_columns_double = st.selectbox("# **Select x-axis column for  Double Bar Chart**", df.columns)

    # Create a dropdown menu in the right column
                with right_columns:    
                    y_columns_double = st.selectbox("# **Select y-axis column for Double Bar Chart**", df.columns)

                with more_columns:
                    z_columns_double = st.selectbox("# **Select y2-axis column for Double Bar chart**:", df.columns)
                
                selected_columns = [x_columns_double, y_columns_double ,z_columns_double]
                
                if len(set(selected_columns)) < len(selected_columns):
                    st.error("Please select different values from all the Drop-Down Menus")
                else:
                                  
                    full_df = df[selected_columns].copy()
                    new_df = full_df.head(11)
                    # st.write(new_df)

                    source=pd.melt(new_df, id_vars=[x_columns_double])
                    chart=alt.Chart(source).mark_bar(strokeWidth=100).encode(
                    x=alt.X('variable:N', title="", scale=alt.Scale(paddingOuter=0.1)),#paddingOuter - you can play with a space between 2 models 
                    y='value:Q',
                    color='variable:N',
                    column=alt.Column(x_columns_double, title="", spacing =0), #spacing =0 removes space between columns, column for can and st 
                    ).properties( width = 100, height = 150, ).configure_header(labelOrient='bottom').configure_view(
                    strokeOpacity=0)

                    st.altair_chart(chart)

            elif i == "Line chart":
                left_columns, right_columns = st.columns(2)

    # Create a dropdown menu in the left column
                with left_columns:    
                    x_columns_line = st.selectbox("# **Select x-axis column for Line chart**", df.columns)

    # Create a dropdown menu in the right column
                with right_columns:    
                    y_columns_line = st.selectbox("# **Select y-axis column for Line chart**", df.columns)

                fig = px.line(df.head(10), x=x_columns_line, y=y_columns_line)
                fig.update_layout(
                    xaxis_title= x_columns_line,
                    yaxis_title= y_columns_line,
                    title=f"Line Chart of {x_columns_line} vs {y_columns_line}"
                )
                st.plotly_chart(fig)
            elif i == "3D Scatter Plot":


                left_columns, right_columns ,more_columns = st.columns(3)

    # Create a dropdown menu in the left column
                with left_columns:    
                    x_columns_3d_scatter = st.selectbox("# **Select x-axis column for 3D Scatter Plot**", df.columns)

    # Create a dropdown menu in the right column
                with right_columns:    
                    y_columns_3d_scatter = st.selectbox("# **Select y-axis column for 3D Scatter Plot**", df.columns)

                with more_columns:
                    z_columns_3d_scatter = st.selectbox("Select z-axis column for 3D Scatter Plot:", df.columns)
                
                fig = px.scatter_3d(df.head(10), x=x_columns_3d_scatter, y=y_columns_3d_scatter , z=z_columns_3d_scatter)
                fig.update_layout(
                    scene=dict(
                        xaxis_title= x_columns_3d_scatter,
                        yaxis_title= y_columns_3d_scatter,
                        zaxis_title= z_columns_3d_scatter
                    ),
                    title=f"3D Scatter Plot of {x_columns_3d_scatter}, {y_columns_3d_scatter}, {z_columns_3d_scatter}"
                )
                st.plotly_chart(fig)
            elif i == "Scatter Plot":

                left_columns, right_columns  = st.columns(2)

    # Create a dropdown menu in the left column
                with left_columns:    
                    x_columns_scatter = st.selectbox("# **Select x-axis column for Scatter Plot**", df.columns)

    # Create a dropdown menu in the right column
                with right_columns:    
                    y_columns_scatter = st.selectbox("# **Select y-axis column for Scatter Plot**", df.columns)

                fig = px.scatter(df.head(10), x=x_columns_scatter, y=y_columns_scatter)
                fig.update_layout(
                    xaxis_title= x_columns_scatter,
                    yaxis_title= y_columns_scatter,
                    title=f"Scatter Plot of {x_columns_scatter} vs {y_columns_scatter}"
                )
                st.plotly_chart(fig)
            else:
                left_columns, right_columns  = st.columns(2)

                # Create a dropdown menu in the left column
                with left_columns:    
                    x_columns_pie = st.selectbox("# **Select x-axis column for Pie Chart**", df.columns)
                # Create a dropdown menu in the right column
                with right_columns:    
                    y_columns_pie = st.selectbox("# **Select y-axis column for Pie Chart**", df.columns)

                fig = px.pie(df.head(10), values=y_columns_pie, names=x_columns_pie)
                fig.update_layout(
                    title=f"Pie Chart of {x_columns_pie} vs {y_columns_pie}"
                )
                st.plotly_chart(fig)

    
    
