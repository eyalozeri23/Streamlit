import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import json
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv()

# Snowflake connection parameters
snowflake_params = {
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "warehouse": os.getenv('SNOWFLAKE_WH'),
    "database": os.getenv('SNOWFLAKE_DB'),
    "schema": os.getenv('SNOWFLAKE_SCHEMA')
}

@st.cache_resource
def init_connection():
    try:
        engine = create_engine(URL(
            account = snowflake_params['account'],
            user = snowflake_params['user'],
            password = snowflake_params['password'],
            database = snowflake_params['database'],
            schema = snowflake_params['schema'],
            warehouse = snowflake_params['warehouse']
        ))
        return engine
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None

def load_data(engine, start_date, end_date):
    query = f"""
        
            SELECT
                GEO_CITY as city,
                COUNT(DISTINCT(CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1:uuid::string)) AS number_of_users,
                SUM(CASE WHEN EVENT_NAME = 'in_app_purchase' THEN 1 ELSE 0 END) AS number_of_purchases
            FROM
                CANDIVORE_TEST_DB.ATOMIC.EVENTS
            WHERE
                derived_tstamp BETWEEN '{start_date}' AND '{end_date}'
                AND CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1 IS NOT NULL
            GROUP BY
                GEO_CITY
            ORDER BY
                number_of_users DESC
            LIMIT 10;
         
    """
    return pd.read_sql(query, engine)

def main():
    st.title("Match Masters Analysis")

    tab1, tab2 = st.tabs(["General Statistics", "Events"])

    # Snowflake connection
    engine = init_connection()

    # General Statistics tab
    with tab1:
        st.header("General Statistics")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", datetime.now() - timedelta(days=7))
        with col2:
            end_date = st.date_input("End Date", datetime.now())

        if st.button("Run"):
            data = load_data(engine, start_date, end_date)
            
            if not data.empty:
                # Distribution of users and purchases by city
                st.subheader("Distribution of Users and Purchases by City")
                fig = px.scatter(data, x='number_of_users', y='number_of_purchases', hover_data=['city'],
                                 labels={'number_of_users': 'Number of Users', 'number_of_purchases': 'Number of Purchases', 'city' : 'City'},
                                 title="Users vs Purchases by City")
                st.plotly_chart(fig)

                # Top 10 cities by number of users
                st.subheader("Top 10 Cities by Number of Users")
                top_10_cities = data.nlargest(10, 'number_of_users')
                fig = px.bar(top_10_cities, x='city', y='number_of_users',
                             labels={'number_of_users': 'Number of Users', 'city' : 'City'},
                             title="Top 10 Cities by Number of Users")
                st.plotly_chart(fig)

                # Correlation matrix
                st.subheader("Correlation Matrix")
                corr_matrix = data[['number_of_users', 'number_of_purchases']].corr()
                fig = px.imshow(corr_matrix, 
                                x=['Number of Users', 'Number of Purchases'], 
                                y=['Number of Users', 'Number of Purchases'], 
                                color_continuous_scale="RdBu_r", 
                                title="Correlation Matrix")
                fig.update_layout(width=500, height=500)
                st.plotly_chart(fig)

                # Display correlation
                correlation = data['number_of_users'].corr(data['number_of_purchases'])
                st.metric("Correlation between Number of Users and Total Purchases", f"{correlation:.2f}")

            else:
                st.write("No data available for the selected date range.")

    # Events tab
    with tab2:
        st.header("Events")
        
        user_id = st.text_input("Enter User ID")
        
        if user_id:  
            try:
                query = f"""
                SELECT 
                    DATE(derived_tstamp) as purchase_date,
                    f.value:user_name::string AS user_name,
                    UNSTRUCT_EVENT_IO_CANDIVORE_IN_APP_PURCHASE_1 as purchase_data,
                    SUM(UNSTRUCT_EVENT_IO_CANDIVORE_IN_APP_PURCHASE_1:iap_price::float) as daily_purchase_amount
                FROM CANDIVORE_TEST_DB.ATOMIC.EVENTS,
                    LATERAL FLATTEN(input => CONTEXTS_IO_CANDIVORE_USER_BASE_STATS_1) f
                WHERE f.value:uuid::string = '{user_id}'
                    AND EVENT_NAME = 'in_app_purchase'
                    AND UNSTRUCT_EVENT_IO_CANDIVORE_IN_APP_PURCHASE_1 IS NOT NULL
                GROUP BY 
                    DATE(derived_tstamp),
                    f.value:user_name::string,
                    UNSTRUCT_EVENT_IO_CANDIVORE_IN_APP_PURCHASE_1
                ORDER BY 
                    purchase_date
                """
                
                # Load the data
                results = pd.read_sql(query, engine)
                
                if not results.empty:
                    user_name = results['user_name'].iloc[0]
                    st.subheader(f"Purchase Data for User: {user_name}")
                    
                    # Display summary chart
                    fig = px.bar(
                        results,
                        x='purchase_date',
                        y='daily_purchase_amount',
                        title=f"Daily Purchase Amounts for {user_name}",
                        labels={
                            'purchase_date': 'Date',
                            'daily_purchase_amount': 'Purchase Amount ($)'
                        }
                    )
                    st.plotly_chart(fig)
                    
                    # Process purchase data
                    results['purchase_details'] = results['purchase_data'].apply(lambda x: json.loads(x) if x else {})
                    
                    # Create DataFrame from JSON data
                    purchase_details_df = pd.DataFrame(results['purchase_details'].tolist())
                    
                    # Add date and user information
                    purchase_details_df['purchase_date'] = results['purchase_date']
                    purchase_details_df['user_name'] = results['user_name']
                    
                    # Move date and user_name to front
                    cols = ['purchase_date', 'user_name'] + [col for col in purchase_details_df.columns if col not in ['purchase_date', 'user_name']]
                    purchase_details_df = purchase_details_df[cols]
                    
                    # Format date
                    purchase_details_df['purchase_date'] = pd.to_datetime(purchase_details_df['purchase_date']).dt.strftime('%Y-%m-%d')
                    
                    # Create dynamic column config
                    column_config = {
                        "purchase_date": "Date",
                        "user_name": "User Name"
                    }
                    
                    # Automatically add number formatting for price-related columns
                    price_columns = [col for col in purchase_details_df.columns if any(term in col.lower() for term in ['price', 'amount'])]
                    for col in price_columns:
                        column_config[col] = st.column_config.NumberColumn(
                            col.replace('_', ' ').title(),
                            format="$.2f"
                        )
                    
                    # Display detailed purchase information
                    st.subheader("Detailed Purchase Information")
                    st.dataframe(
                        purchase_details_df,
                        column_config=column_config,
                        use_container_width=True
                    )
                    
                else:
                    st.warning("No purchase data found for this user ID.")
                    
            except Exception as e:
                st.error(f"Error executing query: {str(e)}")

if __name__ == "__main__":
    main()