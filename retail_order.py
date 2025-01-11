#importing required packagaes or libraries
import kagglehub #to get data from kagglehub
import pandas as pd #for file handling 
from sqlalchemy import create_engine #to push data ino postgreSQL
import psycopg2 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT #to efficiently query the tablefrom python
import streamlit as st #for gui in web application
#for graphical representation
import plotly.express as px 
import plotly.graph_objects as go
import plotly.io as pio
import plotly.colors as colors
pio.templates.default = "plotly_white"

# Download latest version
path = kagglehub.dataset_download("ankitbansal06/retail-orders")

print("Path to dataset files:", path)

#Data handling using pandas
data = pd.read_csv(path + r'\orders.csv')
df = pd.DataFrame(data)

#Dropping NaN records and records with missing critical values
df.dropna(how='all')
df.dropna(subset=['List Price'], inplace=True)

#Filling missing values
df.fillna(0, inplace=True)


#Renaming the columns to required format
df = df.rename(columns={'Order Id': 'order_id', 'Order Date': 'order_date', 
'Ship Mode': 'ship_mode', 'Postal Code': 'postal_code', 'Sub Category': 'sub_category', 
'Product Id': 'product_id','cost price': 'cost_price', 'List Price': 'list_price', 
'Discount Percent': 'discount_percent'})

df.columns = map(str.lower, df.columns)

# Select text columns 
txt_cols = df.select_dtypes(include=['object']).columns

# Apply strip to text columns
df[txt_cols] = df[txt_cols].apply(lambda x: x.str.strip())

#Typecasting the order_date column from object to date
df['order_date'] = pd.to_datetime(df['order_date'])

# Calculate discount amount
df['discount'] = df['list_price'] * df['discount_percent']/100

# Calculate sale price
df['sale_price'] = df['list_price'] - df['discount']

# Calculate profit
df['profit'] = ((df['sale_price']) - (df['cost_price'])) * (df['quantity'])

# Splitting into two equal halves of columns
table1_cols = ['order_id', 'order_date', 'ship_mode', 'segment', 'country', 'city', 'state', 'postal_code', 'region']
table2_cols = ['order_id', 'category', 'sub_category', 'product_id', 'cost_price', 'list_price', 'quantity','discount_percent', 'discount', 'sale_price', 'profit']

# Create separate DataFrames
dfa = df[table1_cols].copy()
dfb = df[table2_cols].copy()

#Pushing data into retailorder database in postgreSQL
host="localhost"
user="postgres"
password="plot4053"
database= "retailorder"

engine = create_engine(f"postgresql://{user}:{password}@{host}/{database}")
table_name1="retail1"
dfa.to_sql(table_name1,engine,if_exists="replace",index=False)
table_name2="retail2"
dfb.to_sql(table_name2,engine,if_exists="replace",index=False)


#Connecting to PostgreSQL
try:
    connection=psycopg2.connect(
    host="localhost",
    user="postgres",
    password="plot4053",
    database= "retailorder"
    )
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    mediator = connection.cursor()
except psycopg2.ProgrammingError as e:
  print("An error occurred:", e)
   
#Displaying the proect using streamlit
st.title('Retail Order Data Analysis')
#Using selectbox to select the required query
select_query = st.selectbox("Select a query", ["1.Find top 10 highest revenue generating products", "2.Find the top 5 cities with the highest profit margins",
"3.Calculate the total discount given for each category", "4.Find the average sale price per product category",
"5.Find the region with the highest average sale price", "6.Find the total profit per category",
"7.Identify the top 3 segments with the highest quantity of orders", "8.Determine the average discount percentage given per region",
"9.Find the product category with the highest total profit", "10.Calculate the total revenue generated per year",
"11. Year over Year growth percentage in total revenue","12.Find the monthly total revenue",
"13.Determine the product with the highest profit margin for each category","14.Find the average profit per order for each segment",
"15.Find the state with the highest number of orders", "16.Find the total revenue earned each quarter of each year",
"17.Find the top3 products that has been ordered in huge quantities","18.Find the profit margin of each subcategory of furniture",
"19.Find the profit margin of each subcategory of technology","20.Find the profit margin of each subcategory of office supplies",
"21.Find the number of orders in each shipment mode"])

#Performing the required query using if, eif and else condition
if select_query == "1.Find top 10 highest revenue generating products":
    mediator.execute('''select product_id, sum(quantity * sale_price) as total_revenue 
                     from retail2 group by product_id order by total_revenue DESC limit 10''')
    result_1 = mediator.fetchall()
    df1=pd.DataFrame(result_1, columns = ["PRODUCT ID","TOTAL REVENUE"])
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df1)
    with col2:
        fig1 = px.bar(df1, 
             x='PRODUCT ID', 
             y='TOTAL REVENUE', 
             title='Perf of highest revenue generating products',
             color_discrete_sequence=['#FFD700'])
        st.plotly_chart(fig1)


elif select_query == "2.Find the top 5 cities with the highest profit margins":
    mediator.execute('''select r1.city, sum(r2.profit)/sum(r2.quantity*r2.sale_price) as profit_margin 
                     from retail1 r1 inner join retail2 r2 on r1.order_id = r2.order_id where sale_price>0 
                     group by city order by profit_margin DESC limit 5''')
    result_2 = mediator.fetchall()
    df2=pd.DataFrame(result_2, columns = ["CITY","PROFIT MARGIN"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df2,use_container_width=True)
    with col2:
        fig2 = px.bar(df2, 
             x='CITY', 
             y='PROFIT MARGIN', 
             title='PROFIT MARGINS FOR TOP 5 CITIES',
             color_discrete_sequence=['#FF6347'])
        st.plotly_chart(fig2)

elif select_query == "3.Calculate the total discount given for each category":
    mediator.execute("select category, sum(discount) as total_discount from retail2 group by category")
    result_3 = mediator.fetchall()
    df3=pd.DataFrame(result_3, columns = ["CATEGORY","TOTAL DISCOUNT"])
  
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df3, use_container_width=True)
    with col2:
        fig3 = px.pie(df3,
            names='CATEGORY',
            values='TOTAL DISCOUNT',
            title='TOTAL DISCOUNT GIVEN FOR EACH CATEGORY')
        st.plotly_chart(fig3)

elif select_query == "4.Find the average sale price per product category":
    mediator.execute("select category, avg(sale_price) as average_saleprice from retail2 group by category")
    result_4= mediator.fetchall()
    df4=pd.DataFrame(result_4, columns = ["CATEGORY","AVERAGE SALES PRICE"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df4)
    with col2:
        fig4 = px.bar(df4, 
             x='CATEGORY', 
             y='AVERAGE SALES PRICE', 
             title='AVERAGE SALES PRICE FOR EACH CATEGORY',
             color_discrete_sequence=['#9370DB'])
        fig4.update_layout(
            bargap=0.4,  # Decrease gap between bars (smaller value = wider bars)
            title_font_size=16
            )
        st.plotly_chart(fig4)
   

elif select_query == "5.Find the region with the highest average sale price":
    mediator.execute('''select r1.region, avg(r2.sale_price) as avg_regional_sale_price 
                     from retail1 r1 inner join retail2 r2 on r1.order_id = r2.order_id 
                     group by r1.region order by avg_regional_sale_price DESC limit 1''')
    result_5= mediator.fetchall()
    df5=pd.DataFrame(result_5, columns = ["REGION","AVERAGE SALES PRICE"])
    st.dataframe(df5)


elif select_query == "6.Find the total profit per category":
    mediator.execute("select category, sum(profit) as total_profit_category from retail2 group by category")
    result_6= mediator.fetchall()
    df6=pd.DataFrame(result_6, columns = ["CATEGORY","TOTAL PROFIT"])
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df6)
    with col2:
        fig6 = px.pie(df6, 
             names='CATEGORY', 
             values='TOTAL PROFIT', 
             title='TOTAL PROFIT FOR EACH CATEGORY')
        st.plotly_chart(fig6)
    

elif select_query == "7.Identify the top 3 segments with the highest quantity of orders":
    mediator.execute('''select segment, sum(order_id) as order_seg 
                     from retail1 group by segment order by order_seg DESC limit 3''')
    result_7 = mediator.fetchall()
    df7=pd.DataFrame(result_7, columns = ["SEGMENT","NO OF ORDERS"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df7)
    with col2:
        fig7 = px.bar(df7, 
             x='SEGMENT', 
             y='NO OF ORDERS', 
             title='NO OF ORDERS PER SEGMENT',
             color_discrete_sequence=['#FFB6C1'])
        fig7.update_layout(
            bargap=0.4,  # Decrease gap between bars (smaller value = wider bars)
            title_font_size=16
            )
        st.plotly_chart(fig7)

elif select_query == "8.Determine the average discount percentage given per region":
    mediator.execute('''select r1.region, avg(r2.discount_percent) as avg_discount_percent 
                     from retail1 r1 inner join retail2 r2 on r1.order_id = r2.order_id group by r1.region''')
    result_8 = mediator.fetchall()
    df8=pd.DataFrame(result_8, columns = ["REGION","AVERAGE DISCOUNT PERCENTAGE"])
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df8)
    with col2:
        fig8 = px.bar(df8, 
             x='REGION', 
             y='AVERAGE DISCOUNT PERCENTAGE', 
             title='AVERAGE DISCOUNT PERCENTAGE PER REGION',
             color_discrete_sequence=['#32CD32'])
        fig8.update_layout(
            bargap=0.4,  # Decrease gap between bars (smaller value = wider bars)
            title_font_size=16
            )
        st.plotly_chart(fig8)

elif select_query == "9.Find the product category with the highest total profit":
    mediator.execute('''select category, sum(profit) as total_profit from retail2 
                     group by category order by total_profit DESC limit 1''')
    result_9 = mediator.fetchall()
    df9=pd.DataFrame(result_9, columns = ["CATEGORY","TOTAL PROFIT"])
    st.dataframe(df9)

elif select_query == "10.Calculate the total revenue generated per year":
    mediator.execute('''select date_part('year', r1.order_date) as order_year, 
                     sum(r2.sale_price * r2.quantity) AS total_revenue 
                     from retail1 r1 inner join retail2 r2 on r1.order_id = r2.order_id 
                     group by date_part('year', r1.order_date) order by order_year''')
    result_10 = mediator.fetchall()
    df10=pd.DataFrame(result_10, columns = ["YEAR","TOTAL REVENUE"])
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(df10)
    with col2:
        fig10 = px.line(df10, 
             x='YEAR', 
             y='TOTAL REVENUE', 
             title='TOTAL REVENUE GENERATED PER YEAR',
             color_discrete_sequence=['#FF7F50'])
        st.plotly_chart(fig10)

elif select_query == "11. Year over Year growth percentage in total revenue":
    mediator.execute('''select date_part('year', r1.order_date) as order_year,
                      sum (r2.sale_price * r2.quantity) as total_revenue,
                      lag(sum(r2.sale_price * r2.quantity)) over(order by date_part('year', r1.order_date)) as previous_year_revenue, 
                     (sum(r2.sale_price * r2.quantity) - lag(sum(r2.sale_price * r2.quantity)) 
                      over(order by date_part('year', r1.order_date))) / lag(sum(r2.sale_price * r2.quantity)) 
                      over (order by date_part('year', r1.order_date)) * 100 as yoy_growth_percentage 
                      from retail1 r1 inner join retail2 r2 on r1.order_id = r2.order_id
                      group by date_part('year', r1.order_date) 
                      order by order_year''')
    result_11 = mediator.fetchall()
    df11=pd.DataFrame(result_11, columns = ["ORDER YEAR","TOTAL REVENUE","PREVIOUS YEAR REVENUE","YOY GROWTH PERCENTAGE"])
    st.dataframe(df11)

elif select_query == "12.Find the monthly total revenue":
    mediator.execute('''select date_part('year', r1.order_date) as order_year, 
                        date_part('month', r1.order_date) as order_month,
                        sum(r2.sale_price * r2.quantity) as monthly_revenue
                        from retail1 r1 join retail2 r2 on r1.order_id = r2.order_id
                        group by date_part('year', r1.order_date), date_part('month', r1.order_date)
                        order by order_year, order_month''')
    result_12 = mediator.fetchall()
    df12=pd.DataFrame(result_12, columns = ["ORDER YEAR","ORDER MONTH","MONTHLY REVENUE"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df12)
    with col2:
        fig12 = px.line(df12, 
             x='ORDER MONTH', 
             y='MONTHLY REVENUE', 
             title='TOTAL REVENUE GENERATED PER MONTH',
             color='ORDER YEAR',  # Group by year to get different colors
             color_discrete_sequence=['#FF7F50', '#32CD32'])
        # Update layout to customize label sizes
        fig12.update_layout(
            title_font_size=14,  # Title font size
            xaxis_title='Month',  # X-axis title
            yaxis_title='Revenue',  # Y-axis title
            legend_title='Order Year',  # Legend title
            xaxis=dict(
                tickfont=dict(size=10),  # X-axis labels (month) font size
                ),
             yaxis=dict(
                tickfont=dict(size=10),  # Y-axis labels (revenue) font size
                ),
            legend=dict(
                font=dict(size=10)  # Font size for legend labels (years)
             )
        )
        st.plotly_chart(fig12)

elif select_query == "13.Determine the product with the highest profit margin for each category":
    mediator.execute('''select category, product_id, profit_margin from (
                            select  r2.category, r2.product_id, 
                            (r2.profit / (r2.cost_price * r2.quantity)) * 100 as profit_margin,
                            row_number() over (partition by r2.category order by (r2.profit / (r2.cost_price * r2.quantity)) DESC) AS margin_rank
                            from retail1 r1 join retail2 r2 on r1.order_id = r2.order_id where r2.cost_price > 0
                            ) as ranked_data
                        where margin_rank = 1''')
    result_13 = mediator.fetchall()
    df13=pd.DataFrame(result_13, columns = ["CATEGORY","PRODUCT ID","PROFIT MARGIN"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df13)
    with col2:
        fig13 = px.pie(df13, 
             names='CATEGORY', 
             values='PROFIT MARGIN',
             title="Top Product Profit Margin by Category")
        st.plotly_chart(fig13)

elif select_query == "14.Find the average profit per order for each segment":
    mediator.execute('''select r1.segment, avg(r2.profit) as avg_profit_per_order
                        from retail1 r1 join retail2 r2 on r1.order_id = r2.order_id
                        group by r1.segment''')
    result_14 = mediator.fetchall()
    df14=pd.DataFrame(result_14, columns = ["SEGMENT","AVERAGE PROFIT"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df14)
    with col2:
        fig14 = px.pie(df14, 
             names='SEGMENT', 
             values='AVERAGE PROFIT',
             title="AVverage Profit for each segment")
        st.plotly_chart(fig14)

elif select_query == "15.Find the state with the highest number of orders":
    mediator.execute('''select state, count(distinct order_id) as num_orders
                     from retail1 group by state order by num_orders DESC limit 1''')
    result_15 = mediator.fetchall()
    df15=pd.DataFrame(result_15, columns = ["STATE","NUMBER OF ORDERS"])
    st.dataframe(df15)

elif select_query == "16.Find the total revenue earned each quarter of each year":
    mediator.execute('''select date_part('year', r1.order_date) as order_year,
                        date_part('quarter', r1.order_date) as order_quarter,
                        sum(r2.sale_price * r2.quantity) AS quarterly_revenue
                        from retail1 r1 join retail2 r2 on r1.order_id = r2.order_id
                        group by order_year, order_quarter
                        order by order_year, order_quarter''')
    result_16 = mediator.fetchall()
    df16=pd.DataFrame(result_16, columns = ["ORDER YEAR","ORDER QUARTER","TOTAL REVENUE"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df16)
    with col2:
        fig16 = px.line(df16, 
             x='ORDER QUARTER', 
             y='TOTAL REVENUE', 
             title='TOTAL REVENUE GENERATED PER QUARTER',
             color='ORDER YEAR',  # Group by year to get different colors
             color_discrete_sequence=['#FF7F50', '#32CD32'])
        # Update layout to customize label sizes
        fig16.update_layout(
            title_font_size=14,  # Title font size
            xaxis_title='Quarter',  # X-axis title
            yaxis_title='Revenue',  # Y-axis title
            legend_title='Order Year',  # Legend title
            xaxis=dict(
                tickfont=dict(size=10),  # X-axis labels (month) font size
                ),
             yaxis=dict(
                tickfont=dict(size=10),  # Y-axis labels (revenue) font size
                ),
            legend=dict(
                font=dict(size=10)  # Font size for legend labels (years)
             )
        )
        st.plotly_chart(fig16)

elif select_query == "17.Find the top3 products that has been ordered in huge quantities":
    mediator.execute('''select product_id, sum(quantity) as total_quantity
                        from retail2 group by product_id order by total_quantity DESC limit 3''')
    result_17 = mediator.fetchall()
    df17=pd.DataFrame(result_17, columns = ["PRODUCT ID","TOTAL QUANTITY"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df17)
    with col2:
        fig17 = px.bar(df17, 
             x='PRODUCT ID', 
             y='TOTAL QUANTITY', 
             title='Top 3 products with high quantity',
             color_discrete_sequence=['#87CEEB'])
        fig17.update_layout(
            bargap=0.4,  # Decrease gap between bars (smaller value = wider bars)
            title_font_size=16
            )
        st.plotly_chart(fig17)

elif select_query == "18.Find the profit margin of each subcategory of furniture":
    mediator.execute('''select sub_category, sum(profit)/sum(quantity*sale_price) as profit_margin 
                        from retail2 where category = 'Furniture' 
                        group by sub_category order by profit_margin DESC''')
    result_18 = mediator.fetchall()
    df18=pd.DataFrame(result_18, columns = ["SUB CATEGORY","PROFIT MARGIN"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df18)
    with col2:
        fig18 = px.bar(df18, 
             x='SUB CATEGORY', 
             y='PROFIT MARGIN', 
             title='The profit margin of each subcategory of furniture',
             color_discrete_sequence=['#DDA0DD'])
        fig18.update_layout(
            bargap=0.4,  # Decrease gap between bars (smaller value = wider bars)
            title_font_size=16
            )
        st.plotly_chart(fig18)

elif select_query == "19.Find the profit margin of each subcategory of technology":
    mediator.execute('''select sub_category, sum(profit)/sum(quantity*sale_price) as profit_margin 
                        from retail2 where category = 'Technology' 
                        group by sub_category order by profit_margin DESC''')
    result_19 = mediator.fetchall()
    df19=pd.DataFrame(result_19, columns = ["SUB CATEGORY","PROFIT MARGIN"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df19)
    with col2:
        fig19 = px.bar(df19, 
             x='SUB CATEGORY', 
             y='PROFIT MARGIN', 
             title='The profit margin of each subcategory of technology',
             color_discrete_sequence=['#A52A2A'])
        fig19.update_layout(
            bargap=0.4,  # Decrease gap between bars (smaller value = wider bars)
            title_font_size=16
            )
        st.plotly_chart(fig19)

elif select_query == "20.Find the profit margin of each subcategory of office supplies":
    mediator.execute('''select sub_category, sum(profit)/sum(quantity*sale_price) as profit_margin
                        from retail2 where category = 'Office Supplies' 
                        group by sub_category order by profit_margin DESC''')
    result_20 = mediator.fetchall()
    df20=pd.DataFrame(result_20, columns = ["SUB CATEGORY","PROFIT MARGIN"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df20)
    with col2:
        fig20 = px.bar(df20, 
             x='SUB CATEGORY', 
             y='PROFIT MARGIN', 
             title='The profit margin of each subcategory of furniture',
             color_discrete_sequence=['#FFFF00'])
        fig20.update_layout(
            bargap=0.4,  # Decrease gap between bars (smaller value = wider bars)
            title_font_size=16
            )
        st.plotly_chart(fig20)

elif select_query == "21.Find the number of orders in each shipment mode":
    mediator.execute("select ship_mode, count(order_id) as order_count from retail1 group by ship_mode")
    result_21 = mediator.fetchall()
    df21=pd.DataFrame(result_21, columns = ["SHIP MODE","ORDER COUNT"])
    col1, col2 = st.columns([3,2])
    with col1:
        st.dataframe(df21)
    with col2:
        fig21 = px.pie(df21, 
             names='SHIP MODE', 
             values='ORDER COUNT',
             title="The number of orders in each shipment mode")
        st.plotly_chart(fig21)

#Incase of further dev we can use temporarily
else:
    st.write("Under Construction")

#Closing the cursor and SQL connection
mediator.close()
connection.close()