import pandas as pd
import sqlite3
try:
    conn = sqlite3.connect(r"C:\Users\Manjit Biswal\Downloads\sqlite\Data Engineer_ETL Assignment.db")
    sql_query = "select c.customer_id as customers,c. age,i.item_name as item,sum(o.quantity) as quantity from sales s left join customers c on s.customer_id=c.customer_id inner join orders o on s.sales_id=o.sales_id inner join items i on o.item_id = i.item_id where (c.age between 18 and 35) and (o.quantity<>0) group by c.customer_id,i.item_name;"
    df= pd.read_sql_query(sql_query,conn)
    conn.close()
    print(df)
    df.to_csv('pandaoutput2.csv', index= False)
    print("CSV has been succesfully created")
except exception as e:
    print("Incorrect SQL Query",e)
