from ollama import chat, ChatResponse
import re




def answer_gen(textual_question, db_engine, model_name):
    schema = """
    

1. country  
- Describes each country.  
- Columns:  
  - country_name (string): Name of the country  
  - trade_port (string): Name of the country's main trade port  
  - development (float): Development level of the country  

2. trading_node  
- Represents a trading node in the network.  
- Columns:  
  - trading_node (string): Node name  
  - local_value (float): Local economic value  
  - node_inland (boolean): Whether the node is inland  
  - total_power (float): Total trading power  
  - outgoing (float): Total flow going out  
  - ingoing (float): Total flow coming in  

3. flow  
- Represents a directed trade connection between two nodes.  
- Columns:  
  - upstream (string): Origin node of the flow  
  - downstream (string): Destination node of the flow  
  - flow (float): Amount of flow between the nodes  

4. node_country  
- Maps each trading node to a country and describes its role.  
- Columns:  
  - node_name (string): Name of the node  
  - country_name (string): The country this node belongs to  
  - is_home (boolean): Whether this is the country's home node  
  - merchant (boolean): Whether this node acts as a merchant  
  - base_trading_power (float): Original trading power  
  - calculated_trading_power (float): Final computed power

    """
    few_shot_example = """
Example 1:
Question: How many nodes are inland?
Reasoning:
- Filter all records in the `trading_node` table where `node_inland = TRUE`.
- Count the number of such nodes.
SQL query:
```sql
SELECT COUNT(trading_node) FROM trading_node WHERE node_inland = TRUE;\n\n

    
Example2:
Question: Which node is connected as the upstream of the highest flow?
Reasoning:
- From the flow table, find the row with the highest flow value.
- Return the upstream node of that row.
SQL query:
```sql
SELECT upstream FROM flow ORDER BY flow DESC LIMIT 1;\n\n

Example3:
Question: For the country with the highest development, what are the upstream nodes of the home node?
Reasoning: 
- First, find the country with the highest development value in the country table.
- Then, find the home node (is_home = TRUE) in the node_country table for that country.
- Then, find all rows in the flow table where downstream matches the home node.
- Finally, return the upstream nodes from those rows only if the upstream node also belongs to the same country.
SQL query:
```sql
SELECT flow.upstream
FROM flow
JOIN node_country ON flow.upstream = node_country.node_name
WHERE flow.downstream = (
    SELECT node_name
    FROM node_country
    WHERE is_home = TRUE
      AND country_name = (
          SELECT country_name
          FROM country
          ORDER BY development DESC
          LIMIT 1
      )
)
AND node_country.country_name = (
    SELECT country_name
    FROM country
    ORDER BY development DESC
    LIMIT 1
);\n\n

Example 4:  
Question: List the top 3 inland trading nodes with the highest local value, sorted by their total outgoing flow in descending order.
Reasoning:
- Filter nodes where node_inland = TRUE in the trading_node table.
- Among these, select nodes with the top 3 highest local_value.
- Sort these top 3 nodes by their outgoing flow in descending order before returning.
SQL query:
```sql
SELECT trading_node 
FROM trading_node 
WHERE node_inland = TRUE
ORDER BY local_value DESC, outgoing DESC
LIMIT 3;\n\n
"""


    
    

    prompt = (
        f"You are an expert in relational databases and MYSQL.\n\n"
        f"schema: {schema}\n\n"
        f"Here are some examples :{few_shot_example}\n\n "
        f"Please write a correct MySQL query to answer the question below.\n\n"
        f"Consider the order of answers(e.g., in decreasing order, highest, most etc.)\n\n"
        f"DO NOT use aliasing (e.g., T1, T2) unless absolutely necessary.\n\n"
        f"Write clean and minimal SQL with correct column and table names.\n\n"
        f"Before returning the SQL query, double-check that all column names used exist in the provided schema."
        f"Only use the columns and tables provided in the schema. Do not invent new columns."
        f"Question:\n{textual_question}\n\n"
        f"SQL query:"
    )

    response: ChatResponse = chat(model=model_name, options={"temperature": 0}, messages=[
        {
            'role': 'user',
            'content': prompt,
        }
    ])

    # fallback 없이 바로 SQL 추출 시 실패 가능 → 개선:
    sql_match = re.search(r"```sql\s*(.*?)\s*```", response.message.content, re.DOTALL)

    if sql_match:
        sql_query = sql_match.group(1).strip()
    else:
    # fallback: 그냥 전체 응답에서 SQL만 뽑기 위한 시도
        sql_lines = response.message.content.strip().splitlines()
        sql_query = "\n".join([line for line in sql_lines if not line.strip().startswith("```")]).strip()

    

    
    print("PROMPT:\n", prompt)
    print("RAW RESPONSE:\n", response.message.content)
    print("PARSED SQL QUERY:\n", sql_query)
    try:
        result_df = db_engine.query(sql_query)

        if result_df.empty:
            return None

        # 결과 후처리
        answer = result_df.iloc[0, 0]

        if isinstance(answer, float):
            answer = round(answer, 2)
        elif isinstance(answer, str):
            result_df = result_df.sort_values(result_df.columns[0], ascending=False)
            answer = result_df.iloc[0, 0]

        return answer
    except Exception as e:
        return None
