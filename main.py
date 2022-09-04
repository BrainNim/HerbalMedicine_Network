# Run the Neo4j DBMS before start main.py
import pandas as pd
from neo4j import GraphDatabase


# Initialize connection to database
driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', '0000'))
session = driver.session()

# Delete all data
q1 = "MATCH (n)-[r]-() DELETE n, r"
q2 = "MATCH (n) DELETE n"
session.run(q1)
session.run(q2)

# Read Excel
df0 = pd.read_excel('한약네트워크.xlsx', sheet_name=0)  # 본초
df1 = pd.read_excel('한약네트워크.xlsx', sheet_name=1)  # 약재(채취)
df2 = pd.read_excel('한약네트워크.xlsx', sheet_name=2)  # 약재(포제)
df3 = pd.read_excel('한약네트워크.xlsx', sheet_name=3)  # 배합물
df4 = pd.read_excel('한약네트워크.xlsx', sheet_name=4)  # 효능
df5 = pd.read_excel('한약네트워크.xlsx', sheet_name=5)  # 질환


# 본초생성
for i in range(len(df0)):
    key_values = df0.to_dict(orient = 'records')[i]

    insert_query = "{"
    for key in key_values.keys():
        insert_query += f"{str(key)}:'{str(key_values[key])}',"
    insert_query = insert_query[:-1] + "}"  #마지막 쉼표 제거하고 } 닫기

    # "CREATE (黃芩:본초 {name:'黃芩', 한글:'황금'})
    query = f"""CREATE ({key_values['name']}:본초 {str(insert_query)})"""
    session.run(query)



# Run the query
q = 'MATCH (n) RETURN n LIMIT 2'
results = list(session.run(q))
record = results[0]
node = record[0]

session.close()



//2 채취 시기별 약재 생성
match (황금:본초{한글:"황금"})
create (황금)-[:약재]->(자금:약재{name:"자금(한자)",한글:"자금",채취:1})
create (황금)-[:약재]->(고금:약재{name:"고금(한자)",한글:"고금",채취:2})

//3 포재약재 생성
match (고금:약재{한글:"고금"})
create (고금)-[:을_포제{method:"none"}]->(:약재{name:"生黃芩",한글:"생황금"})
create (고금)-[:을_포제{method:"alcohol"}]->(:약재{name:"酒黃芩",한글:"주황금"})
create (고금)-[:을_포제{method:"burn"}]->(:약재{name:"炒黃芩",한글:"초황금"})
create (고금)-[:을_포제{method:"char"}]->(:약재{name:"炒炭黃芩",한글:"초탄황금"})

//4 효능생성
MATCH (생:약재{name:"生黃芩"})  
match (초탄:약재{name:"炒炭黃芩"})
match (초:약재{name:"炒黃芩"})
match (주:약재{name:"酒黃芩"})

create (생)-[:의_효능]->(:효능{name:"淸熱燥濕",한글:"청열조습"})
create (생)-[:의_효능]->(:효능{name:"瀉火解毒",한글:"사화해독"})
create (초탄)-[:의_효능]->(:효능{name:"止血",한글:"지혈"})
create (주)-[:의_효능]->(:효능{name:"上焦淸熱",한글:"상초청열"})

create (생)-[:의_효능]->(:효능{name:"少陽經 淸熱",한글:"소양경 청열"})<-[:의_효능]-(:약재{name:"柴胡",한글:"시호"})
create (생)-[:의_효능]->(:효능{name:"大腸 淸熱",한글:"대장 청열"})<-[:의_효능]-(:약재{name:"芍藥",한글:"작약"})
create (생)-[:의_효능]->(:효능{name:"淸肺止咳",한글:"청폐지해"})<-[:의_효능]-(:약재{name:"桑白皮",한글:"상백피"})

create (초)-[:의_효능]->(:효능{name:"淸熱安胎",한글:"청폐지해"})<-[:의_효능]-(:약재{name:"白朮",한글:"백출"})

create (생)-[:의_효능]->(:효능{name:"淸心解毒",한글:"청심해독"})<-[:의_효능]-(:약재{name:"黃連",한글:"황련"})
create (생)-[:의_효능]->(:효능{name:"胸熱解毒",한글:"흉열해독"})<-[:의_효능]-(:약재{name:"山梔",한글:"산치"})
create (생)-[:의_효능]->(기표청열:효능{name:"肌表淸熱",한글:"기표청열"})<-[:의_효능]-(:약재{name:"荊芥",한글:"형개"})
create (생)-[:의_효능]->(기표청열)<-[:의_효능]-(:약재{name:"防風",한글:"방풍"})