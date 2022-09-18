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


# 노드생성 쿼리 만드는 함수
def create_node_query(data, label, excepts=[]):

    """
    data : type = Series
    label : 노드의 라벨
    excepts : 노드의 속성이 아닌, 관계(엣지)생성에 필요한 칼럼
    """

    properties_query = "{"
    for col in data.index:
        if col in excepts:
            continue
        if data.notnull()[col]:
            properties_query += f"{str(col)}:'{str(data[col])}',"
    properties_query = properties_query[:-1] + "}"  #마지막 쉼표 제거하고 } 닫기

    # "CREATE (黃芩:본초 {name:'黃芩', 한글:'황금'})
    query = f"""CREATE ({data['name']}:{label} {str(properties_query)})"""
    return query


# 엣지생성 쿼리 만드는 함수
def create_edge_query(start, destination, label, properties=None):
    
    """
    start : 출발 노드의 name
    destination : 도착 노드의 name
    properties : type=dict
    """

    properties_query = ""
    if properties:
        properties_query = "{"
        for key in properties:
            properties_query+= f"""{str(key)}:"{properties[key]}","""
        properties_query = properties_query[:-1] + "}"  #마지막 쉼표 제거하고 } 닫기
    
    query = f"""MATCH (start_node{{name:"{start}"}})
                MATCH (desti_node{{name:"{destination}"}})
                CREATE (start_node)-[:{label}{properties_query}]->(desti_node)"""

    query = query.replace('\n','    ')
    return query


##### Step 1 #####
# 본초노드 생성
for i in range(len(df0)):
    data = df0.iloc[i]
    query = create_node_query(data, label="본초")
    session.run(query)


##### Step 2 #####
# 약재노드 생성 (단순채취노드)
for i in range(len(df1)):
    data = df1.iloc[i]
    query = create_node_query(data, label="약재", excepts=['원재료'])
    session.run(query)

# 본초->약재 엣지 생성
for i in range(len(df1)):
    data = df1.iloc[i]
    start, destination = data['원재료'], data['name']
    query = create_edge_query(start, destination, label="약재")
    session.run(query)


##### Step 3 #####
# 약재노드 생성 (포제약재) 
for i in range(len(df2)):
    data = df2.iloc[i]
    query = create_node_query(data, label="약재", excepts=['원재료','방법'])
    session.run(query)

# 본초->약재 엣지 생성
for i in range(len(df2)):
    data = df2.iloc[i]
    start, destination = data['원재료'], data['name']
    properties = data[['방법']].to_dict()
    query = create_edge_query(start, destination, label="을_포제", properties=properties)
    session.run(query)


##### Step 4 #####
# 배합물노드 생성
for i in range(len(df3)):
    data = df3.iloc[i]
    query = create_node_query(data, label="배합물", excepts=['원재료','효능'])
    session.run(query)

# 약재->배합물 엣지 생성
for i in range(len(df3)):
    data = df3.iloc[i]
    start, destination = data['원재료'], data['name']

    for sub_start in start.split(','):
        sub_start = sub_start.strip()
        # 원재료가 기존에 있는지 체크
        chk = session.run(f"""MATCH (start_node{{name:"{sub_start}"}}) RETURN (start_node)""")
        if len(list(chk))==0:  # 없다면 약재노드 생성
            query = f"""CREATE (:약재{{name:"{sub_start}"}})"""
            print(query)
            session.run(query)

        query = create_edge_query(sub_start, destination, label="을_배합")
        session.run(query)

# 배합물->효능 엣지 생성


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