import psycopg2
import pandas as pd
import json
import os
from  datetime import timezone,datetime
#
data = dict()
#
output_folder = f'reports_{datetime.now().strftime('%Y_%m_%d')}'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
print("ABC3")
#
path_json = "creds.json"
# Opening JSON file
with open(path_json) as my_file:
    data = json.load(my_file)
    print(my_file.read())
print(data)
#acesso ao BD sergoias
con=psycopg2.connect(
    dbname=data["dbname"], 
    host=data["host"], 
    port=data["port"], 
    user=data["user"], 
    password=data["password"]
)
query_total = """
    select rr."name" as "Nome da Regional", COUNT (distinct uu.username) as "Total de Estudantes"
    FROM sergoias.users_user uu
    left join sergoias.users_user_groups uug on uug.user_id = uu.id 
    left join sergoias.school_classes_schoolclass_users scsu on scsu.user_id = uu.id 
    left join sergoias.school_classes_schoolclass scs on scs.id = scsu.schoolclass_id
    left join public.grades_grade gg on gg.id = scs.grade_id 
    left join sergoias.institutions_institution_users iiu on iiu.user_id = uu.id
    left join sergoias.institutions_institution ii on ii.id = iiu.institution_id 
    left join sergoias.regionals_regional rr on rr.id = ii.regional_id 
    where 1=1
        and uug.group_id = 2
        and uu.is_active = true
        and scs.is_active = true
        and gg.is_active = true 
        and ii.is_active = true
        and rr.is_active = true
        and gg."name" in ('6º Ano','7º Ano','8º Ano','9º Ano')
    group by rr."name"
""" 
df_total = pd.read_sql_query(query_total,con)
#
query_acessos = """
    with CTE as (
        select 
            uar.user_id,count(*)
        from sergoias.users_accessrecord uar
        group by uar.user_id
        having COUNT(*)>0
    )
    select rr."name" as "Nome da Regional", COUNT (distinct uu.username) as "Estudantes com Primeiro Acesso"
    FROM sergoias.users_user uu
    left join sergoias.users_user_groups uug on uug.user_id = uu.id 
    left join sergoias.school_classes_schoolclass_users scsu on scsu.user_id = uu.id 
    left join sergoias.school_classes_schoolclass scs on scs.id = scsu.schoolclass_id
    left join public.grades_grade gg on gg.id = scs.grade_id 
    left join sergoias.institutions_institution_users iiu on iiu.user_id = uu.id
    left join sergoias.institutions_institution ii on ii.id = iiu.institution_id 
    left join sergoias.regionals_regional rr on rr.id = ii.regional_id 
    right join CTE on CTE.user_id = uu.id
    where 1=1
        and uug.group_id = 2
        and uu.is_active = true
        and scs.is_active = true
        and gg.is_active = true 
        and ii.is_active = true
        and rr.is_active = true
        and gg."name" in ('6º Ano','7º Ano','8º Ano','9º Ano')
    group by rr."name"
"""
df_acessos = pd.read_sql_query(query_acessos,con)
con.close()
#
result = pd.merge(df_total, df_acessos, on="Nome da Regional")
result = (
    result
    .assign(Alcance = lambda df: 100*df['Estudantes com Primeiro Acesso']/df['Total de Estudantes'])
    .sort_values(by=['Alcance'], ascending=False)
)
path = f"{output_folder}/visao_seduc.csv"
result.to_csv(path, sep=",", encoding="utf-8", index=False, header=True)
#
print(result.head())
#print(df_acessos.columns)
