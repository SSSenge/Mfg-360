from snowflake.snowpark import Session
# from snowflake.connector import Connect
import json
# from time import gmtime, strftime
from datetime import datetime
# from os import basename
from multipledispatch import dispatch
# from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
# import pandas as pd


def initialize(creds: dict):
    # check for missing parameters then map missing parameters to default values, default role is accountadmin, default warehouse is compute_wh, default database/schema is parameterName_bucketFolder
    empty = {(k, k + '_' + creds['AWS']['bucket'].split('/')[1] + str(datetime.now().date()) + '__' + str(datetime.now().time())) for k, v in creds['Snowflake'].items() if k in ['database', 'schema'] and v == ''}
    
    # make some SQL to create missing database/schema if necessary
    sql = (f"create {empty['database']}; ") if 'database' in empty else '' + (f"create {empty['schema']};") if 'schema' in empty else ''
    
    # add the default warehouse and role if necessary
    conParams = dict(map(lambda x: x if x[0] not in empty else (x[0], 'accountadmin') if x[0] == 'role' else (x[0], 'compute_wh') if x[0] == 'warehouse' else x, creds['Snowflake'].items()))
    
    # start a new session in Snowflake with the required credentials
    new_session = Session.builder.configs(conParams).create()   
    new_session.sql(sql)

    return (new_session, conParams)


def makeBT(max: str):
    # This function writes a sql statement to create a base table with empty varchar columns equal to the maximum column parameter, default is 1000
    max = 1000 if max == '' else int(max)
    BTsql = ''
    bCol = ''
    for i in range(1, max):
        BTsql += f'"{i}" varchar, '
        bCol += f'${i},'
    BTsql += f'"{max}" varchar'
    bCol += f'${max}'
    return BTsql, bCol

def replaceAny(line: str, searchMap: dict):
    # helper function for bigFormat(), is used to format initSQL.txt by each line.
    for k, v in searchMap.items():
        if k in line:
            line = line.replace(k, v)
    return line


def bigFormat(creds: dict, conParams: dict, BTsql: str, bCol: str):
    # read the base sql file
    init = open('auxFiles/initSQL.txt').read().split('\n')
    # helper variable for searchmap
    buck = creds['AWS']['bucket'].split('/')
    # dictionary for use in format loop, {searchTerm: mappedValue}
    searchMap = {'<wh>': conParams['warehouse'],'<db>': conParams['database'], '<schm>': conParams['schema'],
                 '<stgnm>': f"stg_{buck[1]}", '<bucket>': '"s3://' + '/'.join(buck) + '/"', '<id>': creds['AWS']['id'], '<key>': creds['AWS']['key'],
                 '<csvpp>': f"csvpp_{buck[1]}", '<jsnpp>': f"jsnpp_{buck[1]}", '<tblnm>': f"tbl_{buck[1]}", '<bCol>' : f'{bCol}',
                 '<baseTable>': f"create or replace table tbl_{buck[1]}_csv({BTsql});", '<jsnTbl>': f'create or replace table tbl_{buck[1]}_json ("1" variant);'}
    
    bF = []
    for line in init:
        bF.append(replaceAny(line, searchMap) + ' ')
    # returns the fully formatted SQL text
    return bF

def returnSQL(creds: dict):
    BTsql, bCol = makeBT(creds['max'])
    session, conParams = initialize(creds)
    bF = bigFormat(creds, conParams, BTsql, bCol)

    return (session, bF)
    
@dispatch(dict)
def main(creds: dict):
    try:
        session, bF = returnSQL(creds)

        
        for sql in bF:
            session.sql(sql).collect()
        session.close()
        return 'Success.'
    except:
        session.close()
        return 'Failure.'

@dispatch()
def main():
    try:
        start = input('file/path/here.json\n')
        creds = json.load(open(start))

        session, bF = returnSQL(creds)

        for sql in bF:
            print(session.sql(sql).collect())
        session.close()
    except:
        session.close()

