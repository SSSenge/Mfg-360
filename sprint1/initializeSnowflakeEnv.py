from snowflake.snowpark import Session
# from snowflake.connector import Connect
import json
from time import gmtime, strftime
# from os import basename
from multipledispatch import dispatch
# from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
# import pandas as pd

@dispatch(str, str)
def connect(conParams: str, SQL: str):
    with open(conParams) as temp, open(SQL) as query:
        connection_parameters = json.load(temp)
        new_session = Session.builder.configs(connection_parameters).create()
        
        result = new_session.sql(query.read()).collect()
        
        new_session.close()
    return result

@dispatch(tuple)
def connect(credentials: tuple):
    with open(credentials[0]) as temp:
        connection_parameters = json.load(temp)
        new_session = Session.builder.configs(connection_parameters).create()

        result = new_session.sql(f"LIST @{credentials[1]}").collect()
        name = []
        for row in result:
            name.append(row[0])

        new_session.close()
    return name


def chooseParameters():
    conParams = str(input('Type in filepaths.'))
    SQL = str(input())
    return connect(conParams, SQL)


def generateParams():
    with open('auxfiles/connection_parameters.json', 'w') as myConnect:
        mC = {"account": "","user": "","password": "","role": "","warehouse": "","database": "","schema": ""}
        for k in mC.keys():
            mC[k] = input(k)
            if mC[k] == '':
                mC.pop(k)
        
        myConnect.write(json.dumps(mC))
    print('Success!')



def generateSQL():
    credentials = (input('credential parameter file/path/here.txt\n') ,input('stage name here\n'))
    inp = input('filename here, leave blank to find list:\n')
    if inp == '':
        fN = fN = [v[5:] for v in connect(credentials)]
    else:
        fN = [name.split(',')[0][5:] for name in open(inp).read().split('\n')[1:]]
    
    actual_time = strftime("%Y-%m-%d %H-%M-%S", gmtime())
    qualified = json.load(open(credentials[0]))['database'] + '.' + json.load(open(credentials[0]))['schema'] + '.'
    fType = fN[0].split('.')[1]
    numCol = int(input('# of columns?\n'))


    ddl = """
    create table {0}{1}
    ({2})
    ;
    """
    dml = """
    copy into {0}{1}
    auto_ingest = true
    from {2}
    ;
    """
    pipe = """
    create pipe {0}pipe_{1} as
    copy into {0}{1}
    auto_ingest = true
    from {2}
    ;
    """
    stream = """
    create or replace stream {0}strm_{1}
    on table {0}{1}
    ;
    """

    ddl_op_dl, dml_op_dl, pipe_op_dl, stream_op_dl, rec_op_dl = open(f'sprint1/generatedSQL/ddl{actual_time}.txt', 'w'), open(f'sprint1/generatedSQL/dml{actual_time}.txt', 'w'), open(f'sprint1/generatedSQL/pipe{actual_time}.txt', 'w'), open(f'sprint1/generatedSQL/stream{actual_time}.txt', 'w'), open(f'sprint1/generatedSQL/recOP{actual_time}.txt', 'w')
    ddl_output, dml_output, pipe_output, stream_output, rec_output = [], [], [], [], []
    tblCol = ''
    for col in range(numCol):
        tblCol += f'${col + 1} varchar'
        if col == numCol - 1:
            continue
        else:
            tblCol += ','

    fN = [v.split('/') for v in fN]
    for v in fN:
        ddl_output.append(ddl.format(qualified, f"{v[-2]}", tblCol))
        dml_output.append(dml.format(qualified, f"{v[-2]}", credentials[1]))
        pipe_output.append(pipe.format(qualified, f"{v[-2]}", credentials[1]))
        stream_output.append(stream.format(qualified, f"{v[-2]}"))
        rec_output.append(ddl.format(qualified, f"{v[-2]}", tblCol))
        rec_output.append(stream.format(qualified, f"{v[-2]}"))
        rec_output.append(dml.format(qualified, f"{v[-2]}", credentials[1]))

    ddl_op_dl.write('\n'.join(ddl_output))
    dml_op_dl.write('\n'.join(dml_output))
    pipe_op_dl.write('\n'.join(pipe_output))
    stream_op_dl.write('\n'.join(stream_output))
    rec_op_dl.write('\n'.join(rec_output))

    print('Success!')


def main():
    cmd = input('Commands;\ngenerateParams - create a JSON file with user input credentials.\ngenerateSQL - Create SQL file based on filenames.\nrunSQL - Run all SQL commands in chosen file.\n')
    if cmd == 'generateParams':
        generateParams()
    elif cmd == 'generateSQL':
        generateSQL()
    elif cmd == 'runSQL':
        chooseParameters()
    else:
        pass



main()