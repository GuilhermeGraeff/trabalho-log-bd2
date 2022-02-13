import psycopg2
# Conexão com o postgres:
def conecta_db():
	con = psycopg2.connect(host='localhost', 
							database='trab_bd2',
							user='postgres', 
							password='123456')
	return con

def executeQuarry(con, sql):
	cur = con.cursor()
	cur.execute(sql)
	con.commit()


fileName = 'entradaLog.txt'
try:
    file = open(fileName, "r", encoding="utf-8")
except:
    print(
        f"Algo deu errado D:\nArquivo '{fileName}' não encontrado ou inválido")
    exit(0)

fileArray = file.read().splitlines()


log = []
bdInitialState = []
for i in fileArray:
	if(i.startswith("<")):
		log.append(i)
	else:
		bdInitialState.append(i)

spacesCount = 0
for j in bdInitialState:
	if(j == ''):
		spacesCount += 1	
for i in range(0,spacesCount,1):
	bdInitialState.remove('')


# print('BD Initial State:\n')
# for i in bdInitialState:
# 	print(i)

# print('Log:\n')
# for i in log:
# 	print(i)

con = conecta_db()

# Dropando a tabela caso ela já exista
sql = 'DROP TABLE IF EXISTS log_test'
executeQuarry(con, sql)
# Criando a tabela dos deputados
sql = 'CREATE TABLE log_test (id INT, a INT, b INT)'

executeQuarry(con, sql)

bdInitialStateVector = []
for line in bdInitialState:
	splitedLine = line.split('=')
	for i in range(0,len(splitedLine),1):
		splitedLine[i] = splitedLine[i].strip()
		if ',' in splitedLine[i]:
			splitedLine[i] = splitedLine[i].split(',')
	splitedLine.append('Not Inserted')
	bdInitialStateVector.append(splitedLine)
print(bdInitialStateVector)

for item in range(0,len(bdInitialStateVector),1):
	if bdInitialStateVector[item][2] == 'Not Inserted':
		sql = 'INSERT INTO log_test VALUES ('+bdInitialStateVector[item][0][1]+',0,0)'
		executeQuarry(con, sql)
		for itemTemp in range(0,len(bdInitialStateVector),1):
			if bdInitialStateVector[itemTemp][0][1] == bdInitialStateVector[item][0][1]:
				bdInitialStateVector[itemTemp][2] = 'Inserted'

for item in range(0,len(bdInitialStateVector),1):
	if bdInitialStateVector[item][0][0] == 'A':
		sql = 'UPDATE log_test SET id = '+bdInitialStateVector[item][0][1]+', A = '+bdInitialStateVector[item][1]+' WHERE id ='+bdInitialStateVector[item][0][1]
	elif bdInitialStateVector[item][0][0] == 'B':
		sql = 'UPDATE log_test SET id = '+bdInitialStateVector[item][0][1]+', B = '+bdInitialStateVector[item][1]+' WHERE id ='+bdInitialStateVector[item][0][1]
	executeQuarry(con, sql)


con.close()

exit(0)