import psycopg2

# Conexão com o postgres:
def conexaoPostgres():
	con = psycopg2.connect(host='localhost', 
							database='trab_bd2',
							user='postgres', 
							password='123456')
	return con

# Função que executa queries
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


con = conexaoPostgres()

bdInitialStateVector = []
for line in bdInitialState:
	splitedLine = line.split('=')
	for i in range(0,len(splitedLine),1):
		splitedLine[i] = splitedLine[i].strip()
		if ',' in splitedLine[i]:
			splitedLine[i] = splitedLine[i].split(',')
	splitedLine.append('Not Inserted')

	bdInitialStateVector.append(splitedLine)



# Dropando a tabela caso ela já exista
sql = 'DROP TABLE IF EXISTS log_test'
executeQuarry(con, sql)

columnNames = []
sqlColumns = ''
for item in range(0,len(bdInitialStateVector),1):
	if bdInitialStateVector[item][0][0] not in columnNames:
		sqlColumns = sqlColumns+', '+bdInitialStateVector[item][0][0]+' INT'
		columnNames.append(bdInitialStateVector[item][0][0])

sql = 'CREATE TABLE log_test (id INT'+sqlColumns+')'
executeQuarry(con, sql)

numberOfZeros = ''
for i in range(0,len(columnNames),1):
	numberOfZeros = numberOfZeros+',0'

for item in range(0,len(bdInitialStateVector),1):
	if bdInitialStateVector[item][2] == 'Not Inserted':
		sql = 'INSERT INTO log_test VALUES ('+bdInitialStateVector[item][0][1]+numberOfZeros+')'
		executeQuarry(con, sql)
		for itemTemp in range(0,len(bdInitialStateVector),1):
			if bdInitialStateVector[itemTemp][0][1] == bdInitialStateVector[item][0][1]:
				bdInitialStateVector[itemTemp][2] = 'Inserted'

for item in range(0,len(bdInitialStateVector),1):
	sql = 'UPDATE log_test SET id = '+bdInitialStateVector[item][0][1]+', '+bdInitialStateVector[item][0][0]+' = '+bdInitialStateVector[item][1]+' WHERE id ='+bdInitialStateVector[item][0][1]
	executeQuarry(con, sql)

commitedTransactions = {}
checkpointStartLine = 0

teveCkptFuncional = False 
for line in range(len(log)-1,-1,-1):
	if 'CKPT' in log[line] and 'Start' in log[line]:
		checkpointStartLine = line
		for lineEndCkpt in range(len(log)-1,-1,-1):
			if 'End' in log[lineEndCkpt] and lineEndCkpt > line:
				teveCkptFuncional = True
				for lineCkpt in range(line,len(log)-1,1):
					if 'commit' in log[lineCkpt]:
						splitedCommit = log[lineCkpt].split(' ')
						commitedTransactions[splitedCommit[1][:-1]] = 'unvisited'
				break
		
lastStartLine = 0
for line in range(len(log)-1,-1, -1):
	encontreiTodosOsStarts = True
	if 'unvisited' in commitedTransactions.values():
		encontreiTodosOsStarts = False
	if encontreiTodosOsStarts == True:
		break
	if 'start' in log[line] and 'CKPT' not in log[line]:
		splitedStart = log[line].split(' ')
		transaction = splitedStart[1][:-1]
		if transaction in commitedTransactions.keys():
			commitedTransactions[transaction] = 'visited'

	lastStartLine = line

commitedTransactionsIndependentOfCkpt = []
for line in range(0,len(log)-1,1):
	if 'commit' in log[line]:
		splitedCommit = log[line].split(' ')
		commitedTransactionsIndependentOfCkpt.append(splitedCommit[1][:-1])
		
		

if teveCkptFuncional == True:		
	# print('Teve checkpoint válido, estou fazendo o REDO daqueles que comitaram após o ChGuilherme graeffeckpoint:')
	print('Saída')
	for i in commitedTransactions.keys():
		print('Transação',i,'realizou Redo')
	for line in range(lastStartLine, len(log)-1, 1):
		noMoreOrlessLine = log[line][1:-1]
		splitedLine = noMoreOrlessLine.split(',')
		if len(splitedLine) == 4:
			if splitedLine[0] in commitedTransactions.keys():	
				# print('Atualizei o dado de id: ', splitedLine[1],' Coluna: ',splitedLine[2] ,' para: ',splitedLine[3], ' -- Pela transição: ',splitedLine[0] )
				sql = 'UPDATE log_test SET '+splitedLine[2]+ '='+splitedLine[3]+' WHERE id ='+splitedLine[1]
				executeQuarry(con, sql)
else: 
	# print('Como não teve checkpoint válido, estou atualizando os dados de tudo que foi comitado:código do Guilherme graeff')
	print('Saída')
	for i in commitedTransactionsIndependentOfCkpt:
		print('A transação ',i,' realizou REDO')
	for line in range(0, len(log)-1, 1):
		noMoreOrlessLine = log[line][1:-1]
		splitedLine = noMoreOrlessLine.split(',')
		if len(splitedLine) == 4:
			if splitedLine[0] in commitedTransactionsIndependentOfCkpt:	
				# print('Atualizei o dado de id: ', splitedLine[1],' Coluna: ',splitedLine[2] ,' para: ',splitedLine[3], ' -- Pela transição: ',splitedLine[0] )
				sql = 'UPDATE log_test SET '+splitedLine[2]+ '='+splitedLine[3]+' WHERE id ='+splitedLine[1]
				executeQuarry(con, sql)

cursor = con.cursor()
query= "select * from log_test"
cursor.execute(query)
logTestrecords = cursor.fetchall()
print('Estado final do banco de dados:')
print('    ', end = "")
for i in columnNames:
	print(i+'    ' , end = "")
print('')

for row in logTestrecords:
	for i in row:
		print(i,'  ' , end = "")
	print('')

con.close()

exit(0)