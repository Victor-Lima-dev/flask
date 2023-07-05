from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import pandas as pd
import json
from datetime import datetime


app = Flask(__name__)
cors = CORS(app, origins=["https://unipac-embalagem-checklist.up.railway.app"])


def processar_tabela(tabela):
   
    ferias = pd.DataFrame(tabela)
    
    ferias.rename(columns=ferias.iloc[0], inplace=True)
    ferias.drop(ferias.index[0], inplace=True)
    
   
    
       # supondo que o seu DataFrame tem 30 colunas e você quer renomear as colunas 20 e 21
    # você pode criar uma lista com os nomes atuais das colunas
    colunas = ferias.columns.to_list()
    # você pode alterar os nomes das colunas que você quer na lista
    colunas[20] = "Inicio1"
    colunas[21] = "Fim1"
    colunas[25] = "Inicio2"
    colunas[26] = "Fim2"
    colunas[14] = "Status1"
    colunas[17] = "Dias0"

    colunas[22] = "Dias1"

    colunas[27] = "Dias2"
    # você pode atribuir a lista ao atributo columns do seu DataFrame
    ferias.columns = colunas









        #Tabela com as colunas selecionadas
    ferias_manipular1 = ferias[colunas]
    ferias_manipular2 = ferias[colunas]
    ferias_manipular3 = ferias[colunas]
        #Quebrar a Tabela a Primeira vez
    colunas = ["Nome Pessoal","Status1","Saldo","Inicio","Fim","Ini. Per. Aquis.","Fim Per. Aquis.","Desc. Centro de Custo","Dias0"]
    ferias_periodo_1 = ferias_manipular1[colunas]
    ferias_periodo_1 = ferias_periodo_1.assign(Periodo=1)
    ferias_periodo_1 = ferias_periodo_1.assign(SaldoString=ferias_periodo_1["Saldo"]) 
       




        # Usando o índice das colunas em vez dos nomes
    colunas = ["Nome Pessoal","Status1","Saldo","Inicio1","Fim1","Ini. Per. Aquis.","Fim Per. Aquis.","Desc. Centro de Custo","Dias1"]
    ferias_periodo_2 = ferias_manipular2[colunas]
    ferias_periodo_2 = ferias_periodo_2.assign(Periodo=2)
    ferias_periodo_2.rename(columns={"Inicio1":"Inicio", "Fim1":"Fim", "Dias1":"Dias0"}, inplace=True)
    ferias_periodo_2 = ferias_periodo_2.assign(SaldoString=ferias_periodo_2["Saldo"])

    ferias_periodo_2 = ferias_periodo_2.assign(Saldo=0) # Altera o valor da coluna Saldo para 0



         # Usando o índice das colunas em vez dos nomes
    colunas = ["Nome Pessoal","Status1","Saldo","Inicio2","Fim2","Ini. Per. Aquis.","Fim Per. Aquis.","Desc. Centro de Custo","Dias2"]
    ferias_periodo_3 = ferias_manipular3[colunas]
    ferias_periodo_3 = ferias_periodo_3.assign(Periodo=3)
    ferias_periodo_3.rename(columns={"Inicio2":"Inicio", "Fim2":"Fim", "Dias2":"Dias0"}, inplace=True)
    ferias_periodo_3 = ferias_periodo_3.assign(SaldoString=ferias_periodo_3["Saldo"])

    ferias_periodo_3 = ferias_periodo_3.assign(Saldo=0) # Altera o valor da coluna Saldo para 0 





    ferias_nulos = pd.concat([ferias_periodo_1, ferias_periodo_2, ferias_periodo_3], ignore_index=True)
    ferias_sem_nulos = ferias_nulos.dropna()
    # Mostrar os nomes do ferias_nulos que não estão no ferias_sem_nulos
    df = ferias_nulos[~ferias_nulos["Nome Pessoal"].isin(ferias_sem_nulos["Nome Pessoal"])]
    df = df.drop_duplicates(subset=["Nome Pessoal","Status1"])
    # Concatenar os dois data frames
    final = pd.concat([ferias_sem_nulos, df])
    final.rename(columns={"Status1":"Status", "Dias0":"Dias"}, inplace=True)
    final['ExtracaoDados'] = pd.to_datetime(datetime.now().date())
    final = final.fillna(0)


    
    return final

def processar_projecao(tabela):
   
    

    dfOriginal = processar_tabela(tabela)
    dfOriginal['Saldo'] = dfOriginal['Saldo'].astype(float)
    dfOriginal['Dias'] = dfOriginal['Dias'].astype(float)
    #transformar a coluna Inicio em data e a coluna Fim Per. Aquis. em data
    dfOriginal['Inicio'] = pd.to_datetime(dfOriginal['Inicio'], format="%d/%m/%Y", errors='coerce')
    dfOriginal['Fim Per. Aquis.'] = pd.to_datetime(dfOriginal['Fim Per. Aquis.'], format="%d/%m/%Y", errors='coerce')

    

    dfOriginal['Projeção'] = ' 2023'


    dfConcat = dfOriginal


    #criar uma lista com os meses que você quer gerar os arquivos excel
    meses = ["August","July","September","October","November","December","June", "May", "April", "March", "February", "January"]
    #criar uma variável para armazenar o mês atual
    mes_atual = datetime.now().month
    mes_dezembro = 12
    mes_janeiro = 1

    #criar um loop para cada mês da lista
    for mes in meses:
        #criar uma variável para armazenar o número do mês correspondente
        num_mes = datetime.strptime(mes, '%B').month
        #se o número do mês for maior ou igual ao mês atual ou o ano for igual a 2024, então fazer as projeções
        if num_mes > mes_atual or (df['Fim Per. Aquis.'].dt.year == 2024).any():
            #abrir df como Fereias-response.xlsx
            df = processar_tabela(tabela)
            df['Saldo'] = df['Saldo'].astype(float)
            df['Dias'] = df['Dias'].astype(float)

            #transformar a coluna Fim Per. Aquis. em data
            df['Fim Per. Aquis.'] = pd.to_datetime(dfOriginal['Fim Per. Aquis.'], format="%d/%m/%Y", errors='coerce')
            #transformar a coluna Inicio em data
            df['Inicio'] = pd.to_datetime(dfOriginal['Inicio'], format="%d/%m/%Y", errors='coerce')
            #transformar a coluna Saldo em float
            df['Saldo'] = df['Saldo'].astype(float)
            #substituir os valores nulos ou vazios por 0
            df['Saldo'] = df['Saldo'].fillna(0)


            #colocar uma coluna SaldoRetirado com o valor de Saldo se o mes da data for menor ou igual ao número do mês e o ano for igual a 2023 igual ao if anterior
            df.loc[(df['Fim Per. Aquis.'].dt.month <= num_mes) & (df['Fim Per. Aquis.'].dt.year == 2023) & (df['Status'] != 'Vencidas'), 'Adicionado'] = df['Saldo']
            #mudar o valor de status para Vencida se o mes da data for menor ou igual ao número do mês e o ano for igual a 2023 e se o status for diferente de Vencida
            df.loc[(df['Fim Per. Aquis.'].dt.month <= num_mes) & (df['Fim Per. Aquis.'].dt.year == 2023) & (df['Status'] != 'Vencidas'), 'Status'] = 'Vencidas'
            #colocar como valor da coluna Projeção o nome do mês
            df['Projeção'] = mes + ' 2023'
            df['AnoProjeção'] = 2023

             #colocar uma coluna SaldoRetirado com o valor de Saldo se o mes da data for menor ou igual ao número do mês e o ano for igual a 2023 igual ao if anterior
            df.loc[(df['Fim Per. Aquis.'].dt.month <= num_mes + 1) & (df['Fim Per. Aquis.'].dt.year == 2023) & (df['Status'] != 'Vencidas'), 'Saldo a mais do Próximo Mes'] = df['Saldo']
            #mudar o valor de status para Vencida se o mes da data for menor ou igual ao número do mês e o ano for igual a 2023 e se o status for diferente de Vencida
            df.loc[(df['Fim Per. Aquis.'].dt.month <= num_mes + 1) & (df['Fim Per. Aquis.'].dt.year == 2023) & (df['Status'] != 'Vencidas'), 'AnaliseComparação'] = mes
            #coloca que se o mes for igual a 12, então o ano da projeção é 2024 e pega o mes de janeiro
            if num_mes == mes_dezembro:
                df.loc[(df['Fim Per. Aquis.'].dt.month == mes_janeiro) & (df['Fim Per. Aquis.'].dt.year == 2024) & (df['Status'] != 'Vencidas'), 'Saldo a mais do Próximo Mes'] = df['Saldo']




            #mudar o valor de saldo para (Saldo - valor da coluna Dias) se o mesmo da data estiver entre o mês anterior e o mês da projeção e o ano for igual a 2023
            df.loc[(df['Inicio'].dt.month >= mes_atual) & (df['Inicio'].dt.month <= num_mes) & (df['Inicio'].dt.year == 2023), 'Saldo'] = df['Saldo'] - df['Dias']
            #criar uma nova coluna com os dias que foram tirados do saldo, com as mesma condições do if anterior
            df.loc[(df['Inicio'].dt.month >= mes_atual) & (df['Inicio'].dt.month <= num_mes) & (df['Inicio'].dt.year == 2023), 'DiasTirados'] = df['Dias']
             #mudar o valor de saldo para (Saldo - valor da coluna Dias) se o mesmo da data estiver entre o mês anterior e o mês da projeção e o ano for igual a 2023
            df.loc[(df['Inicio'].dt.month == num_mes + 1) & (df['Inicio'].dt.year == 2023), 'Dias Tirados do Proximo Mes'] = df['Dias']

            if num_mes == mes_dezembro:
                df.loc[(df['Inicio'].dt.month == mes_janeiro) & (df['Inicio'].dt.year == 2024), 'Dias Tirados do Proximo Mes'] = df['Dias']

            #concatenar df com dfOriginal

            dfConcat = pd.concat([df, dfConcat])

            #abrir df como Fereias-response.xlsx
            df = processar_tabela(tabela)
            df['Saldo'] = df['Saldo'].astype(float)
            df['Dias'] = df['Dias'].astype(float)
            #transformar a coluna Fim Per. Aquis. em data
            df['Fim Per. Aquis.'] = pd.to_datetime(dfOriginal['Fim Per. Aquis.'], format="%d/%m/%Y", errors='coerce')
            #transformar a coluna Inicio em data
            df['Inicio'] = pd.to_datetime(dfOriginal['Inicio'], format="%d/%m/%Y", errors='coerce')
             #transformar a coluna Saldo em float
            df['Saldo'] = df['Saldo'].astype(float)
            #substituir os valores nulos ou vazios por 0
            df['Saldo'] = df['Saldo'].fillna(0)

            #colocar uma coluna SaldoRetirado com o valor de Saldo se o mes da data for menor ou igual ao número do mês e o ano for igual a 2023 igual ao if anterior
            df.loc[(df['Fim Per. Aquis.'].dt.year == 2023) | ((df['Fim Per. Aquis.'].dt.month <= num_mes) & (df['Fim Per. Aquis.'].dt.year <= 2024)) & (df['Status'] != 'Vencidas'), 'Adicionado'] = df['Saldo']
            #mudar o valor de status para Vencida se o mes da data for menor ou igual ao número do mês e o ano for igual a 2024 e se o status for diferente de Vencida
            df.loc[(df['Fim Per. Aquis.'].dt.year == 2023) | ((df['Fim Per. Aquis.'].dt.month <= num_mes) & (df['Fim Per. Aquis.'].dt.year == 2024)) & (df['Status'] != 'Vencidas'), 'Status'] = 'Vencidas'

            df.loc[((df['Fim Per. Aquis.'].dt.month <= num_mes + 1) & (df['Fim Per. Aquis.'].dt.year == 2024)) & (df['Status'] != 'Vencidas'), 'Saldo a mais do Próximo Mes'] = df['Saldo']










            #colocar como valor da coluna Projeção o nome do mês
            df['Projeção'] = mes + ' 2024'
            df['AnoProjeção'] = "2024"


    # Supondo que você já tenha definido o dataframe e as variáveis

    # Verificando se há alguma linha com o início igual a 2024
            if (df['Inicio'].dt.year == 2024).any():

                # Executando o código para os meses entre o mês atual e dezembro de 2023
                df.loc[(df['Inicio'].dt.month.isin(range(mes_atual, mes_dezembro + 1))) & (df['Inicio'].dt.year == 2023), "Saldo"] = df['Saldo'] - df['Dias']
                df.loc[(df['Inicio'].dt.month.isin(range(mes_atual, mes_dezembro + 1))) & (df['Inicio'].dt.year == 2023), "DiasTirados"] = df['Dias']




                df.loc[(df['Inicio'].dt.month <= num_mes) & (df['Inicio'].dt.year == 2024), 'Saldo'] = df['Saldo'] - df['Dias']
                #criar uma nova coluna com os dias que foram tirados do saldo, com as mesma condições do if anterior
                df.loc[(df['Inicio'].dt.month <= num_mes) & (df['Inicio'].dt.year == 2024), 'DiasTirados'] = df['Dias']

                #mudar o valor de saldo para (Saldo - valor da coluna Dias) se o mesmo da data estiver entre o mês anterior e o mês da projeção e o ano for igual a 2023
                df.loc[(df['Inicio'].dt.month == num_mes + 1) & (df['Inicio'].dt.year == 2024), 'Dias Tirados do Proximo Mes'] = df['Dias']



            dfConcat = pd.concat([df, dfConcat])


            
    
    
    
    
    
    
    
    
    
    
    dfConcat = dfConcat.astype(str)
    return dfConcat



@app.route('/api', methods=['POST'])
@cross_origin()

def api():
    tabela = request.get_json()
    
    
    
    resultado = processar_tabela(tabela)
    resultado = resultado.to_dict(orient="records") # converte o DataFrame em uma lista de dicionários
    return jsonify(resultado)
 
    

@app.route('/api2', methods=['POST'])
@cross_origin()

def api2():
    tabela = request.get_json()
    
    
    
    resultado = processar_projecao(tabela)
    resultado = resultado.to_dict(orient="records") # converte o DataFrame em uma lista de dicionários
    return jsonify(resultado)
 
    



if __name__ == '__main__':
    app.run(port=1008)

    app.run()
