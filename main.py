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
    colunas = ["Nome Pessoal", "Status1", "Saldo","Inicio","Fim","Inicio1","Fim1","Inicio2", "Fim2","Ini. Per. Aquis.","Fim Per. Aquis.","Desc. Centro de Custo","Dias0","Dias1","Dias2"]

    ferias_periodo_1 = ferias_manipular1[colunas]
    ferias_periodo_1 = ferias_periodo_1.assign(Periodo=1)
    ferias_periodo_1 = ferias_periodo_1.assign(SaldoString=ferias_periodo_1["Saldo"]) 
       




        # Usando o índice das colunas em vez dos nomes
    colunas = ["Nome Pessoal","Status1", "Saldo","Inicio1","Fim1","Ini. Per. Aquis.","Fim Per. Aquis.","Desc. Centro de Custo","Dias1"]
    ferias_periodo_2 = ferias_manipular2[colunas]
    ferias_periodo_2 = ferias_periodo_2.assign(Periodo=2)
    ferias_periodo_2.rename(columns={"Inicio1":"Inicio", "Fim1":"Fim", "Dias1":"Dias0"}, inplace=True)
    ferias_periodo_2 = ferias_periodo_2.assign(SaldoString=ferias_periodo_2["Saldo"])

    ferias_periodo_2 = ferias_periodo_2.assign(Saldo=0) # Altera o valor da coluna Saldo para 0



         # Usando o índice das colunas em vez dos nomes
    colunas = ["Nome Pessoal","Status1", "Saldo","Inicio2","Fim2","Ini. Per. Aquis.","Fim Per. Aquis.","Desc. Centro de Custo","Dias2"]
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
    final.rename(columns={"Status1":"Status", "Dias0":"Dias"}, inplace=True) # Renomeia a coluna status1 como status
    final['ExtracaoDados'] = pd.to_datetime(datetime.now().date())
    final = final.fillna(0)


    
    return final

@app.route('/api', methods=['POST'])
@cross_origin()

def api():
    tabela = request.get_json()
    
    
    
    resultado = processar_tabela(tabela)
    resultado = resultado.to_dict(orient="records") # converte o DataFrame em uma lista de dicionários
    return jsonify(resultado)
 
    


if __name__ == '__main__':
    app.run(port=1008)

    app.run()
