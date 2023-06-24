from flask_cors import CORS, cross_origin
import pandas as pd
import json

app = Flask(__name__)
cors = CORS(app, origins=["https://localhost:7171"])


def processar_tabela(tabela):
   
    ferias = pd.DataFrame(tabela)
    
    ferias.rename(columns=ferias.iloc[0], inplace=True)
    ferias.drop(ferias.index[0], inplace=True)
    
    colunas = ferias.columns.to_list()
    # Mostra os índices e os valores das colunas
    for i, col in enumerate(ferias):
          print(i, col)
    
   
    
             
    colunas = ["Nome Pessoal", "Dias", "Desc","Status", "Saldo","Inicio","Fim","Inicio.1","Fim.1","Inicio.2", "Fim.2"]
    #Tabela com as colunas selecionadas
    ferias = ferias[colunas]
    ferias_manipular1 = ferias[colunas]
    ferias_manipular2 = ferias[colunas]
    ferias_manipular3 = ferias[colunas]
    #Quebrar a Tabela a Primeira vez
    colunas = ["Nome Pessoal", "Dias", "Desc","Status","Saldo","Inicio","Fim"]
    ferias_periodo_1 = ferias_manipular1[colunas]
    ferias_periodo_1 = ferias_periodo_1.assign(Periodo=1)
    #Quebrar a tabela a Segunda Vez
    colunas = ["Nome Pessoal", "Dias", "Desc","Status", "Saldo","Inicio.1","Fim.1"]
    ferias_periodo_2 = ferias_manipular2[colunas]
    ferias_periodo_2 = ferias_periodo_2.assign(Periodo=2)
    ferias_periodo_2.rename(columns={"Inicio.1":"Inicio", "Fim.1":"Fim"}, inplace=True)
    #Quebrar a tabela a Terceira Vez
    colunas = ["Nome Pessoal", "Dias", "Desc","Status", "Saldo","Inicio.2","Fim.2"]
    ferias_periodo_3 = ferias_manipular3[colunas]
    ferias_periodo_3 = ferias_periodo_3.assign(Periodo=3)
    ferias_periodo_3.rename(columns={"Inicio.2":"Inicio", "Fim.2":"Fim"}, inplace=True)
    ferias_nulos = pd.concat([ferias_periodo_1, ferias_periodo_2, ferias_periodo_3], ignore_index=True)
    ferias_sem_nulos = ferias_nulos.dropna()
    # Mostrar os nomes do ferias_nulos que não estão no ferias_sem_nulos
    df = ferias_nulos[~ferias_nulos["Nome Pessoal"].isin(ferias_sem_nulos["Nome Pessoal"])]
    df = df.drop_duplicates(subset=["Nome Pessoal"])# Concatenar os dois data frames
    final = pd.concat([ferias_sem_nulos, df])

    
    return final

@app.route('/api', methods=['POST'])
@cross_origin()

def api():
    tabela = request.get_json()
    
    
    
    resultado = processar_tabela(tabela)
    resultado = resultado.to_dict(orient="records") # converte o DataFrame em uma lista de dicionários
    return jsonify(resultado)
    
    
    
    
    
 

if __name__ == '__main__':
    app.run(port=8091)

    app.run()