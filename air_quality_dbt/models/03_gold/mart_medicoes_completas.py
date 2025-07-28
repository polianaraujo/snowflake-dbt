# Importa as bibliotecas necessárias
import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler

# Todo modelo dbt em Python precisa de uma função chamada 'model'
# def model(dbt, session):
#     # 1. Configurações do modelo dbt
#     dbt.config(
#         materialized="table",
#         packages=["pandas", "scikit-learn"]
#     )

#     # 2. Pega os dados da camada Silver
#     silver_df = dbt.ref('sil_poluentes_estacoes').to_pandas()

#     # --- Início da sua lógica de Machine Learning ---

#     # 3. Prepara os dados para o KNN
#     cols_identificacao = ['STATION', 'DATA_MEDICAO']
#     # Adicionamos a nova coluna numérica de data à lista de features
#     cols_numericas = [
#         'PM10', 'PM2_5', 'NO', 'NO2', 'NOX', 'CO', 'OZONO', 
#         'LATITUDE', 'LONGITUDE', 
#         'DATA_MEDICAO_NUM' # <-- NOVA FEATURE
#     ]
    
#     # 3.5. Converte a coluna de data/hora para um formato numérico (timestamp Unix)
#     #      Isso é CRUCIAL para que o KNN considere a proximidade temporal.
#     silver_df['DATA_MEDICAO_NUM'] = (silver_df['DATA_MEDICAO'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    
#     df_numerico = silver_df[cols_numericas]

#     # 4. Escala os dados (boa prática para KNN)
#     scaler = StandardScaler()
#     scaled_data = scaler.fit_transform(df_numerico)

#     # 5. Aplica o KNN Imputer para preencher os valores nulos
#     imputer = KNNImputer(n_neighbors=2)
#     imputed_scaled_data = imputer.fit_transform(scaled_data)

#     # 6. Reverte a escala para obter os valores originais
#     imputed_data = scaler.inverse_transform(imputed_scaled_data)

#     # 7. Cria o DataFrame final com os dados imputados
#     df_imputed = pd.DataFrame(imputed_data, columns=cols_numericas)
    
#     # Junta as colunas de identificação de volta
#     final_df = pd.concat([silver_df[cols_identificacao].reset_index(drop=True), df_imputed], axis=1)

#     # Remove a coluna numérica de data que criamos, pois ela não é necessária no resultado final
#     final_df = final_df.drop(columns=['DATA_MEDICAO_NUM'])

#     # --- Fim da lógica de Machine Learning ---

#     # 8. Retorna o DataFrame final
#     return final_df

# considerando proximidade de data/hora, estação e poluentes
# def model(dbt, session):
#     '''
#     Preenche valores ausentes de poluentes usando KNN Imputer,
#     baseado na proximidade espacial (Latitude, Longitude) e temporal.
#     A lógica é inspirada na função de avaliação, separando dados completos
#     e incompletos para treinar e aplicar a imputação.
#     '''
#     # 1. Configurações do modelo dbt
#     dbt.config(
#         materialized="table",
#         packages=["pandas", "scikit-learn"]
#     )

#     # 2. Pega os dados da camada Silver
#     silver_df = dbt.ref('sil_poluentes_estacoes').to_pandas()

#     # --- Início da Lógica de Machine Learning ---

#     # 3. Definição das colunas
#     # Colunas de identificação que não entram no modelo KNN
#     cols_identificacao = ['STATION', 'DATA_MEDICAO']
#     # Colunas de poluentes que queremos imputar
#     cols_poluentes = ['PM10', 'PM2_5', 'NO', 'NO2', 'NOX', 'CO', 'OZONO']

#     # Cópia para evitar SettingWithCopyWarning
#     df = silver_df.copy()
    
#     # 4. Feature Engineering: Criar discriminador temporal
#     # Lógica idêntica à da função de avaliação para consistência.
#     # Converte DATA_MEDICAO para datetime se ainda não for.
#     df['DATA_MEDICAO'] = pd.to_datetime(df['DATA_MEDICAO'])
#     df['TIME_DISCRIMINATOR'] = (df['DATA_MEDICAO'].dt.dayofyear * 100000 + 
#                                      df['DATA_MEDICAO'].dt.hour * 100 + 
#                                      df['DATA_MEDICAO'].dt.minute)

#     # 5. Separar dados completos dos dados a serem imputados
#     # Identifica as linhas que têm algum valor nulo nas colunas de poluentes
#     idx_para_imputar = df[cols_poluentes].isnull().any(axis=1)
    
#     df_para_imputar = df[idx_para_imputar]
#     df_com_valores = df[~idx_para_imputar]

#     # Se não houver nada para imputar, retorna o dataframe original
#     if df_para_imputar.empty:
#         return silver_df

#     # Se não houver dados completos para treinar, não é possível imputar.
#     # Retorna o dataframe original com uma mensagem de aviso.
#     if df_com_valores.empty:
#         print("Aviso: Não há dados completos para treinar o imputador KNN. Nenhum valor foi preenchido.")
#         return silver_df

#     # 6. Treinar o KNN Imputer
#     # Definimos as features que o KNN usará para encontrar os vizinhos mais próximos
#     features_for_imputation = ['TIME_DISCRIMINATOR', 'LATITUDE', 'LONGITUDE'] + cols_poluentes
    
#     # IMPORTANTE: O imputer é treinado SOMENTE com os dados completos.
#     imputer = KNNImputer(n_neighbors=2)
#     imputer.fit(df_com_valores[features_for_imputation])

#     # 7. Aplicar a imputação para preencher os valores nulos
#     # O transform é aplicado no dataframe que contém os valores nulos
#     imputed_data = imputer.transform(df_para_imputar[features_for_imputation])
    
#     # Cria um DataFrame com os dados imputados, mantendo os nomes das colunas e o índice original
#     df_imputed = pd.DataFrame(imputed_data, columns=features_for_imputation, index=df_para_imputar.index)

#     # 8. Reconstruir o DataFrame final
#     # Atualiza o dataframe que continha nulos com os novos valores preenchidos
#     df_para_imputar.update(df_imputed)
    
#     # Junta as linhas que originalmente não tinham nulos com as linhas que foram corrigidas
#     final_df = pd.concat([df_com_valores, df_para_imputar])
    
#     # Ordena pelo índice para garantir que a ordem original seja mantida
#     final_df = final_df.sort_index()

#     # Remove a coluna auxiliar que criamos
#     final_df = final_df.drop(columns=['TIME_DISCRIMINATOR'])

#     # --- Fim da lógica de Machine Learning ---

#     # 9. Retorna o DataFrame final, que agora está completo
#     # Seleciona as colunas originais para garantir consistência no schema de saída
#     return final_df[silver_df.columns]

# considerando proximidade entre data/hora e estação
def model(dbt, session):
    '''
    Preenche valores ausentes de poluentes usando KNN Imputer.
    Para cada poluente, a busca por vizinhos é baseada SOMENTE na 
    proximidade espacial (Latitude, Longitude) e temporal.
    '''
    # 1. Configurações do modelo dbt
    dbt.config(
        materialized="table",
        packages=["pandas", "scikit-learn"]
    )

    # 2. Pega os dados da camada Silver
    silver_df = dbt.ref('sil_poluentes_estacoes').to_pandas()

    # --- Início da Lógica de Machine Learning ---

    # 3. Definição das colunas para verificação de nulos
    cols_poluentes = ['PM10', 'PM2_5', 'NO', 'NO2', 'NOX', 'CO', 'OZONO']
    
    # Cópia para evitar SettingWithCopyWarning
    df = silver_df.copy()
    
    # 4. Feature Engineering: Criar discriminador temporal
    df['DATA_MEDICAO'] = pd.to_datetime(df['DATA_MEDICAO'])
    df['TIME_DISCRIMINATOR'] = (df['DATA_MEDICAO'].dt.dayofyear * 100000 + 
                                     df['DATA_MEDICAO'].dt.hour * 100 + 
                                     df['DATA_MEDICAO'].dt.minute)

    # 5. Separar dados de treino (completos) dos que precisam de imputação
    # A lógica de separação é a mesma: `df_com_valores` é nossa referência confiável.
    idx_para_imputar = df[cols_poluentes].isnull().any(axis=1)
    df_para_imputar = df[idx_para_imputar]
    df_com_valores = df[~idx_para_imputar]

    if df_para_imputar.empty:
        return silver_df

    if df_com_valores.empty:
        print("Aviso: Não há dados completos para treinar o imputador KNN. Nenhum valor foi preenchido.")
        return silver_df

    # 6. Treinar e Aplicar o KNN Imputer em um loop para cada poluente
    
    # cópia do dataframe que será preenchida gradualmente
    df_final_imputado = df.copy()

    print("Iniciando imputação por poluente (baseado apenas em tempo e espaço)...")
    
    for poluente in cols_poluentes:
        # Apenas continua se houver algum valor nulo para este poluente específico
        if df_final_imputado[poluente].isnull().any():
            print(f"Processando {poluente}...")

            # Define as features para ESTA rodada: apenas tempo, espaço e o poluente alvo.
            features_this_run = ['TIME_DISCRIMINATOR', 'LATITUDE', 'LONGITUDE', poluente]
            
            # Prepara os dados de treino (completos) e os dados a serem imputados
            train_data = df_com_valores[features_this_run]
            impute_data = df_final_imputado[features_this_run]

            # Cria e treina o imputer
            imputer = KNNImputer(n_neighbors=2)
            imputer.fit(train_data)
            
            # Aplica a imputação e obtém os dados preenchidos
            imputed_data = imputer.transform(impute_data)
            
            # Atualiza a coluna do poluente no nosso dataframe final com os valores imputados
            # A última coluna de `imputed_data` (índice -1) corresponde ao nosso `poluente`
            df_final_imputado[poluente] = imputed_data[:, -1]

    print("Imputação finalizada.")
    # 7. Limpeza final
    # Remove a coluna auxiliar que criamos
    # final_df = df_final_imputado.drop(columns=['TIME_DISCRIMINATOR'])
    final_df = df_final_imputado

    # --- Fim da lógica de Machine Learning ---

    # 8. Retorna o DataFrame final, que agora está completo
    # return final_df[silver_df.columns]
    return final_df