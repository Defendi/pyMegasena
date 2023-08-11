import os
import pandas as pd

from app.methods import SqlMethods

def DataToDict(concurso, data, bola, numero):
    return {
        'concurso': concurso,
        'data': data,
        'bola': bola,
        'numero': numero
    }


class Resultados:
    _caminho = ""
    _db = SqlMethods()
    
    def __init__(self, caminho:str=""):
        #TODO: Validar o caminho
        self._caminho = caminho
        
    @property
    def caminho(self):
        if bool(self._caminho):
            return self._caminho
        else:
            False
    
    def caminho(self, value:str):
        if bool(value):
            self._caminho = value
            self.update()
        else:
            self._caminho = ""
        
    def update(self):
        if bool(self._caminho):
            try:
                planilha = pd.read_excel(self._caminho, sheet_name=0, header=0, usecols='A:T')
                total = len(planilha.Concurso)
                count = 0
                for linha in range(len(planilha.Concurso)):
                    percentual = round((count / total) * 100,2) 
                    concurso = planilha.iloc[linha]['Concurso']
                    data = planilha.iloc[linha]['Data do Sorteio']
                    bola1 = planilha.iloc[linha]['Bola1']
                    bola2 = planilha.iloc[linha]['Bola2']
                    bola3 = planilha.iloc[linha]['Bola3']
                    bola4 = planilha.iloc[linha]['Bola4']
                    bola5 = planilha.iloc[linha]['Bola5']
                    bola6 = planilha.iloc[linha]['Bola6']
                    if not self._tem_concurso(concurso, 1):
                        self._db.sql_insert_into(table='sorteio', list_dict_data=[DataToDict(concurso,data,1,bola1)])
                    if not self._tem_concurso(concurso, 2):
                        self._db.sql_insert_into(table='sorteio', list_dict_data=[DataToDict(concurso,data,2,bola2)])
                    if not self._tem_concurso(concurso, 3):
                        self._db.sql_insert_into(table='sorteio', list_dict_data=[DataToDict(concurso,data,3,bola3)])
                    if not self._tem_concurso(concurso, 4):
                        self._db.sql_insert_into(table='sorteio', list_dict_data=[DataToDict(concurso,data,4,bola4)])
                    if not self._tem_concurso(concurso, 5):
                        self._db.sql_insert_into(table='sorteio', list_dict_data=[DataToDict(concurso,data,5,bola5)])
                    if not self._tem_concurso(concurso, 6):
                        self._db.sql_insert_into(table='sorteio', list_dict_data=[DataToDict(concurso,data,6,bola6)])
                    if (count % 15) == 0 or count == 0:
                        print("Salvo {:10.2f}% dos sorteios".format(percentual))
                    count += 1
                        
            except Exception as e:
                print(str(e))
        
    def _tem_concurso(self, concurso:int, bola:int):
        queryCount = f"""SELECT COUNT(*) AS qtde FROM public.sorteio WHERE concurso = {concurso} AND bola = {bola}"""
        count = self._db.sql_set_command(queryCount)[0][0]
        if count > 0:
            return True
        else:
            return False
        
        
        
        