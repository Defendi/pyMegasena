import os
import pandas as pd

from methods import SqlMethods

class Resultados:
    _caminho = ""
    
    def __init__(self, caminho:str=""):
        #TODO: Validar o caminho
        self._caminho = caminho
        
    @property
    def caminho(self):
        if bool(self._caminho):
            return self._caminho
        else:
            False
    
    @caminho.setter
    def caminho(self, value:str):
        if bool(value):
            self._caminho = value
        else:
            self._caminho = ""
        
    def update(self):
        if bool(self._caminho):
            try:
                planilha = pd.read_excel(self._caminho, index_col=None, header=None, usecols='A')
            except:
                raise TypeError('Não foi possível abrir a planilha!')
                