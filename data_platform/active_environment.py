import os
from environment import Environment

# Pega qual é a variável de ambiente que estou usando naquele momento e passa como parâmetro pra classe que 
# criamos o enum e a classe irá atribuir a informação de acordo.
active_environment = Environment[os.environ['ENVIRONMENT']]