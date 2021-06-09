  
from enum import Enum

# Criando uma classe de enum para os ambientes que quero criar.
class Environment(Enum):
    PRODUCTION = 'production'
    STAGING = 'staging'
    DEVELOP = 'develop'