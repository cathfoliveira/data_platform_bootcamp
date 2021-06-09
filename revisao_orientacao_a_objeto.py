class Pessoa:    
    # Metódo para inicialização da classe pessoa
    def __init__(self, nome, sobrenome, idade):
        self.sobrenome = sobrenome
        self.nome = nome
        self.idade = idade

    # Ao dar um print no objeto, o que vai aparecer?
    def __str__(self):
        return f"{self.nome} {self.sobrenome} tem {self.idade} anos."

# Teste 1:
catharina = Pessoa(nome='Catharina', sobrenome='Oliveira',idade='34')        
print(catharina)


class Cachorro:
    def __init__(self, nome, raca, idade):
        self.raca = raca
        self.nome = nome
        self.idade = idade

    def __str__(self):
        return f"{self.nome} é da raça {self.raca} e tem {self.idade} anos."

    def is_cachorro(self):
        return True

# Teste 2:
belisco = Cachorro(nome='Belisco', raca='Lhasa',idade='1.9')        
print(belisco)
print(belisco.is_cachorro())


# Declarando uma classe engenheiro de dados que herda de Pessoa (o DE é uma pessoa).
class EngenheiroDeDados(Pessoa):    
    # Recupero a inicialização, passando para a classe mão os dados dela e iniciando o DE com algo que é 
    # próprio dele, no caso, a experiencia. O.S., estou modificando o método.
    def __init__(self, nome, sobrenome, idade, experiencia):
        super().__init__(nome, sobrenome, idade)
        self.experiencia = experiencia

    # Modificando o método str herdado de Pessoa.
    def __str__(self):
        return f"{self.nome} {self.sobrenome} tem {self.idade} anos, " \
               f"é Engenheiro de Dados e tem {self.experiencia} anos de experiencia"

andre = EngenheiroDeDados(nome='Andre', sobrenome='Sionek', idade=30, experiencia=4)
print(andre)    

# Declaro uma nova classe que herda os métodos originais de Cahorro e adiciona um método a mais.
class CatiorinhoDanadinho(Cachorro):
    def is_danadinho(self):
        return True


belisco = CatiorinhoDanadinho(nome='Belisco', raca='Lhasa', idade=1.5)
print(belisco)
print(belisco.is_danadinho())
print(belisco.is_cachorro())