from enum import Enum
from aws_cdk import core
from aws_cdk import (
    aws_s3 as s3,
)

# Importando a classe Environment criada na raiz do data_platform.
from data_platform.environment import Environment

# Atribuindo um enum para não ficar escrevendo uma string toda hora.
class DataLakeLayer(Enum):
    RAW = 'raw'
    PROCESSED = 'processed'
    AGGREGATED = 'aggregated'

''' 
    Criando uma classe que tem todas as característico de um S3 Bucket
    e quero padronizar para a minha necessidade como um bucket de data lake é criado.
    Ao mudar o método, o self é padrão e o scope que é o app.py é obrigatório para a passagem de parâmetros customizados. 
'''
class BaseDataLakeBucket(s3.Bucket):
    
    # Estou acrescentando o ambiente e a camada que seria o enum acima, mais o nome padrão para novos buckets criados.
    def __init__(self, scope: core.Construct, deploy_env: Environment, layer: DataLakeLayer, **kwargs):
        self.layer = layer
        self.deploy_env = deploy_env
        self.obj_name = f's3-bck-{self.deploy_env.value}-data-lake-{self.layer.value}'

        # Obrigatório, inicializando a camada mãe, passo o scope, o id é o nome lógico. E posso adicionar tudo o que
        # gostaria de configuração, a exemplo do bloqueio de acesso público.
        super().__init__(
            scope,
            id=self.obj_name,
            bucket_name=self.obj_name,
            block_public_access=self.default_block_public_access,   # Quando chamar este default, ele está declarado abaixo
            encryption=self.default_encryption,
            versioned=True,                                         # Se deletar, ex, ele faz a deleção lógica. E com o mesmo nome, ele versiona. 
            **kwargs
        )
        
        self.set_default_lifecycle_rules()

    # Propriedade default acima usada, virá aqui onde defino o que quero um a um.
    @property
    def default_block_public_access(self):
        return s3.BlockPublicAccess(
                ignore_public_acls=True,
                block_public_acls=True,
                block_public_policy=True,
                restrict_public_buckets=True
            )

    # Optando pela criptografia gerenciada pela AWS.
    @property
    def default_encryption(self):
        return s3.BucketEncryption.S3_MANAGED


    def set_default_lifecycle_rules(self):
        """
        Sets lifecycle rule by default
        """
        self.add_lifecycle_rule(
            # Tenho um upload muito grande de várias partes, após 7 dias executando o upload, aborta a operação.
            abort_incomplete_multipart_upload_after=core.Duration.days(7),
            enabled=True
        )

        self.add_lifecycle_rule(            
            noncurrent_version_transitions=[
                # Cinco versões de um mesmo objeto, após 30 dias, move as versões inativas para a camada infrequent_access
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                    transition_after=core.Duration.days(30)
                ),
                # Após 60 dias na camada infrequent, move as versões inativas para a camada glacier
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=core.Duration.days(60)
                )
            ]
        )

        self.add_lifecycle_rule(
            # Expirar uma versão não corrente após 360 dias.
            noncurrent_version_expiration=core.Duration.days(360)
        )    