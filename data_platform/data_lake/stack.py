# Criando a estrutura de armazenamento padrão do meu Data Lake

from data_platform.data_lake.base import BaseDataLakeBucket, DataLakeLayer #Importando as classes de base
from aws_cdk import core
from aws_cdk import (
    aws_s3 as s3,
)

from data_platform import active_environment

# Criando a minha stack de fato, herdo da Stack mãe. Novamente o scope do app.py e o ambiente ativo.
class DataLakeStack(core.Stack):
    def __init__(self, scope: core.Construct, **kwargs) -> None:
        self.deploy_env = active_environment
        super().__init__(scope, id=f'{self.deploy_env.value}-data-lake-stack', **kwargs)

        # Definindo que a stack terá um bucket raw
        self.data_lake_raw_bucket = BaseDataLakeBucket(
            self,
            deploy_env=self.deploy_env,
            layer=DataLakeLayer.RAW
        )

        # Como é uma camada raw, quero definir o lifecycle como abaixo
        self.data_lake_raw_bucket.add_lifecycle_rule(
            transitions=[
                s3.Transition(
                    storage_class=s3.StorageClass.INTELLIGENT_TIERING, # Um pouco mais barato que o standard
                    transition_after=core.Duration.days(90)
                ),
                s3.Transition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=core.Duration.days(360)
                )
            ],
            enabled=True
        )

        # Data Lake Processed
        self.data_lake_processed_bucket = BaseDataLakeBucket(
            self,
            deploy_env=self.deploy_env,
            layer=DataLakeLayer.PROCESSED
        )

        # Data Lake Aggregated
        self.data_lake_aggregated_bucket = BaseDataLakeBucket(
            self,
            deploy_env=self.deploy_env,
            layer=DataLakeLayer.AGGREGATED
        )