from aws_cdk import core

from data_platform.athena.base import BaseAthenaBucket, BaseAthenaWorkgroup
from data_platform import active_environment

''' O Athena vai apontar para o glue_catalog e vai rodar a query em cima do database criado utilizando os metadados advindos do glue.
    Assim, se fizermos um count por exemplo, será rápido pq será um metadado já mapeado no glue.
'''
class AthenaStack(core.Stack):
    def __init__(self, scope: core.Construct, **kwargs) -> None:
        self.deploy_env = active_environment
        super().__init__(scope, id=f'{self.deploy_env.value}-athena', **kwargs)

        self.athena_bucket = BaseAthenaBucket(
            self,
            deploy_env=self.deploy_env
        )

        self.athena_workgroup = BaseAthenaWorkgroup(
            self,
            deploy_env=self.deploy_env,
            athena_bucket=self.athena_bucket,
            gb_scanned_cutoff_per_query=1
        )