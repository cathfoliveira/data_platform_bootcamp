
from aws_cdk import core
from data_platform.data_lake.stack import DataLakeStack
from data_platform.common_stack import CommonStack

app = core.App()

data_lake = DataLakeStack(app)      # Instanciando a stack (Estrutura do data lake)
common_stack = CommonStack(app)     # Instanciando a VPC, políticas, BD PostgreSQL, gateways etc. (RDS + VPC)
dms = DmsStack(app,common_stack=common_stack,data_lake_raw_bucket=data_lake.data_lake_raw_bucket)
# Common_stack pega a vpc e a instância do RDS e o datalake pega o bucket de destino

app.synth()
