
from aws_cdk import core
from data_platform.data_lake.stack import DataLakeStack
from data_platform.common_stack import CommonStack

app = core.App()
data_lake = DataLakeStack(app)      # Instanciando a stack (Estrutura do data lake)
common_stack = CommonStack(app)     # Instanciando a VPC, políticas, BD PostgreSQL, gateways etc. (RDS + VPC)
app.synth()
