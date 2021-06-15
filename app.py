
from aws_cdk import core
from data_platform.data_lake.stack import DataLakeStack
from data_platform.common_stack import CommonStack
from data_platform.dms.stack import DmsStack
from data_platform.kinesis.stack import KinesisStack
from data_platform.glue_catalog.stack import GlueCatalogStack
from data_platform.athena.stack import AthenaStack
from data_platform.redshift.stack import RedshiftStack
from data_platform.databricks.stack import DatabricksStack

app = core.App()

# Instanciando a stack (Estrutura do data lake)
data_lake = DataLakeStack(app)      

# Instanciando a VPC, políticas, BD PostgreSQL, gateways etc. (RDS + VPC)
common_stack = CommonStack(app)     

# Common_stack pega a vpc e a instância do RDS e o datalake pega o bucket de destino
dms = DmsStack(app,
    common_stack=common_stack,
    data_lake_raw_bucket=data_lake.data_lake_raw_bucket)     

kinenis = KinesisStack(app, 
    data_lake_raw_bucket=data_lake.data_lake_raw_bucket)

glue_catalog = GlueCatalogStack(app, 
    data_lake_raw_bucket=data_lake.data_lake_raw_bucket, 
    data_lake_processed_bucket=data_lake.data_lake_processed_bucket)

athena = AthenaStack(app)

redshift = RedshiftStack(app, 
    data_lake_raw=data_lake.data_lake_raw_bucket, 
    data_lake_processed=data_lake.data_lake_processed_bucket, 
    common_stack=common_stack)

# databricks = DatabricksStack(app)    

app.synth()
