from aws_cdk import core

from data_platform import active_environment
from data_platform.data_lake.base import BaseDataLakeBucket
from data_platform.glue_catalog.base import (
    BaseDataLakeGlueDatabase,
    BaseDataLakeGlueRole,
    BaseGlueCrawler,
    OrdersTable,
)


class GlueCatalogStack(core.Stack):
    def __init__(
        self, scope: core.Construct, raw_data_lake_bucket: BaseDataLakeBucket, processed_data_lake_bucket: BaseDataLakeBucket, **kwargs
    ) -> None:
        self.raw_data_lake_bucket = raw_data_lake_bucket
        self.processed_data_lake_bucket = processed_data_lake_bucket
        self.deploy_env = active_environment
        super().__init__(
            scope, id=f"{self.deploy_env.value}-glue-catalog-stack", **kwargs
        )
        # Neste exemplo criamos dois databases (um processed e um raw onde colocamos as duas tabelas dentro do raw)
        # Idealmente pra cada aplicação, criamos um database por fonte. Um database para o rds_producao, um database de pageviews do site, etc.
        self.raw_database = BaseDataLakeGlueDatabase(
            self, data_lake_bucket=self.raw_data_lake_bucket
        )

        self.processed_database = BaseDataLakeGlueDatabase(
            self, data_lake_bucket=self.processed_data_lake_bucket
        )

        self.role = BaseDataLakeGlueRole(self, data_lake_bucket=self.raw_data_lake_bucket)

        self.atomic_events_crawler = BaseGlueCrawler(
            self,
            glue_database=self.raw_database,
            glue_role=self.role,
            table_name="atomic_events",                         # Nome da tabela (kinesis)
            schedule_expression="cron(0/15 * * * ? *)",         # Agendando pra rodar de 15 em 15 minutos
        )

        # Dependências para execução do crawler
        self.atomic_events_crawler.node.add_dependency(self.raw_database)
        self.atomic_events_crawler.node.add_dependency(self.role)

        self.orders_table = OrdersTable(
            self, glue_database=self.raw_database, glue_role=self.role
        )

        self.orders_table.node.add_dependency(self.raw_database)
        self.orders_table.node.add_dependency(self.role)