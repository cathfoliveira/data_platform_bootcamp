from aws_cdk import core
from aws_cdk import aws_redshift as redshift, aws_ec2 as ec2, aws_iam as iam
from data_platform.data_lake.base import BaseDataLakeBucket
from data_platform import active_environment

from data_platform.common_stack import CommonStack
'''
CREATE EXTERNAL SCHEMA [IF NOT EXISTS] data_lake_raw
FROM  DATA CATALOG 
DATABASE 'glue_db_develop_data_lake_raw' #nome da database do glue
REGION 'sa-east-1' 
IAM_ROLE 'arn:aws:iam:0dfshdfuhdsfahfdhfa'

Executamos esta query uma única vez, pq uma vez que criou o schema,
Ele estará criado e a conexão com o glue estará feita.

O create external schema redshift é o que vai possibilitar=> O redshift vai se conectar ao glue 
para pegar o catálogo de dados e vai usar o Athena para ler os dados do S3. Poderia se conectar a outras fontes.

O que fizemos, temos um RDS postgres que através do DMS carregarmos dados para o S3 em arquivos parquet e configuramos 
o glue para termos os metadados e podermos ler de maneira otimizada via Athena que tem algo custo e é indicado para 
inspeções. Agora precisamos ler os dados do S3 e carregarmos no nosso DW (que é o redshift que criamos). 
Para essa leitura, precisamos da tecnologia spectrum.


'''
class SpectrumRole(iam.Role):
    def __init__(
        self, scope: core.Construct, data_lake_raw: BaseDataLakeBucket, data_lake_processed: BaseDataLakeBucket, **kwargs
    ) -> None:
        self.deploy_env = data_lake_raw.deploy_env
        self.data_lake_raw = data_lake_raw
        self.data_lake_processed = data_lake_processed

        super().__init__(
            scope,
            id=f"iam-{self.deploy_env.value}-redshift-spectrum-role",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            description="Role to allow Redshift to access data lake using spectrum",
        )

        policy = iam.Policy(
            scope,
            id=f"iam-{self.deploy_env.value}-redshift-spectrum-policy",
            policy_name=f"iam-{self.deploy_env.value}-redshift-spectrum-policy",
            statements=[
                iam.PolicyStatement(actions=["glue:*", "athena:*"], resources=["*"]),
                iam.PolicyStatement(
                    actions=["s3:Get*", "s3:List*", "s3:Put*"],
                    resources=[
                        # dando acesso tanto no dl raw quanto no dl process
                        self.data_lake_raw.bucket_arn,
                        f"{self.data_lake_raw.bucket_arn}/*",
                        self.data_lake_processed.bucket_arn,
                        f"{self.data_lake_processed.bucket_arn}/*",
                    ],
                ),
            ],
        )
        self.attach_inline_policy(policy)


class RedshiftStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        data_lake_raw: BaseDataLakeBucket,
        data_lake_processed: BaseDataLakeBucket,
        common_stack: CommonStack,
        **kwargs,
    ) -> None:
        self.common_stack = common_stack
        self.data_lake_raw = data_lake_raw
        self.deploy_env = active_environment
        self.data_lake_processed = data_lake_processed
        super().__init__(scope, id=f"{self.deploy_env.value}-redshift-stack", **kwargs)

        self.redshift_sg = ec2.SecurityGroup(
            self,
            f"redshift-{self.deploy_env.value}-sg",
            vpc=self.common_stack.custom_vpc,
            allow_all_outbound=True,
            security_group_name=f"redshift-{self.deploy_env.value}-sg",
        )

        self.redshift_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4("0.0.0.0/0"), connection=ec2.Port.tcp(5439)
        )

        for subnet in self.common_stack.custom_vpc.private_subnets:
            self.redshift_sg.add_ingress_rule(
                peer=ec2.Peer.ipv4(subnet.ipv4_cidr_block), connection=ec2.Port.tcp(5439)
            )

        self.redshift_cluster = redshift.Cluster(
            self,
            f"db-cluster-{self.deploy_env.value}-redshift",
            cluster_name=f"db-cluster-{self.deploy_env.value}-redshift",
            vpc=self.common_stack.custom_vpc,
            cluster_type=redshift.ClusterType.MULTI_NODE,
            node_type=redshift.NodeType.DC2_LARGE,
            default_database_name="dw",
            number_of_nodes=2,
            removal_policy=core.RemovalPolicy.DESTROY,
            master_user=redshift.Login(master_username="admin"),
            publicly_accessible=True,
            roles=[SpectrumRole(self, self.data_lake_raw, self.data_lake_processed)],
            security_groups=[self.redshift_sg],
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
        )   