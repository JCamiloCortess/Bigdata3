import boto3

def lambda_handler(event, context):
    region = 'us-east-1'
    subnet_id = 'subnet-12345678'  # ID de la subred que deseas especificar

    client = boto3.client('emr', region_name=region)

    cluster_id = client.run_job_flow(
        Name='EMR-Spark',
        ReleaseLabel='emr-6.2.0',
        Applications=[{'Name': 'Spark'}],
        Configurations=[{'Classification': 'spark-env', 'Properties': {}, 'Configurations': [{'Classification': 'export', 'Properties': {'PYSPARK_PYTHON': '/usr/bin/python3'}}]}],
        Instances={
            'Ec2SubnetId': subnet_id,  # Aquí especificas la subred
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Worker nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        Steps=[{
            'Name': 'Run Spark script',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ["spark-submit", "--deploy-mode", "cluster", "s3://pybucketdata/untitled.py"]
            }
        }],
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True
    )

    print(f"Started cluster {cluster_id}")
    return {
        'statusCode': 200,
        'body': f"Cluster {cluster_id} started successfully"
    }
