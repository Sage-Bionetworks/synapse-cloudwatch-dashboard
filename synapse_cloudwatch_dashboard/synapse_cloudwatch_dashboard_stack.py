import boto3
from configuration import ConfigurationProvider, AwsProvider
from aws_cdk import (
    Duration,
    Stack,
    aws_cloudwatch as cw
)
from constructs import Construct
import ast
import os

def get_aws_provider(profile_name):
  if profile_name:
    session = boto3.Session(profile_name=profile_name, region_name='us-east-1')
  else:
    session = boto3.Session(region_name='us-east-1')
  return AwsProvider(session=session)

def init_config(stack, aws_provider):
  BUCKET_NAME = f'{stack}.cloudwatch.metrics.sagebase.org'
  FILE_KEY = f'{stack}_cw_configuration.json'
  s3_client = aws_provider.get_client(client_type='s3')
  configuration_provider = ConfigurationProvider(s3_client=s3_client, bucket_name=BUCKET_NAME, file_key=FILE_KEY)
  config = configuration_provider.load_raw_configuration()
  return config


def create_graph_widget(namespace, metric_name, dimension_name, values, title='Title', width=24, height=6):
  metrics = [
    cw.Metric(
      namespace=namespace,
      metric_name=metric_name,
      dimensions_map={dimension_name: instance_id}
    ) for instance_id in values
  ]
  widget = cw.GraphWidget(title=title, width=width, height=height, stacked=False, left=metrics, view=cw.GraphWidgetView.TIME_SERIES)
  return widget

def create_worker_stats_widget(title, stack_versions_to_worker_names_map, metric_name):
  metrics = []
  for sv in stack_versions_to_worker_names_map:
    namespace = f'Worker-Statistics-{sv}'
    version_metrics = [cw.Metric(namespace=namespace, metric_name=metric_name,
        dimensions_map={"Worker Name": worker_name}) for worker_name in stack_versions_to_worker_names_map[sv]]
      
    metrics.extend(version_metrics)
  return cw.GraphWidget(title=title, width=24, height=3,
                        view=cw.GraphWidgetView.TIME_SERIES, stacked=False, period=Duration.seconds(300),
                        left=metrics)


def create_memory_widget(title, stack_versions, environment):
  metrics = []
  for sv in stack_versions:
    namespace = f'{environment}-Memory-{sv}'
    version_metrics = [cw.Metric(namespace=namespace, metric_name='used',
                        dimensions_map={"instance": value}) for value in ['all']]
    metrics.extend(version_metrics)
  return cw.GraphWidget(title=title, width=24, height=3,
                        view=cw.GraphWidgetView.TIME_SERIES, stacked=False, period=Duration.seconds(300),
                        left=metrics)


def create_ec2_cpu_utilization_widget(title, ec2_instance_ids):
  return create_graph_widget("AWS/EC2", "CPUUtilization", "InstanceId", ec2_instance_ids, title, 24, 6)


def create_ec2_network_out_widget(title, ec2_instance_ids):
  return create_graph_widget("AWS/EC2", "NetworkOut", "InstanceId", ec2_instance_ids, title, 24, 3)


'''
  RDS
'''
def rds_ids_from_stack_versions(stack, stack_versions):
  db_types = ['db', 'table-0']
  ids = [f'{stack}-{sv}-{dbt}' for sv in stack_versions for dbt in db_types]
  ids.append(f'{stack}-id-generator-db-3-orange')
  return ids


def create_rds_cpu_utilization_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="CPUUtilization", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=24, height=6)


def create_rds_free_storage_space_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="FreeStorageSpace", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=24, height=3)

def create_rds_read_throughput_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="ReadThroughput", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_write_throughput_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="WriteThroughput", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_read_latency_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="ReadLatency", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_write_latency_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="WriteLatency", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_read_iops_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="ReadIOPS", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


def create_rds_write_iops_widget(title, stack, stack_versions):
  return create_graph_widget(namespace="AWS/RDS", metric_name="WriteIOPS", dimension_name="DBInstanceIdentifier",
                             values=rds_ids_from_stack_versions(stack, stack_versions), title=title, width=12, height=4)


'''
  QueryPerf
'''
def create_query_performance_widget(title, stack, stack_versions):
  metrics = [cw.Metric(namespace="AWS/SQS",
                       metric_name="ApproximateAgeOfOldestMessage",
                       dimensions_map={"QueueName": f'{stack}-{sv}-QUERY'}) for sv in stack_versions]
  widget = cw.GraphWidget(title=title, width=24, height=6, view=cw.GraphWidgetView.TIME_SERIES,
                          left=metrics, period=Duration.seconds(300), stacked=False, statistic='Average')
  return widget

'''
  SES
'''
def create_ses_widget(title):
  bounce_rate_metric = cw.Metric(
    namespace="AWS/SES",
    metric_name="Reputation.BounceRate",
    label="Bounce Rate",
    statistic="Maximum",
    region="us-east-1"
  )
  complaint_rate_metric = cw.Metric(
    namespace="AWS/SES",
    metric_name="Reputation.ComplaintRate",
    label="Complaint Rate",
    statistic="Maximum",
    color="#d62728",
    region="us-east-1"
  )
  bounce_rate_expression = cw.MathExpression(
    expression="100 * m1",
    using_metrics={"m1": bounce_rate_metric},
    period=Duration.hours(1),
    label="Bounce Rate",
    color="#1f77b4"
  )
  complaint_rate_expression = cw.MathExpression(
    expression="100 * m2",
    using_metrics={"m2": complaint_rate_metric},
    period=Duration.hours(1),
    label="Complaint Rate",
    color="#d62728"
  )
  bounce_count_metric = cw.Metric(
    namespace="AWS/SES",
    metric_name="Bounce",
    label="Bounced Count",
    statistic="Sum",
    color="#ff7f0e",
  )
  send_count_metric = cw.Metric(
    namespace="AWS/SES",
    metric_name="Send",
    label="Sent Count",
    statistic="Sum",
    color="#2ca02c",
  )
  widget = cw.GraphWidget(title=title, width=24, height=4,
                          view=cw.GraphWidgetView.TIME_SERIES,
                          left=[
                            # bounce_rate_metric,
                            # complaint_rate_metric,
                            bounce_rate_expression,
                            complaint_rate_expression
                          ],
                          right=[
                            bounce_count_metric,
                            send_count_metric
                          ],
                          period=Duration.hours(1),
                          left_y_axis=cw.YAxisProps(label="Rate", min=0, show_units=False),
                          right_y_axis=cw.YAxisProps(label="Count", min=0, show_units=False)
                          )
  return widget

'''
  FileScanner
'''
def create_filescanner_widget(title, stack_versions):
  left_metrics = []
  right_metrics = []

  for stack_version in stack_versions:
    namespace = f'Asynchronous Workers - {stack_version}'
    left_metrics.extend([
      cw.Metric(namespace=namespace, metric_name="JobCompletedCount",
                        dimensions_map={"workerClass": "FileHandleAssociationScanRangeWorker"},
                        label=f"Jobs Completed - {stack_version}", color="#1f77b4"),
      cw.Metric(namespace=namespace, metric_name="JobFailedCount",
                        label=f"Jobs Failed - {stack_version}", color="#d62728")
    ])
    right_metrics.extend([cw.Metric(namespace=namespace, metric_name="AllJobsCompletedCount",
                        dimensions_map={"workerClass": "FileHandleAssociationScanRangeWorker"},
                        label=f"Scans Completed - {stack_version}", color="#2ca02c")])

  widget = cw.GraphWidget(title=title, width=24, height=4, view=cw.GraphWidgetView.TIME_SERIES,
                          left=left_metrics, right=right_metrics, stacked=False,
                          set_period_to_time_range=True,
                          statistic="Sum")
  return widget


'''
  Active connections
'''
def create_active_connections_metric(namespace, db):
  metric = cw.Metric(
    namespace=namespace,
    metric_name="activeConnectionsCount",
    dimensions_map={"dataSourceId": db}
  )
  return metric

def create_active_connections_widget(title, environment, stack_versions):
  dbs = ["idgen", "main", "tables"]
  metrics = [create_active_connections_metric(f'{environment}-Database-{sv}', db) for sv in stack_versions for db in dbs]
  widget = cw.GraphWidget(title=title, width=24, height=6, left=metrics, statistic="Maximum", view=cw.GraphWidgetView.TIME_SERIES)
  return widget


def create_repo_active_connections_widget(title, stack_versions):
  return create_active_connections_widget(title, "Repository", stack_versions)


def create_workers_active_connections_widget(title, stack_versions):
  return create_active_connections_widget(title, "Workers", stack_versions)


'''
  OpenSearch
'''
def create_opensearch_metric(collection_ids, stack, stack_version):
  collection_name = f'{stack}-{stack_version}-synsearch'
  result = []
  for collection_id in collection_ids:
    metric = cw.Metric(
      namespace="AWS/AOSS",
      metric_name="SearchableDocuments",
      dimensions_map={
        "CollectionId": collection_id,
        "CollectionName": collection_name,
        "ClientId": os.environ.get("CDK_DEFAULT_ACCOUNT")},
      label=f"{stack}-{stack_version}",
      region="us-east-1"
    )
    result.append(metric)
  return result


def create_opensearch_widget(title, collection_id_map, stack):
  metrics = []
  for sv in collection_id_map:
    metrics.extend(create_opensearch_metric(collection_id_map[sv], stack, sv))
  widget = cw.GraphWidget(title=title, width=24, height=4, left=metrics, view=cw.GraphWidgetView.TIME_SERIES)
  return widget


def create_repo_alb_response_widget(title, config, stack_versions):
  metrics = []
  dimensions_values = [item for sublist in [config[f'{sv}-repo-alb-name'] for sv in stack_versions if f'{sv}-repo-alb-name' in config] for item in sublist]
  for dv in dimensions_values:
    metric1 = cw.Metric(namespace='AWS/ApplicationELB', metric_name='TargetResponseTime', dimensions_map={'LoadBalancer': dv},
                       period=Duration.seconds(300), statistic='Average')
    metric2 = cw.Metric(namespace='AWS/ApplicationELB', metric_name='TargetResponseTime', dimensions_map={'LoadBalancer': dv},
                          period=Duration.seconds(300), statistic='p95')
    metrics.append(metric1)
    metrics.append(metric2)
  widget = cw.GraphWidget(title=title, width=24, height=4, view=cw.GraphWidgetView.TIME_SERIES,
                          stacked=False, set_period_to_time_range=True,
                          left=metrics)
  return widget

def create_repo_alb_response_widget_v2(title, config, stack_versions):
  metrics = []

  dimension_pairs = [(sv, dv)
                     for sv in stack_versions
                     if f'{sv}-repo-alb-name' in config
                     for dv in config[f'{sv}-repo-alb-name']]

  for sv, dv in dimension_pairs:
    metric1 = cw.Metric(
      namespace='AWS/ApplicationELB',
      metric_name='TargetResponseTime',
      dimensions_map={'LoadBalancer': dv},
      period=Duration.seconds(300),
      statistic='Average',
      label=f'{sv} - Average'
    )
    metric2 = cw.Metric(
      namespace='AWS/ApplicationELB',
      metric_name='TargetResponseTime',
      dimensions_map={'LoadBalancer': dv},
      period=Duration.seconds(300),
      statistic='p95',
      label=f'{sv} - p95'
    )
    metrics.append(metric1)
    metrics.append(metric2)

  widget = cw.GraphWidget(
    title=title,
    width=24,
    height=4,
    view=cw.GraphWidgetView.TIME_SERIES,
    stacked=False,
    set_period_to_time_range=True,
    left=metrics
  )

  return widget

# title: title for the widget
# stack: dev or prod
# version_lb_name_map: e.g.: {"582-0":"app/repo-dev-582-0/d415d054e3532643"}
def create_repo_ecs_alb_response_widget_v2(title, stack, version_and_incr_lb_name_map):
  metrics = []
  for stack_version_and_incr in version_and_incr_lb_name_map:
    lb_name=next(iter(version_and_incr_lb_name_map[stack_version_and_incr])) # there is only one

    metric1 = cw.Metric(
      namespace='AWS/ApplicationELB',
      metric_name='TargetResponseTime',
      dimensions_map={'LoadBalancer': lb_name},
      period=Duration.seconds(300),
      statistic='Average',
      label=f'{stack_version_and_incr} - Average'
    )
    metric2 = cw.Metric(
      namespace='AWS/ApplicationELB',
      metric_name='TargetResponseTime',
      dimensions_map={'LoadBalancer': lb_name},
      period=Duration.seconds(300),
      statistic='p95',
      label=f'{stack_version_and_incr} - p95'
    )
    metrics.append(metric1)
    metrics.append(metric2)

  widget = cw.GraphWidget(
    title=title,
    width=24,
    height=4,
    view=cw.GraphWidgetView.TIME_SERIES,
    stacked=False,
    set_period_to_time_range=True,
    left=metrics
  )
  return widget

def create_registry_ecs_cpu_widget_v2(stack):
  DIMENSIONS = {
    "ServiceName": "registry-prod-DockerFargateStack-registryprodServiceAFB525D2-UYnZR5jh3Dqx",
    "ClusterName": "registry-prod-DockerFargateStack-registryprodDockerFargateStackCluster47F74A14-MGrtooDf35X9",
  } if stack == 'prod' else {
    "ServiceName": "registry-dev-DockerFargateStack-registrydevService896F8BD4-GC9L2E0ibdbP",
    "ClusterName": "registry-dev-DockerFargateStack-registrydevDockerFargateStackCluster83B3D290-XhfK58ULXLP4",
  }
  widget = create_ecs_cpu_widget("Docker Registry", [{"dimensions":DIMENSIONS,"label":"Docker Registry"}])
  return widget

def create_ecs_cpu_widget(title, dimensions_and_labels_list, width=12):
  # Prefer ECS/ContainerInsights metrics if available
  # Container Insights metrics: CpuUtilized, CpuReserved, etc.
  
  metrics = []
  for dimensions_and_label in dimensions_and_labels_list:
    cpu_utilized = cw.Metric(
      namespace="ECS/ContainerInsights",
      metric_name="CpuUtilized",
      label=f"CpuUtilized-{dimensions_and_label['label']}",
      dimensions_map=dimensions_and_label["dimensions"],
      statistic="Average",
      region="us-east-1"
    )
    cpu_reserved = cw.Metric(
      namespace="ECS/ContainerInsights",
      metric_name="CpuReserved",
      label=f"CpuReserved-{dimensions_and_label['label']}",
      dimensions_map=dimensions_and_label["dimensions"],
      statistic="Average",
      region="us-east-1"
    )
    metrics.extend([cpu_utilized, cpu_reserved])
  widget = cw.GraphWidget(
    title=title+" - CPU Utilization vs Reserved",
    width=width,
    height=4,
    view=cw.GraphWidgetView.TIME_SERIES,
    stacked=False,
    set_period_to_time_range=True,
    left=metrics
  )
  return widget


def create_registry_ecs_network_widget_v2(stack):
  DIMENSIONS = {
    "ServiceName": "registry-prod-DockerFargateStack-registryprodServiceAFB525D2-UYnZR5jh3Dqx",
    "ClusterName": "registry-prod-DockerFargateStack-registryprodDockerFargateStackCluster47F74A14-MGrtooDf35X9",
  } if stack == 'prod' else {
    "ServiceName": "registry-dev-DockerFargateStack-registrydevService896F8BD4-GC9L2E0ibdbP",
    "ClusterName": "registry-dev-DockerFargateStack-registrydevDockerFargateStackCluster83B3D290-XhfK58ULXLP4",
  }
  return create_ecs_network_out_widget([{"dimensions":DIMENSIONS,"label":"Docker Registry"}], title="Docker Registry - Network utilization")
  
def create_ecs_network_out_widget(dimensions_and_labels_list, title, width=12):
  metrics = []
  for dimensions_and_label in dimensions_and_labels_list:
    # Network bandwidth metrics
    network_rx = cw.Metric(
      namespace="ECS/ContainerInsights",
      metric_name="NetworkRxBytes",
      label=f"NetworkRxBytes-{dimensions_and_label['label']}",
      dimensions_map=dimensions_and_label["dimensions"],
      statistic="Sum",
      region="us-east-1"
    )
    network_tx = cw.Metric(
      namespace="ECS/ContainerInsights",
      metric_name="NetworkTxBytes",
      label=f"NetworkTxBytes-{dimensions_and_label['label']}",
      dimensions_map=dimensions_and_label["dimensions"],
      statistic="Sum",
      region="us-east-1"
    )
    metrics.extend([network_rx, network_tx])
  widget = cw.GraphWidget(title=title, width=width, height=4, view=cw.GraphWidgetView.TIME_SERIES,
                          stacked=False, set_period_to_time_range=True,
                          left=metrics)
  return widget

class SynapseCloudwatchDashboardStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
      super().__init__(scope, construct_id, **kwargs)

      stack = self.node.try_get_context(key='stack')
      if stack is None:
        raise ValueError('No stack specified')

      stack_versions_str = self.node.try_get_context(key='stack_versions')
      if stack_versions_str is None:
        raise ValueError('No stack versions specified')

      # Profile name can be undefined if run on EC2
      profile_name = self.node.try_get_context(key='profile_name')

      stack_versions = []
      for s in stack_versions_str.split(','):
        if s is not None and len(s)>0:
          stack_versions.append(s)

      beanstalk_mode=self.node.try_get_context(key='beanstalk_mode')
      beanstalk_mode=ast.literal_eval(beanstalk_mode) 

      dashboard = cw.Dashboard(
        self,
        id="stack-status",
        dashboard_name="Stack-Status",
        default_interval=Duration.days(35),
      )

      aws_provider = get_aws_provider(profile_name)
      if beanstalk_mode:
        config = init_config(stack, aws_provider)
      self.cw_client = aws_provider.get_client(client_type='cloudwatch')
      
      filescanner_widget = create_filescanner_widget(title='FileScanner', stack_versions=stack_versions)
      collection_id_map = self.version_to_opesearch_collection_id_map(stack, stack_versions)
      opensearch_widget = create_opensearch_widget(title='OpenSearch - searchableDocuments', collection_id_map=collection_id_map, stack=stack)
      repo_active_connections_widget = create_repo_active_connections_widget(title='Repo-Active-Connections', stack_versions=stack_versions)
      workers_active_connections_widget = create_workers_active_connections_widget(title='Workers-Active-Connections', stack_versions=stack_versions)
      query_perf_widget = create_query_performance_widget(title="Query Performance", stack=stack, stack_versions=stack_versions)
      ses_widget = create_ses_widget(title='SES')
      rds_cpu_widget = create_rds_cpu_utilization_widget(title='RDS - CPU Utilization', stack=stack, stack_versions=stack_versions)
      rds_freestorage_widget = create_rds_free_storage_space_widget(title='RDS - Free Storage Space', stack=stack, stack_versions=stack_versions)
      if beanstalk_mode:
        repo_ec2_ids = [s for vp in stack_versions for s in config.get(f'{vp}-repo-ec2-instances', [])]
        cpu_repo_widget = create_ec2_cpu_utilization_widget(title="Repo - CPU Utilization", ec2_instance_ids=repo_ec2_ids)
        workers_ec2_ids = [s for vp in stack_versions for s in config.get(f'{vp}-workers-ec2-instances', [])]
        cpu_workers_widget = create_ec2_cpu_utilization_widget(title="Workers - CPU Utilization", ec2_instance_ids=workers_ec2_ids)
        portal_ec2_ids = [s for vp in stack_versions for s in config.get(f'{vp}-portal-ec2-instances', [])]
        cpu_portal_widget = create_ec2_cpu_utilization_widget(title="Portal - CPU Utilization", ec2_instance_ids=portal_ec2_ids)
        network_out_portal_widget = create_ec2_network_out_widget(title="Portal - Network out", ec2_instance_ids=portal_ec2_ids)
      else:
        cpu_repo_widget = create_ecs_cpu_widget("Repo Services", self.create_ecs_dimensions(stack, 'repo', stack_versions), width=24)
        cpu_workers_widget = create_ecs_cpu_widget("Worker Services", self.create_ecs_dimensions(stack, 'workers', stack_versions), width=24)
        cpu_portal_widget = create_ecs_cpu_widget("Portal Services", self.create_ecs_dimensions(stack, 'portal', stack_versions), width=24)
        network_out_portal_widget = create_ecs_network_out_widget(self.create_ecs_dimensions(stack, 'portal', stack_versions), title="Portal - Network out", width=24)
      repo_memory_widget = create_memory_widget(title='Repo - Memory used', stack_versions=stack_versions, environment='Repository')
      workers_memory_widget = create_memory_widget(title='Workers - Memory used', stack_versions=stack_versions, environment='Workers')
      stack_versions_to_worker_names_map = self.version_to_worker_names_map(stack_versions)
      workers_jobs_completed_widget = create_worker_stats_widget(title="Workers stats - Jobs completed", stack_versions_to_worker_names_map=stack_versions_to_worker_names_map, metric_name='Completed Job Count')
      workers_pc_time_widget = create_worker_stats_widget(title="Workers stats - % time running", stack_versions_to_worker_names_map=stack_versions_to_worker_names_map, metric_name='% Time Running')
      workers_cumulative_time_widget = create_worker_stats_widget(title="Workers stats - Cumulative time", stack_versions_to_worker_names_map=stack_versions_to_worker_names_map, metric_name='Cumulative runtime')
      if beanstalk_mode:
        repo_alb_rtime_widget2 = create_repo_alb_response_widget_v2(title='Repo ALB response time', config=config, stack_versions=stack_versions)
      else:
        repo_alb_rtime_widget2 = create_repo_ecs_alb_response_widget_v2('Repo ECS ALB response time', stack, self.version_to_lb_name_map("repo", stack, stack_versions))
      registry_ecs_cpu_widget = create_registry_ecs_cpu_widget_v2(stack)
      registry_ecs_network_widget = create_registry_ecs_network_widget_v2(stack)
      rds_read_throughput_widget = create_rds_read_throughput_widget(title="RDS Read Throughput", stack=stack, stack_versions=stack_versions)
      rds_write_throughput_widget = create_rds_write_throughput_widget(title="RDS Write Throughput", stack=stack, stack_versions=stack_versions)
      rds_read_latency_widget = create_rds_read_latency_widget(title="RDS Read Latency", stack=stack, stack_versions=stack_versions)
      rds_write_latency_widget = create_rds_write_latency_widget(title="RDS Write Latency", stack=stack, stack_versions=stack_versions)
      rds_read_iops_widget = create_rds_read_iops_widget(title="RDS Read Iops", stack=stack, stack_versions=stack_versions)
      rds_write_iops_widget = create_rds_write_iops_widget(title="RDS Write Iops", stack=stack, stack_versions=stack_versions)

      dashboard.add_widgets(cpu_repo_widget)
      dashboard.add_widgets(cpu_workers_widget)
      dashboard.add_widgets(rds_cpu_widget)
      dashboard.add_widgets(rds_freestorage_widget)
      dashboard.add_widgets(cpu_portal_widget)
      dashboard.add_widgets(network_out_portal_widget)
      dashboard.add_widgets(registry_ecs_cpu_widget, registry_ecs_network_widget)
      dashboard.add_widgets(repo_memory_widget)
      dashboard.add_widgets(workers_memory_widget)
      dashboard.add_widgets(repo_active_connections_widget)
      dashboard.add_widgets(workers_active_connections_widget)
      dashboard.add_widgets(workers_jobs_completed_widget)
      dashboard.add_widgets(workers_pc_time_widget)
      dashboard.add_widgets(workers_cumulative_time_widget)
      dashboard.add_widgets(query_perf_widget)
      dashboard.add_widgets(repo_alb_rtime_widget2)
      dashboard.add_widgets(ses_widget)
      dashboard.add_widgets(filescanner_widget)
      dashboard.add_widgets(opensearch_widget)
      dashboard.add_widgets(rds_read_throughput_widget, rds_write_throughput_widget)
      dashboard.add_widgets(rds_read_latency_widget, rds_write_latency_widget)
      dashboard.add_widgets(rds_read_iops_widget, rds_write_iops_widget)

    # Use the CloudWatch client to look up the ECS Task IDs associated with an ECS Service
    def get_ecs_task_ids(self, service_name):
      response = self.cw_client.list_metrics(
        Namespace="ECS/ContainerInsights", 
        MetricName="TaskCpuUtilization",
        Dimensions=[{"Name":"ServiceName","Value":service_name}])
      result=set()
      for metric in response.get('Metrics',[]):
        for dimension in metric.get('Dimensions',[]):
          if "TaskId"==dimension.get('Name',None):
            result.add(dimension['Value'])
      return result

    # Create the dimensions for ECS metrics, given the stack, service, and version list
    #
    # stack: dev or prod
    # service: repo, workers, or portal
    # versions: e.g., [582,583]
    # Note: assumes beanstalk number of 0
    def create_ecs_dimensions(self, stack, service, versions):
    	result = []
    	for version in versions:
    		for incr in ["0", "1"]:
    			service_name=f"{service}-{stack}-{version}-{incr}"
    			for task_id in self.get_ecs_task_ids(service_name):
    				dimensions={
    					"ClusterName":f"synapse-{stack}-{version}", 
    					"ServiceName":service_name,
    					"TaskId":task_id}
    				label=f"{version}-{task_id}"
    				result.append({"dimensions":dimensions, "label":label})
    	return result

    # Look up any attribute given Synapse stack version,
    # returning a map from the latter to the former
    #
    # stack_versions - list of stack versions to check, e.g. [583,584]
    # namespace - CW namespace, e.g., AWS/ElasticBeanstalk
    # attribute_name - name of the attribute to return, e.g. CollectionId (has values like 0go15fi4nx6cfe6cnia7)
    # filter_name - optional name of the attribute to filter on, e.g. CollectionName (has values like dev-583-synsearch)
    # matching_name_prefix - optional prefix to use with filter_name, with 'stack_version' as a placeholder, e.g. dev-{stack_version}-synsearch
    # metric_name - name of some metric found in CW, e.g. TargetResponseTime
    #
    def version_to_attribute_map(self, stack_versions, namespace, attribute_name, filter_name, matching_name_prefix, metric_name):
      next_token=None
      result = {}
      while (True):
        if next_token==None:
          response = self.cw_client.list_metrics(
            Namespace=namespace, 
            MetricName=metric_name)
        else:
           response = self.cw_client.list_metrics(
            Namespace=namespace, 
            MetricName=metric_name,
            NextToken=next_token)
        for metric in response.get('Metrics',[]):
          attribute_value=None
          filter_value=None
          for dimension in metric.get('Dimensions',[]):
            if attribute_name==dimension.get('Name',None):
              attribute_value=dimension['Value']
            if filter_name is not None and filter_name==dimension.get('Name',None):
              filter_value=dimension['Value']
          if attribute_value is not None:
            for stack_version in stack_versions:
                if matching_name_prefix is None or (filter_value is not None and filter_value.startswith(matching_name_prefix.format(stack_version=stack_version))):
                  value_list = result.get(stack_version)
                  if value_list is None:
                    value_list = set()
                    result[stack_version]=value_list
                  value_list.add(attribute_value)
        next_token = response.get("NextToken",None)
        if next_token is None:
          break
      return result
      

    # Look up the Load Balancer names for Synapse stack versions,
    # returning a map from the latter to the former
    def version_to_lb_name_map(self, service, stack, stack_versions):
      stack_versions_and_incrs = []
      for stack_version in stack_versions:
        for incr in ["0", "1"]:
          stack_versions_and_incrs.append(f"{stack_version}-{incr}")
      return self.version_to_attribute_map(
        stack_versions_and_incrs,
        "AWS/ApplicationELB",
        "LoadBalancer",
        "LoadBalancer",
        f"app/{service}-{stack}-{{stack_version}}",
        "TargetResponseTime")

      
    def version_to_worker_names_map(self, stack_versions):
      result = {}
      for sv in stack_versions:
        result.update(self.version_to_attribute_map(
          [sv],
          f"Worker-Statistics-{sv}",
          "Worker Name",
          None,
          None,
          "Cumulative runtime"))
      return result

    # Look up the OpenSearch collection ids for Synapse stack versions,
    # returning a map from the latter to the former
    def version_to_opesearch_collection_id_map(self, stack, stack_versions):
      return self.version_to_attribute_map(
        stack_versions,
        "AWS/AOSS",
        "CollectionId",
        "CollectionName",
        f"{stack}-{{stack_version}}-synsearch",
        "SearchableDocuments")

