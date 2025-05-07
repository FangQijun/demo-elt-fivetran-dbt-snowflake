"""
### EDP DBT SCD2 Fivetran

This dag is for firing off connectors in fivetran and then transforming the data into SCD2 format.
The connectors themselves can be viewed though the [fivetran dashboard](https://fivetran.com/dashboard/connectors).
The configuration of these connectors is part of
[corp_enterprise/edp-saas-ingestion](https://github.com/corp_enterprise/edp-saas-ingestion), any changes to the tables
being ingested should be made there.

Code for this dag is in [corp_enterprise/edp_dbt_scd2_fivetran](https://github.com/corp_enterprise/edp_dbt_scd2_fivetran)

See the [DBT Documentation]({doc}) for more information on the snapshots.

---

Reference: [{ref}](https://github.com/corp_enterprise/edp_dbt_scd2_fivetran/tree/{ref})

Git SHA: [{sha}](https://github.com/corp_enterprise/edp_dbt_scd2_fivetran/commit/{sha})

Image: {img}
"""

import datetime
import os
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.xcom_arg import XComArg
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from fivetran_provider_async.hooks import FivetranHook
from fivetran_provider_async.operators import FivetranOperator
import cosmos.profiles.snowflake
import cosmos

import edp.operators.dbt
import edp.operators.edm
from eda.alerts import default_callbacks
from edp.owners import owners_links

# =============================================================================
# Execution parameters to modify/check
# =============================================================================
ACCOUNT = "saas"
TARGET_DATABASE = "cleansed"
TARGET_SCHEMA = "jira"
OWNER = "MKT_FOUNDATION"
DBT_PROJECT_NAME="edp_dbt_scd2_fivetran"

DBT_VARS = {}
# =============================================================================


def main() -> list[DAG]:
    dbt_dag_config = edp.operators.dbt.DbtDagProjectConfig(
        project_name=DBT_PROJECT_NAME, dag_file_path=__file__, dbt_vars=DBT_VARS
    )
    dbt_target = "dev" if os.getenv("ENVIRONMENT", "dev") == "preprod" else os.getenv("ENVIRONMENT", "dev")
    dbt_target_database = f"{dbt_target}_{TARGET_DATABASE}" if dbt_target != "prod" else TARGET_DATABASE

    dbt_profile_config = edp.operators.dbt.get_snowflake_profile_config(
        target=dbt_target,
        sf_account=ACCOUNT,
        database=dbt_target_database,
        schema=TARGET_SCHEMA,
    )

    dag = DAG(
        dag_id="edp_dbt_scd2_fivetran_jira",
        start_date=datetime.datetime(2024, 7, 25),
        schedule="0 2 * * *",
        catchup=False,
        doc_md=__doc__.format(
            img=dbt_dag_config.image,
            doc=dbt_dag_config.img_documentation,
            ref=dbt_dag_config.img_reference,
            sha=dbt_dag_config.img_sha,
        ),
        tags=["dbt", "snowflake", "fivetran", "Marketing Foundation Data Pipelines High-Priority"],
        default_args={
            "owner": OWNER,
            "depends_on_past": True,
            "extra_metadata": dbt_dag_config.extra_metadata,
        },
        owner_links=owners_links,
        **default_callbacks(slack_channel="#edw-ops", pagerduty_svc="mkt_fdn_data_pipelines_high_priority"),
        dagrun_timeout=datetime.timedelta(minutes=360)
    )

    datasets = ["jira"]

    with dag:
        connectors = lookup_connectors(datasets)

        scd2_groups = [
            fivetran_scd2(
                source=s,
                connectors=connectors,
                profile_config=dbt_profile_config,
                project_config=dbt_dag_config.cosmos_project_config,
                dag_config=dbt_dag_config
            )
            for s in datasets
        ]

        # Disabled until we have a fixed EDM connection
        # dbt_replicate = edp.operators.edm.EdmTriggerDataPropagationOperator(
        #     task_id='replicate',
        #     edm_conn_id='edp.edm.data_propagation'
        # )
        dbt_replicate = EmptyOperator(task_id="replicate")

    scd2_groups >> dbt_replicate

    return dag


@task
def lookup_connectors(datasets) -> list[dict]:
    """
    Pull connection ids from fivetran for the specified datasets
    """
    result = []
    hook = FivetranHook()
    groups = hook.get_groups()

    for group in groups:
        connectors = hook.get_connectors(group['id'])
        for connector in connectors:
            if connector["schema"] in datasets:
                result.append({"dataset": connector["schema"], "id": connector["id"]})

    return result


def select_dataset(dataset: str):
    """Get a function that will skip tasks that aren't attributed to the given dataset"""
    def select(connector) -> list[str]:
        if connector["dataset"] != dataset:
            raise AirflowSkipException(
                f"skipping {connector['id']} because it doesn't belong to {dataset}"
            )
        return connector["id"]

    return select


def fivetran_scd2(
    source: str,
    connectors: XComArg,
    profile_config: cosmos.ProfileConfig,
    project_config: cosmos.ProjectConfig,
    dag_config: edp.operators.dbt.DbtDagProjectConfig,
) -> TaskGroup:
    tg = TaskGroup(group_id=source)
    filtered_connectors = connectors.map(select_dataset(source))

    with tg:
        # The empty operators in this group are just to bookend the task groups, if a more useful operator is added
        # later, they can replace any of these.
        group = EmptyOperator(task_id="group", wait_for_downstream=True)

        fivetran = FivetranOperator.partial(
            task_id="fivetran"
        ).expand(connector_id=filtered_connectors)

        join = EmptyOperator(task_id="join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        dbt_snapshot = edp.operators.dbt.DbtTaskGroup(
            group_id="dbt_snapshot",
            project_config=project_config,
            profile_config=profile_config,
            execution_config=cosmos.ExecutionConfig(
                execution_mode=cosmos.ExecutionMode.KUBERNETES,
            ),
            operator_args={
              "image": dag_config.image
            },
            render_config=cosmos.RenderConfig(
                test_behavior=None, select=[f"tag:dbt:{source}_scd2"]
            ),
            extra_metadata=dag_config.extra_metadata,
        )

        end = EmptyOperator(task_id="end")

    group >> fivetran >> join >> dbt_snapshot >> end
    group >> end

    return tg


main()