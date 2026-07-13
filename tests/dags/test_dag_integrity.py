"""Repository-specific DAG integrity tests."""

import pytest
from airflow.models import DagBag

EXPECTED_DAGS = {
    "full_medallion_pipeline_dag",
    "ducklake_maintenance_dag",
}


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    return DagBag(include_examples=False)


def test_dags_import_without_errors(dag_bag: DagBag) -> None:
    assert dag_bag.import_errors == {}, dag_bag.import_errors


def test_expected_dags_are_registered(dag_bag: DagBag) -> None:
    missing_dags = EXPECTED_DAGS - set(dag_bag.dags)
    assert not missing_dags, f"Missing DAGs: {sorted(missing_dags)}"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_dags_have_tags(dag_bag: DagBag, dag_id: str) -> None:
    assert dag_bag.dags[dag_id].tags, f"{dag_id} must define at least one tag"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_dags_define_retry_policy(dag_bag: DagBag, dag_id: str) -> None:
    retries = dag_bag.dags[dag_id].default_args.get("retries", 0)
    assert retries >= 2, f"{dag_id} must define at least two retries"
