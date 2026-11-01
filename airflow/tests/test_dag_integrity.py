"""Integridade das DAGs: carregam sem erro e têm as tarefas, dependências e pools esperados.

Roda dentro do container (`make test-airflow`); fora dele o módulo é pulado.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.airflow
pytest.importorskip("airflow.sdk")  # a pasta airflow/ do repositório não conta

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # 3.1
        from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_sem_erros_de_importacao(dagbag):
    assert dagbag.import_errors == {}


def edges(dag) -> set[tuple[str, str]]:
    return {(t.task_id, d) for t in dag.tasks for d in t.downstream_task_ids}


def test_microbatch_tarefas_e_ordem(dagbag):
    dag = dagbag.get_dag("worldbank_projects_microbatch")
    assert dag is not None
    assert edges(dag) == {
        ("kafka_lag", "has_new_files"),
        ("has_new_files", "databricks_projects"),
        ("has_new_files", "mark_processed"),  # XCom do file_id limite
        # marca só depois do sync: se o sync falhar, os arquivos seguem pendentes e a próxima execução refaz
        ("databricks_projects", "sync_dw"),
        ("sync_dw", "mark_processed"),
        ("mark_processed", "data_quality"),
    }


def test_microbatch_agenda_e_concorrencia(dagbag):
    dag = dagbag.get_dag("worldbank_projects_microbatch")
    assert dag.timetable.summary == "*/30 * * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tres_dags_carregadas(dagbag):
    assert set(dagbag.dag_ids) == {
        "worldbank_projects_microbatch",
        "worldbank_indicators_weekly",
        "worldbank_bootstrap_kaggle",
    }


def test_semanal_tarefas_e_ordem(dagbag):
    dag = dagbag.get_dag("worldbank_indicators_weekly")
    assert edges(dag) == {
        ("indicator_list", "extract"),
        ("extract", "coverage_check"),
        ("countries", "coverage_check"),
        ("coverage_check", "databricks_indicators"),
        ("extract", "mark_processed"),  # file_ids da execução
        ("countries", "mark_processed"),
        ("databricks_indicators", "sync_dw"),
        ("sync_dw", "mark_processed"),
        ("mark_processed", "data_quality"),
    }
    assert dag.timetable.summary == "10 6 * * 1"
    assert dag.catchup is False and dag.max_active_runs == 1


def test_semanal_extract_e_mapeado(dagbag):
    # Na 3.1 a DAG parseada usa o MappedOperator do Task SDK (não o de airflow.models)
    assert dagbag.get_dag("worldbank_indicators_weekly").get_task("extract").is_mapped


def test_bootstrap_manual_tarefas_e_ordem(dagbag):
    dag = dagbag.get_dag("worldbank_bootstrap_kaggle")
    assert edges(dag) == {
        ("load_kaggle_history", "databricks_indicators"),
        ("load_kaggle_history", "mark_processed"),
        ("databricks_indicators", "sync_dw"),
        ("sync_dw", "mark_processed"),
    }
    assert dag.timetable.summary in {"None", "Never"}  # sem agenda: só disparo manual


@pytest.mark.parametrize(
    ("dag_id", "task_id", "job"),
    [
        ("worldbank_projects_microbatch", "databricks_projects", "worldbank_projects"),
        ("worldbank_indicators_weekly", "databricks_indicators", "worldbank_indicators"),
        ("worldbank_bootstrap_kaggle", "databricks_indicators", "worldbank_indicators"),
    ],
)
def test_jobs_databricks_no_pool_com_parametros(dagbag, dag_id, task_id, job):
    task = dagbag.get_dag(dag_id).get_task(task_id)
    assert task.pool == "databricks"
    params = task.json["job_parameters"]
    assert params["job"] == job
    assert params["run_id"] == "{{ run_id }}"
    assert {"catalog", "schema", "volume"} <= set(params)


@pytest.mark.parametrize(
    "dag_id", ["worldbank_projects_microbatch", "worldbank_indicators_weekly", "worldbank_bootstrap_kaggle"]
)
def test_sync_dw_no_pool_de_um_slot(dagbag, dag_id):
    # Dois syncs ao mesmo tempo espelhariam agg_country_year em paralelo (deadlock no Postgres)
    assert dagbag.get_dag(dag_id).get_task("sync_dw").pool == "dw_sync"
