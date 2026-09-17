from pyspark.sql.streaming import StreamingQueryListener
from src.common.audit import registrar_auditoria

STREAMING_DATASETS = {"eventos_clientes", "sensores_iot"}

class AuditStreamingListener(StreamingQueryListener):
    def __init__(self, spark):
        self.spark_session = spark
        # Mapeo interno: query_id -> {"dataset_name": str, "rows": int}
        self._active_queries = {}

    def _clean_dataset_name(self, query_name: str) -> str:
        if not query_name:
            return "unknown_source"
        return (
            str(query_name)
            .replace("bronze-farmia-", "")
            .replace("farmia_", "")
            .replace("_streaming", "")
        )

    def onQueryStarted(self, event):
        query_id = str(event.id)
        dataset_name = self._clean_dataset_name(event.name)
        
        # Guardar en memoria el nombre asignado a este UUID de consulta
        self._active_queries[query_id] = {
            "dataset_name": dataset_name,
            "rows": 0
        }
        
        registrar_auditoria(
            spark=self.spark_session,
            dataset_name=dataset_name,
            execution_type="STREAMING" if dataset_name in STREAMING_DATASETS else "BATCH",
            records_processed=0,
            status="STARTED",
            error_message=f"Query started: {query_id}",
            batch_id="batch_0"
        )

    def onQueryProgress(self, event):
        progress = event.progress
        query_id = str(progress.id)
        rows_processed = progress.numInputRows

        # Acumular/actualizar el total de filas procesadas para esta consulta
        if query_id in self._active_queries:
            self._active_queries[query_id]["rows"] += rows_processed
        else:
            dataset_name = self._clean_dataset_name(progress.name)
            self._active_queries[query_id] = {
                "dataset_name": dataset_name,
                "rows": rows_processed
            }

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        query_id = str(event.id)
        
        # Recuperar el nombre real del dataset y las filas acumuladas desde la memoria
        query_info = self._active_queries.pop(query_id, {})
        dataset_name = query_info.get("dataset_name", "unknown_source")
        rows_written = query_info.get("rows", 0)

        if event.exception:
            status = "FAILURE"
            error_msg = str(event.exception)
        else:
            status = "SUCCESS"
            error_msg = "Query terminated successfully."

        registrar_auditoria(
            spark=self.spark_session,
            dataset_name=dataset_name,
            execution_type="STREAMING" if dataset_name in STREAMING_DATASETS else "BATCH",
            records_processed=rows_written,
            status=status,
            error_message=error_msg,
            batch_id="batch_0"
        )