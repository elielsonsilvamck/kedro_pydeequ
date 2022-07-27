from kedro.framework.hooks import hook_impl
from pyspark.sql.session import SparkSession


class PyDeequHooks:

    @hook_impl
    def after_pipeline_run(self) -> None:
        self.shutdown_spark_gateway()

    def shutdown_spark_gateway(self):
        """

            After you’ve ran your jobs with PyDeequ, be sure to shut down your Spark session to prevent any hanging processes.
            Link: https://pydeequ.readthedocs.io/en/latest/README.html#wrapping-up

            Returns: None

        """
        spark = SparkSession.builder.getOrCreate()
        spark.sparkContext._gateway.shutdown_callback_server()
        spark.stop()

    @hook_impl
    def on_pipeline_error(self) -> None:
        self.shutdown_spark_gateway()